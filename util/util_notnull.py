import os
from tkinter import E
import pandas as pd
from .util_helper import *

################################
## Not NULL result analysis
################################
# If the field is '0' - means there is no check
def sum_not_null_check(x):
    l = x.split(", ")
    length = len(l)
    num = 0
    for item in l:
        if item == "0":
            num += 1
    if num == 0:
        return "all_has_check"
    elif num == length:
        return "no_check"
    else:
        return "some_check"


def has_filter_in_source(x):
    return "filter" in x


def compare_db_not_null_constraint(
    find_con, db_constraint, model_class, path, result, history_issue
):
    find_con["column"] = find_con["column"].apply(remove_id_sort)
    db_constraint["column"] = db_constraint["column"].apply(remove_id_sort)

    find_con = (
        find_con.groupby(by=["table", "column"], as_index=False)
        .agg(
            {
                "class": string_join_without_dup,
                "usage": string_join_without_dup,
                "lineno": ", ".join,
                "file": ", ".join,
                "extra": string_join_without_dup,
                "source": ", ".join,
                "has_check": ", ".join,
            }
        )
        .reset_index()
    )

    find_con["is_alltest"] = find_con["file"].apply(is_all_test_files)
    find_con["sum_check"] = find_con["has_check"].apply(sum_not_null_check)

    # We filter some results, e.g. slf.a = None, they can be not null.
    find_con["filtered"] = find_con["source"].apply(has_filter_in_source)

    tmpdf = find_con[find_con["source"].apply(has_filter_in_source)]
    find_con = find_con[find_con["filtered"] != True]

    #######
    ## Filter the columns we are not interested in, like ID, third-Party and CharField
    #######
    find_con = filter_null_cons(
        find_con, model_class, filter_char=True, history=history_issue
    )

    # We also filter the SET_NULL in the detected constraints.
    find_con = find_con[find_con["on_delete"] != "SET_NULL"]

    # For detected constraints, mark existing in db as exist.
    rst = []
    for index, row in find_con.iterrows():
        if row["is_alltest"] == 1:
            rst.append("filtered")
            continue

        t_name = row["table"]

        coexist = db_constraint[db_constraint["table"] == t_name]
        flag = False
        for cols in list(coexist.column):
            if a_includes_b(row["column"].strip(), cols):
                flag = True

        # Fail over and try out match the parent table.
        if flag == False:
            # 1. get the tables that have the column from the same base model
            parent_table = row["parent"]
            col_name = row["column"].strip()
            silbing_tables = list(
                model_class[
                    (model_class["parent"] == parent_table)
                    & (model_class["column"] == col_name)
                ].table
            )
            # 2. find them in the db constraints.
            tmpdf = db_constraint[
                (db_constraint["table"].isin(silbing_tables))
                & (db_constraint["column"] == col_name)
            ]
            if not tmpdf.empty:
                flag = True

        # Existed constaint
        if flag:
            rst.append("z-exist")
        else:
            # Filter items with a non-existing column (or just Relationship)
            tmpexist = model_class[
                (model_class["table"] == t_name)
                & (model_class["column"] == row["column"].strip())
                & (
                    ~model_class["field_type"].isin(
                        ["OneToOneRel", "ManyToOneRel", "TextIDGenericRelation"]
                    )
                )
            ]
            if tmpexist.empty:
                rst.append("filtered")
            # All has checks in the code
            elif row["sum_check"] == "all_has_check" or row["extra"] == "filter":
                rst.append("filtered")
            # default with non assigned
            elif row["source"] == "filter_default" or row["source"] == "eq":
                rst.append("filtered")
            # Potential missing ones
            elif row["sum_check"] == "no_check":
                rst.append("candidate")
            else:
                rst.append("check")

    find_con["exist"] = rst

    find_con = find_con.drop(columns=["is_alltest", "filtered", "auto_now"])
    find_con.loc[:, "pattern_type"] = find_con["source"].apply(source_to_pattern_type)
    find_con = add_char_field_not_null(model_class, path, find_con)

    if history_issue:
        gen_history_issue_result(db_constraint, model_class, find_con, path, result)
        return

    subdf = find_con
    subdf = find_con[find_con["exist"] == "candidate"]
    pa_n1 = subdf[subdf["pattern_type"] == "PA_n1"].shape[0]
    pa_n2 = subdf[subdf["pattern_type"] == "PA_n2"].shape[0]
    pa_n3 = subdf[subdf["pattern_type"] == "PA_n3"].shape[0]
    print(
        "------> [Detected New Constraints]: PA_n1: ",
        pa_n1,
        " PA_n2: ",
        pa_n2,
        " PA_n3: ",
        pa_n3,
        " total: ",
        subdf.shape[0],
    )

    result["PA_1"] = pa_n1
    result["PA_2"] = pa_n2
    result["PA_3"] = pa_n3
    result["total"] = subdf.shape[0]

    if not os.path.exists(path):
        os.makedirs(path)
    # Save detected new constraints to file
    subdf = subdf.sort_values(
        by=["pattern_type", "class", "table"],
        ascending=[True, True, True],
    )
    subdf.reset_index(drop=True, inplace=True)
    subdf[["class", "table", "column", "pattern_type", "file", "lineno"]].to_csv(
        path + "[null]newly_detected.csv", index=False
    )

    ## Get the TP and FN
    not_null_FN(db_constraint, find_con, model_class, path, result)


def source_to_pattern_type(x):
    s_p1 = ["field", "method", "operator", "funcCall", "fk", "eq"]
    s_p2 = ["if_raise", "assert", "if_assign"]
    s_p3 = ["default"]
    if vals_in_x(x, s_p1):
        return "PA_n1"
    elif vals_in_x(x, s_p2):
        return "PA_n2"
    elif vals_in_x(x, s_p3):
        return "PA_n3"
    else:
        print("Wrong source pattern!")
        return "PA_n1"


#
# Given the constraints from null_cons (in fact not null constraints table)
# And the detected results table,
# Calculate the num of detected constraints & num of missing constriants
#
def filter_detected_ones(null_cons, detected, model_class):
    rst = []
    file = []
    lineno = []

    for index, row in null_cons.iterrows():
        current_file, current_lineno = "", ""
        t_name = row["table"]
        col_name = row["column"]
        parent_table = row["parent"]

        df_empty_flag = True
        df = detected[(detected["table"] == t_name) & (detected["column"] == col_name)]
        # Note here if has default value, then won't find the match in parent model, making size of first two category small.
        if not df.empty:
            df_empty_flag = False
            current_file = current_file + ", ".join(list(df.file))
            current_lineno = current_lineno + ", ".join(list(df.lineno))
        else:
            # Fail over and try out match the parent table.
            # 1. get the tables that have the column from the same base model
            silbing_tables = list(
                model_class[
                    (model_class["parent"] == parent_table)
                    & (model_class["column"] == col_name)
                ].table
            )
            # 2.
            df = detected[
                (detected["table"].isin(silbing_tables))
                & (detected["column"] == col_name)
            ]
            if not df.empty:
                # print("145", t_name, col_name, " get same from ", list(df.table))
                df_empty_flag = False
                current_file = current_file + ", ".join(list(df.file))
                current_lineno = current_lineno + ", ".join(list(df.lineno))

        if df_empty_flag:
            rst.append(0)
        else:
            rst.append(1)
        file.append(current_file)
        lineno.append(current_lineno)

    return rst, file, lineno


def add_model_info_to_NULL_constraints(model_class, null_cons):
    rst_field_type = []
    rst_on_delete = []
    rst_auto_now = []
    rst_related_names = []
    rst_m2mfield = []
    rst_parent_model = []

    for index, row in null_cons.iterrows():
        t_name = row["table"]
        col_name = row["column"]

        overlap = model_class[
            (model_class["table"] == t_name) & (model_class["column"] == col_name)
        ]
        if not overlap.empty:
            rst_field_type.append(list(overlap.field_type)[0])
            rst_on_delete.append(list(overlap.on_delete)[0])
            rst_auto_now.append(list(overlap.auto_now)[0])
            rst_related_names.append(list(overlap.related_names)[0])
            rst_m2mfield.append(list(overlap.is_m2m_field)[0])
            rst_parent_model.append(list(overlap.parent)[0])
        else:
            rst_field_type.append("")
            rst_on_delete.append("")
            rst_auto_now.append("")
            rst_related_names.append("")
            rst_m2mfield.append("")
            rst_parent_model.append("")

    return (
        rst_field_type,
        rst_on_delete,
        rst_auto_now,
        rst_related_names,
        rst_m2mfield,
        rst_parent_model,
    )


#
# 0) Not app- specific fields (E.g., Django)
# 1) Auto generated fields, including ID, M2M fields, auto_now for Datetime, (AutoSlug for Oscar)
# 2) CharField, Doc says no need to specify them.
#
def filter_null_cons(null_cons, model_class, filter_char=False, history=False):
    # Filter third party models/fields
    null_cons = null_cons[null_cons["table"].apply(filter_by_kws)]
    null_cons = null_cons[null_cons["column"].apply(filter_by_kws_fields)]

    # Filter id
    model_class["column"] = model_class["field"].apply(remove_id_sort)
    null_cons["column"] = null_cons["column"].apply(remove_id_sort)
    null_cons = null_cons[null_cons["column"] != "id"].reset_index(drop=True)
    # Add more model information
    (
        rst_field_type,
        rst_on_delete,
        rst_auto_now,
        rst_related_names,
        rst_m2mfield,
        rst_parent_model,
    ) = add_model_info_to_NULL_constraints(model_class, null_cons)
    null_cons["field_type"] = rst_field_type
    null_cons["on_delete"] = rst_on_delete
    null_cons["auto_now"] = rst_auto_now
    null_cons["related_names"] = rst_related_names
    null_cons["is_m2m_field"] = rst_m2mfield
    null_cons["parent"] = rst_parent_model
    if history:
        return null_cons
    # Filter columns auto-generated for M2M - In a more correct way, whose definition not appear in source code.
    null_cons = null_cons[
        (null_cons["field_type"] != "") & (null_cons["field_type"] != "AutoField")
    ]
    # # Filter columns auto generated.
    null_cons = null_cons[~null_cons["auto_now"].isin(["1", "2"])]

    # Filter CharField
    if filter_char:
        null_cons = null_cons[~null_cons["field_type"].isin(charList)].reset_index(
            drop=True
        )
    return null_cons


def not_null_FN(null_cons, result, model_class, path, table_result):
    ### Filter the model_class
    null_cons = filter_null_cons(null_cons, model_class, filter_char=True)

    detected = result[(result["exist"] == "z-exist") | (result["exist"] == "filtered")]

    detected_list, file, lineno = filter_detected_ones(null_cons, detected, model_class)
    null_cons["file"] = file
    null_cons["lineno"] = lineno
    null_cons["detect"] = detected_list
    total_detected = null_cons[null_cons["detect"] == 1].shape[0]

    # missing_constraints = deduplicate_costrs(missing_constraints)
    print(
        "------> [Detected Existing Constraints]: total detected %s, total existing %s, recall %s "
        % (
            total_detected,
            null_cons.shape[0],
            round(total_detected / null_cons.shape[0], 2),
        )
    )

    table_result["total_detected"] = total_detected
    table_result["total_existing"] = null_cons.shape[0]
    table_result["recall"] = round(total_detected / null_cons.shape[0], 2)

    if not os.path.exists(path):
        os.makedirs(path)
    null_cons[["table", "column", "file", "lineno", "detect"]].sort_values(
        by=["detect", "table", "column"],
        ascending=[False, True, True],
    ).to_csv(path + "[null]existing_constraints.csv", index=False)


def gen_history_issue_result(null_cons, model_class, detected, path, result):
    null_cons = filter_null_cons(null_cons, model_class, filter_char=True, history=True)
    detected_list, file, lineno = filter_detected_ones(null_cons, detected, model_class)
    null_cons["file"] = file
    null_cons["lineno"] = lineno
    null_cons["detect"] = detected_list
    total_detected = null_cons[null_cons["detect"] == 1].shape[0]

    result["detected"] = total_detected
    result["total"] = null_cons.shape[0]

    if not os.path.exists(path):
        os.makedirs(path)
    null_cons[["table", "column", "file", "lineno", "detect"]].sort_values(
        by=["detect", "table", "column"],
        ascending=[False, True, True],
    ).to_csv(path + "[null]detected_history_constraints.csv", index=False)


def add_char_field_not_null(model_class, path, find_con):
    tmp_constraints = []
    if ("history_issue" in path) or ("wagtail" in path):
        field_char_not_null = model_class[
            (model_class["field_type"].isin(["CharField", "TextField"]))
            & (model_class["null"].isin(["True", "TRUE"]))
        ]
        for index, row in field_char_not_null.iterrows():
            tmp_constraints.append(
                {
                    "class": row["model"],
                    "table": row["table"],
                    "column": row["field"],
                    "pattern_type": "PA_n3",
                    "file": "",
                    "lineno": "",
                    "exist": "candidate",
                }
            )
    find_con = pd.concat(
        [find_con, pd.DataFrame(tmp_constraints)], ignore_index=True, sort=False
    )
    find_con["column"] = find_con["column"].apply(remove_id_sort)

    return find_con


def add_fields_with_default(SOFTWARE, extract_constraints):
    model_class = pd.read_csv(
        "data/" + SOFTWARE.lower() + "_model_class.csv", keep_default_na=False
    )

    fields_with_default = model_class[
        (model_class["default"] != "") & (~model_class["field_type"].isin(charList))
    ]
    tmp_extract_constraints = []

    for index, row in fields_with_default.iterrows():
        tmp_extract_constraints.append(
            {
                "class": row["model"],
                "table": row["table"],
                "column": row["field"],
                "usage": "",
                "lineno": "",
                "source": "default",
                "file": "PA_n3",
                "extra": "",
                "has_check": "0",
            }
        )

    tmp_extract_constraints = pd.DataFrame(tmp_extract_constraints)
    all_constraints = pd.concat(
        [extract_constraints, tmp_extract_constraints], ignore_index=True, sort=False
    )

    return all_constraints
