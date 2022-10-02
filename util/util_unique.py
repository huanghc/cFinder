import os
from pyexpat import model
import pandas as pd
from .util_helper import *
import itertools

################################
## Unique result analysis
################################
def compare_db_unique_constraint(
    find_con, db_constraint, model_class, path, result, history_issue
):

    find_con["column"] = find_con["column"].apply(remove_id_sort)
    db_constraint["column"] = db_constraint["column"].apply(remove_id_sort)
    find_con["source"] = find_con["source"].apply(
        lambda x: ",".join(set(x.split(", ")))
    )

    find_con = (
        find_con.groupby(by=["class", "table", "column", "source"], as_index=False)
        .agg(
            {
                "usage": ", ".join,
                "lineno": ", ".join,
                "file": ", ".join,
                "extra": "; ".join,
            }
        )
        .reset_index()
    )

    find_con["source"] = find_con["source"].apply(
        lambda x: ",".join(set(x.split(", ")))
    )
    find_con["is_alltest"] = find_con["file"].apply(is_all_test_files)
    find_con["filtered"] = find_con["column"].apply(is_id_pk_column_in_list)
    find_con = clean_file_lineno(find_con)

    if history_issue:
        gen_history_issue_result(db_constraint, model_class, find_con, path, result)
        return

    ## Filter the columns we are not interested in, like third-Party
    find_con = filter_unique_cons(model_class, find_con)
    db_constraint = filter_unique_cons(model_class, db_constraint)

    # For DB constraints table, mark existing in detected cols with pattern source.
    db_constraint = match_existing_constrasints_with_source_label(
        db_constraint, find_con
    )

    # finally, mark all inherited fields with the detected constraints.
    db_constraint = handle_inherit_models(db_constraint, model_class)

    # Print output and save to results.
    total_detected = db_constraint[db_constraint["detect"] == 1].shape[0]
    print(
        "------> [Detected Existing Constraints]: total detected %s, total existing %s, recall %s "
        % (
            total_detected,
            db_constraint.shape[0],
            round(total_detected / db_constraint.shape[0], 2),
        )
    )
    result["total_detected"] = total_detected
    result["total_existing"] = db_constraint.shape[0]
    result["recall"] = round(total_detected / db_constraint.shape[0], 2)

    db_constraint = clean_file_lineno(db_constraint)

    if not os.path.exists(path):
        os.makedirs(path)
    db_constraint[["table", "column", "file", "lineno", "detect"]].sort_values(
        by=["detect", "table", "column"],
        ascending=[False, True, True],
    ).to_csv(path + "[unique]existing_constraints.csv", index=False)

    ################################################
    # For detected constraints, mark existing in db as exist.
    ################################################
    exist_status = check_whether_newly_detected(find_con, db_constraint, model_class)
    find_con["exist"] = exist_status

    # Filter out the missing ones all from the tests files.
    find_con = find_con[
        (find_con["exist"] == "z-exist") | (find_con["is_alltest"] == 0)
    ]
    find_con = find_con[find_con["filtered"] == False]

    subdf = find_con[(find_con["exist"] == "candidate")].copy()
    subdf.loc[:, "pattern_type"] = subdf["source"].apply(source_to_pattern_type)
    subdf = remove_duplicate_rows(subdf)

    # Print the results
    pa_u1 = subdf[subdf["pattern_type"].apply(lambda x: "PA_u1" in x)].shape[0]
    pa_u2 = subdf[subdf["pattern_type"].apply(lambda x: "PA_u2" in x)].shape[0]
    print(
        "------> [Detected New Constraints]: PA_u1: ",
        pa_u1,
        " PA_u2: ",
        pa_u2,
        " total: ",
        subdf.shape[0],
    )
    result["PA_1"] = pa_u1
    result["PA_2"] = pa_u2
    result["PA_3"] = 0
    result["total"] = subdf.shape[0]

    # Save detected new constraints to file
    subdf = subdf.sort_values(
        by=["pattern_type", "class", "table"],
        ascending=[True, True, True],
    )
    subdf.reset_index(drop=True, inplace=True)
    subdf[["class", "table", "column", "pattern_type", "file", "lineno"]].to_csv(
        path + "[unique]newly_detected.csv", index=False
    )
    return


def match_existing_constrasints_with_source_label(db_constraint, find_con):
    from_get = []
    from_M2M = []
    from_complex = []
    file = []
    lineno = []
    for index, row in db_constraint.iterrows():
        current_file, current_lineno = "", ""
        t_name = row["table"]

        coexist = find_con[
            (find_con["table"] == t_name) & (find_con["source"] == "get_type")
        ]

        flag = False
        for cols in list(coexist.column):
            if a_includes_b(cols, row["column"].strip()):
                flag = True
                current_file = current_file + ", ".join(list(coexist.file))
                current_lineno = current_lineno + ", ".join(list(coexist.lineno))

        if flag:
            from_get.append("get_type")
        else:
            from_get.append(0)

        # Deal with M2M
        subdf = find_con[find_con["column"] == row["column"].strip()]
        subsource = ", ".join(list(set(", ".join(list(subdf.source)).split(", "))))

        if (
            len(row["column"].split(",")) == 2
            and (not subdf.empty)
            and "M2M" in subsource
        ):
            from_M2M.append("M2M")
            current_file = current_file + ", ".join(list(subdf.file))
            current_lineno = current_lineno + ", ".join(list(subdf.lineno))
        else:
            from_M2M.append(0)

        coexist = find_con[
            (find_con["table"] == t_name)
            & (
                find_con["source"].isin(
                    [
                        "_check_qs_UnaryOp",
                        "_check_qs_exists",
                        "_check_length_compare_with_0_1",
                    ]
                )
            )
        ]

        flag = False
        for cols in list(coexist.column):
            if a_includes_b(cols, row["column"].strip()):
                flag = True

        if flag:
            from_complex.append("complex")
            current_file = current_file + ", ".join(list(coexist.file))
            current_lineno = current_lineno + ", ".join(list(coexist.lineno))
        else:
            from_complex.append(0)

        file.append(current_file)
        lineno.append(current_lineno)

    db_constraint["from_get"] = from_get
    db_constraint["from_M2M"] = from_M2M
    db_constraint["from_complex"] = from_complex
    db_constraint["file"] = file
    db_constraint["lineno"] = lineno

    sum_list = []
    for a, b, c in zip(from_get, from_M2M, from_complex):
        if a or b or c:
            sum_list.append(1)
        else:
            sum_list.append(0)
    db_constraint["detect"] = sum_list

    return db_constraint


def handle_inherit_models(db_constraint, model_class):
    for index, row in db_constraint.iterrows():
        if row["detect"] == 0:
            # 1. get the tables that have the column from the same base model
            silbing_tables = list(
                model_class[(model_class["parent"] == row["parent"])].table
            )
            # 2. check if silbing is detected.
            tmpdf = db_constraint[
                (db_constraint["table"].isin(silbing_tables))
                & (db_constraint["detect"] == 1)
            ]

            if not tmpdf.empty:
                for sub_index, col_item in tmpdf.iterrows():
                    if row["column"] == col_item["column"]:
                        db_constraint.at[index, "detect"] = 1
                        db_constraint.at[index, "file"] = col_item["file"]
                        db_constraint.at[index, "lineno"] = col_item["lineno"]
                        break
    return db_constraint


def check_whether_newly_detected(find_con, db_constraint, model_class):
    is_edx_commerce = "ecommerce" in model_class.loc[1, "module"]
    rst = []
    for index, row in find_con.iterrows():
        result = 0
        t_name = row["table"]

        coexist = db_constraint[db_constraint["table"] == t_name]
        flag = False

        for cols in list(coexist.column):
            if a_includes_b(row["column"].strip(), cols) or a_includes_b(
                cols, row["column"].strip()
            ):
                flag = True
        if flag:
            result = 1

        # Special case for M2M
        subdf = db_constraint[db_constraint["column"] == row["column"].strip()]
        if (
            len(row["column"].split(",")) == 2
            and (not subdf.empty)
            and row["source"] == "M2M"
        ):
            result = 1

        if result:
            rst.append("z-exist")
        else:
            if (
                row["filtered"] == True
                or row["is_alltest"] == 1
                or len(row["column"].split(",")) >= 4
            ):
                rst.append("filtered")
            # Filter column not exists in models
            elif row["source"] != "M2M" and (
                not test_column_exists(
                    row["source"], row["column"].split(","), t_name, model_class
                )
            ):
                rst.append("filtered")
            elif row["source"] == "M2M" and len(row["column"].split(",")) < 2:
                rst.append("filtered")
            # commerce's columns from third party.
            elif third_party_columns(row, model_class) and is_edx_commerce:
                rst.append("filtered")
            else:
                rst.append("candidate")

    return rst


#
# Results may contain rows with the same table and columns or sub-columns.
# Should remove them as duplicates
#
def remove_duplicate_rows(subdf):
    subdf = (
        subdf.groupby(by=["class", "table", "column"], as_index=False)
        .agg(
            {
                "pattern_type": string_join_without_dup,
                "lineno": ", ".join,
                "file": ", ".join,
            }
        )
        .reset_index(drop=True)
    )

    gb = subdf.groupby(by=["class", "table"], as_index=False)
    dup_index = {}
    global dup_num
    dup_num = 10000

    def apply_func(x):
        global dup_num
        index = list(x.index)
        values = list(x.values)
        val_index = {}

        for i in range(len(index)):
            val_index[values[i]] = index[i]
        if len(values) >= 2:
            for (v1, v2) in itertools.combinations(values, 2):
                if a_includes_b_exact(v1, v2) or a_includes_b_exact(v2, v1):
                    dup_index[val_index[v1]] = dup_num
                    dup_index[val_index[v2]] = dup_num
                    dup_num += 1
        return x

    gb.agg({"column": lambda x: apply_func(x)})

    dup_column = []
    for idx in subdf.index:
        if idx in dup_index:
            dup_column.append(dup_index[idx])
        else:
            dup_column.append(idx)
    subdf["dupindex"] = dup_column

    subdf = (
        subdf.groupby(by=["dupindex"], as_index=False)
        .agg(
            {
                "class": "first",
                "table": "first",
                "column": "last",
                "pattern_type": string_join_without_dup,
                "lineno": ", ".join,
                "file": ", ".join,
            }
        )
        .reset_index(drop=True)
    )

    return subdf


def third_party_columns(row, model_class):
    third_model_class = pd.read_csv("data/oscar_model_class.csv", keep_default_na=False)
    third_model_class["field_filter"] = third_model_class["field"].apply(
        lambda x: x.replace("_id", "").replace("_", "").replace(" ", "").strip()
    )
    model_class["field_filter"] = model_class["field"].apply(
        lambda x: x.replace("_id", "").replace("_", "").replace(" ", "").strip()
    )
    df = third_model_class[third_model_class["table"] == row["table"]]
    df_init = model_class[model_class["table"] == row["table"]]

    flag = True
    for col in row["column"].split(","):
        common = df_init[df_init["field_filter"] == col]
        third_common = df[df["field_filter"] == col]
        if not common.empty and (
            "Rel" in list(common.field_type)[0]
            or "ManyToManyField" == list(common.field_type)[0]
        ):
            return True
        if third_common.empty:
            flag = False
    return flag


def source_to_pattern_type(x):
    s_p1 = [
        "_check_qs_UnaryOp",
        "_check_qs_exists",
        "_check_length_compare_with_0_1",
    ]
    s_p2 = ["get_type", "M2M"]
    if vals_in_x(x, s_p1):
        return "PA_u1"
    elif vals_in_x(x, s_p2):
        return "PA_u2"
    else:
        print("Wrong source pattern!")
        return "PA_u1"


# Test if all columns exist in the models
def test_column_exists(source, collist, t_name, model_class):
    overlap = model_class[(model_class["table"] == t_name)]
    overlap_2 = overlap[
        overlap.field_type.apply(
            lambda x: x
            not in ["ManyToOneRel", "OneToOneRel", "ManyToManyRel", "ManyToManyField"]
        )
    ]
    if source == "get_type":
        overlap = overlap_2
    if overlap.empty:
        return False

    aval_cols = list(overlap.column)
    for col in collist:
        if col in aval_cols:
            return True
    return False


#
# 0) Not app- specific fields (E.g., Django)
# 1) Auto generated fields, including ID, M2M fields, auto_now for Datetime, (AutoSlug for Oscar)
# 2) CharField, Doc says no need to specify them.
#
def filter_unique_cons(model_class, unique_cons):
    # Filter third party
    unique_cons = unique_cons[unique_cons["table"].apply(filter_by_kws)]
    unique_cons = unique_cons[unique_cons["column"].apply(filter_by_kws_fields)]
    # Filter id
    model_class["column"] = model_class["field"].apply(remove_id_sort)
    unique_cons = add_model_info_to_unique_constraints(model_class, unique_cons)
    unique_cons = unique_cons[unique_cons["field_type"] != "AutoField"]
    unique_cons = unique_cons[unique_cons["primary_key"] != "True"]
    unique_cons = unique_cons[unique_cons["field_type"] != "InternalIdentifierField"]
    return unique_cons


def add_model_info_to_unique_constraints(model_class, unique_cons):
    rst_field_type = []
    rst_pk = []
    rst_parent_model = []
    for index, row in unique_cons.iterrows():
        t_name = row["table"]
        col_name = row["column"]
        cols = col_name.split(",")
        if len(cols) > 0:
            col_name = cols[0].strip()

        overlap = model_class[
            (model_class["table"] == t_name) & (model_class["column"] == col_name)
        ]
        if not overlap.empty:
            rst_field_type.append(list(overlap.field_type)[0])
            rst_pk.append(list(overlap.primary_key)[0])
            rst_parent_model.append(list(overlap.parent)[0])
        else:
            rst_field_type.append("")
            rst_pk.append("")
            rst_parent_model.append("")

    unique_cons["field_type"] = rst_field_type
    unique_cons["primary_key"] = rst_pk
    unique_cons["parent"] = rst_parent_model

    return unique_cons


# For history issues only.
def gen_history_issue_result(unique_cons, model_class, detected, path, result):
    model_class["column"] = model_class["field"].apply(remove_id_sort)
    unique_cons = add_model_info_to_unique_constraints(model_class, unique_cons)
    match_existing_constrasints_with_source_label(unique_cons, detected)
    unique_cons = handle_inherit_models(unique_cons, model_class)

    total_detected = unique_cons[unique_cons["detect"] == 1].shape[0]

    result["detected"] = total_detected
    result["total"] = unique_cons.shape[0]

    if not os.path.exists(path):
        os.makedirs(path)
    unique_cons[["table", "column", "file", "lineno", "detect"]].sort_values(
        by=["detect", "table", "column"],
        ascending=[False, True, True],
    ).to_csv(path + "[unique]detected_history_constraints.csv", index=False)
