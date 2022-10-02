import os
import pandas as pd
from .util_helper import *

################################
## FK
################################
def compare_db_fk_constraint(
    find_con, db_constraint, model_class, path, result, history_issue
):
    find_con = find_con.astype(str)
    find_con.drop_duplicates(
        subset=[
            "referenced_class",
            "referenced_col",
            "referenced_usage",
            "dependent_class",
            "dependent_col",
        ],
        keep="first",
    )

    keep_idx = []
    for index, row in find_con.iterrows():
        if row["dependent_table"] != row["referenced_table"]:
            keep_idx.append(index)
    find_con = find_con.iloc[keep_idx]

    if not find_con.empty:
        find_con = (
            find_con.groupby(
                by=["dependent_class", "dependent_table", "dependent_col", "filtered"],
                as_index=False,
            )
            .agg(string_join_without_dup)
            .reset_index()
        )
        find_con["dependent_col"] = find_con["dependent_col"].apply(remove_id_sort)
    model_class["column"] = model_class["field"].apply(remove_id_sort)

    if history_issue:
        gen_history_issue_result(db_constraint, model_class, find_con, path, result)
        return

    find_con = filter_resulst(find_con, model_class)

    # Gen results
    if not os.path.exists(path):
        os.makedirs(path)

    find_con.loc[:, "pattern_type"] = find_con["source"].apply(source_to_pattern_type)
    cols = [
        "dependent_class",
        "dependent_table",
        "dependent_col",
        "referenced_class",
        "referenced_table",
        "referenced_col",
        "pattern_type",
        "file",
        "lineno",
    ]
    find_con = find_con[cols][find_con["filtered"] == "candidate"]
    find_con.sort_values(
        by=[
            "dependent_table",
            "dependent_col",
            "pattern_type",
            "referenced_class",
        ],
        ascending=[True, True, True, True],
    )
    find_con.reset_index(drop=True, inplace=True)
    find_con.to_csv(path + "[FK]newly_detected.csv", index=False)
    pa_f1 = find_con[find_con["pattern_type"] == "PA_f1"].shape[0]
    pa_f2 = find_con[find_con["pattern_type"] == "PA_f2"].shape[0]

    print(
        "------> [Detected New Constraints]: PA_f1: ",
        pa_f1,
        " PA_f2: ",
        pa_f2,
        " total: ",
        find_con.shape[0],
    )

    result["PA_1"] = pa_f1
    result["PA_2"] = pa_f2
    result["PA_3"] = 0
    result["total"] = find_con.shape[0]


def source_to_pattern_type(x):
    s_p1 = ["AssignPK"]
    s_p2 = ["get_type", "KeyValuePK"]
    if vals_in_x(x, s_p1):
        return "PA_f1"
    elif vals_in_x(x, s_p2):
        return "PA_f2"
    else:
        print("Wrong source pattern!")
        return "PA_f1"


def filter_resulst(find_con, model_class):
    # Clean and filter the results.
    filtered = []
    for index, row in find_con.iterrows():
        t_name = row["dependent_table"]

        coexist = model_class[
            (model_class["table"] == t_name)
            & (model_class["column"] == row["dependent_col"])
        ]

        # Filter not matched table col.
        if coexist.empty:
            filtered.append("filtered")
        else:
            if list(set(coexist.field_type))[0] in [
                "ManyToOneRel",
                "ManyToManyRel",
                "ManyToManyField",
            ]:
                filtered.append("filtered")
            # Find existing FK.
            elif (
                list(coexist["field_type"])[0]
                in [
                    "ForeignKey",
                    "ManyToManyRel",
                    "OneToOneField",
                ]
                + edx_third_party_fields
            ):
                filtered.append("z-exist")
            # Filter not extract dependent_table
            elif "False" not in row["filtered"].split(", "):
                filtered.append("filtered")
            # Filter existing PK.
            elif list(coexist["primary_key"])[0] == "True":
                filtered.append("filtered")
            else:
                filtered.append("candidate")
    find_con["filtered"] = filtered

    return find_con


# For history issues only
def gen_history_issue_result(fk_cons, model_class, detected, path, result):
    if fk_cons.shape[0] == 0:
        result["detected"] = 0
        result["total"] = 0
        return

    if detected.empty:
        fk_cons["detect"] = 0
        total_detected = 0
    else:
        fk_cons["detect"] = filter_detected_ones(fk_cons, detected, model_class)
        total_detected = fk_cons[fk_cons["detect"] == 1].shape[0]

    result["detected"] = total_detected
    result["total"] = fk_cons.shape[0]

    if not os.path.exists(path):
        os.makedirs(path)
    fk_cons[["table", "column", "detect"]].sort_values(
        by=["detect", "table", "column"],
        ascending=[False, True, True],
    ).to_csv(path + "[fk]detected_history_constraints.csv", index=False)


# For history issues only
def filter_detected_ones(fk_cons, detected, model_class):
    rst = []

    for index, row in fk_cons.iterrows():
        t_name = row["table"]
        col_name = row["column"]

        df_empty_flag = True
        df = detected[
            (detected["dependent_table"] == t_name)
            & (detected["dependent_col"] == col_name)
        ]
        if not df.empty:
            df_empty_flag = False
        if df_empty_flag:
            rst.append(0)
        else:
            rst.append(1)

    return rst
