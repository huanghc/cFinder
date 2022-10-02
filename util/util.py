from . import util_unique
from . import util_notnull
from . import util_fk

# Standard
import os, json
import pandas as pd
import dotenv

# Load configs
dotenv.read_dotenv("env")
cons_list = json.loads(os.environ["CONS_LIST"])
software_list = json.loads(os.environ["SOFTWARE_LIST"])


def compare_result_entry(history_issue, pattern_type, SOFTWARE, find_con, path, result):
    find_con_dedup = find_con.copy()
    model_path = "data/"
    if history_issue:
        model_path = "data/history_issues/"

    model_class = pd.read_csv(
        model_path + SOFTWARE.lower() + "_model_class.csv", keep_default_na=False
    )

    result["app"] = SOFTWARE.lower()
    result["cons_type"] = pattern_type

    if pattern_type == "unique":
        db_constraint = pd.read_csv(
            model_path + SOFTWARE.lower() + "_unique.csv", keep_default_na=False
        )
        util_unique.compare_db_unique_constraint(
            find_con_dedup, db_constraint, model_class, path, result, history_issue
        )

    elif pattern_type == "fk":
        db_constraint = pd.read_csv(
            model_path + SOFTWARE.lower() + "_fk.csv", keep_default_na=False
        )
        util_fk.compare_db_fk_constraint(
            find_con_dedup, db_constraint, model_class, path, result, history_issue
        )

    elif pattern_type == "null":
        db_constraint = pd.read_csv(
            model_path + SOFTWARE.lower() + "_null.csv", keep_default_na=False
        )
        util_notnull.compare_db_not_null_constraint(
            find_con_dedup, db_constraint, model_class, path, result, history_issue
        )


def file_path_check(filepath, cons):
    if (
        "node_modules" in filepath
        or "ipynb_checkpoints" in filepath
        or "lib/python" in filepath
    ):
        return False
    if "test" in filepath and cons in ["null", "fk"]:
        return False
    return True


def result_to_tables(df):
    COMP_NUM = 52
    # Table 4
    tab4 = []
    for app in software_list:
        rst = {}
        app = app.lower()
        appdf = df[df["app"] == app]
        rst["App"] = app
        rst["Detected_existing"] = int(
            float(list(appdf[appdf["cons_type"] == "unique"]["total_detected"])[0])
            + float(list(appdf[appdf["cons_type"] == "null"]["total_detected"])[0])
        )
        rst["Detected_missing"] = sum(list(appdf["total"]))
        tab4.append(rst)
    # row for total
    sdf = pd.DataFrame(tab4)
    rst = {}
    rst["App"] = "Total"
    rst["Detected_existing"] = sdf["Detected_existing"].sum()
    rst["Detected_missing"] = sdf["Detected_missing"].sum() + COMP_NUM
    tab4.append(rst)
    pd.DataFrame(tab4).to_csv("result/table_4_total_detected_num.csv", index=False)

    # Table 6
    tab6 = []
    for app in software_list:
        rst = {}
        app = app.lower()
        appdf = df[df["app"] == app]

        rst["App"] = app
        udf = appdf[appdf["cons_type"] == "unique"]
        rst["PA_u1"] = list(udf["PA_1"])[0]
        rst["PA_u2"] = list(udf["PA_2"])[0]
        rst["total_U"] = list(udf["total"])[0]
        udf = appdf[appdf["cons_type"] == "null"]
        rst["PA_n1"] = list(udf["PA_1"])[0]
        rst["PA_n2"] = list(udf["PA_2"])[0]
        rst["PA_n3"] = list(udf["PA_3"])[0]
        rst["total_N"] = list(udf["total"])[0]
        udf = appdf[appdf["cons_type"] == "fk"]
        rst["PA_f1"] = list(udf["PA_1"])[0]
        rst["PA_f2"] = list(udf["PA_2"])[0]
        rst["total_F"] = list(udf["total"])[0]

        tab6.append(rst)

    # row for total
    sdf = pd.DataFrame(tab6)
    rst = {}
    rst["App"] = "Total"
    rst["PA_u1"] = sdf["PA_u1"].sum()
    rst["PA_u2"] = sdf["PA_u2"].sum()
    rst["total_U"] = sdf["total_U"].sum()
    rst["PA_n1"] = sdf["PA_n1"].sum()
    rst["PA_n2"] = sdf["PA_n2"].sum()
    rst["PA_n3"] = sdf["PA_n3"].sum()
    rst["total_N"] = sdf["total_N"].sum()
    rst["PA_f1"] = sdf["PA_f1"].sum()
    rst["PA_f2"] = sdf["PA_f2"].sum()
    rst["total_F"] = sdf["total_F"].sum()
    tab6.append(rst)

    pd.DataFrame(tab6).to_csv(
        "result/table_6_breakdown_detected_missing_constraints.csv", index=False
    )

    # Table 8
    tab8 = []
    for app in software_list:
        rst = {}
        app = app.lower()

        appdf = df[df["app"] == app]

        rst["App"] = app
        udf = appdf[appdf["cons_type"] == "unique"]
        ndf = appdf[appdf["cons_type"] == "null"]
        # print(udf, ndf)
        rst["Already_set_Unique"] = list(udf["total_existing"])[0]
        rst["Already_set_Not_null"] = list(ndf["total_existing"])[0]
        rst["Cfinder_cover_Unique"] = list(udf["recall"])[0]
        rst["Cfinder_cover_Not_null"] = list(ndf["recall"])[0]

        tab8.append(rst)

    pd.DataFrame(tab8).to_csv(
        "result/table_8_percentage_existing_constraints_already_set_covered.csv",
        index=False,
    )


def history_result_to_tables(df):
    # Table 9
    tab9 = []
    rst = {}

    for con in ["fk", "unique", "null"]:
        condf = df[df["cons_type"] == con]
        if con == "unique":
            rst["Total_Dataset_Unique"] = condf["total"].sum()
            rst["Cfinder_cover_Unique"] = round(
                float(condf["detected"].sum()) / condf["total"].sum(), 2
            )
        elif con == "null":
            rst["Total_Dataset_Not_null"] = condf["total"].sum()
            rst["Cfinder_cover_Not_null"] = round(
                float(condf["detected"].sum()) / condf["total"].sum(), 2
            )
        elif con == "fk":
            rst["Total_Dataset_FK"] = condf["total"].sum()
            rst["Cfinder_cover_FK"] = round(
                float(condf["detected"].sum()) / condf["total"].sum(), 2
            )
    tab9.append(rst)
    columns = [
        "Total_Dataset_Unique",
        "Total_Dataset_Not_null",
        "Total_Dataset_FK",
        "Cfinder_cover_Unique",
        "Cfinder_cover_Not_null",
        "Cfinder_cover_FK",
    ]
    pd.DataFrame(tab9)[columns].to_csv(
        "result/table_9_percentage_constraints_in_collected_dataset_covered.csv",
        index=False,
    )
