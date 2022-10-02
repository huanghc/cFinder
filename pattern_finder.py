# Standard
import os, time, json
import pandas as pd
import dotenv

# Tool libs
from analysis import analysis
from util import util, basic_loading

# Load configs
dotenv.read_dotenv("env")
cons_list = json.loads(os.environ["CONS_LIST"])
software_list = json.loads(os.environ["SOFTWARE_LIST"])
SOFTWARE = os.environ["APP"]
cons_type = os.environ["CONS_TYPE"]


def find_patterns(app, cons, root, model_class):
    extract_constraints, init_usage = [], []

    for roots, dirs, files in os.walk(root):
        for file in files:
            if file.endswith(".py"):
                filepath = os.path.join(roots, file)

                if not util.file_path_check(filepath, cons):
                    continue
                analysis.pattern_finder(
                    filepath, extract_constraints, init_usage, model_class, cons
                )

    extract_constraints = pd.DataFrame(extract_constraints)
    if cons == "null":
        extract_constraints = util.util_notnull.add_fields_with_default(
            app, extract_constraints
        )
    return extract_constraints


def main():
    print("-" * 30)
    rst_exe_time = []
    # Specify the target constraints.
    target_cons = []
    if cons_type == "all":
        target_cons = cons_list
    else:
        target_cons.append(cons_type)

    # Specify the target apps.
    target_apps = []
    if SOFTWARE == "all":
        target_apps = software_list
    else:
        target_apps.append(SOFTWARE)

    # Main funcs.
    overall_result = []
    for app in target_apps:
        start = time.time()
        print("--> Application: ", app)
        root, model_class = basic_loading.load(app)

        for cons in target_cons:
            result = {}
            print("----> Constraint: ", cons)
            extract_constraints = find_patterns(app, cons, root, model_class)
            # Compare and persist the `missing` constraints.
            util.compare_result_entry(
                False,
                cons,
                app,
                extract_constraints,
                "result/" + app.lower() + "/",
                result,
            )
            overall_result.append(result)

        end = time.time()
        rst_exe_time.append({"APP": app, "Analysis_time": end - start})
        print("--> Exec time: %.2f s" % (end - start))

    # Save results
    pd.DataFrame(overall_result).to_csv("result/overall.csv", index=False)
    util.result_to_tables(pd.DataFrame(overall_result))
    pd.DataFrame(rst_exe_time).to_csv(
        "result/table_10_time_to_run_analysis.csv", index=False
    )


main()
