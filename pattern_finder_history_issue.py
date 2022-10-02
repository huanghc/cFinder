# Standard
import os
import pandas as pd

# Tool libs
from analysis import analysis
from util import util, basic_loading


def find_patterns(app, cons, root, model_class):
    extract_constraints, init_usage = [], []

    for roots, dirs, files in os.walk(root):
        for file in files:
            if file.endswith(".py"):
                filepath = os.path.join(roots, file)

                analysis.pattern_finder(
                    filepath, extract_constraints, init_usage, model_class, cons
                )

    extract_constraints = pd.DataFrame(extract_constraints)
    if cons == "null":
        extract_constraints = util.util_notnull.add_fields_with_default(
            app, extract_constraints
        )
    return extract_constraints


def history_run():
    print("\nRun for history issues")
    target_cons = ["unique", "null", "fk"]
    target_apps = ["OSCAR", "SALEOR", "SHUUP", "ZULIP", "WAGTAIL"]

    overall_result = []
    for app in target_apps:
        print("--> Application: ", app)
        root, model_class = basic_loading.load_history(app)

        for cons in target_cons:
            result = {}
            print("----> Constraint: ", cons)
            extract_constraints = find_patterns(app, cons, root, model_class)
            path = "result_history_issues/" + app.lower() + "/"

            util.compare_result_entry(
                True, cons, app, extract_constraints, path, result
            )
            overall_result.append(result)

    # Save results
    pd.DataFrame(overall_result).to_csv(
        "result_history_issues/overall.csv", index=False
    )
    util.history_result_to_tables(pd.DataFrame(overall_result))


history_run()
