import os
import dotenv
import pandas as pd
import subprocess

# Load env
dotenv.read_dotenv("env")
SOFTWARE = os.environ["APP"]


def main():

    if SOFTWARE == "EDX":
        subprocess.call(["python", "util/parse_one_model_info.py", "1"])
        subprocess.call(["python", "util/parse_one_model_info.py", "2"])
        merge_two_results()
    else:
        subprocess.call(["python", "util/parse_one_model_info.py", "0"])


def merge_two_results():
    model_class = pd.read_csv(
        "data/" + SOFTWARE.lower() + "_model_class_1.csv", keep_default_na=False
    )
    model_class_2 = pd.read_csv(
        "data/" + SOFTWARE.lower() + "_model_class_2.csv", keep_default_na=False
    )

    model_class_merge = model_class_2.append(model_class)
    model_class_merge = model_class_merge.drop_duplicates(keep="first")

    model_class_merge.to_csv(
        "data/" + SOFTWARE.lower() + "_model_class.csv", index=False, na_rep=""
    )
    print(
        "Gen model table info, saved to file: "
        + "data/"
        + SOFTWARE.lower()
        + "_model_class.csv"
    )
    print("Total columns : %d" % (model_class_merge.shape[0]))


main()
