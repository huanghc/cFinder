import dotenv
import os
import pandas as pd


def load(SOFTWARE):
    root = os.environ[SOFTWARE + "_PROJECT_DIR"]
    model_class = pd.read_csv(
        "data/" + SOFTWARE.lower() + "_model_class.csv", keep_default_na=False
    )

    return root, model_class


def load_history(SOFTWARE):
    root = "data/history_issues/history_app/" + SOFTWARE.lower()
    model_class = pd.read_csv(
        "data/history_issues/" + SOFTWARE.lower() + "_model_class.csv",
        keep_default_na=False,
    )

    return root, model_class
