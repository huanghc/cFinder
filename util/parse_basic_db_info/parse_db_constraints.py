import pandas as pd
import os, json
from django.db import connection
import dotenv
from util import env_loading, parse_db_helper, util_helper
from django.apps import apps

dotenv.read_dotenv("env")
cons_list = json.loads(os.environ["CONS_LIST"])
software_list = json.loads(os.environ["SOFTWARE_LIST"])
SOFTWARE = os.environ["APP"]
DB_NAME = os.environ["DB_NAME"]
DB_TYPE = os.environ["DB_TYPE"]


def get_constrasints():
    root, model_class = env_loading.load(SOFTWARE)
    cur = connection.cursor()

    # foreign key, unique, notnull
    cons_type_list = ["f", "u", "n"]

    print("-" * 30)
    for cons_type in cons_type_list:
        print(
            "Generating for DB (%s) SOFTWARE (%s) constraints (%s) "
            % (DB_TYPE, SOFTWARE, cons_type)
        )
        rst = parse_db_helper.fetch_constraints_rst(
            cur, cons_type, SOFTWARE, DB_NAME, DB_TYPE
        )
        dic = parse_db_helper.parse_rst(rst, cons_type, DB_TYPE)
        parse_db_helper.save_rst(dic, cons_type, SOFTWARE)
        print("-" * 30)


def clean_third_party(software):
    for cons_type in ["unique", "fk", "null"]:
        cons = pd.read_csv("data/" + software.lower() + "_" + cons_type + ".csv")
        # Filter third party models
        cons = cons[cons["table"].apply(util_helper.filter_by_kws)]
        # Filter third party fields
        cons = cons[cons["column"].apply(util_helper.filter_by_kws_fields)]

        cons.to_csv("data/" + software.lower() + "_" + cons_type + ".csv", index=False)


get_constrasints()
clean_third_party(SOFTWARE)
