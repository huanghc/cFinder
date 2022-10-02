# from analysis import analysis
# from importlib import reload
import os, sys
import pandas as pd
import numpy as np
from django.apps import apps
import dotenv
from util import env_loading
from IPython.display import display, HTML

if sys.version_info.major >= 3:
    from importlib import reload
reload(env_loading)
from django.db import connection


def is_app_model_by_module(module, SOFTWARE):
    # print(module)
    app_words = ["bakerydemo", "confirmation.models", "zerver"]
    edx_words = ["cms", "common", "lms", "openedx", "ecommerce"]
    if SOFTWARE.lower() in module:
        return True
    for item in app_words:
        if item in module:
            return True
    for item in edx_words:
        if item in module:
            return True
    return False


def get_field_source(field, model):
    if field.name in model.__dict__:
        return model._meta.db_table
    for parent_m in model.__mro__:
        if field.name in parent_m.__dict__:
            return parent_m._meta.db_table
    return model._meta.db_table


def get_info_from_fields(SOFTWARE):
    model_table = []
    for model in apps.get_models():

        # Only keep the models defined by the application itself.
        if not is_app_model_by_module(model.__module__, SOFTWARE):
            continue

        fields = model._meta.get_fields(include_hidden=False)

        for field in fields:
            parent_model = get_field_source(field, model)

            rst = {
                "model": model.__name__,
                "module": model.__module__,
                "app": model._meta.app_label,
                "table": model._meta.db_table,
                "field": field.name,
                "field_type": type(field).__name__,
                "parent": parent_model,
            }

            # print(field.name, field.__dict__)

            if type(field).__name__ == "ManyToManyField":
                M2M_model = field.remote_field.through
                rst["through_model"] = M2M_model.__name__
            else:
                rst["through_model"] = ""

            try:
                rst["related_names"] = field.remote_field.related_name
            except:
                rst["related_names"] = ""

            try:
                rst.update(
                    {
                        "primary_key": field.primary_key,
                        "max_length": field.max_length,
                        "unique": field._unique,
                        "null": field.null,
                    }
                )
            except:
                rst.update(
                    {"primary_key": "", "max_length": "", "unique": "", "null": ""}
                )
            try:
                rst["related_model"] = field.related_model._meta.db_table
            except:
                rst["related_model"] = ""

            try:
                rst["foreign_type"] = type(field.field).__name__
            except:
                rst["foreign_type"] = ""

            try:
                dft = field.default
                # print(dft.__name__)
                if hasattr(dft, "__name__") and "NOT_PROVIDED" in dft.__name__:
                    rst["default"] = ""
                else:
                    rst["default"] = dft  # str(dft)
            except Exception as e:
                rst["default"] = ""

            try:
                if hasattr(field, "auto_now") and field.auto_now:
                    rst["auto_now"] = "1"
            except:
                rst["auto_now"] = ""
            try:
                if hasattr(field, "auto_now_add") and field.auto_now_add:
                    rst["auto_now"] = "2"
            except:
                rst["auto_now"] = ""

            try:
                rst["on_delete"] = field.on_delete.__name__
            except:
                rst["on_delete"] = ""

            try:
                rst["on_delete"] = field.remote_field.on_delete.__name__
            except:
                rst["on_delete"] = rst["on_delete"]

            if type(field).__name__ in ["ManyToManyField"]:
                rst["foreign_type"] = "ManyToManyField"

            rst["is_m2m_field"] = "no"
            model_table.append(rst)

    model_table = pd.DataFrame(model_table)
    # print(model_table.columns)
    return add_m2m_info(model_table)


#
# Find the M2M fields and set the flag - is_m2m_field = '1'.
#
def add_m2m_info(model_table):
    for model in apps.get_models():
        fields = model._meta.get_fields(include_hidden=False)
        for field in fields:
            if type(field).__name__ == "ManyToManyField":
                model1 = model._meta.db_table
                model2 = field.remote_field.model._meta.db_table
                M2M_model = field.remote_field.through

                for m in [model1, model2]:
                    subdf = model_table[model_table["model"] == M2M_model.__name__]
                    if not subdf.empty:
                        subindex = np.array(subdf[subdf["related_model"] == m].index)
                        model_table.loc[subindex, "is_m2m_field"] = "yes"

    return model_table


def main():
    # Load args.
    if len(sys.argv) > 1:
        conf_idx = sys.argv[1]
    else:
        raise Exception("Require one args. (= 0,1,2)")

    # Load env
    dotenv.read_dotenv("env")
    SOFTWARE = os.environ["APP"]
    env_loading.load_for_model_info(SOFTWARE, conf_idx)

    # Main logic
    print("-" * 30)
    model_table = get_info_from_fields(SOFTWARE)

    file_name = "_model_class"
    if conf_idx != "0":
        file_name = file_name + "_" + str(conf_idx)

    model_table.to_csv(
        "data/" + SOFTWARE.lower() + file_name + ".csv", index=False, na_rep=""
    )
    print(
        "Gen model table info, saved to file: "
        + "data/"
        + SOFTWARE.lower()
        + file_name
        + ".csv"
    )
    print(
        "Total tables : %d, columns : %d"
        % (len(set(model_table.table)), model_table.shape[0])
    )
    print("-" * 30)


main()
