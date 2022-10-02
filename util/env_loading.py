import dotenv
import sys
import os
import django
import pandas as pd

dotenv.read_dotenv("../code_analysis/env")
SOFTWARE = os.environ["APP"]


def load(SOFTWARE):

    if SOFTWARE == "COMP":
        sys.path.append(os.environ["COMP_PROJECT_ENV_DIR"])
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", os.environ["COMP_SETTINGS_DIR"])
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        django.setup()
        model_class = pd.read_csv("data/comp_model_class.csv", keep_default_na=False)
        root = os.environ["COMP_PROJECT_DIR"]

    ## Django-Oscar
    elif SOFTWARE == "OSCAR":
        sys.path.append(os.path.join(os.environ["OSCAR_PROJECT_DIR"], "src"))
        os.environ.setdefault(
            "DJANGO_SETTINGS_MODULE", "app_code.django-oscar.sandbox.settings"
        )
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        django.setup()
        model_class = pd.read_csv("data/oscar_model_class.csv", keep_default_na=False)
        root = os.environ["OSCAR_PROJECT_DIR"]

    ## Saleor
    elif SOFTWARE == "SALEOR":
        sys.path.append(os.environ["SALEOR_PROJECT_DIR"])
        os.environ.setdefault(
            "DJANGO_SETTINGS_MODULE", "app_code.saleor.saleor.settings"
        )
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        django.setup()
        model_class = pd.read_csv("data/saleor_model_class.csv", keep_default_na=False)
        root = os.environ["SALEOR_PROJECT_DIR"]

    elif SOFTWARE == "ZULIP":
        sys.path.append(os.environ["ZULIP_PROJECT_DIR"])
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "zproject.settings")
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        django.setup()
        model_class = pd.read_csv("data/zulip_model_class.csv", keep_default_na=False)
        root = "/home/zulip/deployments/current/"

    elif SOFTWARE == "SHUUP":
        sys.path.append(os.environ["SHUUP_PROJECT_DIR"])
        os.environ.setdefault(
            "DJANGO_SETTINGS_MODULE",
            "app_code.shuup.shuup_workbench.settings.base_settings",
        )
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        django.setup()
        model_class = pd.read_csv("data/shuup_model_class.csv", keep_default_na=False)
        root = os.environ["SHUUP_PROJECT_DIR"]

    elif SOFTWARE == "WAGTAIL":
        sys.path.append(os.environ["WAGTAIL_DEMO_PROJECT_DIR"])
        os.environ.setdefault(
            "DJANGO_SETTINGS_MODULE", "app_code.bakerydemo.bakerydemo.settings.local"
        )
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        django.setup()
        model_class = pd.read_csv("data/wagtail_model_class.csv", keep_default_na=False)
        root = os.environ["WAGTAIL_PROJECT_DIR"]

    elif SOFTWARE == "EDX":
        sys.path.append(os.environ["EDX_PROJECT_DIR"])
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "lms.envs.devstack_docker")
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        django.setup()
        model_class = pd.read_csv("data/edx_model_class.csv", keep_default_na=False)
        root = os.environ["EDX_PROJECT_DIR"]

    elif SOFTWARE == "EDX_COMMERCE":
        sys.path.append(os.environ["EDX_COMMERCE_PROJECT_DIR"])
        os.environ.setdefault(
            "DJANGO_SETTINGS_MODULE", os.environ["EDX_COMMERCE_SETTINGS_DIR"]
        )
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        django.setup()
        model_class = pd.read_csv(
            "data/edx_commerce_model_class.csv", keep_default_na=False
        )
        root = os.environ["EDX_COMMERCE_PROJECT_DIR"]

    else:
        raise ("SOFTWARE in env not valid; need specify in env_loading.py")
    return root, model_class


def load_for_model_info(SOFTWARE, conf_idx):

    if SOFTWARE == "COMP":
        conf_idx = int(conf_idx)
        if conf_idx < 2:
            sys.path.append(os.environ["COMP_PROJECT_ENV_DIR"])
            os.environ.setdefault(
                "DJANGO_SETTINGS_MODULE", os.environ["COMP_SETTINGS_DIR"]
            )
            os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
            django.setup()
        elif conf_idx == 2:
            sys.path.append(os.environ["COMP_PROJECT_ENV_DIR_2"])
            os.environ.setdefault(
                "DJANGO_SETTINGS_MODULE", os.environ["COMP_SETTINGS_DIR_2"]
            )
            os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
            django.setup()
        else:
            raise Exception("Wrong arg value!")

    elif SOFTWARE == "EDX":
        conf_idx = int(conf_idx)
        if conf_idx < 2:
            sys.path.append(os.environ["EDX_PROJECT_DIR"])
            os.environ.setdefault(
                "DJANGO_SETTINGS_MODULE", os.environ["EDX_SETTINGS_DIR"]
            )
            os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
            django.setup()
        elif conf_idx == 2:
            sys.path.append(os.environ["EDX_PROJECT_DIR_2"])
            os.environ.setdefault(
                "DJANGO_SETTINGS_MODULE", os.environ["EDX_SETTINGS_DIR_2"]
            )
            os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
            django.setup()
        else:
            raise Exception("Wrong arg value!")

    else:
        load(SOFTWARE)
