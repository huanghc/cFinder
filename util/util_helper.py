filter_third_party_kws_fields = [
    "lft",
    "rght",
    "tree_id",
    "tree",
    "level",
    "languagecode",
    "language_code",
    "ptr",
    "content_type",
    "polymorphic",
    "enterprise_",
]

filter_third_party_kws = [
    "django",
    "auth_",
    "thumbnail",
    "otp_",
    "two_factor_",
    "celery",
    "kombu",
    "socialaccount_",
    "dripping_",
    "taggit_",
    "_translation",
    "logentry",
    "filer",
    "parler",
    "oauth",
    "historical",
    "enterprise_",
    "waffle_",
    "edxval_",
    "wiki_",
    "assessment_",
    "milestones_",
    "lti1p3_",
    "lti_",
    "proctoring_",
    "submission",
    "xapi_",
    "workflow_",
    "organizations_",
    "blackboard_",
    "canvas_",
]

#
# Filter third-party table names
#
def filter_by_kws(
    x,
    filter_kws=filter_third_party_kws,
):
    for i in filter_kws:
        if i in x:
            return False
    return True


# third party library fields.
def filter_by_kws_fields(
    x,
    filter_kws=filter_third_party_kws_fields,
):
    for i in filter_kws:
        if i in x:
            return False
        if remove_id_sort(i) in x:
            return False
    return True


def remove_id_sort(x):
    x = x.replace("_id", "").replace(" ", "").strip()

    fitlered = []
    for item in x.split(","):
        rst = item.find("__")
        if rst != -1:
            fitlered.append(item[:rst])
        else:
            fitlered.append(item)

    x = ",".join(sorted(set(fitlered)))
    x = x.replace("_", "")

    return x


def a_includes_b(stra, strb):
    lista = stra.split(",")
    listb = strb.split(",")
    # print("a: ", lista, "b: ", listb)
    for i in listb:
        if (i not in lista) and (i not in lista[0]):
            return False
    return True


def a_includes_b_exact(stra, strb):
    lista = stra.split(",")
    listb = strb.split(",")
    for i in listb:
        if i not in lista:
            return False
    return True


def is_all_test_files(x):
    for file in x.split(", "):
        if (
            "test" not in file
            and "random_data" not in file
            and "migrations" not in file
        ):
            return 0
    return 1


def is_id_pk_column_in_list(x):
    if not x:
        return True
    for item in x.split(","):
        if "id" == item or "pk" == item:
            return True
    return False


def clean_file_lineno(df):
    for index, row in df.iterrows():
        keep_file = []
        keep_lineno = []
        all_files = row["file"].split(", ")
        all_linenos = row["lineno"].split(", ")
        for idx in range(len(all_files)):
            if "test" not in all_files[idx]:
                keep_file.append(all_files[idx])
                keep_lineno.append(all_linenos[idx])
        if len(keep_file) > 0:
            df.at[index, "file"] = ", ".join(keep_file)
            df.at[index, "lineno"] = ", ".join(keep_lineno)
    return df


def vals_in_x(x, vals):
    for val in vals:
        if val in x:
            return True
    return False


def string_join_without_dup(x):
    values = set(list(x.values))
    return ", ".join(values)


charList = [
    "CharField",
    "TextField",
    "NullCharField",
    "UppercaseCharField",
    "EmailField",
    "URLField",
    "ExtendedURLField",
    "AutoSlugField",
    "MoneyField",
    "TaxedMoneyField",
    "RichTextField",
    "StreamField",
    "JSONField",
]

edx_third_party_fields = [
    "BlockTypeKeyField",
    "UsageKeyWithRunField",
    "CourseKeyField",
    "LearningContextKeyField",
    "UsageKeyField",
]
