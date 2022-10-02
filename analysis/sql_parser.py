import sqlite3
import sqlparse
import pandas as pd

# import mysql.connector
from dotenv import dotenv_values

config = dotenv_values("env")
from psycopg2 import connect

#
#  Copy from sqlparse
#
def extract_definitions(token_list):
    # assumes that token_list is a parenthesis
    definitions = []
    tmp = []
    par_level = 0
    for token in token_list.flatten():
        if token.is_whitespace:
            continue
        elif token.match(sqlparse.tokens.Punctuation, "("):
            par_level += 1
            continue
        if token.match(sqlparse.tokens.Punctuation, ")"):
            if par_level == 0:
                break
            else:
                par_level += 1
        elif token.match(sqlparse.tokens.Punctuation, ","):
            if tmp:
                definitions.append(tmp)
            tmp = []
        else:
            tmp.append(token)
    if tmp:
        definitions.append(tmp)
    return definitions


#
#  Get all DDLs
#
def get_ddls_from_schema(path):
    con = sqlite3.connect(config.get(path))
    cur = con.cursor()
    cur.execute("select sql from sqlite_schema where type='table'")
    schemas = cur.fetchall()
    con.close()

    res = [schema[0].lower().strip() for schema in schemas if "sqlite" not in schema[0]]

    return res


#
#  Get all index
#
def get_indexs_from_schema(path):
    con = sqlite3.connect(config.get(path))
    cur = con.cursor()
    cur.execute("select sql from sqlite_master where type='index'")
    schemas = cur.fetchall()
    con.close()

    res = [
        schema[0].lower().strip()
        for schema in schemas
        if ((schema[0] != None) and ("UNIQUE" in schema[0]))
    ]

    return res


#
#  Get from sqlparse
#
def get_table_columns(ddls):
    table_columns = {}
    for raw in ddls:
        parsed = sqlparse.parse(raw)[0]
        table_name = parsed.tokens[4].get_name()

        # extract the parenthesis which holds column definitions
        _, par = parsed.token_next_by(i=sqlparse.sql.Parenthesis)
        columns = extract_definitions(par)
        table_columns[table_name] = columns
    return table_columns


#
#  Get from sqlparse
#
def get_alter_fk_column(ddls):
    if type(ddls) != list:
        ddls = [ddls]

    table_columns = {}
    for raw in ddls:
        parsed = sqlparse.parse(raw)[0]
        table_name = parsed.tokens[4].get_name()

        # extract the parenthesis which holds column definitions
        _, par = parsed.token_next_by(i=sqlparse.sql.Parenthesis)
        if table_name not in table_columns:
            table_columns[table_name] = []

        # print(table_name, par)
        table_columns[table_name].append(str(par))

    return table_columns


#  Get unique index
# @return Example: 'auth_group_permissions': ['group_id,permission_id', 'group_id,partner_sku']
#
def get_unique_index(ddls):
    table_columns = {}
    for raw in ddls:
        parsed = sqlparse.parse(raw)[0]
        table_name = parsed.tokens[10].get_name()

        # extract the parenthesis which holds column definitions
        _, par = parsed.token_next_by(i=sqlparse.sql.Parenthesis)
        columns = extract_definitions(par)
        if table_name not in table_columns:
            table_columns[table_name] = []
        table_columns[table_name].append(columns)

    constraint = {}

    #     return table_columns
    for table in table_columns:
        for column in table_columns[table]:
            part_unqiue_list = []
            for col in column:
                column_name = str(col[0]).replace('"', "")
                part_unqiue_list.append(str(column_name))

            if table not in constraint:
                constraint[table] = []
            constraint[table].append(",".join(part_unqiue_list))

    return constraint


#
#  Find one type of constraint from generated table_columns
#
def get_one_constraint(table_columns, constraint_name):
    constraint = {}

    for table in table_columns:
        for column in table_columns[table]:
            column_name = str(column[0]).replace('"', "")
            definition = " ".join(str(t) for t in column[1:])

            if constraint_name in definition:
                if table not in constraint:
                    constraint[table] = []
                constraint[table].append(column_name)

    return constraint


def print_one_constaint(constraint):
    for key, val in constraint.items():
        for v in val:
            print("{name!s:55}; {definition}".format(name=key, definition=v))


def get_num_in_constaint(constraint):
    num = 0
    for key, val in constraint.items():
        num += len(val)
    return num


def intersect_two_constraints(lst1, lst2):
    lst_common = []
    lst_1_only = []
    lst_2_only = []
    for value in lst1:
        if value in lst2:
            lst_common.append(value)
        else:
            lst_1_only.append(value)

    lst_2_only = [value for value in lst2 if value not in lst_common]

    return lst_common, lst_1_only, lst_2_only


def table_name_convert_to_class(table_name):
    tmp = table_name.split("_")
    app, c_name = tmp[0], tmp[1].lower()
    return app + "." + c_name


#
#  Merge two dict
#
def merge_dict(d1, d2):
    d3 = {}
    for key in d1:
        d3[key] = []
        if key in d2:
            print("exist same keys")
            d3[key] = list(set(d1[key] + d2[key]))
        else:
            d3[key] = d1[key]

    for key in d2:
        d3[key] = []
        if key in d1:
            continue
        else:
            d3[key] = d2[key]
    return d3


#
#  check if the value of a dict[key] is from unique_together(has two items)
# @return False if not
# @return [col1, col2] if yes.
#
def is_uni_together(val):
    tmp = val.split(",")
    if len(tmp) <= 1:
        return False
    else:
        return tmp
