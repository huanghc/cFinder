import pickle
import pandas as pd
import re


def fetch_constraints_rst(cur, cons_type, SOFTWARE, DB_NAME, DB_TYPE):
    ## Postgre
    sql = (
        """select conrelid::regclass AS table_from, conname, pg_get_constraintdef(c.oid)
    from   pg_constraint c
    join   pg_namespace n ON n.oid = c.connamespace
    where  contype in ('%s') order by contype"""
        % cons_type
    )
    notnull_sql = (
        """SELECT table_name, column_name FROM information_schema.columns
    WHERE is_nullable = 'NO' and table_catalog='%s' and table_schema='public';"""
        % DB_NAME.lower()
    )

    ## MySQL - unique and fk
    # Find the DB_NAME: select distinct CONSTRAINT_SCHEMA from INFORMATION_SCHEMA.KEY_COLUMN_USAGE;
    mysql_unique_fk = (
        """select CONSTRAINT_NAME,COLUMN_NAME,TABLE_NAME,REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE where CONSTRAINT_SCHEMA='%s' and CONSTRAINT_NAME!='PRIMARY';"""
        % DB_NAME
    )

    # # For zulip
    if (
        SOFTWARE == "ZULIP"
        or SOFTWARE == "COMP"
        or SOFTWARE == "EDX"
        or SOFTWARE == "EDX_COMMERCE"
    ):
        notnull_sql = (
            """SELECT table_name, column_name FROM information_schema.columns
        WHERE is_nullable = 'NO' and table_schema='%s';"""
            % DB_NAME.lower()
        )

    # constraints type
    if cons_type == "n":
        sql = notnull_sql

    # DB type
    if DB_TYPE == "mysql":
        if cons_type == "u" or cons_type == "f":
            sql = mysql_unique_fk

    print(sql)
    cur.execute(sql)  # 'f', 'p','c','u'
    rst = cur.fetchall()

    return rst


def parse_rst(rst, cons_type, DB_TYPE):
    dic = []

    if DB_TYPE == "mysql":
        return mysql_parser_tmp(cons_type, rst)

    elif DB_TYPE == "postgre":
        for item in rst:
            table_name = item[0]
            if cons_type == "u":
                # For unique constraints, the format is like Unique (columns), so filter by [8:-1]
                column = item[2][8:-1]
                dic.append({"table": table_name, "column": column})
            elif cons_type == "f":
                pattern = r"FOREIGN KEY \((.*)\) REFERENCES (.*)\((.*)\)"
                column, parent, parent_col = re.match(pattern, item[2]).groups()
                dic.append(
                    {
                        "table": table_name,
                        "column": column.replace("_id", ""),
                        "parent": parent,
                        "prt_col": parent_col,
                    }
                )
            elif cons_type == "c":
                dic.append({"table": table_name, "column": item})
            elif cons_type == "n":
                dic.append({"table": table_name, "column": item[1]})

        dic = pd.DataFrame(dic)
        # display(HTML(dic.iloc[:10, :].to_html(index=False)))
        return dic


def composite_count_unique(x):
    count = len(x.split(","))
    return count


def gen_unique_constraits(unique_cons):
    uniq = unique_cons
    uniq["count_composite"] = uniq.column.apply(composite_count_unique)
    return uniq


def save_rst(dic, cons_type, SOFTWARE):
    if cons_type == "u":
        unique_cons = gen_unique_constraits(dic)
        unique_cons = unique_cons.reset_index(
            drop=True
        )  # [["table", "column", "count_composite", 'constraint']]
        unique_cons.to_csv("data/" + SOFTWARE.lower() + "_unique.csv", index=False)

    elif cons_type == "f":
        dic.to_csv("data/" + SOFTWARE.lower() + "_fk.csv", index=False)

    elif cons_type == "n":
        dic.to_csv("data/" + SOFTWARE.lower() + "_null.csv", index=False)

    print(
        "Save DB constraints to file: data/%s_xxx.csv for cons type %s"
        % (SOFTWARE.lower(), cons_type)
    )


## MySQL
def mysql_parser_tmp(cons_type, rst):
    dic = []
    for item in rst:
        constraint = item[0]
        column = item[1]
        if cons_type == "u":
            if "_fk" not in constraint:  # constraint.endswith('_uniq'):
                dic.append(
                    {"table": item[2], "constraint": constraint, "column": column}
                )
        elif cons_type == "f":
            if "_fk" in constraint:  # constraint.endswith('_uniq'):
                dic.append(
                    {
                        "table": item[2],
                        "constraint": constraint,
                        "column": column,
                        "parent": item[3],
                        "prt_col": item[4],
                    }
                )
        elif cons_type == "n":
            dic.append({"table": item[0], "column": item[1]})

    dic = pd.DataFrame(dic)
    if cons_type == "u":
        dic = dic.groupby(["constraint", "table"], as_index=False).agg(
            {"column": ",".join}
        )

    return dic
