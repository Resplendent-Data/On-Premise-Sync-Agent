from datetime import datetime
from urllib.parse import quote_plus as qp

import connectorx as cx
import pandas as pd
from pandas.api.types import is_numeric_dtype
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

has_row_updates = True


def format_creds(creds_row):
    source_creds = creds_row["creds"]
    creds = [
        f'{creds_row["engine_type"]}://{qp(source_creds["user"])}:',
        qp(creds_row["key"]),
        f'@{source_creds["ip"]}:{source_creds["port"]}/{qp(source_creds["db_name"])}',
    ]

    return {
        "source_name": creds_row["source_name"],
        "key": creds_row["key"],
        "conn": creds,
        "creds": creds,
        "creds_uri": None,
        "connected": False,
        "connection_type": creds_row["engine_type"],
        "tables": {},
    }


def refresh_conn(source):
    source["creds_uri"] = "".join(source["creds"])

    source["conn"] = create_engine(source["creds_uri"], poolclass=NullPool)
    conn = source["conn"].connect()
    conn.close()
    source["connected"] = True


def get_tables_and_views(source):
    table_sql = """
        SELECT tablename AS "TABLE_NAME"
        FROM pg_catalog.pg_tables
        WHERE schemaname != 'pg_catalog'
            AND schemaname != 'information_schema'
        ORDER BY "TABLE_NAME" ASC;
        """

    view_sql = """
            select table_name as "VIEW_NAME"
            from information_schema.views
            where table_schema not in ('information_schema', 'pg_catalog')
            order by "VIEW_NAME" ASC;
            """
    db_tables = pd.read_sql(table_sql, source["conn"])
    db_views = pd.read_sql(view_sql, source["conn"])
    table_names = db_tables["TABLE_NAME"].tolist()
    view_names = db_views["VIEW_NAME"].tolist()
    return table_names, view_names


def get_table_preview(source, table_name, number_of_rows):
    sql = f"""
        SELECT *
        FROM "{table_name}"
        LIMIT {number_of_rows};
    """
    df = pd.read_sql(sql, source["conn"])
    return df


def get_old_rows(table_object, message, source, batch_pull_size):
    """function for pulling in old rows"""
    ordering_key, relevant_columns, table_name = (
        table_object["last_update"],
        table_object["relevant_columns"],
        table_object["table_name"],
    )
    data_connection = source["conn"]
    sql = f"""
        SELECT "{'", "'.join(relevant_columns)}"
        FROM {table_name}
        {create_where_clause(table_object,source)}
        ORDER BY "{ordering_key}" DESC
        LIMIT {batch_pull_size} OFFSET {batch_pull_size*table_object['crawler_step']};
    """

    new_rows_df = pd.read_sql(sql, data_connection)

    if len(new_rows_df) < batch_pull_size:
        message["crawler_step_info"] = "completed"
        table_object["crawler_step_info"] = "completed"

    return new_rows_df


def get_updated_rows(table_object, source):
    """function for getting new rows from a table"""
    data_connection = source["conn"]
    ordering_key, relevant_columns, last_pulled_update, table_name = (
        table_object["last_update"],
        table_object["relevant_columns"],
        table_object["last_update_value"],
        table_object["table_name"],
    )
    where = create_where_clause(table_object, source, no_where=True)
    if where != "":
        where = f" and ({where})"
    if str(last_pulled_update).isdecimal():
        sql = f"""
            SELECT "{'", "'.join(relevant_columns)}"
            FROM {table_name}
            WHERE "{ordering_key}" > {last_pulled_update} {where}
            """
    else:
        sql = f"""
            SELECT "{'", "'.join(relevant_columns)}"
            FROM {table_name}
            WHERE "{ordering_key}" > '{last_pulled_update}' {where}
            """
    return pd.read_sql(sql, data_connection)


def get_primary_keys(table_object, source, number_of_rows=20000):
    """function for getting the primary keys of the most recent rows"""
    data_connection = source["conn"]
    primary_key, ordering_key, table_name = (
        table_object["primary_key"],
        table_object["last_update"],
        table_object["table_name"],
    )
    sql = f"""
        SELECT "{primary_key}"
        FROM {table_name}
        {create_where_clause(table_object,source)}
        ORDER BY "{ordering_key}" DESC
        LIMIT {number_of_rows};
    """
    return pd.read_sql(sql, data_connection)


def initial_pull(table_object, source, batch_pull_size):
    """function for doing initial pulls on tables"""

    sql = f"""
        SELECT "{'", "'.join(table_object['relevant_columns'])}"
        FROM "{table_object['table_name']}"
        {create_where_clause(table_object,source)}
        ORDER BY "{table_object['last_update']}" DESC
        LIMIT {batch_pull_size}
    """
    return cx.read_sql(source["creds_uri"], sql)


def create_where_clause(table_object, source, no_where=False):
    sql = ""
    first = True
    logical_operators = {"and": "and", "or": "or"}
    relational_operators = {"=": "=", "!=": "!=", "<": "<", ">": ">"}
    if "use_query_filter" in table_object and table_object["use_query_filter"]:
        df = get_table_preview(source, table_object["table_name"], 100)
        if isinstance(table_object["query_filter"], list):
            filters = table_object["query_filter"]
        else:
            filters = table_object["query_filter"]["items"]
        for filter_object in filters:
            try:
                subsql = f"""{'' if first else logical_operators[filter_object["logical_operator"]]} "{filter_object['column']}" {relational_operators[filter_object["relational_operator"]]}"""
                if is_numeric_dtype(df[filter_object["column"]]):
                    subsql += f""" {filter_object['value']} """
                else:
                    subsql += f""" E'{sql_escape(filter_object['value'])}' """
                if sql == "" and not no_where:
                    sql = "where "
                sql += subsql
                first = False
            except:
                pass
    return sql


def sql_escape(s):
    """escape single quotes and backslashes so you can put anything in a string"""
    return s.replace("\\", "\\\\").replace("'", "''")
