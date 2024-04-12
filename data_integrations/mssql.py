from datetime import datetime
from os import listdir
from os.path import isfile, join
from urllib.parse import quote_plus as qp

import pandas as pd
from pandas.api.types import is_numeric_dtype
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from functions import log, log_error

has_row_updates = True

odbc_driver_path = "/opt/microsoft/msodbcsql17/lib64/"


class NoMSSQLdriver(Exception):
    """Thrown when the MS SQL ODBC driver didn't get installed properly"""


def format_creds(creds_row):
    """Formats the credentials for the MS SQL connection"""
    try:
        source_creds = creds_row["creds"]
        driver_name = [
            f for f in listdir(odbc_driver_path) if isfile(join(odbc_driver_path, f))
        ][0]
        if "libmsodbcsql" in driver_name:
            driver_engine = "mssql+pyodbc"
            driver = f"{odbc_driver_path}{driver_name}"
        else:
            raise NoMSSQLdriver("MS SQL driver was not installed properly.")

        creds = [
            f'{driver_engine}://{qp(source_creds["user"])}:',
            qp(creds_row["key"]),
            f'@{source_creds["ip"]}:{source_creds["port"]}/{qp(source_creds["db_name"])}?driver={driver}',
        ]

        return {
            "source_name": creds_row["source_name"],
            "key": creds_row["key"],
            "creds_uri": None,
            "conn": creds,
            "creds": creds,
            "connected": False,
            "connection_type": creds_row["engine_type"],
            "tables": {},
        }
    except Exception as e:
        log_error(e, reraise=True)


def refresh_conn(source):
    source["creds_uri"] = "".join(source["creds"])
    connection_uri = source["creds"][2].split("?")
    driver_name = [
        f for f in listdir(odbc_driver_path) if isfile(join(odbc_driver_path, f))
    ][0]
    if "libmsodbcsql" in driver_name:
        driver = f"{odbc_driver_path}{driver_name}"

        connection_uri[1] = f"driver={driver}"
        connection_uri = "?".join(connection_uri)
        source["creds"][2] = connection_uri
    else:
        raise NoMSSQLdriver("MS SQL driver was not installed properly.")

    try:
        source["conn"].dispose()
    except:
        del source["conn"]

    creds_uri = "".join(source["creds"])
    try:
        source["conn"] = create_engine(
            creds_uri, connect_args={"timeout": 4}, poolclass=NullPool
        )
        conn = source["conn"].connect()
        conn.close()
        source["connected"] = True
    except Exception as e:
        log_error(e)
        source["connected"] = False
        source["error"] = str(e)


def get_tables_and_views(source):
    # Gets all table names in MS SQL database
    table_sql = """
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME ASC;
        """

    # Gets all view names in MS SQL database
    view_sql = """
            SELECT v.name AS VIEW_NAME
            FROM sys.views as v
            ORDER BY VIEW_NAME ASC;
            """
    db_tables = pd.read_sql(table_sql, source["conn"])
    db_views = pd.read_sql(view_sql, source["conn"])

    # Convert the discovered table and view names to lists
    table_names = db_tables["TABLE_NAME"].tolist()
    view_names = db_views["VIEW_NAME"].tolist()
    return table_names, view_names


def get_table_preview(source, table_name, number_of_rows):
    sql = f"""
        SELECT TOP {number_of_rows} *
        FROM {table_name}
        ORDER BY 1 DESC;
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
        SELECT {','.join(relevant_columns)}
        FROM {table_name}
        {create_where_clause(table_object,source)}
        ORDER BY "{ordering_key}" DESC
        OFFSET {batch_pull_size*table_object['crawler_step']} ROWS
        FETCH NEXT {batch_pull_size} ROWS ONLY;
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

    # If the last update value is a number, don't update its format
    if pd.api.types.is_numeric_dtype(type(last_pulled_update)):
        sql = f"""
            SELECT {','.join(relevant_columns)}
            FROM {table_name}
            WHERE {ordering_key} > {last_pulled_update} {where}
            """
    else:
        # First, try to convert the last_pulled_update to a datetime and then
        # back to a date string with a format of YYYY-MM-DD HH:MM:SS.SSS
        try:
            last_pulled_update_dt = pd.to_datetime(last_pulled_update)
            last_pulled_update = last_pulled_update_dt.strftime("%Y-%m-%d %H:%M:%S.%f")
            # We only want the first 3 digits of the milliseconds since MSSQL
            # only supports 3 digits of milliseconds
            last_pulled_update = last_pulled_update[:-3]
            sql = f"""
                SELECT {','.join(relevant_columns)}
                FROM {table_name}
                WHERE {ordering_key} > '{last_pulled_update}' {where}
                """
        except ValueError:
            # get all the rows that have  been updated since the last pull
            sql = f"""
                SELECT {','.join(relevant_columns)}
                FROM {table_name}
                WHERE {ordering_key} > '{last_pulled_update}' {where}
                """

    df = pd.read_sql(sql, data_connection)
    return df


def get_primary_keys(table_object, source, number_of_rows=20000):
    """function for getting the primary keys of the most recent rows"""
    data_connection = source["conn"]
    primary_key, ordering_key, table_name = (
        table_object["primary_key"],
        table_object["last_update"],
        table_object["table_name"],
    )
    sql = f"""
        SELECT TOP {number_of_rows} "{primary_key}"
        FROM {table_name}
        {create_where_clause(table_object,source)}
        ORDER BY {ordering_key} DESC;
    """
    return pd.read_sql(sql, data_connection)


def initial_pull(table_object, source, batch_pull_size):
    """function for doing initial pulls on tables"""
    ordering_key, relevant_columns, table_name = (
        table_object["last_update"],
        table_object["relevant_columns"],
        table_object["table_name"],
    )
    sql = f"""
        SELECT TOP {batch_pull_size} {','.join(relevant_columns)}
        FROM {table_name}
        {create_where_clause(table_object,source)}
        ORDER BY {ordering_key} DESC;
    """
    return pd.read_sql(sql, source["conn"])


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
                # get non null values for column
                values = df[pd.notna(df[filter_object["column"]])]
                sub_sql = f"""{'' if first else logical_operators[filter_object["logical_operator"]]} "{filter_object['column']}" {relational_operators[filter_object["relational_operator"]]}"""

                # Check if the column is a number
                if is_numeric_dtype(df[filter_object["column"]]):
                    sub_sql += f""" {filter_object['value']} """

                # Check if the column is a date
                elif len(values) > 0 and isinstance(
                    values.loc[0, filter_object["column"]], datetime
                ):
                    sub_sql += f""" CAST('{sql_escape(filter_object['value'])}' AS DATETIME2) """

                # Otherwise, treat it as a string
                else:
                    sub_sql += f""" '{sql_escape(filter_object['value'])}' """
                if sql == "" and not no_where:
                    sql = "where "
                sql += sub_sql
                first = False
            except Exception as e:
                log(
                    f'Error in creating where clause for table {table_object["table_name"]}',
                    e,
                )
    return sql


def sql_escape(s):
    """escape single quotes and backslashes so you can put anything in a string"""
    return s.replace("\\", "\\\\").replace("'", "''")
