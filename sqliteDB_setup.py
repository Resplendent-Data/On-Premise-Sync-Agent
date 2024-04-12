import sqlite3
from sqlite3 import Error
import time
import pandas as pd


def sql_escape(s):
    """escape single quotes and backslashes so you can put anything in a string"""
    return (
        s.replace("\\", "\\\\").replace("'", "''").replace("%", "%%").replace("\n", " ")
    )


def create_connection(db_file, sql_to_run=None, return_values=False):
    """create a database connection to a SQLite database"""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()

        # Gets all tables
        sql = """
            SELECT name FROM sqlite_master WHERE type='table';
            """
        c.execute(sql)
        available_tables = c.fetchall()

        # If there aren't any tables, make the appropriate tables.
        if len(available_tables) == 0:
            sql = """
                CREATE TABLE connection_info
                (connection_name, connection_uuid, connection_status, connection_error, last_update);
                
                CREATE TABLE sync_info
                (sync_time, last_update);

                CREATE TABLE agent_errors
                (error, status, last_update);

                CREATE TABLE agent_commands
                (command, last_update);

                CREATE TABLE table_sync_info
                (table_uuid, last_update, in_progress, heartbeat, checked_for_deleted_rows);

                INSERT INTO sync_info (sync_time, last_update)
                VALUES (0, CURRENT_TIMESTAMP);

                INSERT INTO agent_errors (error, status, last_update)
                VALUES ('authentication', 'Not Authenticated', CURRENT_TIMESTAMP);
                INSERT INTO agent_errors (error, status, last_update)
                VALUES ('agent_connection', 'Not Connected', CURRENT_TIMESTAMP);
                INSERT INTO agent_errors (error, status, last_update)
                VALUES ('agent_failure', 'Failed', CURRENT_TIMESTAMP);

                INSERT INTO agent_commands (command, last_update)
                VALUES ('continue', CURRENT_TIMESTAMP);
                """
            c.executescript(sql)
            conn.commit()

        if sql_to_run is not None:
            # Return selected values
            if return_values:
                df = pd.read_sql(sql_to_run, con=conn)
                return df

            # Otherwise, commit the inserted data
            else:
                c.executescript(sql_to_run)
                conn.commit()

    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


def generate_connection_info_insert(
    connection_uuid, connection_name, connection_status, connection_error
):
    """Used to insert connection info including name, uuid, and status"""

    sql = f"""
        update "connection_info"
        set "connection_name" = '{connection_name}', "connection_status" = '{connection_status}', "connection_error" = '{sql_escape(connection_error)}', "last_update" = CURRENT_TIMESTAMP
        where "connection_uuid" = '{connection_uuid}';
        insert into connection_info (connection_name, connection_uuid, connection_status, connection_error, last_update)
            select '{connection_name}', '{connection_uuid}', '{connection_status}', '{sql_escape(connection_error)}', CURRENT_TIMESTAMP
            where not exists (select 1 from "connection_info" where "connection_uuid" = '{connection_uuid}');
        """
    return sql


def generate_sync_info_insert(sync_time):
    # Inserts new sync times and deletes sync times that are 30 days old
    sql = f"""
        INSERT INTO "sync_info" (sync_time, last_update)
        VALUES ({sync_time}, CURRENT_TIMESTAMP);
        
        DELETE FROM "sync_info"
        WHERE last_update < DATE('now', '-1 days');
        """

    return sql


def generate_select(table):
    """Used to retrieve the connection info"""
    sql = f"""
        SELECT *
        FROM {table};
        """
    return sql


def update_auth_status(error, status):
    sql = f"""
        UPDATE agent_errors
        SET "status" = '{sql_escape(status)}', "last_update" = CURRENT_TIMESTAMP
        WHERE "error" = '{error}';
        """
    return sql


def update_command(command):
    sql = f"""
        UPDATE agent_commands
        SET "command" = '{command}', "last_update" = CURRENT_TIMESTAMP;
        """
    return sql


def set_table_sync_info(table_uuid) -> None:
    res = create_connection(
        "sync_info.db",
        f"""select * from table_sync_info where table_uuid='{table_uuid}'""",
        return_values=True,
    )
    if len(res) == 0:
        create_connection(
            "sync_info.db",
            f"""INSERT INTO table_sync_info (table_uuid, last_update) VALUES ('{table_uuid}', {time.time()});""",
        )
    else:
        create_connection(
            "sync_info.db",
            f"""update table_sync_info set last_update={time.time()} where table_uuid='{table_uuid}';""",
        )


def set_checked_for_deleted_rows(table_uuid):
    create_connection(
        "sync_info.db",
        f"""update table_sync_info set checked_for_deleted_rows={time.time()} where table_uuid='{table_uuid}';""",
    )


def get_table_sync_info(table_uuid) -> pd.DataFrame:
    return create_connection(
        "sync_info.db",
        f"""select * from table_sync_info where table_uuid='{table_uuid}'""",
        return_values=True,
    )


def reset_big_table_last_sync_time(table_uuid):
    create_connection(
        "sync_info.db",
        f"""update table_sync_info set last_update=0 where table_uuid='{table_uuid}';""",
    )


def big_table_worker_heartbeat(table_uuid):
    create_connection(
        "sync_info.db",
        f"""update table_sync_info set in_progress='true', heartbeat={time.time()} where table_uuid='{table_uuid}';""",
    )


def big_table_worker_finished(table_uuid):
    create_connection(
        "sync_info.db",
        f"""update table_sync_info set in_progress='false' where table_uuid='{table_uuid}';""",
    )


if __name__ == "__main__":
    pass
    # print(generate_connection_info_insert('deez', 'Ouch', 'Failing'))
    # create_connection('sync_info.db', generate_connection_info_insert('deez', 'Ouch', 'Failing'))
    # print(create_connection('sync_info.db', generate_connection_info_select(), return_values=True))
