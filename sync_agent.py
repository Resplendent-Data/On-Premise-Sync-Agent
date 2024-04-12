"""Main functions module for handling data syncing"""
import asyncio
import base64
import concurrent.futures
import json
import os
import random
import sys
import time
from multiprocessing import Manager, Process
from multiprocessing import Queue as MPQueue
from threading import Thread
from typing import Any, Dict, Optional

import pandas as pd
import psutil
import requests
from Crypto.Cipher import AES, DES3, Blowfish  # bandit: disable=B110
from pbkdf2 import PBKDF2
from websockets.client import WebSocketClientProtocol, connect

import sqliteDB_setup
from functions import TableAlreadyProcessingData, df_to_dict, log, log_error
from integration_mapping import integration_map


def manage_sync_agent():
    """stuff"""
    ping_queue = MPQueue()
    timeout = 120

    last_ping = time.time()
    p = Process(target=sync_agent_class, args=(ping_queue,))
    p.start()

    while True:
        try:
            if not ping_queue.empty():
                last_ping = time.time()
            if p.is_alive():
                if time.time() - last_ping < timeout:
                    time.sleep(1)
                else:
                    log("timed out")
                    kill(p)
                    p = Process(target=sync_agent_class, args=(ping_queue,))
                    p.start()
                    time.sleep(5)
                    last_ping = time.time()
            else:
                log("The p was dead")
                p = Process(target=sync_agent_class, args=(ping_queue,))
                p.start()
                time.sleep(5)

            # Command given from the config_server
            res = sqliteDB_setup.create_connection(
                "sync_info.db", sqliteDB_setup.generate_select("agent_commands"), True
            )
            if res is None:
                agent_command = "continue"
            else:
                agent_command = res.values[0][0]

            if agent_command == "restart":
                # change the command back to continue
                sqliteDB_setup.create_connection(
                    "sync_info.db", sqliteDB_setup.update_command("continue")
                )
                kill(p)
                p = Process(target=sync_agent_class, args=(ping_queue,))
                p.start()
                time.sleep(5)
        except Exception as e:
            ping_queue = MPQueue()
            last_ping = time.time()
            log("sync_agent manager error: ", e)
            time.sleep(5)


def kill(p: Process):
    try:
        pref = psutil.Process(p.pid)
        for child in pref.children(recursive=True):
            child.terminate()
        pref.terminate()

    except Exception as e:
        log("failed to terminate attempting kill: ", e)
        if p.is_alive():
            pref = psutil.Process(p.pid)
            for child in pref.children(recursive=True):
                child.kill()
            pref.kill()


class sync_agent_class:
    """stuff"""

    def __init__(self, ping_queue: MPQueue):
        self.ping_queue = ping_queue
        self.manager = Manager()

        # some constant variables
        try:
            # env.json can show whether the sync_agent is in dev mode or not.
            env_items = json.load(open("sync_agent_configs/env.json", encoding="utf-8"))
            if env_items["debug"]:
                if "url" in env_items.keys():
                    self.uri = env_items["url"]
                else:
                    self.uri = (
                        "wss://dev.resplendentdata.com:8001/slave-driver/websocket/"
                    )
                log(self.uri)
            else:
                self.uri = "wss://api.resplendentdata.com/slave-driver/websocket/"
            config = json.load(
                open("sync_agent_configs/sync_agent.json", encoding="utf-8")
            )

        except Exception as e:
            log("failed to load env.json: ", e)
            quit()

        # key and uuid for agent auth
        self.key = config["key"]
        self.uuid = config["uuid"]

        self.big_table_last_update_values = self.manager.dict()
        self.data_sources: Dict[str, Any] = {}
        self.time_to_sleep = 20
        self.token: str
        self.token_dict: Dict[str, Any]

        self.websocket: Optional[WebSocketClientProtocol] = None

        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        tasks = asyncio.wait(
            [
                self.loop.create_task(task)
                for task in [
                    self.start_websocket(),
                    self.listen(),
                    self.sync_loop(),
                ]
            ]
        )
        self.loop.run_until_complete(tasks)

    async def send(self, message_type, message_body=None):
        log("sending: ", message_type, f"message is {get_size_in_mb(message_body)} MB")
        message = {
            "token": self.token,
            "message_type": message_type,
            "message_body": message_body,
        }
        message = json.dumps(message)
        if self.websocket is not None:
            await self.websocket.send(message)

    async def heartbeat(self):
        """Send a heartbeat to the server"""
        try:
            await self.send("heartbeat", {"agent_uuid": self.uuid})
        except Exception as e:
            log("heartbeat error: ", e)

    async def start_websocket(self):
        while True:
            try:
                self.websocket = await connect(self.uri)
                self.websocket.close_timeout = 2

                sqliteDB_setup.create_connection(
                    "sync_info.db",
                    sqliteDB_setup.update_auth_status("agent_connection", "Connected"),
                )
                # authenticate the websocket by requesting a token
                auth_message = json.dumps({"agent_uuid": self.uuid, "key": self.key})

                await self.websocket.send(auth_message)

                if await self.websocket.wait_closed():
                    await self.websocket.close()

            except Exception as exx:
                log("websocket died for", self.uri, ": ", exx)
                sqliteDB_setup.create_connection(
                    "sync_info.db",
                    sqliteDB_setup.update_auth_status(
                        "agent_connection", "Not connected"
                    ),
                )
                await asyncio.sleep(2.5)

    async def listen(self):
        """start listening to the slave_driver websocket server"""
        while True:
            try:
                if self.websocket is not None and self.websocket.open:
                    message = json.loads(await self.websocket.recv())
                    message_type = message["message_type"]
                    message_body = message["message_body"]
                    asyncio.ensure_future(
                        self.message_handler(message_type, message_body), loop=self.loop
                    )

                else:
                    await asyncio.sleep(0.1)
            except Exception as exx:
                log("websocket error while listening: ", exx)
                await asyncio.sleep(1)

    async def message_handler(self, message_type, message_body):
        """handle messages from the slave_driver websocket server"""
        if message_type == "auth":
            if message_body is False:
                sqliteDB_setup.create_connection(
                    "sync_info.db",
                    sqliteDB_setup.update_auth_status(
                        "authentication", "Not Authenticated"
                    ),
                )
            else:
                sqliteDB_setup.create_connection(
                    "sync_info.db",
                    sqliteDB_setup.update_auth_status(
                        "authentication", "Authenticated"
                    ),
                )
                self.token = message_body
                b = message_body.split(".")[1]
                padding = "=" * (len(b) % 4)
                self.token_dict = json.loads(base64.b64decode(b + padding))

        elif message_type == "agent_info":
            try:
                for key, value in json.loads(message_body).items():
                    self.data_sources[key] = value
                # decrypt the database passwords
                tasks = [
                    asyncio.create_task(self.set_datasource_creds(source_uuid))
                    for source_uuid in self.data_sources
                ]
                if len(tasks) > 0:
                    await asyncio.wait(tasks)

                # run the format creds function for each source
                for source_uuid, value in self.data_sources.items():
                    new_source = format_creds(self.data_sources[source_uuid])

                    new_source["tables"] = self.data_sources[source_uuid]["tables"]
                    self.data_sources[source_uuid] = new_source

                # create connection pools to the databases
                tasks = [
                    asyncio.create_task(self.refresh_conn(source_uuid))
                    for source_uuid in self.data_sources
                ]
                if len(tasks) > 0:
                    await asyncio.wait(tasks)

                # run a sync
                asyncio.ensure_future(self.sync())

            except Exception as exx:
                log_error(exx)

        else:
            try:
                Response = None
                error_message = ""
                if message_type == "GET_TABLES_AND_VIEWS":
                    Response = await self.loop.run_in_executor(
                        None, self.get_tables_and_views, message_body["source_uuid"]
                    )

                elif message_type == "GET_TABLE_PREVIEW":
                    number_of_rows = 100
                    if "number_of_rows" in message_body:
                        number_of_rows = message_body["number_of_rows"]
                    table_preview = df_to_dict(
                        await self.loop.run_in_executor(
                            None,
                            self.get_table_preview,
                            message_body["source_uuid"],
                            message_body["table_name"],
                            number_of_rows,
                        )
                    )

                    source = self.data_sources[message_body["source_uuid"]]
                    # Some APIs have required fields that need to be specified on the frontend.
                    requirements = None
                    # If the source integration module has a get_table_requirements function,
                    # use it to get the requirements for the table
                    if hasattr(
                        integration_map[source["connection_type"]],
                        "get_table_requirements",
                    ):
                        requirements = await self.loop.run_in_executor(
                            None,
                            integration_map[
                                source["connection_type"]
                            ].get_table_requirements,
                            source,
                            message_body["table_name"],
                        )
                        requirements = requirements.model_dump(by_alias=True)

                    Response = {
                        "table_preview": table_preview,
                        "source_uuid": message_body["source_uuid"],
                        "table_name": message_body["table_name"],
                        "table_requirements": requirements,
                    }

                elif message_type == "GET_TABLE_COLUMNS":
                    one_row_df = df_to_dict(
                        await self.loop.run_in_executor(
                            None,
                            self.get_table_preview,
                            message_body["source_uuid"],
                            message_body["table_name"],
                            1,
                        )
                    )
                    Response = one_row_df["columns"]

                elif message_type == "UPDATE_TABLE_INFO":
                    table_object = message_body["table_info"]
                    table_object["table_name"] = message_body["table_name"]
                    table_object["sync_status"] = 1
                    table_object["crawler_step"] = 1
                    table_object["crawler_step_info"] = None
                    table_object["primary_key_value"] = None
                    table_object["last_update_value"] = None

                    self.data_sources[message_body["fk_source_uuid"]]["tables"][
                        message_body["pk_table_uuid"]
                    ] = table_object

                    self.data_sources[message_body["fk_source_uuid"]]["tables"][
                        message_body["pk_table_uuid"]
                    ]["dirty"] = True
                    sqliteDB_setup.reset_big_table_last_sync_time(
                        message_body["pk_table_uuid"]
                    )
                    Response = True
                elif message_type == "SAVE_DATA_SOURCE":
                    Response = {
                        "source_uuid": message_body["pk_source_uuid"],
                        "encrypted_password": None,
                        "status": False,
                    }

                    if "key" in message_body:
                        encrypted_key = message_body["key"]
                        source_key = message_body["source_key"]
                        source_uuid = message_body["pk_source_uuid"]
                        decrypted_key = decrypt_fast(
                            encrypted_key, source_key, source_uuid
                        )
                        message_body["key"] = decrypted_key

                    try:
                        # rebuild the source object that was updated
                        new_source = format_creds(message_body)
                    except Exception as exx:
                        log_error(exx)
                        raise ValueError("Invalid credentials.") from exx

                    if (
                        message_body["pk_source_uuid"] in self.data_sources
                        and "tables"
                        in self.data_sources[message_body["pk_source_uuid"]]
                    ):
                        new_source["tables"] = self.data_sources[
                            message_body["pk_source_uuid"]
                        ]["tables"]

                    self.data_sources[message_body["pk_source_uuid"]] = new_source

                    log("refreshing conn")
                    # initialize the new connection pool
                    await self.refresh_conn(message_body["pk_source_uuid"])
                    Response["error"] = (
                        self.data_sources[message_body["pk_source_uuid"]]["error"]
                        if "error" in self.data_sources[message_body["pk_source_uuid"]]
                        else "No error message."
                    )

                    log("setting status")
                    if (
                        self.data_sources[message_body["pk_source_uuid"]]["error"]
                        == "No error message."
                    ):
                        connection_sql = sqliteDB_setup.generate_connection_info_insert(
                            message_body["pk_source_uuid"],
                            message_body["source_name"],
                            True,
                            "Good to go!",
                        )
                    else:
                        connection_sql = sqliteDB_setup.generate_connection_info_insert(
                            message_body["pk_source_uuid"],
                            message_body["source_name"],
                            False,
                            self.data_sources[message_body["pk_source_uuid"]]["error"],
                        )

                    sqliteDB_setup.create_connection(
                        "sync_info.db", sql_to_run=connection_sql
                    )
                    # set response variable for the connection status of the pool
                    Response["status"] = self.data_sources[
                        message_body["pk_source_uuid"]
                    ]["connected"]

                elif message_type == "CHECK_SOURCE_STATUS":
                    await self.refresh_conn(message_body["source_uuid"])
                    Response = {
                        "status": self.data_sources[message_body["source_uuid"]][
                            "connected"
                        ],
                        "error": self.data_sources[message_body["source_uuid"]]["error"]
                        if "error" in self.data_sources[message_body["source_uuid"]]
                        else "No error message.",
                        "source_uuid": message_body["source_uuid"],
                    }
                elif message_type == "DELETE_SOURCE":
                    del self.data_sources[message_body["source_uuid"]]
                elif message_type == "DELETE_TABLE":
                    del self.data_sources[message_body["source_uuid"]]["tables"][
                        message_body["table_uuid"]
                    ]

                elif message_type == "GET_COLUMN_VALUES_FROM_AGENT":
                    df = await self.loop.run_in_executor(
                        None,
                        self.get_table_preview,
                        message_body["source_uuid"],
                        message_body["table_name"],
                        2000,
                    )
                    col_values = {}
                    for col in df.columns:
                        vals = pd.Series(df[col].unique()).head(500)
                        vals.where(pd.isna(vals), "NULL", inplace=True)
                        col_values[col] = vals.astype(str).tolist()
                    Response = col_values
                    log(col_values.keys())
                elif message_type == "CHECK_DATASET_ACCESS":
                    Response = {
                        "source_names": message_body["source_names"],
                        "access": {},
                    }
                    for source_uuid, tables in message_body["tables_by_source"].items():
                        try:
                            Response["access"][source_uuid] = {
                                "error": None,
                                "tables": {},
                            }
                            for table in tables:
                                try:
                                    table_preview = df_to_dict(
                                        await self.loop.run_in_executor(
                                            None,
                                            self.get_table_preview,
                                            source_uuid,
                                            table,
                                            1,
                                        )
                                    )
                                    Response["access"][source_uuid]["tables"][table] = {
                                        "error": None,
                                        "success": True,
                                    }
                                except Exception as e:
                                    Response["access"][source_uuid]["tables"][table] = {
                                        "error": str(e),
                                        "success": False,
                                    }
                        except Exception as e:
                            Response["access"][source_uuid] = {"error": str(e)}
            except Exception as e:
                log_error(e)
                error_message = str(e)
                Response = False

            if "request_id" in message_body:
                await self.send(
                    message_type,
                    {
                        "message": Response,
                        "error_message": error_message,
                        "request_id": message_body["request_id"],
                        "queue_name": message_body["queue_name"],
                    },
                )

    async def set_datasource_creds(self, source_uuid):
        """
        Decrypts the encrypted key and sets the decrypted key in the data_sources dict
        """
        encrypted_key = self.data_sources[source_uuid]["key"]
        source_key = self.data_sources[source_uuid]["source_key"]
        self.data_sources[source_uuid]["key"] = await async_decrypt(
            source_key, encrypted_key, source_uuid, self.loop
        )

    async def refresh_conn(self, source_uuid):
        await self.loop.run_in_executor(None, self.refresh_conn_sync, source_uuid)

    def refresh_conn_sync(self, source_uuid):
        try:
            if "connection_type" not in self.data_sources[source_uuid]:
                raise ValueError(self.data_sources[source_uuid]["error"])
            # reset the error message
            self.data_sources[source_uuid]["error"] = "No error message."
            # refresh conn
            integration_map[
                self.data_sources[source_uuid]["connection_type"]
            ].refresh_conn(self.data_sources[source_uuid])
        except Exception as e:
            self.data_sources[source_uuid]["error"] = str(e)
            self.data_sources[source_uuid]["connected"] = False
            log_error(e)
            log(f"failed to refresh connection for {source_uuid}")

    def get_table_preview(self, source_uuid, table_name, number_of_rows):
        """this gets rows from a table for a preview"""
        return integration_map[
            self.data_sources[source_uuid]["connection_type"]
        ].get_table_preview(self.data_sources[source_uuid], table_name, number_of_rows)

    def get_tables_and_views(self, source_uuid):
        try:
            table_names, view_names = integration_map[
                self.data_sources[source_uuid]["connection_type"]
            ].get_tables_and_views(self.data_sources[source_uuid])

            table_dict = {
                "source_uuid": source_uuid,
                "TableNames": table_names,
                "ViewNames": view_names,
            }
            return table_dict
        except Exception as e:
            log("failed to get tables and views: ", e)
            return False

    async def sync_loop(self):
        """main loop that triggers every minute"""
        # wait to start syncing until everything is loaded
        while True:
            then = time.time()
            try:
                if self.websocket is not None and self.websocket.open:
                    # sending the agent_info request will trigger the sync
                    await self.send("agent_info")
                    await self.heartbeat()
                    # make the loop happen every 60 seconds or if it took longer than 60 seconds just start it again
                    time_taken = time.time() - then

                    self.time_to_sleep = 60 - time_taken
                    while self.time_to_sleep > 0:
                        await asyncio.sleep(1)
                        self.time_to_sleep -= 1
                else:
                    log("waiting on websocket to be ready")
                    await asyncio.sleep(5)
            except Exception as e:
                log_error(e)
                time_taken = time.time() - then
                self.time_to_sleep = 60 - time_taken
                while self.time_to_sleep > 0:
                    await asyncio.sleep(1)
                    self.time_to_sleep -= 1

    async def sync(self):
        try:
            sqliteDB_setup.create_connection(
                "sync_info.db",
                sqliteDB_setup.update_auth_status("agent_failure", "Ready"),
            )
            then = time.time()
            tasks = []
            if not self.token_dict["paused"]:
                for source_uuid, data_source in self.data_sources.items():
                    try:
                        try:
                            if (
                                "connected" not in data_source
                                or not data_source["connected"]
                            ):
                                raise ConnectionError("Not connected")
                            if "source_name" in data_source.keys():
                                # Creates the SQL to send to the local database that says the sync succeeded.
                                connection_sql = (
                                    sqliteDB_setup.generate_connection_info_insert(
                                        source_uuid,
                                        data_source["source_name"],
                                        True,
                                        "Good to go!",
                                    )
                                )
                            else:
                                connection_sql = ""

                            for table_uuid, table_object in data_source[
                                "tables"
                            ].items():
                                # add table to tasks
                                tasks.append(
                                    asyncio.create_task(
                                        self.sync_table(
                                            table_object,
                                            data_source,
                                            data_source["connection_type"],
                                            source_uuid,
                                            table_uuid,
                                        )
                                    )
                                )
                        except ConnectionError:
                            # If not connected try to reconnect
                            await self.refresh_conn(source_uuid)
                            if data_source["connected"]:
                                connection_sql = (
                                    sqliteDB_setup.generate_connection_info_insert(
                                        source_uuid,
                                        data_source["source_name"],
                                        True,
                                        "Good to go!",
                                    )
                                )
                                for table_uuid, table_object in data_source[
                                    "tables"
                                ].items():
                                    # add table to tasks
                                    tasks.append(
                                        asyncio.create_task(
                                            self.sync_table(
                                                table_object,
                                                data_source,
                                                data_source["connection_type"],
                                                source_uuid,
                                                table_uuid,
                                            )
                                        )
                                    )
                            else:
                                # Creates the SQL to send to the local database that says the sync failed.
                                connection_sql = (
                                    sqliteDB_setup.generate_connection_info_insert(
                                        source_uuid,
                                        data_source["source_name"],
                                        False,
                                        f"""{data_source['error']}""",
                                    )
                                )
                    except Exception as e:
                        log(f"Wow. How could you break? {e}")
                        connection_sql = sqliteDB_setup.generate_connection_info_insert(
                            source_uuid,
                            data_source["source_name"],
                            False,
                            f"""{e}""",
                        )

                    # Only update the sqlite database if there's sql to run.
                    if len(connection_sql) > 0:
                        sqliteDB_setup.create_connection(
                            "sync_info.db", sql_to_run=connection_sql
                        )

                if len(tasks) > 0:
                    await asyncio.wait(tasks)
            else:
                log("customer paused, skipping sync")
            # ping the manager through the queue
            self.ping_queue.put("ping")

            # Stores the sync time in the database to be displayed on the config_server
            sqliteDB_setup.create_connection(
                "sync_info.db",
                sql_to_run=sqliteDB_setup.generate_sync_info_insert(time.time() - then),
            )
        except Exception as e:
            time.sleep(5)
            log(f"General Failure when doing a sync: {e}")
            sqliteDB_setup.create_connection(
                "sync_info.db",
                sqliteDB_setup.update_auth_status("agent_failure", str(e)),
            )

    async def sync_table(
        self,
        table_object,
        source,
        conn_type,
        source_uuid,
        table_uuid,
        run_datasets=True,
    ):
        try:
            # pull the data from the client db
            if "large_table" in table_object and table_object["large_table"]:
                res = sqliteDB_setup.get_table_sync_info(table_uuid)
                last_update = (
                    float(str(res.loc[0, "last_update"]))
                    if not res.empty and res.loc[0, "last_update"] is not None
                    else 0
                )
                heartbeat = (
                    float(str(res.loc[0, "heartbeat"]))
                    if not res.empty and res.loc[0, "heartbeat"] is not None
                    else 0
                )
                # check to make sure there isn't already a worker running on this table
                # and that the right amount of time has passed since the last one ran
                if res.empty or (
                    time.time() - last_update > 60 * 15
                    and (
                        str(res.loc[0, "in_progress"]) != "true"
                        or time.time() - heartbeat > 60
                    )
                ):
                    sqliteDB_setup.set_table_sync_info(table_uuid)
                    # if "dirty" in table_object and table_object["dirty"]:
                    #     self.data_sources[source_uuid]["tables"][table_uuid]["dirty"] = False
                    #     self.data_sources[source_uuid]["tables"][table_uuid]["sync_status"] = 1
                    # else:
                    #     if table_uuid in self.big_table_last_update_values:
                    #         self.data_sources[source_uuid]["tables"][table_uuid]["sync_status"] = 3
                    #         self.data_sources[source_uuid]["tables"][table_uuid][
                    #             "last_update_value"
                    #         ] = self.big_table_last_update_values[table_uuid]
                    # spawn a worker
                    p = Process(
                        target=start_big_table_sync,
                        args=(
                            table_object,
                            source,
                            conn_type,
                            source_uuid,
                            table_uuid,
                            self.token,
                            self.big_table_last_update_values,
                        ),
                    )
                    p.start()
            else:
                try:
                    message = await self.loop.run_in_executor(
                        None,
                        batch_pull,
                        self.uuid,
                        table_object,
                        table_uuid,
                        source,
                        conn_type,
                        run_datasets,
                    )
                    sqliteDB_setup.set_table_sync_info(table_uuid)
                    asyncio.ensure_future(
                        self.send("data_update", message), loop=self.loop
                    )

                except TableAlreadyProcessingData as e:
                    log(f"Table already processing data: {e}")

        except Exception as e:
            log_error(e)


async def async_decrypt(dbkey, password, source_uuid, loop: asyncio.AbstractEventLoop):
    """Calls the decrypt function in a thread"""
    if password is None:
        return None

    with concurrent.futures.ProcessPoolExecutor() as pool:
        password = await loop.run_in_executor(
            pool, decrypt_fast, password, dbkey, source_uuid
        )
        return password


def start_big_table_sync(
    table_object,
    source,
    conn_type,
    source_uuid,
    table_uuid,
    token,
    big_table_last_update_values,
):
    t = Thread(
        target=big_table_sync,
        args=(
            table_object,
            source,
            conn_type,
            table_uuid,
            token,
            big_table_last_update_values,
        ),
    )
    t.start()
    while t.is_alive():
        sqliteDB_setup.big_table_worker_heartbeat(table_uuid)
        time.sleep(10)
    sqliteDB_setup.big_table_worker_finished(table_uuid)


def big_table_sync(
    table_object,
    source,
    conn_type,
    table_uuid,
    token,
    big_table_last_update_values,
):
    env_items = json.load(open("sync_agent_configs/env.json", encoding="utf-8"))
    if env_items["debug"]:
        url = "http://slave-driver:8001/slave-driver/data-ingest/"
    else:
        url = "https://api.resplendentdata.com/slave-driver/data-ingest/"
    log("doing big sync: ", table_object["sync_status"])
    if str(table_object["sync_status"]) == "1":
        min_last_update = None
        number_of_rows = 500000
        for page in range(int(table_object["large_table_row_limit"] / number_of_rows)):
            log("doing page: ", page)
            table_object["crawler_step"] = page
            times = {}
            then = time.time()
            df = integration_map[conn_type].get_old_rows(
                table_object, {}, source, number_of_rows
            )
            times["querying"] = time.time() - then

            not_null_last_update = df[pd.notnull(df[table_object["last_update"]])][
                table_object["last_update"]
            ]
            rows_pulled = len(df)
            if page == 0:
                last_pulled_update = not_null_last_update.max()
                big_table_last_update_values[table_uuid] = last_pulled_update
                df[0:0].to_feather(f"{table_uuid}.feather")
                # send the columns and dtypes in a .feather file
                with open(f"{table_uuid}.feather", "rb") as f:
                    requests.post(
                        url,
                        f.read(),
                        headers={
                            "Auth": token,
                            "Table-Uuid": table_uuid,
                            "Message-Type": "table_metadata",
                        },
                        timeout=60,
                    )
                os.remove(f"{table_uuid}.feather")
            else:
                # get rid of duplicate values
                if min_last_update is not None:
                    df = df[df[table_object["last_update"]] < min_last_update]

            min_last_update = not_null_last_update.min()

            # # batches of 100,000 rows
            # for i in range(int(number_of_rows/100000)):
            then = time.time()

            df.to_csv(f"{table_uuid}.csv", header=False, na_rep="\\N", index=False)

            # Print the file size of the csv
            file_size = os.path.getsize(f"{table_uuid}.csv")
            log("file size: ", file_size)

            times["to_csv"] = time.time() - then
            del df

            # send the full csv
            then = time.time()
            with open(f"{table_uuid}.csv", "rb") as f:
                requests.post(
                    url,
                    f.read(),
                    headers={
                        "Auth": token,
                        "Table-Uuid": table_uuid,
                        "Message-Type": "initial_table_data",
                    },
                    timeout=60,
                )
            times["sending"] = time.time() - then
            os.remove(f"{table_uuid}.csv")

            log(json.dumps(times, indent=4))
            if rows_pulled < number_of_rows:
                log(f"only {rows_pulled} rows pulled stopping data import")
                break

        sqliteDB_setup.set_checked_for_deleted_rows(table_uuid)

    elif str(table_object["sync_status"]) == "3":
        df = integration_map[conn_type].get_updated_rows(table_object, source)
        log(f"got {len(df)} new rows")
        not_null_df = df[pd.notnull(df[table_object["last_update"]])]
        if len(not_null_df) > 0:
            big_table_last_update_values[table_uuid] = not_null_df[
                table_object["last_update"]
            ].max()
        df.to_csv(f"{table_uuid}.csv", header=False, na_rep="\\N", index=False)
        with open(f"{table_uuid}.csv", "rb") as f:
            requests.post(
                url,
                f.read(),
                headers={
                    "Auth": token,
                    "Table-Uuid": table_uuid,
                    "Message-Type": "update_table_data",
                    "Primary-Key": table_object["primary_key"],
                    "Columns": json.dumps(table_object["relevant_columns"]),
                },
                timeout=60,
            )
        os.remove(f"{table_uuid}.csv")
        last_del_check = sqliteDB_setup.get_table_sync_info(table_uuid).loc[
            0, "checked_for_deleted_rows"
        ]

        if (
            last_del_check == ""
            or last_del_check is None
            or time.time() - float(str(last_del_check)) > 60 * 60
        ):
            df = integration_map[conn_type].get_primary_keys(
                table_object, source, number_of_rows=5000000
            )
            df.to_feather(f"{table_uuid}.feather")
            with open(f"{table_uuid}.feather", "rb") as f:
                requests.post(
                    url,
                    f.read(),
                    headers={
                        "Auth": token,
                        "Table-Uuid": table_uuid,
                        "Message-Type": "check_for_deleted_rows",
                        "Primary-Key": table_object["primary_key"],
                        "Ordering-Key": table_object["last_update"],
                    },
                    timeout=60,
                )
            os.remove(f"{table_uuid}.feather")
            sqliteDB_setup.set_checked_for_deleted_rows(table_uuid)

    log("finished big pull")


def batch_pull(
    agent_uuid: str,
    table_object: dict,
    table_uuid: str,
    source,
    client_db_type: str,
    run_datasets: bool,
) -> Dict[str, Any]:
    """function for pulling data from customer db's and sending it to the resplendent servers"""
    try:
        # Check if the table is already processing data and if it's been more than 15 minutes since the last update
        it_has_been_15_minutes_since_last_sync = (
            time.time() - table_object["last_sync"] > 60 * 15
        )

        if table_object["processing_data"]:
            # If it hasn't been 15 minutes since the last sync, then we raise an error
            if not it_has_been_15_minutes_since_last_sync:
                raise TableAlreadyProcessingData()

        # if api doesn't have row updates keep sync status at 1 no matter what
        if not integration_map[client_db_type].has_row_updates:
            table_object["sync_status"] = 1

        # set local variables from the dataset_group dictionary
        primary_key = table_object["primary_key"]
        ordering_key = table_object["last_update"]
        last_pulled_update = table_object["last_update_value"]
        last_pulled_pk = (
            table_object["last_update_pk"] if "last_update_pk" in table_object else None
        )

        sync_info = sqliteDB_setup.create_connection(
            "sync_info.db",
            sqliteDB_setup.generate_select("sync_info"),
            return_values=True,
        )
        if sync_info is None:
            raise Exception("sync_info.db not found")
        # Get latest time from the database
        last_sync = sync_info.iloc[[-1]]["last_update"].values[0]
        if "batch_pull_size" in table_object:
            batch_pull_size = table_object["batch_pull_size"]
        else:
            batch_pull_size = 10000
        # create message dictionary
        message = {
            "sync_status": table_object["sync_status"],
            "agent_uuid": agent_uuid,
            "table_uuid": table_uuid,
            "primary_key": primary_key,
            "crawler_step": table_object["crawler_step"],
            "crawler_step_info": table_object["crawler_step_info"],
            "new_rows": {},
            "updated_rows": {},
            "deleted_rows_check": {},
            "check_for_deleted_rows_counter": table_object[
                "check_for_deleted_rows_counter"
            ]
            if "check_for_deleted_rows_counter" in table_object
            else 0,
            "last_sync": last_sync,
            "run_datasets": run_datasets,
        }

        if table_object["sync_status"] == "3" or table_object["sync_status"] == 3:
            new_rows_df = pd.DataFrame()
            updated_rows = pd.DataFrame()

            # code for pulling in old rows after the initial pull
            if (
                "import_old_rows" in table_object
                and table_object["import_old_rows"]
                and table_object["crawler_step_info"] != "completed"
            ):
                new_rows_df = integration_map[client_db_type].get_old_rows(
                    table_object, message, source, batch_pull_size
                )

                message["new_rows"] = df_to_dict(new_rows_df, table_object)

            # code for pulling in new rows after the initial pull
            if ordering_key is not None and last_pulled_update is not None:
                updated_rows = integration_map[client_db_type].get_updated_rows(
                    table_object, source
                )
                if not updated_rows.empty:
                    if (
                        primary_key in updated_rows.columns
                        and last_pulled_pk is not None
                    ):
                        row_with_last_pulled_pk = updated_rows[
                            updated_rows[primary_key] == last_pulled_pk
                        ]
                        if not row_with_last_pulled_pk.empty:
                            last_pulled_update_iso = last_pulled_update.replace(
                                " ", "T"
                            )
                            # Get the last update value of the row with the last pulled pk
                            last_update_value = row_with_last_pulled_pk.iloc[0][
                                ordering_key
                            ]
                            # The last update value will be a datetime object
                            if "time" in str(type(last_update_value)).lower():
                                last_update_value = last_update_value.isoformat()
                            if last_pulled_update_iso == last_update_value:
                                # Filter out any rows that are less than or equal to the last pulled update
                                updated_rows = updated_rows[
                                    updated_rows[ordering_key] > last_update_value
                                ]

                    # set the message variable for updated rows
                    message["updated_rows"] = df_to_dict(updated_rows, table_object)

            # code for checking for deleted rows
            if table_object["check_for_deleted_rows_counter"] >= 10 and (
                table_object["crawler_step_info"] == "completed"
                or table_object["crawler_step_info"] is None
            ):
                pk_df = integration_map[client_db_type].get_primary_keys(
                    table_object, source
                )
                message["deleted_rows_check"] = df_to_dict(pk_df)
                message["check_for_deleted_rows_counter"] = 0

            else:
                message["check_for_deleted_rows_counter"] += 1

        # code for initial pull
        elif table_object["sync_status"] == "1" or table_object["sync_status"] == 1:
            log("doing a full pull!?", table_uuid)

            new_rows_df = integration_map[client_db_type].initial_pull(
                table_object, source, batch_pull_size
            )

            # set the message variable
            message["new_rows"] = df_to_dict(new_rows_df, table_object)

        if "force_dtypes" in table_object:
            if message["new_rows"]:
                for i, col in enumerate(message["new_rows"]["columns"]):
                    if (
                        col in table_object["force_dtypes"]
                        and table_object["force_dtypes"][col] != "none"
                    ):
                        message["new_rows"]["dtypes"][i] = table_object["force_dtypes"][
                            col
                        ]
            if message["updated_rows"]:
                for i, col in enumerate(message["updated_rows"]["columns"]):
                    if (
                        col in table_object["force_dtypes"]
                        and table_object["force_dtypes"][col] != "none"
                    ):
                        message["updated_rows"]["dtypes"][i] = table_object[
                            "force_dtypes"
                        ][col]
            if message["deleted_rows_check"]:
                for i, col in enumerate(message["deleted_rows_check"]["columns"]):
                    if (
                        col in table_object["force_dtypes"]
                        and table_object["force_dtypes"][col] != "none"
                    ):
                        message["deleted_rows_check"]["dtypes"][i] = table_object[
                            "force_dtypes"
                        ][col]

        return message

    except TableAlreadyProcessingData as e:
        raise e

    except Exception as e:
        log("died in batch pull!: ", client_db_type, " ", table_uuid)
        log_error(e)
        raise e


def df_from_dict(df_dict):
    """convert dictionary from df_to_dict back to dataframe"""
    df = pd.DataFrame(data=json.loads(df_dict["values"]), columns=df_dict["columns"])
    for col_index, column in enumerate(df):
        if df_dict["dtypes"][col_index] == "timedelta64[ns]":
            df[column] = pd.to_timedelta(df[column])
        else:
            df[column] = df[column].astype(df_dict["dtypes"][col_index])
    return df


def format_creds(creds_row):
    return integration_map[creds_row["engine_type"]].format_creds(creds_row)


def decrypt_fast(s, key, salt, iters=2000) -> str:
    try:
        # generate keys
        key1 = PBKDF2(key, salt).read(32)
        key2 = PBKDF2(key1[:16], key1[16:32]).read(32)
        key3 = key1[:16] + key2[16:32]
        key4 = b""
        for index in range(0, 32, 2):
            if index % 4 == 0:
                key4 += key1[index : index + 2]
            else:
                key4 += key2[index : index + 2]

        # seed random
        random.seed(key1)
        if random.randint(0, 1):
            random.seed(key1 + key2 + key4)
        else:
            random.seed(key4 + key2 + key1)

        # set cyphers
        cyphers = [AES.new, DES3.new, Blowfish.new]

        key_list: list[int] = []
        cypher_list: list[int] = []
        for _ in range(iters):
            key_list.append(random.randint(0, 3))
            cypher_list.append(random.randint(0, 2))
        key_list.reverse()
        cypher_list.reverse()

        if isinstance(s, str):
            s = s.encode("utf-8")
        b = base64.b64decode(s)
        if isinstance(b, str):
            b = b.encode("utf-8")

        for index in range(iters):
            if key_list[index] == 0:
                current_key = key1
            elif key_list[index] == 1:
                current_key = key2
            elif key_list[index] == 2:
                current_key = key3
            else:
                current_key = key4

            if cypher_list[index] == 1:
                current_key = current_key[:24]

            b = cyphers[cypher_list[index]](current_key, AES.MODE_ECB).decrypt(b)

        b = base64.b64decode(b)

        return b.decode("utf-8")
    except Exception as e:
        log_error(e)
        raise e


def get_size_in_mb(my_string):
    bytes_size = sys.getsizeof(my_string)
    mb_size = bytes_size / 1048576  # Convert bytes to MB
    return mb_size


if __name__ == "__main__":
    manage_sync_agent()
