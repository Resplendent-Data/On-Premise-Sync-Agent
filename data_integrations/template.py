"""
The abstract base class for all data integrations.
"""
from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from data_integrations.models.general import TableRequirements


class IntegrationSyncFunctions(ABC):
    """
    The abstract base class for all data integrations.
    """

    def __init__(self):
        # setting has_row_updates to false makes the that api
        # only use initial pull and keeps sync_status as 1
        self.has_row_updates: bool

    # This is the template for adding new api's/databases to the sync agent
    # all the below functions are the standard functions to include in a new file
    @abstractmethod
    def format_creds(self, creds_row: dict[str, Any]) -> dict[str, Any]:
        """
        This function makes the source dictionary from the row from the database/slave_driver.
        All the keys in the dictionary shown here are required for the function to work.
        The connection variable can be whatever you need it to be and is what gets
        passed to the other functions as the "data_connection" argument.
        It's fine to add more keys if you want to and you can
        access them in any of the functions that get passed the "source" argument
        """
        # source_creds = creds_row['creds']
        # key = creds_row['key']

        # return {
        #     'source_name': creds_row['source_name'],
        #     'key': creds_row['key'],
        #     'conn': 'this gets passed to data functions as data_connection',
        #     'connected': False,
        #     'connection_type': creds_row['engine_type'],
        #     'tables': {}
        # }

    @abstractmethod
    def refresh_conn(self, source: dict[str, Any]) -> None:
        """
        This function makes sure a connection can be made to
        the data and throws an exception if it can't connect.
        If it can connect it sets the "connected" key to True
        """
        # source['connected'] = True

    @abstractmethod
    def get_tables_and_views(
        self, source: dict[str, Any]
    ) -> tuple[list[str], list[str]]:
        """This function returns available tables/view for a data source"""
        # table_names: 'list[str]' = list()
        # view_names: 'list[str]' = list()

        # return table_names, view_names

    @abstractmethod
    def get_table_preview(
        self, source: dict[str, Any], table_name: str, number_of_rows: int
    ) -> pd.DataFrame:
        """
        This function returns a preview of a table and is
        for getting a preview as well as getting the columns from a table
        """
        # df = pd.DataFrame()
        # return df

    @abstractmethod
    def get_table_requirements(
        self,
        source: dict[str, Any],
        table_name: str,
    ) -> TableRequirements:
        """
        This function returns the requirements for a table.
        This is used to pre-build column links and prefill the
        last updated and primary key columns
        """
        # return TableRequirements(
        #     primary_key=primary_key,
        #     last_update=ordering_column,
        #     prebuilt_column_links=column_url_objects,
        #     required_relevant_columns=[],
        # )

    @abstractmethod
    def get_old_rows(
        self,
        table_object: dict[str, Any],
        message: dict[str, Any],
        source: dict[str, Any],
        batch_pull_size: int,
    ) -> pd.DataFrame:
        """This function is for crawling through older database rows"""
        # ordering_key, relevant_columns, table_name = table_object['last_update'],table_object['relevant_columns'],table_object['table_name']
        # #example of sql that would be used
        # sql = f"""
        #     SELECT {','.join(relevant_columns)}
        #     FROM {table_name}
        #     ORDER BY `{ordering_key}` DESC
        #     LIMIT {batch_pull_size} OFFSET {batch_pull_size*table_object['crawler_step']};
        # """

        # new_rows_df = pd.read_sql(sql, source['conn'])

        # # update the crawler step on the sync_agent memory and in the message to be sent to the slave_driver
        # if len(new_rows_df) < batch_pull_size:
        #     message['crawler_step_info'] = 'completed'
        #     table_object['crawler_step_info'] = 'completed'
        # return new_rows_df

    @abstractmethod
    def get_updated_rows(
        self, table_object: dict[str, Any], source: dict[str, Any]
    ) -> pd.DataFrame:
        """This function returns new rows from a table based on the ordering column"""
        # ordering_key, relevant_columns, last_pulled_update, table_name = table_object['last_update'], table_object['relevant_columns'],table_object['last_update_value'], table_object['table_name']
        # sql example
        # also try to support any type of ordering key ex. number, datetime, string
        # try:
        #     float(last_pulled_update)
        #     sql = f"""
        #         SELECT {','.join(relevant_columns)}
        #         FROM {table_name}
        #         WHERE {ordering_key} > {last_pulled_update}
        #         """
        # except:
        #     sql = f"""
        #         SELECT {','.join(relevant_columns)}
        #         FROM {table_name}
        #         WHERE {ordering_key} > '{last_pulled_update}'
        #         """
        # return pd.read_sql(sql, source['conn'])

    @abstractmethod
    def get_primary_keys(
        self,
        table_object: dict[str, Any],
        source: dict[str, Any],
        number_of_rows: int = 100,
    ) -> pd.DataFrame:
        """This function returns the primary keys of the most recent rows for checking for deleted rows"""
        # primary_key, ordering_key, table_name = table_object['primary_key'], table_object['last_update'], table_object['table_name']
        # # sql example
        # sql = f"""
        #     SELECT `{primary_key}`, `{ordering_key}`
        #     FROM {table_name}
        #     ORDER BY `{ordering_key}` DESC
        #     LIMIT {number_of_rows};
        # """
        # return pd.read_sql(sql, source['conn'])

    @abstractmethod
    def initial_pull(
        self, table_object: dict[str, Any], source: dict[str, Any], batch_pull_size: int
    ) -> pd.DataFrame:
        """This function returns the top 10000 rows on a table for the initial data pull"""
        # sql example
        # sql = f"""
        #     SELECT {','.join(table_object['relevant_columns'])}
        #     FROM {table_object['table_name']}
        #     ORDER BY `{table_object['last_update']}` DESC
        #     LIMIT {batch_pull_size};
        # """
        # return pd.read_sql(sql, source['conn'])
