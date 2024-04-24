import sys
from datetime import datetime
from typing import List, Optional, Tuple, Any

import httpx
import os
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from clickhouse_connect.driver.query import QueryResult
from dotenv import load_dotenv

load_dotenv()

class MissingEnvironmentVariable(Exception):
    pass


def get_my_env_var(var_name: str, default: Any = None) -> str:
    try:
        return os.environ[var_name]
    except KeyError as e:
        if default is None:
            raise MissingEnvironmentVariable(f'{var_name} does not exist') from e
        else:
            return str(default)


class ClickHouse:

    def __init__(self):
        self.client: Client = self.connect_db()
        self.nw_period: str = self.get_nw_period()
        self.fea_period: str = self.get_fea_period()

    @staticmethod
    def connect_db() -> Client:
        try:
            client: Client = get_client(host=get_my_env_var('HOST'), database=get_my_env_var('DATABASE'),
                                        username=get_my_env_var('USERNAME_DB'), password=get_my_env_var('PASSWORD'))
        except httpx.ConnectError as ex_connect:
            sys.exit(1)
        return client

    def get_nw_period(self) -> str:
        query: QueryResult = self.client.query('SELECT month,year from nw_statistic group by month,year')
        return self.get_information_to_table(query)

    def get_fea_period(self) -> str:
        query: QueryResult = self.client.query('SELECT month,year from fea_statistic group by month,year')
        return self.get_information_to_table(query)

    @staticmethod
    def get_information_to_table(query: QueryResult) -> str:
        result: List[Tuple[int]] = query.result_rows
        if not result:
            return []
        result = sorted(result, key=lambda x: (-x[1], -x[0]))[0]
        last_date = datetime(year=result[1], month=result[0], day=1).strftime('%Y-%m')
        return last_date
