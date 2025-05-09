from contextlib import contextmanager
from typing import Generator, Iterator, Optional, Sequence, Tuple
from dagster._core.storage.db_io_manager import TableSlice
from google.cloud.bigquery import Client
import ibis
from ibis.backends.bigquery import Backend as BigQueryBackend
import pandas as pd


@contextmanager
def get_bigquery_connection(
    connection: Optional[Client | BigQueryBackend] = None,
    resource_config: Optional[dict[str, str]] = None,
) -> Iterator[Client]:
    if (connection is None) and (resource_config is not None):
        bigquery_con = Client(**resource_config)
    elif (connection is None) and (resource_config is None):
        raise ValueError("both connection and project_str cannot be None!")
    else:
        assert connection is not None
        if isinstance(connection, BigQueryBackend):
            bigquery_con: Client = connection.client
        else:
            bigquery_con = connection

    yield bigquery_con


def cleanup_table(
    table_slice: TableSlice,
    connection: Optional[Client | BigQueryBackend] = None,
    resource_config: Optional[dict[str, str]] = None,
):
    with get_bigquery_connection(connection, resource_config) as bigquery_con:
        drop_query = f"DROP TABLE IF EXISTS {table_slice.schema}.{table_slice.table}"
        print(drop_query)
        result = bigquery_con.query(drop_query).result()


def query_test_db(
    query: str,
    connection: Optional[Client | BigQueryBackend] = None,
    resource_config: Optional[dict[str, str]] = None,
) -> pd.DataFrame:
    with get_bigquery_connection(connection, resource_config) as bigquery_con:
        result = bigquery_con.query(query)
        return result.to_dataframe()


def get_table_slice_db(
    table_slice: TableSlice,
    connection: Optional[Client | BigQueryBackend] = None,
    resource_config: Optional[dict[str, str]] = None,
) -> pd.DataFrame:
    query = f"SELECT * FROM {table_slice.schema}.{table_slice.table} ORDER BY a"
    return query_test_db(query, connection, resource_config)
