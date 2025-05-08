from contextlib import contextmanager
from typing import Generator, Iterator, Optional, Sequence, Tuple
from dagster._core.storage.db_io_manager import TableSlice
from duckdb import DuckDBPyConnection, DuckDBPyRelation
import duckdb
import ibis
import pandas as pd


@contextmanager
def get_duckdb_connection(
    connection: Optional[DuckDBPyConnection | ibis.BaseBackend] = None,
    connection_str: Optional[str] = None,
) -> Iterator[DuckDBPyConnection]:
    if (connection is None) and (connection_str is not None):
        duckdb_con = duckdb.connect(connection_str)
        close_connection = True
    elif (connection is None) and (connection_str is None):
        raise ValueError("both connection and connection_str cannot be None!")
    else:
        assert connection is not None
        close_connection = False

        if isinstance(connection, ibis.BaseBackend):
            duckdb_con: DuckDBPyConnection = connection.con
        else:
            duckdb_con = connection

    yield duckdb_con

    if close_connection:
        duckdb_con.close()


def cleanup_table(
    table_slice: TableSlice,
    connection: Optional[DuckDBPyConnection | ibis.BaseBackend] = None,
    connection_str: Optional[str] = None,
):
    with get_duckdb_connection(connection, connection_str) as duckdb_con:
        drop_query = f"DROP TABLE IF EXISTS {table_slice.schema}.{table_slice.table}"
        print(drop_query)
        duckdb_con.sql(drop_query)


def query_test_db(
    query: str,
    connection: Optional[DuckDBPyConnection | ibis.BaseBackend] = None,
    connection_str: Optional[str] = None,
) -> pd.DataFrame:
    with get_duckdb_connection(connection, connection_str) as duckdb_con:
        return duckdb_con.sql(query).df()


def get_table_slice_db(
    table_slice: TableSlice,
    connection: Optional[DuckDBPyConnection | ibis.BaseBackend] = None,
    connection_str: Optional[str] = None,
) -> pd.DataFrame:
    query = f"SELECT * FROM {table_slice.schema}.{table_slice.table}"
    return query_test_db(query, connection, connection_str)
