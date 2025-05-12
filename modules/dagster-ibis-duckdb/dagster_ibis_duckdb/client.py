from contextlib import contextmanager, suppress
from typing import Iterator, Union

import dagster as dg
import ibis
from dagster._core.storage.db_io_manager import TableSlice
from dagster._utils.backoff import backoff
from dagster_duckdb.io_manager import _get_cleanup_statement, DuckDbClient
from duckdb import CatalogException
from ibis.backends.duckdb import Backend as DuckDbBackend


class IbisDuckDbClient(DuckDbClient):
    @staticmethod
    def execute_sql(context: dg.OutputContext, query: str, connection: DuckDbBackend):
        context.log.debug(f"Executing query:\n{query}")
        result = connection.raw_sql(query)
        return result

    @staticmethod
    def delete_table_slice(
        context: dg.OutputContext,
        table_slice: TableSlice,
        connection: DuckDbBackend,
    ) -> None:
        query = _get_cleanup_statement(table_slice)
        with suppress(CatalogException):
            IbisDuckDbClient.execute_sql(context, query, connection)

    @staticmethod
    def ensure_schema_exists(
        context: dg.OutputContext,
        table_slice: TableSlice,
        connection: DuckDbBackend,
    ) -> None:
        query = f"CREATE SCHEMA IF NOT EXISTS {table_slice.schema}"

        connection.create_database(table_slice.schema, force=True)

    @staticmethod
    def get_select_statement(table_slice: TableSlice) -> str:
        return DuckDbClient.get_select_statement(table_slice)

    @staticmethod
    @contextmanager
    def connect(  # pyright: ignore[reportIncompatibleMethodOverride]
        context: Union[dg.OutputContext, dg.InputContext],
        _,
    ) -> Iterator[DuckDbBackend]:
        resource_config = context.resource_config
        assert resource_config is not None

        try:
            conn: DuckDbBackend = backoff(  # type: ignore
                fn=ibis.connect,
                retry_on=(RuntimeError, ibis.IbisError),
                args=(resource_config["database"],),
                max_retries=10,
            )
        except ValueError:
            conn: DuckDbBackend = backoff(
                fn=ibis.duckdb.connect,
                retry_on=(RuntimeError, ibis.IbisError),
                args=(resource_config["database"],),
                max_retries=10,
            )

        yield conn

        conn.disconnect()
