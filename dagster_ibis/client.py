from contextlib import contextmanager, suppress
from typing import Iterator, Union
from dagster._core.storage.db_io_manager import DbClient, TableSlice
from dagster._utils.backoff import backoff
from dagster_duckdb.io_manager import _get_cleanup_statement, DuckDbClient
import dagster as dg
from duckdb import CatalogException
import ibis


class IbisClient(DbClient[ibis.BaseBackend]):
    @staticmethod
    def execute_sql(
        context: dg.OutputContext,
        query: str,
        connection: ibis.BaseBackend,
    ):
        try:
            context.log.debug(f"Executing query:\n{query}")
            result = connection.raw_sql(query)  # type: ignore
            return result
        except AttributeError:
            raise NotImplementedError(
                f"Connection of type ({type(connection)}) has no ability to execute sql"
            )

    @staticmethod
    def delete_table_slice(
        context: dg.OutputContext,
        table_slice: TableSlice,
        connection: ibis.BaseBackend,
    ) -> None:
        query = _get_cleanup_statement(table_slice)
        with suppress(CatalogException):
            IbisClient.execute_sql(context, query, connection)

    @staticmethod
    def ensure_schema_exists(
        context: dg.OutputContext,
        table_slice: TableSlice,
        connection: ibis.BaseBackend,
    ) -> None:
        query = f"create schema if not exists {table_slice.schema}"
        IbisClient.execute_sql(context, query, connection)

    @staticmethod
    def get_select_statement(table_slice: TableSlice) -> str:
        return DuckDbClient.get_select_statement(table_slice)

    @staticmethod
    @contextmanager
    def connect(
        context: Union[dg.OutputContext, dg.InputContext],
        table_slice: TableSlice,
    ) -> Iterator[ibis.BaseBackend]:
        resource_config = context.resource_config
        assert resource_config is not None

        conn = backoff(
            fn=ibis.connect,
            retry_on=(RuntimeError, ibis.IbisError),
            args=(resource_config["database"],),
            max_retries=10,
        )

        yield conn

        conn.disconnect()
