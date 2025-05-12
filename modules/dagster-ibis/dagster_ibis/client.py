from contextlib import contextmanager, suppress
import re
from typing import Iterator, Sequence, Union, cast
from dagster._core.storage.db_io_manager import (
    DbClient,
    TablePartitionDimension,
    TableSlice,
)
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._utils.backoff import backoff
from dagster_duckdb.io_manager import _get_cleanup_statement, DuckDbClient
import dagster as dg
from duckdb import CatalogException
import ibis
from ibis import _
from ibis.backends.sql import SQLBackend


class IbisClient(DbClient[SQLBackend]):
    @staticmethod
    def execute_sql(
        context: dg.OutputContext,
        query: str,
        connection: SQLBackend,
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
    def _partition_where_clause(
        table_slice: TableSlice,
        connection: SQLBackend,
    ) -> ibis.Table:
        table = connection.table(table_slice.table, database=table_slice.schema)
        if table_slice.partition_dimensions is None:
            return table

        partition_dims = table_slice.partition_dimensions
        for partition_dim in partition_dims:
            if isinstance(partition_dim.partitions, TimeWindow):
                table = IbisClient._time_window_where_clause(partition_dim, table)
            else:
                table = IbisClient._static_where_clause(partition_dim, table)

        return table

    @staticmethod
    def _time_window_where_clause(
        partition_dim: TablePartitionDimension,
        table: ibis.Table,
    ) -> ibis.Table:
        partition = cast(TimeWindow, partition_dim.partitions)
        partition_expr = partition_dim.partition_expr
        start_dt, end_dt = partition
        return table.filter(table[partition_expr].between(start_dt, end_dt))

    @staticmethod
    def _static_where_clause(
        partition_dim: TablePartitionDimension,
        table: ibis.Table,
    ) -> ibis.Table:
        partition_expr = partition_dim.partition_expr
        partitions = cast(Sequence[str], partition_dim.partitions)
        return table.filter(table[partition_expr].isin(partitions))

    @staticmethod
    def delete_table_slice(
        context: dg.OutputContext,
        table_slice: TableSlice,
        connection: SQLBackend,
    ) -> None:
        expression = IbisClient._partition_where_clause(table_slice, connection)
        query_select = expression.compile()
        query_delete = re.sub(r"SELECT.*FROM", "DELETE FROM", query_select)
        with suppress(CatalogException):
            IbisClient.execute_sql(context, query_delete, connection)

    @staticmethod
    def ensure_schema_exists(
        context: dg.OutputContext,
        table_slice: TableSlice,
        connection: SQLBackend,
    ) -> None:
        query = f"CREATE SCHEMA IF NOT EXISTS {table_slice.schema}"
        IbisClient.execute_sql(context, query, connection)

    @staticmethod
    def get_select_statement(table_slice: TableSlice, connection) -> str:
        table = ibis.table(name=table_slice.table, database=table_slice.schema)
        IbisClient._partition_where_clause()
        expression = IbisClient._partition_where_clause(table_slice)

    @staticmethod
    @contextmanager
    def connect(
        context: Union[dg.OutputContext, dg.InputContext],
        table_slice: TableSlice,
    ) -> Iterator[SQLBackend]:
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
