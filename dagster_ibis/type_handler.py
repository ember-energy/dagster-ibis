import dagster as dg
from typing import Any, Sequence, Type

from dagster._core.storage.db_io_manager import DbTypeHandler
from dagster._core.storage.db_io_manager import TableSlice
from dagster._check import CheckError
from duckdb import DuckDBPyConnection
import ibis
from ibis.backends.duckdb import Backend as DuckDBBackend


class IbisTableTypeHandler(DbTypeHandler):
    """
    Base-class to be used when creating type handlers that follow the
    logic of the `custom_db_io_manager`.
    """

    @staticmethod
    def connection_to_backend(connection: Any) -> ibis.BaseBackend:
        ...

    def handle_output(
        self,
        context: dg.OutputContext,
        table_slice: TableSlice,
        obj: ibis.Table,
        connection: Any,
    ):
        backend: ibis.BaseBackend = connection
        if table_slice.table in backend.list_tables(database=table_slice.schema):
            backend.insert(table_slice.table, obj=obj, database=table_slice.schema)
        else:
            backend.create_table(
                table_slice.table,
                obj=obj,
                database=table_slice.schema,
            )

    def load_input(
        self,
        context: dg.InputContext,
        table_slice: TableSlice,
        connection: Any,
    ) -> ibis.Table:
        backend: ibis.BaseBackend = connection

        # NOTE: for first materialisation of self-dependent assets
        if table_slice.partition_dimensions and len(context.asset_partition_keys) == 0:
            return ibis.memtable({})

        table = backend.table(table_slice.table, database=table_slice.schema)
        column_schema = table.schema()
        try:
            context.log.debug(column_schema)
            context.add_input_metadata(
                {"schema": dg.JsonMetadataValue(dict(column_schema))}
            )
        except (CheckError, AttributeError):
            context.log.debug(column_schema)
        return table

    @property
    def supported_types(self) -> Sequence[Type[object]]:
        return [ibis.Table]


class DuckDBIbisTableTypeHandler(IbisTableTypeHandler):
    @staticmethod
    def connection_to_backend(connection: DuckDBPyConnection) -> DuckDBBackend:
        return ibis.duckdb.from_connection(connection)

    def handle_output(
        self,
        context: dg.OutputContext,
        table_slice: TableSlice,
        obj: ibis.Table,
        connection: DuckDBPyConnection,
    ):
        backend = self.connection_to_backend(connection)
        super().handle_output(context, table_slice, obj, backend)

    def load_input(
        self,
        context: dg.InputContext,
        table_slice: TableSlice,
        connection: DuckDBPyConnection,
    ) -> ibis.Table:
        backend = self.connection_to_backend(connection)
        return super().load_input(context, table_slice, backend)
