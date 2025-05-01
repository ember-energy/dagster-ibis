import dagster as dg
from abc import abstractmethod
from typing import Sequence, Type, TypeVar

from dagster._core.storage.db_io_manager import DbTypeHandler
from dagster._core.storage.db_io_manager import TableSlice
import ibis


class IbisTableTypeHandler(DbTypeHandler):
    """
    Base-class to be used when creating type handlers that follow the
    logic of the `custom_db_io_manager`.
    """

    def handle_output(
        self,
        context: dg.OutputContext,
        table_slice: TableSlice,
        obj: ibis.Table,
        connection: ibis.BaseBackend,
    ):
        ...

    def load_input(
        self,
        context: dg.InputContext,
        table_slice: TableSlice,
        connection: ibis.BaseBackend,
    ) -> ibis.Table:
        ...

    @property
    def supported_types(self) -> Sequence[Type[object]]:
        return [ibis.Table]


class DuckDBIbisTableTypeHandler(IbisTableTypeHandler):
    def handle_output(
        self,
        context: dg.OutputContext,
        table_slice: TableSlice,
        obj: ibis.Table,
        connection: ibis.BaseBackend,
    ):
        connection.create_table(f"{table_slice.schema}.{table_slice.table}", obj=obj)
        return

    def load_input(
        self,
        context: dg.InputContext,
        table_slice: TableSlice,
        connection: ibis.BaseBackend,
    ) -> ibis.Table:
        table = connection.table(f"{table_slice.schema}.{table_slice.table}")
        # FIX: Seems to be a DuckDBPyRelation, not a Table
        context.log.debug(table)
        context.log.debug(type(table))
        return table
