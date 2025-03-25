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
    ): ...

    def load_input(
        self,
        context: dg.InputContext,
        table_slice: TableSlice,
        connection: ibis.BaseBackend,
    ) -> ibis.Table: ...

    @property
    def supported_types(self) -> Sequence[Type[object]]:
        return [ibis.Table]
