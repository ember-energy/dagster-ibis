from contextlib import contextmanager
from typing import Iterator, Union
from dagster._core.storage.db_io_manager import DbClient
import dagster as dg
import ibis


class IbisClient(DbClient[ibis.BaseBackend]):
    def __init__(self, ibis_backend: ibis.BaseBackend) -> None:
        self.ibis_backend = ibis_backend
        super().__init__()

    @staticmethod
    def delete_table_slice(
        context: dg.OutputContext,
        table_slice: dg.TableSlice,
        connection: ibis.BaseBackend,
    ) -> None: ...

    @staticmethod
    def ensure_schema_exists(
        context: dg.OutputContext,
        table_slice: dg.TableSlice,
        connection: ibis.BaseBackend,
    ) -> None: ...

    @staticmethod
    def get_select_statement(table_slice: dg.TableSlice) -> str: ...

    @staticmethod
    @contextmanager
    def connect(
        context: Union[dg.OutputContext, dg.InputContext],
        table_slice: dg.TableSlice,
    ) -> Iterator[ibis.BaseBackend]: ...
