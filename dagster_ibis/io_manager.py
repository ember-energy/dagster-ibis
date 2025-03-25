from typing import (
    Optional,
    Sequence,
    Type,
    Any,
    cast,
)

from dagster._core.storage.db_io_manager import DbIOManager, DbClient
import dagster as dg
import dagster._check as check
import ibis

from dagster_ibis.client import IbisClient
from dagster_ibis.type_handler import IbisTableTypeHandler


class IbisIOManager(DbIOManager):
    def __init__(
        self,
        *,
        ibis_backend: ibis.BaseBackend,
        database: str,
        schema: Optional[str] = None,
    ):
        self._db_client = IbisClient(ibis_backend)
        super().__init__(
            type_handlers=[IbisTableTypeHandler()],
            db_client=self._db_client,
            database=database,
            schema=schema,
            io_manager_name="ibis_io_manager",
            default_load_type=ibis.Table,
        )

    def handle_output(self, context: dg.OutputContext, obj: object) -> None:
        obj_type = type(obj)
        self._check_supported_type(obj_type)
        obj = cast(ibis.Table, obj)

        table_slice = self._get_table_slice(context, context)
        handler = self._handlers_by_type[obj_type]
        with self._db_client.connect(context, table_slice) as conn:
            handler.handle_output(context, table_slice, obj, conn)

    def load_input(self, context: dg.InputContext) -> object:
        return super().load_input(context)


def build_ibis_io_manager(
    io_manager_base: dg.ConfigurableIOManagerFactory,
    ibis_backend: ibis.BaseBackend,
) -> dg.IOManagerDefinition:
    @dg.io_manager(config_schema=io_manager_base.to_config_schema())
    def duckdb_io_manager(init_context):
        return IbisIOManager(
            ibis_backend=ibis_backend,
            database=init_context.resource_config["database"],
            schema=init_context.resource_config.get("schema"),
        )

    return duckdb_io_manager
