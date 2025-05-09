from abc import abstractmethod
from typing import Optional, Sequence, Type

import ibis
from dagster import IOManagerDefinition, io_manager
from dagster._config.pythonic_config import ConfigurableIOManagerFactory
from dagster._core.storage.db_io_manager import DbIOManager, DbTypeHandler
from pydantic import Field

from dagster_ibis.client import IbisClient
from dagster_ibis.type_handler import IbisTypeHandler


def build_ibis_io_manager(
    db_client: IbisClient,
    type_handlers=(IbisTypeHandler(),),
) -> IOManagerDefinition:
    @io_manager(config_schema=IbisIOManager.to_config_schema())
    def ibis_io_manager(init_context):
        return DbIOManager(
            type_handlers=list(type_handlers),
            db_client=db_client,
            io_manager_name="IbisIOManager",
            database=init_context.resource_config["database"],
            schema=None,
            default_load_type=ibis.Table,
        )

    return ibis_io_manager


class IbisIOManager(ConfigurableIOManagerFactory):
    database: str = Field(description="Ibis connection string.")

    @staticmethod
    @abstractmethod
    def type_handlers() -> Sequence[DbTypeHandler]: ...

    @staticmethod
    def default_load_type() -> Optional[Type]:
        return None

    def create_io_manager(self, context) -> DbIOManager:
        return DbIOManager(
            db_client=IbisClient(),
            database=self.database,
            schema=None,
            type_handlers=self.type_handlers(),
            default_load_type=self.default_load_type(),
            io_manager_name="IbisIOManager",
        )
