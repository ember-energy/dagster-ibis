import ibis
from dagster import IOManagerDefinition

from dagster_ibis_duckdb.type_handler import DuckDBIbisTypeHandler
from dagster_duckdb import build_duckdb_io_manager


def build_duckdb_ibis_io_manager(
    type_handlers=(DuckDBIbisTypeHandler(),),
    default_load_type=ibis.Table,
) -> IOManagerDefinition:
    return build_duckdb_io_manager(
        type_handlers=list(type_handlers),
        default_load_type=default_load_type,
    )
