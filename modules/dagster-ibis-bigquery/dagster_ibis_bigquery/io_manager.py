import ibis
from dagster import IOManagerDefinition

from dagster_ibis_bigquery.type_handler import BigQueryIbisTypeHandler
from dagster_gcp import build_bigquery_io_manager


def build_bigquery_ibis_io_manager(
    type_handlers=(BigQueryIbisTypeHandler(),),
    default_load_type=ibis.Table,
) -> IOManagerDefinition:
    return build_bigquery_io_manager(
        type_handlers=list(type_handlers),
        default_load_type=default_load_type,
    )
