import ibis
import types
from google.cloud.bigquery import Client
from ibis.backends.bigquery import Backend as BigQueryDbBackend

from dagster_ibis import IbisTypeHandler
from dagster_ibis_bigquery.backend_fix import _register_in_memory_table


class BigQueryIbisTypeHandler(IbisTypeHandler):
    @staticmethod
    def connection_to_backend(connection: Client) -> BigQueryDbBackend:
        backend = ibis.bigquery.from_connection(connection)
        backend._register_in_memory_table = types.MethodType(
            _register_in_memory_table, backend
        )
        return backend
