import ibis
from google.cloud.bigquery import Client
from ibis.backends.bigquery import Backend as BigQueryDbBackend

from dagster_ibis import IbisTypeHandler


class BigQueryIbisTypeHandler(IbisTypeHandler):
    @staticmethod
    def connection_to_backend(connection: Client) -> BigQueryDbBackend:
        return ibis.bigquery.from_connection(connection)
