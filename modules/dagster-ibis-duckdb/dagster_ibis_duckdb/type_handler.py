import ibis
from duckdb import DuckDBPyConnection
from ibis.backends.duckdb import Backend as DuckDbBackend

from dagster_ibis import IbisTypeHandler


class DuckDBIbisTypeHandler(IbisTypeHandler):
    @staticmethod
    def connection_to_backend(connection: DuckDBPyConnection) -> DuckDbBackend:
        return ibis.duckdb.from_connection(connection)
