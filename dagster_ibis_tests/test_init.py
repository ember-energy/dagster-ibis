from dagster._core.storage.db_io_manager import TableSlice
import ibis
import dagster as dg
from dagster_ibis.client import IbisClient
from duckdb import DuckDBPyConnection

from dagster_ibis.io_manager import IbisIOManager

RESOURCE_CONFIG = {"database": "duckdb://"}
TABLE = "my_table"
SCHEMA = "my_schema"
TABLE_SLICE = TableSlice(TABLE, SCHEMA)


def test_ibis_client():
    client = IbisClient()

    context = dg.build_output_context(resource_config=RESOURCE_CONFIG)
    with client.connect(context, TABLE_SLICE) as connection:
        assert connection is not None
        IbisClient.ensure_schema_exists(context, TABLE_SLICE, connection)
        IbisClient.execute_sql(f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE}", connection)
        result = IbisClient.execute_sql(
            f"CREATE TABLE {SCHEMA}.{TABLE} AS (SELECT 1 AS test)", connection
        )
        assert result is not None

        select = IbisClient.get_select_statement(TableSlice(TABLE, SCHEMA))
        assert select == f"SELECT * FROM {SCHEMA}.{TABLE}"

        IbisClient.delete_table_slice(context, TABLE_SLICE, connection)
        result = IbisClient.execute_sql(
            f"SELECT COUNT(*) FROM {SCHEMA}.{TABLE}", connection
        )

        if isinstance(result, DuckDBPyConnection):
            count = result.fetchone()
            assert count is not None
            assert count[0] == 0


def test_io_manager():
    io_manager = IbisIOManager(database=RESOURCE_CONFIG["database"])
    assert io_manager is not None
