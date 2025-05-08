from dagster._core.storage.db_io_manager import TableSlice
import ibis
import dagster as dg
import pandas as pd
from dagster_ibis.client import IbisClient
from duckdb import DuckDBPyConnection

from dagster_ibis_tests.helper_duckdb import cleanup_table, query_test_db

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
        IbisClient.execute_sql(
            context,
            f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE}",
            connection,
        )
        result = IbisClient.execute_sql(
            context,
            f"CREATE TABLE {SCHEMA}.{TABLE} AS (SELECT 1 AS test)",
            connection,
        )
        assert result is not None

        select = IbisClient.get_select_statement(TableSlice(TABLE, SCHEMA))
        assert select == f"SELECT * FROM {SCHEMA}.{TABLE}"

        IbisClient.delete_table_slice(context, TABLE_SLICE, connection)
        result = IbisClient.execute_sql(
            context,
            f"SELECT COUNT(*) FROM {SCHEMA}.{TABLE}",
            connection,
        )

        df = query_test_db(
            f"SELECT COUNT(*) AS COUNT FROM {SCHEMA}.{TABLE}",
            connection,
        )
        assert df.equals(pd.DataFrame({"COUNT": [0]}))
        cleanup_table(TABLE_SLICE, connection)
