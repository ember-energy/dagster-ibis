from dagster._core.storage.db_io_manager import TableSlice
import dagster as dg
import pandas as pd
from dagster_ibis_duckdb.client import IbisDuckDbClient

from dagster_ibis_duckdb_tests.helper_duckdb import cleanup_table, query_test_db

RESOURCE_CONFIG = {"database": "duckdb://"}
TABLE = "my_table"
SCHEMA = "my_schema"
TABLE_SLICE = TableSlice(TABLE, SCHEMA)


def test_ibis_client():
    client = IbisDuckDbClient()

    context = dg.build_output_context(resource_config=RESOURCE_CONFIG)
    with client.connect(context, TABLE_SLICE) as connection:
        assert connection is not None
        IbisDuckDbClient.ensure_schema_exists(context, TABLE_SLICE, connection)
        IbisDuckDbClient.execute_sql(
            context,
            f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE}",
            connection,
        )
        result = IbisDuckDbClient.execute_sql(
            context,
            f"CREATE TABLE {SCHEMA}.{TABLE} AS (SELECT 1 AS test)",
            connection,
        )
        assert result is not None

        select = IbisDuckDbClient.get_select_statement(TableSlice(TABLE, SCHEMA))
        assert select == f"SELECT * FROM {SCHEMA}.{TABLE}"

        IbisDuckDbClient.delete_table_slice(context, TABLE_SLICE, connection)
        result = IbisDuckDbClient.execute_sql(
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
