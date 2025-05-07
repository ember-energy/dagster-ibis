from dagster._core.storage.db_io_manager import TableSlice
from dagster_duckdb_pandas import DuckDBPandasTypeHandler
from dagster_duckdb import build_duckdb_io_manager
import ibis
from ibis import _
import dagster as dg
import pandas as pd
from dagster_ibis.client import IbisClient
from duckdb import DuckDBPyConnection

from dagster_ibis.io_manager import build_ibis_io_manager
from dagster_ibis.type_handler import DuckDBIbisTableTypeHandler


DATABASE = "database.duckdb"
RESOURCE_CONFIG = {"database": f"duckdb://{DATABASE}"}
TABLE = "my_table"
SCHEMA = "my_schema"
TABLE_SLICE = TableSlice(TABLE, SCHEMA)
RESOURCES = {
    "ibis_io_manager": build_ibis_io_manager().configured(RESOURCE_CONFIG),
    "duckdb_io_manager": build_duckdb_io_manager(
        type_handlers=[DuckDBPandasTypeHandler(), DuckDBIbisTableTypeHandler()]
    ).configured({"database": DATABASE}),
}


def test_io_manager_duckdb_io_manager():
    @dg.asset(io_manager_key="duckdb_io_manager")
    def my_table() -> pd.DataFrame:
        return pd.DataFrame({"a": [1, 2, 3]})

    @dg.asset(io_manager_key="duckdb_io_manager")
    def my_ibis_table(context, my_table: ibis.Table) -> ibis.Table:
        return my_table.filter(_.a > 1)

    result = dg.materialize(assets=[my_table, my_ibis_table], resources=RESOURCES)
    assert result.success


def test_io_manager_ibis_io_manager():
    @dg.asset(io_manager_key="ibis_io_manager")
    def my_table() -> ibis.Table:
        return ibis.memtable({"a": [1, 2, 3]})

    @dg.asset(io_manager_key="ibis_io_manager")
    def my_ibis_table(context, my_table: ibis.Table) -> ibis.Table:
        return my_table.filter(_.a > 1)

    result = dg.materialize(assets=[my_table, my_ibis_table], resources=RESOURCES)
    assert result.success
