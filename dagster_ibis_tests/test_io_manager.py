from dagster._core.storage.db_io_manager import TableSlice
from dagster_duckdb_pandas import DuckDBPandasTypeHandler
from dagster_duckdb import build_duckdb_io_manager
import ibis
from ibis import _
import dagster as dg
import pandas as pd

from dagster_ibis.io_manager import build_ibis_io_manager
from dagster_ibis.type_handler import DuckDBIbisTableTypeHandler
from dagster_ibis_tests.helper_duckdb import (
    cleanup_table,
    get_table_slice_db,
)


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


def check_values_and_cleanup():
    my_table_slice = TableSlice("my_table", "public")
    my_table_df = get_table_slice_db(my_table_slice, connection_str=DATABASE)
    assert my_table_df.equals(pd.DataFrame({"a": [1, 2, 3]}))

    my_ibis_table_slice = TableSlice("my_ibis_table", "public")
    my_ibis_table_df = get_table_slice_db(my_ibis_table_slice, connection_str=DATABASE)
    assert my_ibis_table_df.equals(pd.DataFrame({"a": [2, 3]}))

    cleanup_table(my_table_slice, connection_str=DATABASE)
    cleanup_table(my_ibis_table_slice, connection_str=DATABASE)


def test_io_manager_duckdb_io_manager():
    @dg.asset(io_manager_key="duckdb_io_manager")
    def my_table() -> pd.DataFrame:
        return pd.DataFrame({"a": [1, 2, 3]})

    @dg.asset(io_manager_key="duckdb_io_manager")
    def my_ibis_table(context, my_table: ibis.Table) -> ibis.Table:
        return my_table.filter(_.a > 1)

    result = dg.materialize(assets=[my_table, my_ibis_table], resources=RESOURCES)
    assert result.success
    check_values_and_cleanup()


def test_io_manager_ibis_io_manager():
    @dg.asset(io_manager_key="ibis_io_manager")
    def my_table() -> ibis.Table:
        return ibis.memtable({"a": [1, 2, 3]})

    @dg.asset(io_manager_key="ibis_io_manager")
    def my_ibis_table(context, my_table: ibis.Table) -> ibis.Table:
        return my_table.filter(_.a > 1)

    result = dg.materialize(assets=[my_table, my_ibis_table], resources=RESOURCES)
    assert result.success
    check_values_and_cleanup()
