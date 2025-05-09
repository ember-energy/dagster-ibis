from dagster._core.storage.db_io_manager import TableSlice
from dagster_duckdb_pandas import DuckDBPandasTypeHandler
from dagster_duckdb import build_duckdb_io_manager
import ibis
from ibis import _
import dagster as dg
import pandas as pd

from dagster_ibis_duckdb.io_manager import build_duckdb_ibis_io_manager
from dagster_ibis_duckdb.type_handler import DuckDBIbisTypeHandler
from dagster_ibis_duckdb_tests.helper_duckdb import (
    cleanup_table,
    get_table_slice_db,
)


DATABASE = "database.duckdb"
RESOURCE_CONFIG = {"database": DATABASE}
RESOURCES = {
    "ibis_io_manager": build_duckdb_ibis_io_manager().configured(RESOURCE_CONFIG),
    "duckdb_io_manager": build_duckdb_io_manager(
        type_handlers=[DuckDBPandasTypeHandler(), DuckDBIbisTypeHandler()]
    ).configured(RESOURCE_CONFIG),
}


def check_values(
    expected_values: dict[dg.AssetsDefinition, pd.DataFrame],
    schema: str = "public",
    cleanup=True,
):
    for asset, df_expected in expected_values.items():
        table_slice = TableSlice(asset.key.path[-1], schema=schema)
        df_actual = get_table_slice_db(table_slice, connection_str=DATABASE)
        print(df_actual)
        if cleanup:
            cleanup_table(table_slice, connection_str=DATABASE)
        assert df_actual.equals(df_expected)


def test_duckdb_io_manager():
    @dg.asset(io_manager_key="duckdb_io_manager")
    def my_table() -> ibis.Table:
        return ibis.memtable({"a": [1, 2, 3]})

    @dg.asset(io_manager_key="duckdb_io_manager")
    def my_ibis_table(context, my_table: ibis.Table) -> ibis.Table:
        return my_table.filter(_.a > 1)

    # NOTE: TEST create_table
    result = dg.materialize(assets=[my_table, my_ibis_table], resources=RESOURCES)
    assert result.success
    check_values(
        {
            my_table: pd.DataFrame({"a": [1, 2, 3]}),
            my_ibis_table: pd.DataFrame({"a": [2, 3]}),
        },
        cleanup=False,
    )

    # NOTE: TEST insert
    result = dg.materialize(assets=[my_table, my_ibis_table], resources=RESOURCES)
    assert result.success
    check_values(
        {
            my_table: pd.DataFrame({"a": [1, 2, 3]}),
            my_ibis_table: pd.DataFrame({"a": [2, 3]}),
        },
    )


def test_ibis_io_manager():
    @dg.asset(io_manager_key="ibis_io_manager")
    def my_table() -> ibis.Table:
        return ibis.memtable({"a": [1, 2, 3]})

    @dg.asset(io_manager_key="ibis_io_manager")
    def my_ibis_table(context, my_table: ibis.Table) -> ibis.Table:
        return my_table.filter(_.a > 1)

    # NOTE: TEST create_table
    result = dg.materialize(assets=[my_table, my_ibis_table], resources=RESOURCES)
    assert result.success
    check_values(
        {
            my_table: pd.DataFrame({"a": [1, 2, 3]}),
            my_ibis_table: pd.DataFrame({"a": [2, 3]}),
        },
        cleanup=False,
    )

    # NOTE: TEST insert
    result = dg.materialize(assets=[my_table, my_ibis_table], resources=RESOURCES)
    assert result.success
    check_values(
        {
            my_table: pd.DataFrame({"a": [1, 2, 3]}),
            my_ibis_table: pd.DataFrame({"a": [2, 3]}),
        },
    )


def test_ibis_io_manager_partitioned():
    partition = dg.StaticPartitionsDefinition(["1", "2"])

    @dg.asset(
        io_manager_key="ibis_io_manager",
        metadata={"partition_expr": "a"},
        partitions_def=partition,
    )
    def my_table(context: dg.AssetExecutionContext) -> ibis.Table:
        partition = context.partition_key
        return ibis.memtable({"a": [partition] * 2})

    # NOTE: TEST CREATED
    result = dg.materialize(
        assets=[my_table],
        resources=RESOURCES,
        partition_key="1",
    )
    assert result.success
    check_values(
        {
            my_table: pd.DataFrame({"a": ["1", "1"]}),
        },
        cleanup=False,
    )

    # NOTE: TEST NEW PARTITION APPENDED
    result = dg.materialize(
        assets=[my_table],
        resources=RESOURCES,
        partition_key="2",
    )
    assert result.success
    check_values(
        {
            my_table: pd.DataFrame({"a": ["1", "1", "2", "2"]}),
        },
        cleanup=False,
    )

    # NOTE: TEST PARTITION IS NOT RE-APPENDED
    result = dg.materialize(
        assets=[my_table],
        resources=RESOURCES,
        partition_key="2",
    )
    assert result.success
    check_values(
        {
            my_table: pd.DataFrame({"a": ["1", "1", "2", "2"]}),
        },
        cleanup=True,
    )


def test_ibis_io_manager_custom_schema():
    test_schema = "test"

    @dg.asset(io_manager_key="ibis_io_manager", metadata={"schema": test_schema})
    def my_table(context: dg.AssetExecutionContext) -> ibis.Table:
        return ibis.memtable({"a": [1, 2, 3]})

    result = dg.materialize(assets=[my_table], resources=RESOURCES)
    assert result.success
    check_values(
        {
            my_table: pd.DataFrame({"a": [1, 2, 3]}),
        },
        schema=test_schema,
    )

    @dg.asset(io_manager_key="ibis_io_manager", key_prefix=[test_schema])
    def my_table1(context: dg.AssetExecutionContext) -> ibis.Table:
        return ibis.memtable({"a": [1, 2, 3]})

    result = dg.materialize(assets=[my_table1], resources=RESOURCES)
    assert result.success
    check_values(
        {
            my_table1: pd.DataFrame({"a": [1, 2, 3]}),
        },
        schema=test_schema,
    )


def test_ibis_io_manager_self_dependency():
    @dg.asset(
        io_manager_key="ibis_io_manager",
        partitions_def=dg.MonthlyPartitionsDefinition(start_date="2025-01-01"),
        metadata={"partition_expr": "a"},
        ins={
            "my_table": dg.AssetIn(
                key=dg.AssetKey("my_table"),
                partition_mapping=dg.TimeWindowPartitionMapping(
                    start_offset=-1, end_offset=-1
                ),
            )
        },
    )
    def my_table(context: dg.AssetExecutionContext, my_table: ibis.Table) -> ibis.Table:
        if context.partition_key == "2025-01-01":
            return ibis.memtable(
                {"a": [context.partition_key + " 00:00:00"] * 3, "b": [1, 1, 1]}
            )
        else:
            return my_table.select(
                a=ibis.literal(context.partition_key + " 00:00:00"),
                b=(_.b + 1),
            )

    result = dg.materialize(
        assets=[my_table],
        resources=RESOURCES,
        partition_key="2025-01-01",
    )
    assert result.success
    check_values(
        {
            my_table: pd.DataFrame({"a": ["2025-01-01 00:00:00"] * 3, "b": [1, 1, 1]}),
        },
        cleanup=False,
    )

    result = dg.materialize(
        assets=[my_table],
        resources=RESOURCES,
        partition_key="2025-02-01",
    )
    assert result.success
    check_values(
        {
            my_table: pd.DataFrame(
                {
                    "a": ["2025-01-01 00:00:00"] * 3 + ["2025-02-01 00:00:00"] * 3,
                    "b": [1, 1, 1, 2, 2, 2],
                }
            ),
        },
        cleanup=True,
    )
