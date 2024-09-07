import pyarrow as pa
import pyarrow.compute as pc
import pytest
import ibis
import subframe
import tempfile
import os
from ibis_substrait.compiler.core import SubstraitCompiler
from .consumers.consumer import SubstraitConsumer
from .consumers.duckdb import DuckDbSubstraitConsumer
from .consumers.acero import AceroSubstraitConsumer
from .consumers.datafusion import DatafusionSubstraitConsumer


orders_raw = [
    ("order_id", "int64", [1, 2, 3, 4]),
    ("fk_store_id", "int64", [1, 1, 2, 2]),
    ("fk_customer_id", "int64", [10, 11, 13, 13]),
    ("description", "string", ["A", "B", "C", "D"]),
    ("order_total", "float", [10.0, 32.3, 32.0, 140.0]),
]

stores_raw = [("store_id", "int64", [1, 2, 3]), ("city", "string", ["NY", "LA", "NY"])]

customers_raw = [
    ("customer_id", "int64", [10, 11, 13]),
    ("name", "string", ["Ann", "Bob", "Chris"]),
]

datasets = {
    "orders": pa.Table.from_pydict({x[0]: x[2] for x in orders_raw}),
    "stores": pa.Table.from_pydict({x[0]: x[2] for x in stores_raw}),
    "customers": pa.Table.from_pydict({x[0]: x[2] for x in customers_raw}),
}

orders = ibis.table([(x[0], x[1]) for x in orders_raw], name="orders")
stores = ibis.table([(x[0], x[1]) for x in stores_raw], name="stores")
customers = ibis.table([(x[0], x[1]) for x in customers_raw], name="customers")


orders_sf = subframe.table([(x[0], x[1]) for x in orders_raw], name="orders")

consumer = DuckDbSubstraitConsumer().with_tables(datasets)


def sort_pyarrow_table(table: pa.Table):
    sort_keys = [(name, "ascending") for name in table.column_names]
    sort_indices = pc.sort_indices(table, sort_keys)
    return pc.take(table, sort_indices)


def run_query_duckdb(query, datasets):
    with tempfile.TemporaryDirectory() as tempdir:
        con = ibis.duckdb.connect(os.path.join(tempdir, "temp.db"))
        for table_name, pa_table in datasets.items():
            con.create_table(name=table_name, obj=ibis.memtable(pa_table))

        # TODO con.to_pyarrow(query) in duckdb backend doesn't work with latest ibis and pyarrow versions
        res = pa.Table.from_pandas(con.to_pandas(query))
        con.disconnect()
        return res


def run_parity_test(
    consumer: SubstraitConsumer, expr: ibis.Table, expr_sf: subframe.Table
):
    res_duckdb = sort_pyarrow_table(run_query_duckdb(expr, datasets))

    plan_ibis = SubstraitCompiler().compile(expr)
    plan_sf = expr_sf.to_plan()
    res_sf = sort_pyarrow_table(consumer.execute(plan_sf))
    res_ibis = sort_pyarrow_table(consumer.execute(plan_ibis))

    print(res_duckdb)
    print("---------------")
    print(res_sf)
    print("---------------")
    print(res_ibis)

    assert res_sf.to_pandas().equals(res_duckdb.to_pandas())
    assert res_ibis.to_pandas().equals(res_duckdb.to_pandas())


@pytest.fixture
def acero_consumer():
    return AceroSubstraitConsumer().with_tables(datasets)


@pytest.fixture
def datafusion_consumer():
    return DatafusionSubstraitConsumer().with_tables(datasets)


@pytest.fixture
def duckdb_consumer():
    return DuckDbSubstraitConsumer().with_tables(datasets)


@pytest.mark.parametrize(
    "consumer", ["acero_consumer", "datafusion_consumer", "duckdb_consumer"]
)
def test_projection(consumer, request):

    def transform(table, module):
        return table.select(
            "order_id",
            "description",
            table["fk_store_id"] + table["order_id"],
            table["fk_store_id"] - table["order_id"],
            module.literal(1, type="int32").name("one"),
            two=module.literal(2, type="int32"),
        )

    ibis_expr = transform(orders, ibis)
    sf_expr = transform(orders_sf, subframe)

    run_parity_test(request.getfixturevalue(consumer), ibis_expr, sf_expr)
