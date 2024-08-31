import pyarrow as pa
import pyarrow.compute as pc
import ibis
import subframe
from ibis_substrait.compiler.core import SubstraitCompiler
from .consumer_utils import DatafusionSubstraitConsumer


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

consumer = DatafusionSubstraitConsumer().with_tables(datasets)


def sort_pyarrow_table(table: pa.Table):
    sort_keys = [(name, "ascending") for name in table.column_names]
    sort_indices = pc.sort_indices(table, sort_keys)
    return pc.take(table, sort_indices)


def test_bla():

    def transform(table, module):
        return table.select(
            "order_id",
            "description",
            table["fk_store_id"] + table["order_id"],
            module.literal(1, type="int32").name("one"),
            two=module.literal(2, type="int32"),
        )

    plan_ibis = SubstraitCompiler().compile(transform(orders, ibis))
    plan_sf = transform(orders_sf, subframe).to_plan()
    print(plan_sf)

    res_sf = sort_pyarrow_table(consumer.execute(plan_sf))
    res_ibis = sort_pyarrow_table(consumer.execute(plan_ibis))

    print(res_ibis)
    print(res_sf)

    assert res_ibis.equals(res_sf)
