import ibis
import subframe
from ibis_substrait.compiler.core import SubstraitCompiler

from ibis.expr.operations import Value, Column

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

orders = ibis.table([(x[0], x[1]) for x in orders_raw], name="orders")
stores = ibis.table([(x[0], x[1]) for x in stores_raw], name="stores")
customers = ibis.table([(x[0], x[1]) for x in customers_raw], name="customers")

expr = orders.select(
    "order_id",
    "description",
    orders["fk_store_id"] + orders["fk_customer_id"],
    ibis.literal(1).name("one"),
    two=ibis.literal(2),
)
plan = SubstraitCompiler().compile(expr)
print(plan)

orders_sf = subframe.table([(x[0], x[1]) for x in orders_raw], name="orders")
expr = orders_sf.select(
    "order_id",
    "description",
    orders_sf["order_total"],
    subframe.literal(100).name("one"),
    two=subframe.literal(2),
)
print(expr.plan)
