import subframe

orders = subframe.table(
    [
        ("order_id", "int64"),
        ("fk_store_id", "int64"),
        ("fk_customer_id", "int64"),
        ("description", "string"),
        ("order_total", "float"),
    ],
    name="orders",
)

table = orders.select(
    "order_id",
    "description",
    orders["fk_store_id"] + orders["fk_customer_id"],
    subframe.literal(1).name("one"),
    two=subframe.literal(2),
)

# No dialect support yet, tested with duckdb
sql = subframe.to_sql(table)
print(sql)
