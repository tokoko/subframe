# subframe

Subframe is a POC project implementing python dataframe API on top of substrait. It aims to expose an API identical to ibis (maybe, roughly, eventually), but use substrait directly as an IR. It also aims to generate SQL in various dialects directly from substrait.

tldr: ibis w/o ibis IR or sqlglot

```
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
    "fk_store_id",
    "description",
    orders["fk_store_id"] + orders["fk_customer_id"],
    subframe.literal(1).name("one"),
    two=subframe.literal(2),
)

table = table.filter(table["order_id"] > table["fk_store_id"])

# No dialect support yet, tested with duckdb
sql = subframe.to_sql(table)
print(sql)
```

Unlike ibis, subframe will probably not have a concept of a Backend, it will integrate with adbc instead.

```
import adbc_driver_duckdb.dbapi
import pyarrow
import subframe

data = pyarrow.record_batch(
    [[1, 2, 3, 4], ["a", "b", "c", "d"]],
    names=["ints", "strs"],
)

with adbc_driver_duckdb.dbapi.connect(":memory:") as conn:
    with conn.cursor() as cur:
        cur.adbc_ingest("AnswerToEverything", data)

        cur.executescript("INSTALL substrait;")
        cur.executescript("LOAD substrait;")

        table = subframe.named_table("AnswerToEverything", conn)
        table = table.select((table["ints"] + subframe.literal(100)).name("col"))

        cur.execute(table.to_substrait().SerializeToString())
        print(cur.fetch_arrow_table())
```