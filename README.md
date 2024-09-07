# subframe

Subframe is a POC project implementing python dataframe API on top of substrait. It aims to expose an API identical to ibis (maybe, roughly, eventually), but use substrait directly as an IR. It also aims to generate SQL in various dialects directly from substrait.

tldr: ibis w/o ibis IR or sqlglot

```
import subframe

orders = subframe.table([
    ("order_id", "int64"),
    ("fk_store_id", "int64"),
    ("fk_customer_id", "int64"),
    ("description", "string"),
    ("order_total", "float"),
], name="orders")

table = orders.select(
    "order_id",
    "description",
    orders["fk_store_id"] + orders["fk_customer_id"],
    subframe.literal(1).name("one"),
    two=subframe.literal(2),
)

substrait_plan = table.to_plan()

from subframe.sql import translate_plan

# No dialect support yet, tested with duckdb
# Queries are a lot uglier than ibis alternatives because substrait doesn't keep track of intermediate field names
sql = translate_plan(substrait_plan)

print(sql)
```