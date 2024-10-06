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

        cur.execute(table.to_plan().SerializeToString())
        print(cur.fetch_arrow_table())
