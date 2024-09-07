import pyarrow as pa
import duckdb
from .consumer import SubstraitConsumer
from subframe.sql import translate_plan


class DuckDbSubstraitConsumer(SubstraitConsumer):
    def __init__(self) -> None:
        self.conn = duckdb.connect(":memory:")
        self.datasets = {}

    def with_tables(self, datasets: dict[str, pa.Table]):
        self.datasets = datasets
        return self

    def execute(self, plan) -> pa.Table:

        for t, pa_table in self.datasets.items():
            locals()[t] = pa_table

        query = translate_plan(plan)
        return duckdb.sql(query).to_arrow_table()
