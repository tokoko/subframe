import pyarrow as pa
import datafusion
from .consumer import SubstraitConsumer


class DatafusionSubstraitConsumer(SubstraitConsumer):
    def __init__(self) -> None:
        self.connection = datafusion.SessionContext()

    def with_tables(self, datasets: dict[str, pa.Table]):
        for k, v in datasets.items():
            self.connection.deregister_table(k)
            self.connection.register_record_batches(k, [v.to_batches()])
        return self

    def execute(self, plan) -> pa.Table:
        plan_data = plan.SerializeToString()
        substrait_plan = datafusion.substrait.serde.deserialize_bytes(plan_data)
        logical_plan = datafusion.substrait.consumer.from_substrait_plan(
            self.connection, substrait_plan
        )

        df = self.connection.create_dataframe_from_logical_plan(logical_plan)
        for column_number, column_name in enumerate(df.schema().names):
            df = df.with_column_renamed(
                column_name, plan.relations[0].root.names[column_number]
            )
        record_batch = df.collect()
        return pa.Table.from_batches(record_batch)
