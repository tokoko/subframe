import pyarrow as pa

from .consumer import SubstraitConsumer


class AceroSubstraitConsumer(SubstraitConsumer):
    def __init__(self) -> None:
        super().__init__()

    def with_tables(self, datasets: dict[str, pa.Table]):
        self.datasets = datasets
        return self

    def execute(self, plan) -> pa.Table:
        import pyarrow.substrait as pa_substrait

        def get_table_provider(datasets):
            def table_provider(names, schema):
                return datasets[names[0]]

            return table_provider

        query_bytes = plan.SerializeToString()
        result = pa_substrait.run_query(
            pa.py_buffer(query_bytes),
            table_provider=get_table_provider(self.datasets),
        )

        return result.read_all()
