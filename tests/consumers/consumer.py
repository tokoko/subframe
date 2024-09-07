from abc import ABC, abstractmethod
import pyarrow as pa


class SubstraitConsumer(ABC):
    @abstractmethod
    def with_tables(self, datasets: dict[str, pa.Table]):
        pass

    @abstractmethod
    def execute(self, plan) -> pa.Table:
        pass
