from .value import Value, AggregateValue
from .table import Table
from substrait.gen.proto import algebra_pb2 as stalg
from substrait.gen.proto import type_pb2 as stt


# *by: str | ir.Value | Iterable[str] | Iterable[ir.Value] | None,
#     **key_exprs: str | ir.Value | Iterable[str] | Iterable[ir.Value],
class GroupedTable:
    def __init__(
        self, table: Table, by: list[str | Value], key_exprs: dict[str, str | Value]
    ) -> None:
        self.table = table
        self.by = by
        self.key_exprs = key_exprs

    def agg(self, *exprs: AggregateValue):
        combined_exprs = self.table._to_values(self.by, self.key_exprs)

        return self.table.aggregate(metrics=exprs, by=combined_exprs)
