from .value import AggregateValue
from .table import Table
from substrait.gen.proto import algebra_pb2 as stalg


# *by: str | ir.Value | Iterable[str] | Iterable[ir.Value] | None,
#     **key_exprs: str | ir.Value | Iterable[str] | Iterable[ir.Value],
class GroupedTable:
    def __init__(self, table: Table, by: list[str]) -> None:
        self.table = table
        self.by = by

    def agg(self, expr: AggregateValue):
        rel = stalg.Rel(
            aggregate=stalg.AggregateRel(
                input=self.table.plan.input,
                groupings=[
                    stalg.AggregateRel.Grouping(
                        grouping_expressions=[self.table[exp].expression]
                    )
                    for exp in self.by
                ],
                measures=[stalg.AggregateRel.Measure(measure=expr.aggregate_function)],
            )
        )

        names = self.by + (expr.name,)

        return Table(
            plan=stalg.RelRoot(input=rel, names=names),
            struct=self.table.struct,  # TODO
            extensions=self.table._merged_extensions([expr]),
        )
