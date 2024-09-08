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

        rel = stalg.Rel(
            aggregate=stalg.AggregateRel(
                input=self.table.plan.input,
                groupings=[
                    stalg.AggregateRel.Grouping(
                        grouping_expressions=[val.expression for val in combined_exprs]
                    )
                ],
                measures=[
                    stalg.AggregateRel.Measure(measure=expr.aggregate_function)
                    for expr in exprs
                ],
            )
        )

        names = [c._name for c in combined_exprs] + [expr.name for expr in exprs]

        schema = [c.data_type for c in combined_exprs] + [
            expr.data_type for expr in exprs
        ]

        struct = stt.Type.Struct(
            types=schema,
            nullability=stt.Type.Nullability.NULLABILITY_NULLABLE,
        )

        return Table(
            plan=stalg.RelRoot(input=rel, names=names),
            struct=struct,
            extensions=self.table._merged_extensions([expr for expr in exprs]),
        )
