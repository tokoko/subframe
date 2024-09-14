import itertools
from substrait.gen.proto import algebra_pb2 as stalg
from substrait.gen.proto import plan_pb2 as stp
from substrait.gen.proto import type_pb2 as stt
from substrait.gen.proto.extensions import extensions_pb2 as ste
from .value import Value, Column, AggregateValue


class Table:
    def __init__(
        self, plan: stalg.RelRoot, struct: stt.Type.Struct, extensions={}
    ) -> None:
        self.plan = plan
        self.extensions = extensions
        self.struct = struct

    def __getitem__(self, what: str):
        expression = stalg.Expression(
            selection=stalg.Expression.FieldReference(
                root_reference=stalg.Expression.FieldReference.RootReference(),
                direct_reference=stalg.Expression.ReferenceSegment(
                    struct_field=stalg.Expression.ReferenceSegment.StructField(
                        field=list(self.plan.names).index(what),
                    ),
                ),
            )
        )
        return Column(
            expression,
            data_type=self.struct.types[list(self.plan.names).index(what)],
            name=what,
            table=self,
        )

    def to_plan(self) -> stp.Plan:
        return stp.Plan(
            extension_uris=[
                ste.SimpleExtensionURI(extension_uri_anchor=i, uri=e)
                for i, e in enumerate(self.extensions.keys())
            ],
            extensions=[
                ste.SimpleExtensionDeclaration(
                    extension_function=ste.SimpleExtensionDeclaration.ExtensionFunction(
                        extension_uri_reference=i,
                        function_anchor=fn_anchor,
                        name=fn_name,
                    )
                )
                for i, e in enumerate(self.extensions.items())
                for fn_name, fn_anchor in e[1].items()
            ],
            version=stp.Version(minor_number=54, producer="subframe"),
            relations=[stp.PlanRel(root=self.plan)],
        )

    def _merged_extensions(self, exprs):
        # TODO deep merge utility
        extensions = self.extensions  # TODO deepcopy
        for c in exprs:
            if c.extensions:
                for k, v in c.extensions.items():
                    if k in extensions:
                        for k1, v1 in c.extensions[k].items():
                            extensions[k][k1] = v1
                    else:
                        extensions[k] = v

        return extensions

    def _to_values(self, exprs: list[Value | str], named_exprs: dict[str, Value | str]):
        combined_exprs = [(e if type(e) == str else e._name, e) for e in exprs] + list(
            named_exprs.items()
        )

        combined_exprs = [
            (self[c[1]] if type(c[1]) == str else c[1]).name(c[0])
            for c in combined_exprs
        ]

        return combined_exprs

    def select(
        self,
        *exprs: Value | str,  # TODO | Iterable[Value | str],
        **named_exprs: Value | str,
    ):
        combined_exprs = self._to_values(exprs, named_exprs)

        mapping_counter = itertools.count(len(self.plan.names))

        rel = stalg.Rel(
            project=stalg.ProjectRel(
                input=self.plan.input,
                common=stalg.RelCommon(
                    emit=stalg.RelCommon.Emit(
                        output_mapping=[next(mapping_counter) for _ in combined_exprs]
                    )
                ),
                expressions=[c.expression for c in combined_exprs],
            )
        )

        names = [c._name for c in combined_exprs]

        schema = [c.data_type for c in combined_exprs]

        struct = stt.Type.Struct(
            types=schema,
            nullability=stt.Type.Nullability.NULLABILITY_NULLABLE,
        )

        return Table(
            plan=stalg.RelRoot(input=rel, names=names),
            struct=struct,
            extensions=self._merged_extensions(combined_exprs),
        )

    # TODO *predicates: ir.BooleanValue | Sequence[ir.BooleanValue] | IfAnyAll,
    def filter(self, *predicates: Value):
        assert len(predicates) == 1
        predicate = predicates[0]  # TODO ignores all predicates except the first one
        rel = stalg.Rel(
            filter=stalg.FilterRel(
                input=self.plan.input, condition=predicate.expression
            )
        )

        return Table(
            plan=stalg.RelRoot(input=rel, names=self.plan.names),
            struct=self.struct,
            extensions=self._merged_extensions(predicates),
        )

    # def group_by(
    #     self,
    #     *by: str | ir.Value | Iterable[str] | Iterable[ir.Value] | None,
    #     **key_exprs: str | ir.Value | Iterable[str] | Iterable[ir.Value],
    # ) -> GroupedTable:
    def group_by(self, *by: Value | str, **key_exprs: Value | str):
        from .grouped_table import GroupedTable

        return GroupedTable(self, by, key_exprs)

    # def aggregate(
    #     self,
    #     metrics: Sequence[ir.Scalar] | None = (),
    #     by: Sequence[ir.Value] | None = (),
    #     having: Sequence[ir.BooleanValue] | None = (),
    #     **kwargs: ir.Value,
    # ) -> Table:
    def aggregate(self, metrics: list[AggregateValue], by: list[Value | str]):
        combined_exprs = self._to_values(by, {})

        rel = stalg.Rel(
            aggregate=stalg.AggregateRel(
                input=self.plan.input,
                groupings=[
                    stalg.AggregateRel.Grouping(
                        grouping_expressions=[val.expression for val in combined_exprs]
                    )
                ],
                measures=[
                    stalg.AggregateRel.Measure(measure=expr.aggregate_function)
                    for expr in metrics
                ],
            )
        )

        names = [c._name for c in combined_exprs] + [expr.name for expr in metrics]

        schema = [c.data_type for c in combined_exprs] + [
            expr.data_type for expr in metrics
        ]

        struct = stt.Type.Struct(
            types=schema,
            nullability=stt.Type.Nullability.NULLABILITY_NULLABLE,
        )

        return Table(
            plan=stalg.RelRoot(input=rel, names=names),
            struct=struct,
            extensions=self._merged_extensions([expr for expr in metrics]),
        )

    def as_scalar(self):
        expression = stalg.Expression(
            subquery=stalg.Expression.Subquery(
                scalar=stalg.Expression.Subquery.Scalar(input=self.plan.input)
            )
        )

        return Value(
            expression,
            data_type=self.struct.types[0],
            name="ScalarSubquery()",  # TODO why??
            extensions=self.extensions,
        )
