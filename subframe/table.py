import itertools
from substrait.gen.proto import algebra_pb2 as stalg
from substrait.gen.proto import plan_pb2 as stp
from substrait.gen.proto import type_pb2 as stt
from substrait.gen.proto.extensions import extensions_pb2 as ste
from .value import Value, AggregateValue
from .utils import merge_extensions


class Table:
    def __init__(
        self,
        rel: stalg.Rel,
        names: list[str],
        struct: stt.Type.Struct,
        extensions,
        relations,
    ) -> None:
        self.rel = rel
        self.names = names
        self.struct = struct
        self.extensions = extensions
        self.relations = relations

    def __getitem__(self, what: str):
        expression = stalg.Expression(
            selection=stalg.Expression.FieldReference(
                root_reference=stalg.Expression.FieldReference.RootReference(),
                direct_reference=stalg.Expression.ReferenceSegment(
                    struct_field=stalg.Expression.ReferenceSegment.StructField(
                        field=list(self.names).index(what),
                    ),
                ),
            )
        )
        return Value(
            expression,
            data_type=self.struct.types[list(self.names).index(what)],
            name=what,
            tables=[self],
        )

    def to_substrait(self) -> stp.Plan:

        rel_root = stalg.RelRoot(input=self.rel, names=self.names)
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
            relations=[stp.PlanRel(rel=rel) for rel in self.relations]
            + [stp.PlanRel(root=rel_root)],
        )

    def _merged_extensions(self, exprs):
        return merge_extensions(self.extensions, exprs)

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

        mapping_counter = itertools.count(len(self.names))

        rel = stalg.Rel(
            project=stalg.ProjectRel(
                input=self.rel,
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
            rel=rel,
            names=names,
            struct=struct,
            extensions=self._merged_extensions(combined_exprs),
            relations=self.relations,
        )

    def filter(self, *predicates: Value):
        assert len(predicates) == 1
        # TODO ignores all predicates except the first one
        predicate = predicates[0]
        rel = stalg.Rel(
            filter=stalg.FilterRel(input=self.rel, condition=predicate.expression)
        )

        return Table(
            rel=rel,
            names=self.names,
            struct=self.struct,
            extensions=self._merged_extensions(predicates),
            relations=self.relations,
        )

    def group_by(self, *by: Value | str, **key_exprs: Value | str):
        from .grouped_table import GroupedTable

        return GroupedTable(self, by, key_exprs)

    def aggregate(self, metrics: list[AggregateValue], by: list[Value | str]):
        combined_exprs = self._to_values(by, {})

        rel = stalg.Rel(
            aggregate=stalg.AggregateRel(
                input=self.rel,
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
            rel=rel,
            names=names,
            struct=struct,
            extensions=self._merged_extensions([expr for expr in metrics]),
            relations=self.relations,
        )

    def limit(self, n: int | None, offset: int):
        rel = stalg.Rel(fetch=stalg.FetchRel(input=self.rel, offset=offset, count=n))

        return Table(
            rel=rel,
            names=self.names,
            struct=self.struct,
            extensions=self.extensions,
            relations=self.relations,
        )

    def union(self, table: "Table", *rest: "Table", distinct: bool = True):
        tables = [table] + list(rest)
        rel = stalg.Rel(
            set=stalg.SetRel(
                inputs=[self.rel] + [t.rel for t in tables],
                op=(
                    stalg.SetRel.SetOp.SET_OP_UNION_DISTINCT
                    if distinct
                    else stalg.SetRel.SetOp.SET_OP_UNION_ALL
                ),
            )
        )

        return Table(
            rel=rel,
            names=self.names,
            struct=self.struct,
            extensions=self._merged_extensions(tables),
            relations=[rel for t in tables for rel in t.relations],
        )

    def intersect(self, table: "Table", *rest: "Table", distinct: bool = True):
        tables = [table] + list(rest)
        rel = stalg.Rel(
            set=stalg.SetRel(
                inputs=[self.rel] + [t.rel for t in tables],
                op=(
                    stalg.SetRel.SetOp.SET_OP_INTERSECTION_PRIMARY
                    if distinct
                    else stalg.SetRel.SetOp.SET_OP_INTERSECTION_PRIMARY
                ),
            )
        )

        return Table(
            rel=rel,
            names=self.names,
            struct=self.struct,
            extensions=self._merged_extensions(tables),
            relations=self.relations,
        )

    def difference(self, table: "Table", *rest: "Table", distinct: bool = True):
        tables = [table] + list(rest)
        rel = stalg.Rel(
            set=stalg.SetRel(
                inputs=[self.rel] + [t.rel for t in tables],
                op=(
                    stalg.SetRel.SetOp.SET_OP_MINUS_PRIMARY
                    if distinct
                    else stalg.SetRel.SetOp.SET_OP_MINUS_PRIMARY
                ),
            )
        )

        return Table(
            rel=rel,
            names=self.names,
            struct=self.struct,
            extensions=self._merged_extensions(tables),
            relations=self.relations,
        )

    def order_by(self, *by: str):
        rel = stalg.Rel(
            sort=stalg.SortRel(
                input=self.rel,
                sorts=[
                    stalg.SortField(
                        expr=self[e].expression,
                        direction=stalg.SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST,
                    )
                    for e in by
                ],
            )
        )

        return Table(
            rel=rel,
            names=self.names,
            struct=self.struct,
            extensions=self.extensions,
            relations=self.relations,
        )

    def as_scalar(self):
        expression = stalg.Expression(
            subquery=stalg.Expression.Subquery(
                scalar=stalg.Expression.Subquery.Scalar(input=self.rel)
            )
        )

        return Value(
            expression,
            data_type=self.struct.types[0],
            name="ScalarSubquery()",  # TODO why??
            extensions=self.extensions,
            tables=[],
        )

    # TODO add rest
    def cross_join(
        self, table: "Table", *rest: "Table", lname: str = "", rname: str = "_right"
    ):
        rel = stalg.Rel(
            cross=stalg.CrossRel(
                left=self.rel,
                right=table.rel,
            )
        )

        return Table(
            rel=rel,
            names=list(self.names) + list(table.names),
            struct=self._merge_structs(table.struct),
            extensions=self._merged_extensions([table]),
            relations=self.relations,
        )

    def join(
        self, right: "Table", predicates: list[Value], how: str = "inner"
    ) -> "Table":
        join_mapping = {
            "inner": stalg.JoinRel.JoinType.JOIN_TYPE_INNER,
            "left": stalg.JoinRel.JoinType.JOIN_TYPE_LEFT,
            "right": stalg.JoinRel.JoinType.JOIN_TYPE_RIGHT,
            "outer": stalg.JoinRel.JoinType.JOIN_TYPE_OUTER,
        }

        rel = stalg.Rel(
            join=stalg.JoinRel(
                left=self.rel,
                right=right.rel,
                expression=predicates[0].expression,
                type=join_mapping[how],
            )
        )

        return Table(
            rel=rel,
            names=list(self.names) + list(right.names),
            struct=self._merge_structs(right.struct),
            extensions=self._merged_extensions([right, *predicates]),
            relations=self.relations,
        )

    def view(self):
        return Table(
            rel=stalg.Rel(
                reference=stalg.ReferenceRel(subtree_ordinal=0),
            ),
            names=self.names,
            struct=self.struct,
            extensions=self.extensions,
            relations=[self.rel],
        )

    def _merge_structs(self, struct):
        return stt.Type.Struct(types=list(self.struct.types) + list(struct.types))
