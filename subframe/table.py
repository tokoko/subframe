import itertools
from substrait.gen.proto import algebra_pb2 as stalg
from substrait.gen.proto import plan_pb2 as stp
from substrait.gen.proto import type_pb2 as stt
from substrait.gen.proto.extensions import extensions_pb2 as ste
from .value import Value


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
        return Value(
            expression,
            data_type=self.struct.types[list(self.plan.names).index(what)],
            name=what,
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

    def select(
        self,
        *exprs: Value | str,  # TODO | Iterable[Value | str],
        **named_exprs: Value | str,
    ):
        mapping_counter = itertools.count(len(self.plan.names))

        combined_exprs = [(e if type(e) == str else e._name, e) for e in exprs] + list(
            named_exprs.items()
        )

        # TODO deep merge utility
        extensions = self.extensions  # TODO deepcopy
        for c in combined_exprs:
            if type(c[1]) != str and c[1].extensions:
                for k, v in c[1].extensions.items():
                    if k in extensions:
                        for k1, v1 in c[1].extensions[k].items():
                            extensions[k][k1] = v1
                    else:
                        extensions[k] = v

        rel = stalg.Rel(
            project=stalg.ProjectRel(
                input=self.plan.input,
                common=stalg.RelCommon(
                    emit=stalg.RelCommon.Emit(
                        output_mapping=[next(mapping_counter) for _ in combined_exprs]
                    )
                ),
                expressions=[
                    (
                        stalg.Expression(
                            selection=stalg.Expression.FieldReference(
                                root_reference=stalg.Expression.FieldReference.RootReference(),
                                direct_reference=stalg.Expression.ReferenceSegment(
                                    struct_field=stalg.Expression.ReferenceSegment.StructField(
                                        field=list(self.plan.names).index(c[1]),
                                    ),
                                ),
                            )
                        )
                        if type(c[1]) == str
                        else c[1].expression
                    )
                    for c in combined_exprs
                ],
            )
        )

        names = [c[0] for c in combined_exprs]

        schema = [
            (
                self.struct.types[list(self.plan.names).index(c[1])]
                if type(c[1]) == str
                else c[1].data_type
            )
            for c in combined_exprs
        ]

        struct = stt.Type.Struct(
            types=schema,
            nullability=stt.Type.Nullability.NULLABILITY_NULLABLE,
        )

        return Table(
            plan=stalg.RelRoot(input=rel, names=names),
            struct=struct,
            extensions=extensions,
        )
