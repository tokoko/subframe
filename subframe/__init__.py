from typing import Any
from builtins import type as ptype

from substrait.gen.proto import type_pb2 as stt
from substrait.gen.proto import algebra_pb2 as stalg
from .table import Table
from .value import Value


def substrait_type_from_string(type: str):
    if type == "int64":
        return stt.Type(i64=stt.Type.I64(nullability=stt.Type.NULLABILITY_NULLABLE))
    elif type == "float":
        return stt.Type(fp64=stt.Type.FP64(nullability=stt.Type.NULLABILITY_NULLABLE))
    elif type == "string":
        return stt.Type(
            string=stt.Type.String(nullability=stt.Type.NULLABILITY_NULLABLE)
        )
    else:
        raise Exception("Unknown Type")


def table(schema, name):
    column_names = [c[0] for c in schema]
    struct = stt.Type.Struct(
        types=[substrait_type_from_string(c[1]) for c in schema],
        nullability=stt.Type.Nullability.NULLABILITY_NULLABLE,
    )
    schema = stt.NamedStruct(
        names=column_names,
        struct=struct,
    )

    rel = stalg.Rel(
        read=stalg.ReadRel(
            common=stalg.RelCommon(direct=stalg.RelCommon.Direct()),
            base_schema=schema,
            named_table=stalg.ReadRel.NamedTable(names=[name]),
        )
    )

    plan: stalg.RelRoot = stalg.RelRoot(input=rel, names=column_names)

    return Table(plan=plan, struct=struct)


def literal(value: Any, type: str = None) -> Value:
    # TODO assumes i32

    if ptype(value) == int:
        expr = stalg.Expression(
            literal=stalg.Expression.Literal(i32=value, nullable=True)
        )
        return Value(
            expression=expr,
            data_type=stt.Type(
                i32=stt.Type.I32(nullability=stt.Type.NULLABILITY_NULLABLE)
            ),
        )
    else:
        raise Exception("Unknown literal")
