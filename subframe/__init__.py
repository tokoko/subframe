from typing import Any
from builtins import type as ptype

from substrait.gen.proto import type_pb2 as stt
from substrait.gen.proto import algebra_pb2 as stalg
from .table import Table
from .value import Value
from .utils import infer_literal_type
from .extension_registry import FunctionRegistry
from .case_builder import CaseBuilder

registry = FunctionRegistry()


def case():
    return CaseBuilder()


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

    # plan: stalg.RelRoot = stalg.RelRoot(input=rel, names=column_names)

    return Table(
        rel=rel, names=column_names, struct=struct, extensions={}, relations=[]
    )


def pyarrow_to_substrait_type(pa_type):
    import pyarrow

    if pa_type == pyarrow.int64():
        return stt.Type(i64=stt.Type.I64(nullability=stt.Type.NULLABILITY_NULLABLE))
    elif pa_type == pyarrow.float64():
        return stt.Type(fp64=stt.Type.FP64(nullability=stt.Type.NULLABILITY_NULLABLE))
    elif pa_type == pyarrow.string():
        return stt.Type(
            string=stt.Type.String(nullability=stt.Type.NULLABILITY_NULLABLE)
        )


def named_table(name, conn):
    pa_schema = conn.adbc_get_table_schema(name)

    column_names = pa_schema.names
    struct = stt.Type.Struct(
        types=[
            pyarrow_to_substrait_type(pa_schema.field(c).type) for c in column_names
        ],
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
    if not type:
        if ptype(value) == int:
            type = "i32"
        elif ptype(value) == bool:
            type = "boolean"
        elif ptype(value) == str:
            type = "string"

    if type == "boolean":
        literal = stalg.Expression.Literal(boolean=value, nullable=True)
    elif type in ("i8", "int8"):
        literal = stalg.Expression.Literal(i8=value, nullable=True)
    elif type in ("i16", "int16"):
        literal = stalg.Expression.Literal(i16=value, nullable=True)
    elif type in ("i32", "int32"):
        literal = stalg.Expression.Literal(i32=value, nullable=True)
    elif type in ("i64", "int64"):
        literal = stalg.Expression.Literal(i64=value, nullable=True)
    elif type == "fp32":
        literal = stalg.Expression.Literal(fp32=value, nullable=True)
    elif type == "fp64":
        literal = stalg.Expression.Literal(fp64=value, nullable=True)
    elif type == "string":
        literal = stalg.Expression.Literal(string=value, nullable=True)
    else:
        raise Exception(f"Unknown literal type - {type}")

    print(literal)

    return Value(
        expression=stalg.Expression(literal=literal),
        data_type=infer_literal_type(literal),
    )


def to_sql(table: Table) -> str:
    from subframe.sql import translate_plan

    return translate_plan(table.to_substrait())
