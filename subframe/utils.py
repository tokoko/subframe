import re
from substrait.gen.proto import type_pb2 as stt
from substrait.gen.proto.type_pb2 import Type
from substrait.gen.proto.algebra_pb2 import Rel, RelCommon
import substrait.gen.proto.algebra_pb2 as stalg


def to_substrait_type(dtype: str):
    if dtype in ("bool", "boolean"):
        return Type(bool=Type.Boolean())
    elif dtype == "i8":
        return Type(i8=Type.I8())
    elif dtype == "i16":
        return Type(i16=Type.I16())
    elif dtype == "i32":
        return Type(i32=Type.I32())
    elif dtype == "i64":
        return Type(i64=Type.I64())
    elif dtype == "fp32":
        return Type(fp32=Type.FP32())
    elif dtype == "fp64":
        return Type(fp64=Type.FP64())
    elif dtype == "timestamp":
        return Type(timestamp=Type.Timestamp())
    elif dtype == "timestamp_tz":
        return Type(timestamp_tz=Type.TimestampTZ())
    elif dtype == "date":
        return Type(date=Type.Date())
    elif dtype == "time":
        return Type(time=Type.Time())
    elif dtype == "interval_year":
        return Type(interval_year=Type.IntervalYear())
    elif dtype.startswith("decimal") or dtype.startswith("DECIMAL"):
        (_, scale, precision, _) = re.split(r"\W+", dtype)

        return Type(decimal=Type.Decimal(scale=int(scale), precision=int(precision)))
    else:
        raise Exception(f"Unknown type - {dtype}")


def apply_emit(raw_schema: stt.Type.Struct, common: RelCommon) -> stt.Type.Struct:
    emit_kind = common.WhichOneof("emit_kind")
    emit_kind = "direct" if not emit_kind else emit_kind
    if emit_kind == "direct":
        return raw_schema
    else:
        return stt.Type.Struct(
            types=[raw_schema.types[i] for i in common.emit.output_mapping],
            nullability=raw_schema.nullability,
        )


def infer_literal_type(literal: stalg.Expression.Literal) -> Type:
    literal_type = literal.WhichOneof("literal_type")

    nullability = (
        stt.Type.Nullability.NULLABILITY_NULLABLE
        if literal.nullable
        else stt.Type.Nullability.NULLABILITY_REQUIRED
    )

    if literal_type == "boolean":
        return Type(bool=Type.Boolean(nullability=nullability))
    elif literal_type == "i8":
        return Type(i8=Type.I8(nullability=nullability))
    elif literal_type == "i16":
        return Type(i16=Type.I16(nullability=nullability))
    elif literal_type == "i32":
        return Type(i32=Type.I32(nullability=nullability))
    elif literal_type == "i64":
        return Type(i64=Type.I64(nullability=nullability))
    elif literal_type == "fp32":
        return Type(fp32=Type.FP32(nullability=nullability))
    elif literal_type == "fp64":
        return Type(fp64=Type.FP64(nullability=nullability))
    elif literal_type == "string":
        return Type(string=Type.String(nullability=nullability))
    else:
        raise Exception(f"Unknown literal_type {literal_type}")


def infer_expression_type(
    expression: stalg.Expression, parent_schema: stt.Type.Struct
) -> Type:
    rex_type = expression.WhichOneof("rex_type")

    if rex_type == "selection":
        root_type = expression.selection.WhichOneof("root_type")
        assert root_type == "root_reference"

        reference_type = expression.selection.WhichOneof("reference_type")

        if reference_type == "direct_reference":
            segment = expression.selection.direct_reference

            segment_reference_type = segment.WhichOneof("reference_type")

            if segment_reference_type == "struct_field":
                return parent_schema.types[segment.struct_field.field]
            else:
                raise Exception(f"Unknown reference_type {reference_type}")
        else:
            raise Exception(f"Unknown reference_type {reference_type}")

    elif rex_type == "literal":
        return infer_literal_type(expression.literal)
    elif rex_type == "scalar_function":
        return expression.scalar_function.output_type
    # WindowFunction window_function = 5;
    # IfThen if_then = 6;
    # SwitchExpression switch_expression = 7;
    # SingularOrList singular_or_list = 8;
    # MultiOrList multi_or_list = 9;
    # Cast cast = 11;
    # Subquery subquery = 12;
    # Nested nested = 13;
    else:
        raise Exception(f"Unknown rex_type {rex_type}")


def infer_rel_schema(rel: Rel) -> stt.Type.Struct:
    rel_type = rel.WhichOneof("rel_type")

    if rel_type == "read":
        (common, struct) = (rel.read.common, rel.read.base_schema.struct)
    elif rel_type == "filter":
        (common, struct) = (rel.filter.common, infer_rel_schema(rel.filter.input))
    elif rel_type == "project":
        parent_schema = infer_rel_schema(rel.project.input)
        expression_types = [
            infer_expression_type(e, parent_schema) for e in rel.project.expressions
        ]
        raw_schema = stt.Type.Struct(
            types=list(parent_schema.types) + expression_types,
            nullability=parent_schema.nullability,
        )

        (common, struct) = (rel.project.common, raw_schema)
    else:
        raise Exception(f"Unhandled rel_type {rel_type}")

    return apply_emit(struct, common)
