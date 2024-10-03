from substrait.gen.proto import algebra_pb2 as stalg
from substrait.gen.proto import type_pb2 as stt
from substrait.gen.proto import plan_pb2 as stp


def _add_padding(sql: str):
    return "  " + "  ".join(sql.splitlines(True))


class SqlTable:
    def __init__(self, sql: str, columns: list[str]) -> None:
        self.sql = sql
        self.columns = columns


class ExpressionContext:
    def __init__(self, fields: list[str]) -> None:
        self.fields = fields


def translate_struct_field(
    struct_field: stalg.Expression.ReferenceSegment.StructField,
    extension_functions,
    context: ExpressionContext,
):
    return context.fields[struct_field.field]  # f"f{struct_field.field}"


def translate_reference_segment(
    reference_segment: stalg.Expression.ReferenceSegment,
    extension_functions,
    context: ExpressionContext,
):
    reference_type = reference_segment.WhichOneof("reference_type")
    if reference_type == "struct_field":
        return translate_struct_field(
            reference_segment.struct_field, extension_functions, context
        )
    else:
        raise Exception(f"Unknown reference_type {reference_type}")


def translate_field_reference(
    field_reference: stalg.Expression.FieldReference,
    extension_functions,
    context: ExpressionContext,
):
    root_type = field_reference.WhichOneof("root_type")
    assert root_type == "root_reference"

    reference_type = field_reference.WhichOneof("reference_type")

    if reference_type == "direct_reference":
        return translate_reference_segment(
            field_reference.direct_reference, extension_functions, context
        )
    else:
        raise Exception(f"Unknown reference_type {reference_type}")


def translate_literal(literal: stalg.Expression.Literal, extension_functions):
    literal_type = literal.WhichOneof("literal_type")

    if literal_type == "i32":
        return literal.i32
    elif literal_type == "boolean":
        return literal.boolean
    else:
        raise Exception(f"Unknown literal_type {literal_type}")


def translate_function_argument(
    function_argument: stalg.FunctionArgument,
    extension_functions,
    context: ExpressionContext,
):
    arg_type = function_argument.WhichOneof("arg_type")

    if arg_type == "value":
        return translate_expression(
            function_argument.value, extension_functions, context
        )
    else:
        raise Exception(f"Unknown arg_type {arg_type}")


def translate_scalar_function(
    scalar_function: stalg.Expression.ScalarFunction,
    extension_functions,
    context: ExpressionContext,
):
    func = extension_functions[scalar_function.function_reference]
    arguments = [
        translate_function_argument(argument, extension_functions, context)
        for argument in scalar_function.arguments
    ]
    if func == "add:i64_i64":
        return " + ".join(arguments)
    elif func == "subtract:i64_i64":
        return " - ".join(arguments)
    elif func == "equal:any_any":
        return " = ".join(arguments)
    elif func == "not_equal:any_any":
        return " != ".join(arguments)
    elif func == "gt:any_any":
        return " > ".join(arguments)
    elif func == "gte:any_any":
        return " >= ".join(arguments)
    elif func == "lt:any_any":
        return " < ".join(arguments)
    elif func == "lte:any_any":
        return " <= ".join(arguments)
    else:
        raise Exception(f"Unknown function {func}")


def translate_expression(
    expression: stalg.Expression, extension_functions, context: ExpressionContext
):
    rex_type = expression.WhichOneof("rex_type")

    if rex_type == "selection":
        return translate_field_reference(
            expression.selection, extension_functions, context
        )
    elif rex_type == "literal":
        return translate_literal(expression.literal, extension_functions)
    elif rex_type == "scalar_function":
        return translate_scalar_function(
            expression.scalar_function, extension_functions, context
        )
    else:
        raise Exception(f"Unknown rex_type {rex_type}")


def translate_project(project_rel: stalg.ProjectRel, extension_functions) -> SqlTable:
    table = translate_rel(project_rel.input, extension_functions)

    context = ExpressionContext([f"t.{i}" for i in table.columns])

    select_clause = ", ".join(
        [
            f"{translate_expression(expression, extension_functions, context)} AS f{i}"
            for i, expression in enumerate(project_rel.expressions)
        ]
    )

    columns = [f"f{i}" for i, _ in enumerate(project_rel.expressions)]

    return SqlTable(
        f"""SELECT {select_clause}\nFROM (\n{_add_padding(table.sql)}\n) AS t""",
        columns,
    )


def translate_read(read_rel: stalg.ReadRel, extension_functions) -> SqlTable:
    read_type = read_rel.WhichOneof("read_type")

    select_clause = ", ".join([name for name in read_rel.base_schema.names])

    if read_type == "named_table":
        names = read_rel.named_table.names
        full_name = ".".join(names)
        sql = f'SELECT {select_clause}\nFROM "{full_name}"'
        return SqlTable(sql, columns=read_rel.base_schema.names)
    else:
        raise Exception(f"Unknown read_type {read_type}")


def translate_filter(filter_rel: stalg.FilterRel, extension_functions) -> SqlTable:
    table = translate_rel(filter_rel.input, extension_functions)

    context = ExpressionContext([f"t.{i}" for i in table.columns])

    expr = translate_expression(filter_rel.condition, extension_functions, context)

    return SqlTable(
        f"""SELECT *\nFROM (\n{_add_padding(table.sql)}\n) AS t\nWHERE {expr}""",
        table.columns,
    )


def translate_rel(rel: stalg.Rel, extension_functions) -> SqlTable:
    rel_type = rel.WhichOneof("rel_type")

    if rel_type == "project":
        return translate_project(rel.project, extension_functions)
    elif rel_type == "read":
        return translate_read(rel.read, extension_functions)
    elif rel_type == "filter":
        return translate_filter(rel.filter, extension_functions)
    else:
        raise Exception(f"Unknown rel_type {rel_type}")


def translate_rel_root(rel_root: stalg.RelRoot, extension_functions) -> SqlTable:
    names = rel_root.names

    table = translate_rel(rel_root.input, extension_functions)

    select_clause = ", ".join(
        [f'{col} AS "{name}"' for col, name in zip(table.columns, names)]
    )

    return f"""SELECT {select_clause}\nFROM (\n{_add_padding(table.sql)}\n) AS t"""


def translate_plan(plan: stp.Plan):
    extension_functions = {
        extension.extension_function.function_anchor: extension.extension_function.name
        for extension in plan.extensions
    }

    return translate_rel_root(plan.relations[0].root, extension_functions)
