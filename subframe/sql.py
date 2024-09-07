from substrait.gen.proto import algebra_pb2 as stalg
from substrait.gen.proto import type_pb2 as stt
from substrait.gen.proto import plan_pb2 as stp


def translate_struct_field(
    struct_field: stalg.Expression.ReferenceSegment.StructField, extension_functions
):
    return f"f{struct_field.field}"


def translate_reference_segment(
    reference_segment: stalg.Expression.ReferenceSegment, extension_functions
):
    reference_type = reference_segment.WhichOneof("reference_type")
    if reference_type == "struct_field":
        return translate_struct_field(
            reference_segment.struct_field, extension_functions
        )
    else:
        raise Exception(f"Unknown reference_type {reference_type}")


def translate_field_reference(
    field_reference: stalg.Expression.FieldReference, extension_functions
):
    root_type = field_reference.WhichOneof("root_type")
    assert root_type == "root_reference"

    reference_type = field_reference.WhichOneof("reference_type")

    if reference_type == "direct_reference":
        return translate_reference_segment(
            field_reference.direct_reference, extension_functions
        )
    else:
        raise Exception(f"Unknown reference_type {reference_type}")


def translate_literal(literal: stalg.Expression.Literal, extension_functions):
    literal_type = literal.WhichOneof("literal_type")

    if literal_type == "i32":
        return literal.i32
    else:
        raise Exception(f"Unknown literal_type {literal_type}")


def translate_function_argument(
    function_argument: stalg.FunctionArgument, extension_functions
):
    arg_type = function_argument.WhichOneof("arg_type")

    if arg_type == "value":
        return translate_expression(function_argument.value, extension_functions)
    else:
        raise Exception(f"Unknown arg_type {arg_type}")


def translate_scalar_function(
    scalar_function: stalg.Expression.ScalarFunction, extension_functions
):
    func = extension_functions[scalar_function.function_reference]
    print(func)
    arguments = [
        translate_function_argument(argument, extension_functions)
        for argument in scalar_function.arguments
    ]
    if func == "add:i64_i64":
        return " + ".join(arguments)
    else:
        raise Exception(f"Unknown function {func}")


def translate_expression(expression: stalg.Expression, extension_functions):
    rex_type = expression.WhichOneof("rex_type")

    if rex_type == "selection":
        return translate_field_reference(expression.selection, extension_functions)
    elif rex_type == "literal":
        return translate_literal(expression.literal, extension_functions)
    elif rex_type == "scalar_function":
        return translate_scalar_function(
            expression.scalar_function, extension_functions
        )
    else:
        raise Exception(f"Unknown rex_type {rex_type}")


def translate_project(project_rel: stalg.ProjectRel, extension_functions):
    select_clause = ", ".join(
        [
            f"{translate_expression(expression, extension_functions)} AS f{i}"
            for i, expression in enumerate(project_rel.expressions)
        ]
    )

    return f"""SELECT {select_clause} FROM ({translate_rel(project_rel.input, extension_functions)}) AS t"""


def translate_read(read_rel: stalg.ReadRel, extension_functions):
    read_type = read_rel.WhichOneof("read_type")

    select_clause = ", ".join(
        [f"{name} AS f{i}" for i, name in enumerate(read_rel.base_schema.names)]
    )

    if read_type == "named_table":
        names = read_rel.named_table.names
        full_name = ".".join(names)
        return f'SELECT {select_clause} FROM "{full_name}"'
    else:
        raise Exception(f"Unknown read_type {read_type}")


def translate_rel(rel: stalg.Rel, extension_functions):
    rel_type = rel.WhichOneof("rel_type")

    if rel_type == "project":
        return translate_project(rel.project, extension_functions)
    elif rel_type == "read":
        return translate_read(rel.read, extension_functions)
    else:
        raise Exception(f"Unknown rel_type {rel_type}")


def translate_rel_root(rel_root: stalg.RelRoot, extension_functions):
    names = rel_root.names

    select_clause = ", ".join([f'f{i} AS "{name}"' for i, name in enumerate(names)])

    return f"""SELECT {select_clause} FROM ({translate_rel(rel_root.input, extension_functions)}) AS t"""


def translate_plan(plan: stp.Plan):
    extension_functions = {
        extension.extension_function.function_anchor: extension.extension_function.name
        for extension in plan.extensions
    }

    return translate_rel_root(plan.relations[0].root, extension_functions)
