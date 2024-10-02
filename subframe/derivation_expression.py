from .utils import to_substrait_type
from typing import Optional
from substrait.gen.proto.type_expressions_pb2 import DerivationExpression
from substrait.gen.proto.type_pb2 import Type
from pyparsing import (
    Forward,
    Literal,
    ParseResults,
    Word,
    ZeroOrMore,
    identchars,
    infix_notation,
    nums,
    oneOf,
    opAssoc,
)

expr = Forward()


def parse_dtype(tokens: ParseResults):
    tokens_dict = tokens.as_dict()
    dtype = tokens_dict["dtype"].lower()
    if dtype == "decimal":
        return DerivationExpression(
            decimal=DerivationExpression.ExpressionDecimal(
                scale=tokens_dict["scale"], precision=tokens_dict["precision"]
            )
        )
    elif tokens_dict["dtype"] == "boolean":
        return DerivationExpression(bool=Type.Boolean())
    elif tokens_dict["dtype"] == "i8":
        return DerivationExpression(i8=Type.I8())
    elif tokens_dict["dtype"] == "i16":
        return DerivationExpression(i16=Type.I16())
    elif tokens_dict["dtype"] == "i32":
        return DerivationExpression(i32=Type.I32())
    elif tokens_dict["dtype"] == "i64":
        return DerivationExpression(i64=Type.I64())
    elif tokens_dict["dtype"] == "fp32":
        return DerivationExpression(fp32=Type.FP32())
    elif tokens_dict["dtype"] == "fp64":
        return DerivationExpression(fp64=Type.FP64())
    else:
        raise Exception(f"Unknown dtype - {tokens_dict['dtype']}")


dtype = (
    Literal("i8")("dtype")
    | Literal("i16")("dtype")
    | Literal("i32")("dtype")
    | Literal("i64")("dtype")
    | Literal("fp32")("dtype")
    | Literal("fp64")("dtype")
    | Literal("boolean")("dtype")
    | oneOf("DECIMAL decimal")("dtype")
    + Literal("<").suppress()
    + expr("scale")
    + Literal(",").suppress()
    + expr("precision")
    + Literal(">").suppress()
).set_parse_action(parse_dtype)

supported_functions = ["max", "min"]


def parse_binary_fn(tokens: ParseResults):
    if tokens[0] == "min":
        op_type = DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_MIN
    elif tokens[0] == "max":
        op_type = DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_MAX
    else:
        raise Exception(f"Unknown operation {tokens[0]}")

    return DerivationExpression(
        binary_op=DerivationExpression.BinaryOp(
            op_type=op_type, arg1=tokens[1], arg2=tokens[2]
        )
    )


binary_fn = (
    oneOf(supported_functions)("fn")
    + Literal("(").suppress()
    + expr
    + Literal(",").suppress()
    + expr
    + Literal(")").suppress()
).set_parse_action(parse_binary_fn)

integer_literal = Word(nums).set_parse_action(
    lambda toks: DerivationExpression(integer_literal=int(toks[0]))
)


def parse_parameter(pr: ParseResults):
    return DerivationExpression(integer_parameter_name=pr[0])


parameter = Word(identchars + nums).set_parse_action(parse_parameter)

operand = integer_literal | binary_fn | dtype | parameter


def parse_binary_op(pr):
    tokens = pr[0]
    prev_expression = None
    for i in range(1, len(tokens), 2):
        if tokens[i] == "*":
            op_type = DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_MULTIPLY
        elif tokens[i] == "+":
            op_type = DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_PLUS
        elif tokens[i] == "-":
            op_type = DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_MINUS
        elif tokens[i] == ">":
            op_type = (
                DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_GREATER_THAN
            )
        elif tokens[i] == "<":
            op_type = (
                DerivationExpression.BinaryOp.BinaryOpType.BINARY_OP_TYPE_LESS_THAN
            )
        else:
            raise Exception(f"Unknown operation {tokens[i]}")

        prev_expression = DerivationExpression(
            binary_op=DerivationExpression.BinaryOp(
                op_type=op_type,
                arg1=prev_expression if prev_expression else tokens[i - 1],
                arg2=tokens[i + 1],
            )
        )

    return prev_expression


def parse_ternary(pr):
    tokens = pr[0]
    return DerivationExpression(
        if_else=DerivationExpression.IfElse(
            if_condition=tokens[0], if_return=tokens[1], else_return=tokens[2]
        )
    )


expr << infix_notation(
    operand,
    [
        (oneOf("* /")("binary_op"), 2, opAssoc.LEFT, parse_binary_op),
        (oneOf("+ -")("binary_op"), 2, opAssoc.LEFT, parse_binary_op),
        (oneOf("> <")("binary_op"), 2, opAssoc.LEFT, parse_binary_op),
        (
            (Literal("?").suppress(), Literal(":").suppress()),
            3,
            opAssoc.RIGHT,
            parse_ternary,
        ),
    ],
)


def parse_assignment(toks):
    tokens_dict = toks.as_dict()
    return DerivationExpression.ReturnProgram.Assignment(
        name=tokens_dict["name"], expression=tokens_dict["expression"]
    )


assignment = (
    Word(identchars + nums)("name") + Literal("=").suppress() + expr("expression")
).set_parse_action(parse_assignment)


def parse_return_program(toks):
    return DerivationExpression(
        return_program=DerivationExpression.ReturnProgram(
            assignments=toks.as_dict()["assignments"],
            final_expression=toks.as_dict()["final_expression"],
        )
    )


return_program = (
    ZeroOrMore(assignment)("assignments") + expr("final_expression")
).set_parse_action(parse_return_program)


def to_proto(txt: str):
    return return_program.parseString(txt)[0]


def evaluate_expression(de: DerivationExpression, values: Optional[dict] = None):
    kind = de.WhichOneof("kind")
    if kind == "return_program":
        for assign in de.return_program.assignments:
            values[assign.name] = evaluate_expression(assign.expression, values)
        return evaluate_expression(de.return_program.final_expression, values)
    elif kind == "integer_literal":
        return de.integer_literal
    elif kind == "integer_parameter_name":
        return values[de.integer_parameter_name]
    elif kind == "binary_op":
        binary_op = de.binary_op
        arg1_eval = evaluate_expression(binary_op.arg1, values)
        arg2_eval = evaluate_expression(binary_op.arg2, values)
        if binary_op.op_type == DerivationExpression.BinaryOp.BINARY_OP_TYPE_PLUS:
            return arg1_eval + arg2_eval
        elif binary_op.op_type == DerivationExpression.BinaryOp.BINARY_OP_TYPE_MINUS:
            return arg1_eval - arg2_eval
        elif binary_op.op_type == DerivationExpression.BinaryOp.BINARY_OP_TYPE_MULTIPLY:
            return arg1_eval * arg2_eval
        elif binary_op.op_type == DerivationExpression.BinaryOp.BINARY_OP_TYPE_MIN:
            return min(arg1_eval, arg2_eval)
        elif binary_op.op_type == DerivationExpression.BinaryOp.BINARY_OP_TYPE_MAX:
            return max(arg1_eval, arg2_eval)
        elif (
            binary_op.op_type
            == DerivationExpression.BinaryOp.BINARY_OP_TYPE_GREATER_THAN
        ):
            return arg1_eval > arg2_eval
        elif (
            binary_op.op_type == DerivationExpression.BinaryOp.BINARY_OP_TYPE_LESS_THAN
        ):
            return arg1_eval < arg2_eval
        else:
            raise Exception(f"Unknown binary op type - {binary_op.op_type}")
    elif kind == "if_else":
        if_else = de.if_else
        if_return_eval = evaluate_expression(if_else.if_return, values)
        if_condition_eval = evaluate_expression(if_else.if_condition, values)
        else_return_eval = evaluate_expression(if_else.else_return, values)
        return if_return_eval if if_condition_eval else else_return_eval
    elif kind == "decimal":
        decimal = de.decimal
        scale_eval = evaluate_expression(decimal.scale, values)
        precision_eval = evaluate_expression(decimal.precision, values)
        return to_substrait_type(f"decimal<{scale_eval},{precision_eval}>")
    elif kind in ("i8", "i16", "i32", "i64", "fp32", "fp64"):
        return to_substrait_type(kind)
    elif kind == "bool":
        return to_substrait_type("boolean")
    else:
        raise Exception(f"Unknown derivation expression type - {kind}")


def evaluate(txt: str, values: Optional[dict] = None):
    if not values:
        values = {}
    return evaluate_expression(to_proto(txt), values)
