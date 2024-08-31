from substrait.gen.proto import algebra_pb2 as stalg
from substrait.gen.proto import type_pb2 as stt


# TODO use ExpressionReference instead??
class Value:
    def __init__(
        self, expression: stalg.Expression, name: str = "", extensions={}
    ) -> None:
        self.expression = expression
        self._name = name
        self.extensions = extensions

    def name(self, name: str):
        return Value(expression=self.expression, name=name)

    def __add__(self, other: "Value"):
        # TODO unique placeholder for function_reference???
        return Value(
            expression=stalg.Expression(
                scalar_function=stalg.Expression.ScalarFunction(
                    function_reference=1,
                    output_type=stt.Type(
                        i64=stt.Type.I64(nullability=stt.Type.NULLABILITY_NULLABLE)
                    ),
                    arguments=[
                        stalg.FunctionArgument(value=self.expression),
                        stalg.FunctionArgument(value=other.expression),
                    ],
                )
            ),
            name=f"Add({self._name}, {other._name})",
            extensions={
                "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml": {
                    "add:i64_i64": 1  # TODO type inference
                }
            },
        )
