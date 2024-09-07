from substrait.gen.proto import algebra_pb2 as stalg
from substrait.gen.proto import type_pb2 as stt


def substrait_type_from_substrait_str(data_type: str) -> stt.Type:
    if data_type == "i64":
        return stt.Type(i64=stt.Type.I64(nullability=stt.Type.NULLABILITY_NULLABLE))
    else:
        raise Exception(f"Unknown data type {data_type}")


class Value:
    def __init__(
        self,
        expression: stalg.Expression,
        data_type: stt.Type,
        name: str = "",
        extensions={},
    ) -> None:
        self.expression = expression
        self._name = name
        self.extensions = extensions
        self.data_type = data_type

    def name(self, name: str):
        return Value(
            expression=self.expression,
            data_type=self.data_type,
            name=name,
            extensions=self.extensions,
        )

    def _apply_function(self, other: "Value", url: str, func: str, col_name: str):
        from subframe import registry

        res = registry.lookup_scalar_function(url, func).lookup_signature(
            [self.data_type.WhichOneof("kind"), other.data_type.WhichOneof("kind")]
        )

        output_type = substrait_type_from_substrait_str(res[2]["return"])

        return Value(
            expression=stalg.Expression(
                scalar_function=stalg.Expression.ScalarFunction(
                    function_reference=res[0],
                    output_type=output_type,
                    arguments=[
                        stalg.FunctionArgument(value=self.expression),
                        stalg.FunctionArgument(value=other.expression),
                    ],
                )
            ),
            data_type=output_type,
            name=f"{col_name}({self._name}, {other._name})",
            extensions={
                "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml": {
                    res[1]: res[0]
                }
            },
        )

    def __add__(self, other: "Value"):
        return self._apply_function(
            other=other,
            url="https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml",
            func="add",
            col_name="Add",
        )

    def __sub__(self, other: "Value"):
        return self._apply_function(
            other=other,
            url="https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml",
            func="subtract",
            col_name="Subtract",
        )
