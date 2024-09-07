from substrait.gen.proto import algebra_pb2 as stalg
from substrait.gen.proto import type_pb2 as stt


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

    def __add__(self, other: "Value"):
        from subframe import registry

        res = registry.lookup_scalar_function(
            "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml",
            "add",
        ).lookup_signature(
            [self.data_type.WhichOneof("kind"), other.data_type.WhichOneof("kind")]
        )

        return_type = res[2]["return"]

        if return_type == "i64":
            output_type = stt.Type(
                i64=stt.Type.I64(nullability=stt.Type.NULLABILITY_NULLABLE)
            )
        else:
            raise Exception(f"Unknown return type {return_type}")

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
            name=f"Add({self._name}, {other._name})",
            extensions={
                "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml": {
                    res[1]: res[0]
                }
            },
        )
