from substrait.gen.proto import algebra_pb2 as stalg
from substrait.gen.proto import type_pb2 as stt

# from .table import Table


def substrait_type_from_substrait_str(data_type: str) -> stt.Type:
    data_type = data_type.replace("?", "")  # TODO
    if data_type == "i64":
        return stt.Type(i64=stt.Type.I64(nullability=stt.Type.NULLABILITY_NULLABLE))
    elif data_type == "fp64":
        return stt.Type(fp64=stt.Type.FP64(nullability=stt.Type.NULLABILITY_NULLABLE))
    else:
        raise Exception(f"Unknown data type {data_type}")


class Value:
    def __init__(
        self,
        expression: stalg.Expression,
        data_type: stt.Type,
        name: str = "",
        extensions={},
    ):
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
            extensions={url: {res[1]: res[0]}},
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


class Column(Value):
    def __init__(
        self,
        expression: stalg.Expression,
        data_type: stt.Type,
        table,
        name: str = "",
        extensions={},
    ):
        super().__init__(
            expression=expression, data_type=data_type, name=name, extensions=extensions
        )
        self.table = table

    def max(self):
        from subframe import registry

        url = "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"
        func = "max"

        res = registry.lookup_aggregate_function(url, func).lookup_signature(
            [self.data_type.WhichOneof("kind")]
        )

        output_type = substrait_type_from_substrait_str(res[2]["return"])

        aggregate_function = stalg.AggregateFunction(
            function_reference=res[0],
            phase=stalg.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT,
            arguments=[stalg.FunctionArgument(value=self.expression)],
            output_type=output_type,
        )

        return AggregateValue(
            aggregate_function=aggregate_function,
            data_type=output_type,
            extensions={url: {res[1]: res[0]}},
            name=f"Max({self._name})",
        )


class AggregateValue:
    def __init__(
        self,
        aggregate_function: stalg.AggregateFunction,
        data_type: stt.Type,
        name: str,
        extensions={},
    ) -> None:
        self.aggregate_function = aggregate_function
        self.data_type = data_type
        self.name = name
        self.extensions = extensions
