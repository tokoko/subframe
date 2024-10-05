from substrait.gen.proto import algebra_pb2 as stalg
from substrait.gen.proto import type_pb2 as stt

# from .table import Table


def substrait_type_from_substrait_str(data_type: str) -> stt.Type:
    data_type = data_type.replace("?", "")  # TODO
    if data_type == "i64":
        return stt.Type(i64=stt.Type.I64(nullability=stt.Type.NULLABILITY_NULLABLE))
    elif data_type == "fp64":
        return stt.Type(fp64=stt.Type.FP64(nullability=stt.Type.NULLABILITY_NULLABLE))
    elif data_type == "boolean":
        return stt.Type(
            bool=stt.Type.Boolean(nullability=stt.Type.NULLABILITY_NULLABLE)
        )
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

        (func_entry, rtn) = registry.lookup_function(
            url,
            function_name=func,
            signature=[
                self.data_type,
                other.data_type,
            ],
        )

        output_type = rtn

        return Value(
            expression=stalg.Expression(
                scalar_function=stalg.Expression.ScalarFunction(
                    function_reference=func_entry.anchor,
                    output_type=output_type,
                    arguments=[
                        stalg.FunctionArgument(value=self.expression),
                        stalg.FunctionArgument(value=other.expression),
                    ],
                )
            ),
            data_type=output_type,
            name=f"{col_name}({self._name}, {other._name})",
            extensions={func_entry.uri: {str(func_entry): func_entry.anchor}},
        )

    def __add__(self, other: "Value"):
        return self._apply_function(
            other=other,
            url="functions_arithmetic.yaml",
            func="add",
            col_name="Add",
        )

    def __sub__(self, other: "Value"):
        return self._apply_function(
            other=other,
            url="functions_arithmetic.yaml",
            func="subtract",
            col_name="Subtract",
        )

    def __eq__(self, other: "Value"):
        return self._apply_function(
            other=other,
            url="functions_comparison.yaml",
            func="equal",
            col_name="Equals",
        )

    def __ne__(self, other: "Value"):
        return self._apply_function(
            other=other,
            url="functions_comparison.yaml",
            func="not_equal",
            col_name="NotEquals",
        )

    def __lt__(self, other: "Value"):
        return self._apply_function(
            other=other, url="functions_comparison.yaml", func="lt", col_name="Less"
        )

    def __le__(self, other: "Value"):
        return self._apply_function(
            other=other,
            url="functions_comparison.yaml",
            func="lte",
            col_name="LessEqual",
        )

    def __gt__(self, other: "Value"):
        return self._apply_function(
            other=other, url="functions_comparison.yaml", func="gt", col_name="Greater"
        )

    def __ge__(self, other: "Value"):
        return self._apply_function(
            other=other,
            url="functions_comparison.yaml",
            func="gte",
            col_name="GreaterEqual",
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

    def _apply_aggregate_function(self, url: str, func: str, col_name: str):
        from subframe import registry

        (func_entry, rtn) = registry.lookup_function(
            url, function_name=func, signature=[self.data_type]
        )

        output_type = rtn

        aggregate_function = stalg.AggregateFunction(
            function_reference=func_entry.anchor,
            phase=stalg.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT,  # TODO
            arguments=[stalg.FunctionArgument(value=self.expression)],
            output_type=output_type,
        )

        return AggregateValue(
            aggregate_function=aggregate_function,
            data_type=output_type,
            extensions={func_entry.uri: {str(func_entry): func_entry.anchor}},
            name=f"{col_name}({self._name})",
        )

    def max(self):

        return self._apply_aggregate_function(
            url="functions_arithmetic.yaml",
            func="max",
            col_name="Max",
        )

    def min(self):

        return self._apply_aggregate_function(
            url="functions_arithmetic.yaml",
            func="min",
            col_name="Min",
        )

    def mean(self):

        return self._apply_aggregate_function(
            url="functions_arithmetic.yaml",
            func="avg",
            col_name="Mean",
        )

    def mode(self):

        return self._apply_aggregate_function(
            url="functions_arithmetic.yaml",
            func="mode",
            col_name="Mode",
        )

    def count(self):

        return self._apply_aggregate_function(
            url="functions_aggregate_generic.yaml",
            func="count",
            col_name="Count",
        )

    # TODO lacks required option
    def median(self):

        return self._apply_aggregate_function(
            url="functions_arithmetic.yaml",
            func="median",
            col_name="Median",
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
