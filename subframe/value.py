from substrait.gen.proto import algebra_pb2 as stalg
from substrait.gen.proto import type_pb2 as stt
from subframe.utils import field_reference_transformer, visit

# from .table import Table


def off(tables):
    count = 0
    offsets = {}
    for t in tables:
        new_count = count + len(t.names)
        offsets[t] = (count, new_count)
        count = new_count

    return offsets


def tran(offsets, new_offsets):
    transforms = []

    for t, offset in offsets.items():
        incr = new_offsets[t][0] - offset[0]
        if incr != 0:
            transforms.append((offset, incr))

    return transforms


class Value:
    def __init__(
        self,
        expression: stalg.Expression,
        data_type: stt.Type,
        tables: list,
        name: str = "",
        extensions={},
    ):
        self.expression = expression
        self._name = name
        self.tables = tables
        self.extensions = extensions
        self.data_type = data_type

    def name(self, name: str):
        return Value(
            expression=self.expression,
            data_type=self.data_type,
            name=name,
            tables=self.tables,
            extensions=self.extensions,
        )

    def readjust(self, new_tables):
        offsets = off(self.tables)
        new_offsets = off(new_tables)
        transforms = tran(offsets, new_offsets)

        if transforms:
            new_expression = stalg.Expression()
            new_expression.CopyFrom(self.expression)
            visit(new_expression, field_reference_transformer(transforms))
        else:
            new_expression = self.expression

        return Value(
            expression=new_expression,
            data_type=self.data_type,
            name=self._name,
            tables=new_tables,
            extensions=self.extensions,
        )

    def _apply_function(self, other: "Value", url: str, func: str, col_name: str):
        from subframe import registry

        new_tables = []

        for t in self.tables:
            new_tables.append(t)

        for t in other.tables:
            if t not in self.tables:
                new_tables.append(t)

        other = other.readjust(new_tables)

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
            tables=new_tables,
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

    def _apply_window_function(
        self, additional_arguments: list["Value"], url: str, func: str, col_name: str
    ):
        from subframe import registry

        (func_entry, rtn) = registry.lookup_function(
            url,
            function_name=func,
            signature=[
                self.data_type,
                *[a.data_type for a in additional_arguments],
            ],
        )

        output_type = rtn

        expression = stalg.Expression(
            window_function=stalg.Expression.WindowFunction(
                function_reference=func_entry.anchor,
                arguments=[
                    stalg.FunctionArgument(value=self.expression),
                    *[
                        stalg.FunctionArgument(value=a.expression)
                        for a in additional_arguments
                    ],
                ],
                options=[],
                phase=stalg.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT,
                sorts=[],
                partitions=[],
            )
        )

        return Value(
            expression=expression,
            data_type=output_type,
            name=f"{col_name}({self._name}, {' ,'.join([a._name for a in additional_arguments])})",
            extensions={func_entry.uri: {str(func_entry): func_entry.anchor}},
            tables=self.tables,
        )

    def lead(self, offset):  # TODO default
        from subframe import literal

        if type(offset) == int:
            offset = literal(offset, type="i32")

        return self._apply_window_function(
            [offset], "functions_arithmetic.yaml", "lead", "Lead"
        )

    def lag(self, offset):  # TODO default
        from subframe import literal

        if type(offset) == int:
            offset = literal(offset, type="i32")

        return self._apply_window_function(
            [offset], "functions_arithmetic.yaml", "lag", "Lag"
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
