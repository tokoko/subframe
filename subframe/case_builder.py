from substrait.gen.proto import algebra_pb2 as stalg
from .value import Value


def merge_extensions(extensions, exprs):
    for c in exprs:
        if c.extensions:
            for k, v in c.extensions.items():
                if k in extensions:
                    for k1, v1 in c.extensions[k].items():
                        extensions[k][k1] = v1
                else:
                    extensions[k] = v

    return extensions


class CaseBuilder:
    def __init__(self):
        self.cases = []
        self.otherwise = None
        self.extensions = {}

    def when(self, if_value: Value, then_value: Value):
        self.cases.append((if_value, then_value))
        self.extensions = merge_extensions(self.extensions, [if_value, then_value])
        return self

    def else_(self, else_value: Value):
        self.otherwise = else_value
        self.extensions = merge_extensions(self.extensions, [else_value])
        return self

    def end(self):
        def build_if_clause(case):
            if_clause = stalg.Expression.IfThen.IfClause(
                **{"if": case[0].expression, "then": case[1].expression}
            )
            return if_clause

        if_then = stalg.Expression.IfThen(
            **{
                "ifs": [build_if_clause(case) for case in self.cases],
                "else": self.otherwise.expression,
            }
        )

        return Value(
            expression=stalg.Expression(if_then=if_then),
            data_type=self.otherwise.data_type,  # TODO validate type, allow ommitin else
            name="IfThen",  # TODO update to match ibis
            extensions=self.extensions,
        )
