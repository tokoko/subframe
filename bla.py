from subframe.extension_registry import FunctionRegistry

registry = FunctionRegistry()


# from pprint import pprint
# from subframe.derivation_expression import to_proto, evaluate_expression

# txt = """
#     init_scale = max(S1,S2)
#     init_prec = init_scale + max(P1 - S1, P2 - S2) + 1
#     min_scale = min(init_scale, 6)
#     delta = init_prec - 38
#     prec = min(init_prec, 38)
#     scale_after_borrow = max(init_scale - delta, min_scale)
#     scale = init_prec > 38 ? scale_after_borrow : init_scale
#     DECIMAL<prec, scale>
#     """

# p = to_proto(txt)
# print(p)

# print(evaluate_expression(p, {"P1": 10, "S1": 8, "P2": 14, "S2": 2}))
