import substrait.gen.proto.algebra_pb2 as stalg
import subframe
from subframe.utils import infer_rel_schema

orders_raw = [
    ("order_id", "int64"),
    ("fk_store_id", "int64"),
    ("fk_customer_id", "int64"),
    ("description", "string"),
    ("order_total", "float"),
]

orders = subframe.table([(x[0], x[1]) for x in orders_raw], name="orders")


def test_inference_read():
    assert orders.struct == infer_rel_schema(orders.plan.input)


def test_inference_project_emit():
    table = orders.select("order_id", "fk_customer_id")

    removed_with_emit = stalg.Rel(
        project=stalg.ProjectRel(
            input=orders.plan.input,
            common=stalg.RelCommon(emit=stalg.RelCommon.Emit(output_mapping=[0, 2])),
        )
    )

    assert table.struct == infer_rel_schema(removed_with_emit)


def test_inference_filter():
    table = orders.filter(orders["order_id"] > orders["fk_store_id"])
    assert table.struct == infer_rel_schema(table.plan.input)


def test_inference_project_simple():
    table = orders.select("order_id", "fk_store_id")
    assert table.struct == infer_rel_schema(table.plan.input)


def test_inference_project_literal():
    table = orders.select("order_id", "fk_store_id", subframe.literal(True))
    assert table.struct == infer_rel_schema(table.plan.input)


def test_inference_project_scalar():
    table = orders.select(
        orders["order_id"] > orders["fk_store_id"], subframe.literal(True)
    )
    assert table.struct == infer_rel_schema(table.plan.input)
