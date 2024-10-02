import pyarrow as pa
import pyarrow.compute as pc
import pytest
import ibis
import subframe
import tempfile
import os
from ibis_substrait.compiler.core import SubstraitCompiler
from .consumers.consumer import SubstraitConsumer
from .consumers.duckdb import DuckDbSubstraitConsumer
from .consumers.acero import AceroSubstraitConsumer
from .consumers.datafusion import DatafusionSubstraitConsumer


orders_raw = [
    ("order_id", "int64", [1, 2, 3, 4]),
    ("fk_store_id", "int64", [1, 1, 2, 2]),
    ("fk_customer_id", "int64", [10, 11, 13, 13]),
    ("description", "string", ["A", "B", "C", "D"]),
    ("order_total", "float", [10.0, 32.3, 32.0, 140.0]),
]

stores_raw = [("store_id", "int64", [1, 2, 3]), ("city", "string", ["NY", "LA", "NY"])]

customers_raw = [
    ("customer_id", "int64", [10, 11, 13]),
    ("name", "string", ["Ann", "Bob", "Chris"]),
]

datasets = {
    "orders": pa.Table.from_pydict({x[0]: x[2] for x in orders_raw}),
    "stores": pa.Table.from_pydict({x[0]: x[2] for x in stores_raw}),
    "customers": pa.Table.from_pydict({x[0]: x[2] for x in customers_raw}),
}

# orders = ibis.table([(x[0], x[1]) for x in orders_raw], name="orders")
# stores = ibis.table([(x[0], x[1]) for x in stores_raw], name="stores")
# customers = ibis.table([(x[0], x[1]) for x in customers_raw], name="customers")


# orders_sf = subframe.table([(x[0], x[1]) for x in orders_raw], name="orders")

consumer = DuckDbSubstraitConsumer().with_tables(datasets)


def _orders(module):
    return module.table([(x[0], x[1]) for x in orders_raw], name="orders")


def _stores(module):
    return module.table([(x[0], x[1]) for x in stores_raw], name="stores")


def sort_pyarrow_table(table: pa.Table):
    sort_keys = [(name, "ascending") for name in table.column_names]
    sort_indices = pc.sort_indices(table, sort_keys)
    return pc.take(table, sort_indices)


def run_query_duckdb(query, datasets):
    with tempfile.TemporaryDirectory() as tempdir:
        con = ibis.duckdb.connect(os.path.join(tempdir, "temp.db"))
        for table_name, pa_table in datasets.items():
            con.create_table(name=table_name, obj=ibis.memtable(pa_table))

        # TODO con.to_pyarrow(query) in duckdb backend doesn't work with latest ibis and pyarrow versions
        res = pa.Table.from_pandas(con.to_pandas(query))
        con.disconnect()
        return res


def run_parity_test(
    consumer: SubstraitConsumer, expr: ibis.Table, expr_sf: subframe.Table
):
    res_duckdb = sort_pyarrow_table(run_query_duckdb(expr, datasets))

    # plan_ibis = SubstraitCompiler().compile(expr)
    plan_sf = expr_sf.to_plan()
    res_sf = sort_pyarrow_table(consumer.execute(plan_sf))
    # res_ibis = sort_pyarrow_table(consumer.execute(plan_ibis))

    print(res_duckdb.to_pandas())
    print("---------------")
    print(res_sf.to_pandas())
    print("---------------")
    # print(res_ibis.to_pandas())

    assert res_sf.to_pandas().equals(res_duckdb.to_pandas())
    # assert res_ibis.to_pandas().equals(res_duckdb.to_pandas())


@pytest.fixture
def acero_consumer():
    return AceroSubstraitConsumer().with_tables(datasets)


@pytest.fixture
def datafusion_consumer():
    return DatafusionSubstraitConsumer().with_tables(datasets)


@pytest.fixture
def duckdb_consumer():
    return DuckDbSubstraitConsumer().with_tables(datasets)


@pytest.mark.parametrize(
    "consumer", ["acero_consumer", "datafusion_consumer", "duckdb_consumer"]
)
def test_projection(consumer, request):

    def transform(module):
        table = _orders(module)
        return table.select(
            "order_id",
            "description",
            table["fk_store_id"] + table["order_id"],
            table["fk_store_id"] - table["order_id"],
            module.literal(1, type="int32").name("one"),
            two=module.literal(2, type="int32"),
        )

    ibis_expr = transform(ibis)
    sf_expr = transform(subframe)

    run_parity_test(request.getfixturevalue(consumer), ibis_expr, sf_expr)


@pytest.mark.parametrize(
    "consumer",
    [
        "acero_consumer",
        "datafusion_consumer",
        pytest.param(
            "duckdb_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
    ],
)
def test_projection_comparisions(consumer, request):

    def transform(module):
        table = _orders(module)
        return table.select(
            table["order_id"] == table["fk_store_id"],
            table["order_id"] != table["fk_store_id"],
            table["order_id"] > table["fk_store_id"],
            table["order_id"] >= table["fk_store_id"],
            table["order_id"] < table["fk_store_id"],
            table["order_id"] <= table["fk_store_id"],
        )

    ibis_expr = transform(ibis)
    sf_expr = transform(subframe)

    run_parity_test(request.getfixturevalue(consumer), ibis_expr, sf_expr)


@pytest.mark.parametrize(
    "consumer",
    [
        "acero_consumer",
        "datafusion_consumer",
        pytest.param(
            "duckdb_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
    ],
)
def test_projection_literals(consumer, request):

    def transform(module):
        table = _orders(module)
        return table.select(
            module.literal(1, type="int32").name("one"), module.literal(True)
        )

    ibis_expr = transform(ibis)
    sf_expr = transform(subframe)

    run_parity_test(request.getfixturevalue(consumer), ibis_expr, sf_expr)


@pytest.mark.parametrize(
    "consumer",
    [
        "acero_consumer",
        "datafusion_consumer",
        pytest.param(
            "duckdb_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
    ],
)
def test_filter(consumer, request):

    def transform(module):
        table = _orders(module)
        return table.filter(module.literal(True)).filter(
            table["order_id"] == table["fk_store_id"]
        )

    ibis_expr = transform(ibis)
    sf_expr = transform(subframe)

    run_parity_test(request.getfixturevalue(consumer), ibis_expr, sf_expr)


@pytest.mark.parametrize(
    "consumer",
    [
        "acero_consumer",
        "datafusion_consumer",
        pytest.param(
            "duckdb_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
    ],
)
def test_aggregate_group_by(consumer, request):

    def transform(module):
        table = _orders(module)
        return (
            table.group_by(fk_store_id="fk_store_id").agg(
                table["order_total"].count(),
                table["order_total"].max(),
                table["order_total"].min(),
                # table["order_total"].median(),
                table["order_total"].mean(),
                # table["order_total"].mode(),
            )
            # TODO datafusion workaround, remove later
            .filter(module.literal(True))
        )

    ibis_expr = transform(ibis)
    sf_expr = transform(subframe)

    run_parity_test(request.getfixturevalue(consumer), ibis_expr, sf_expr)


@pytest.mark.parametrize(
    "consumer",
    [
        "acero_consumer",
        "datafusion_consumer",
        pytest.param(
            "duckdb_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
    ],
)
def test_aggregate(consumer, request):

    def transform(module):
        table = _orders(module)
        return table.aggregate(
            by=["fk_store_id"],
            metrics=[table["order_total"].max(), table["order_total"].min()],
        ).filter(
            module.literal(True)
        )  # TODO datafusion workaround, remove later

    ibis_expr = transform(ibis)
    sf_expr = transform(subframe)

    run_parity_test(request.getfixturevalue(consumer), ibis_expr, sf_expr)


@pytest.mark.parametrize(
    "consumer",
    [
        "acero_consumer",
        "datafusion_consumer",
        pytest.param(
            "duckdb_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
    ],
)
def test_limit(consumer, request):

    def transform(module):
        table = _orders(module)
        return table.limit(2, 0)

    ibis_expr = transform(ibis)
    sf_expr = transform(subframe)

    run_parity_test(request.getfixturevalue(consumer), ibis_expr, sf_expr)


@pytest.mark.parametrize(
    "consumer",
    [
        pytest.param(
            "acero_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
        "datafusion_consumer",
        pytest.param(
            "duckdb_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
    ],
)
def test_union_all(consumer, request):

    def transform(module):
        t1 = _orders(module)

        t2 = _orders(module)
        t3 = _orders(module)
        t1 = t1.select(
            "order_id",
            "description",
            ids=t1["fk_store_id"] > t1["order_id"],
        )
        t2 = t2.select(
            "order_id",
            "description",
            ids=t2["fk_store_id"] < t2["order_id"],
        )
        t3 = t3.select(
            "order_id",
            "description",
            ids=t3["fk_store_id"] > t3["order_id"],
        )
        return t1.union(t2, t3, distinct=False)

    ibis_expr = transform(ibis)
    sf_expr = transform(subframe)

    run_parity_test(request.getfixturevalue(consumer), ibis_expr, sf_expr)


@pytest.mark.parametrize(
    "consumer",
    [
        pytest.param(
            "acero_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
        pytest.param(
            "datafusion_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
        pytest.param(
            "duckdb_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
    ],
)
def test_union_distinct(consumer, request):

    def transform(module):
        t1 = _orders(module)
        t2 = _orders(module)
        return t1.union(t2)

    ibis_expr = transform(ibis)
    sf_expr = transform(subframe)

    run_parity_test(request.getfixturevalue(consumer), ibis_expr, sf_expr)


@pytest.mark.parametrize(
    "consumer",
    [
        pytest.param(
            "acero_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
        pytest.param(
            "datafusion_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
        pytest.param(
            "duckdb_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
    ],
)
def test_intersect(consumer, request):

    def transform(module):
        t1 = _orders(module)
        t2 = _orders(module)
        return t1.intersect(t2, distinct=False)

    ibis_expr = transform(ibis)
    sf_expr = transform(subframe)

    run_parity_test(request.getfixturevalue(consumer), ibis_expr, sf_expr)


@pytest.mark.parametrize(
    "consumer",
    [
        pytest.param(
            "acero_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
        pytest.param(
            "datafusion_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
        pytest.param(
            "duckdb_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
    ],
)
def test_difference(consumer, request):

    def transform(module):
        t1 = _orders(module)
        t2 = _orders(module)
        return t1.intersect(t2, distinct=False)

    ibis_expr = transform(ibis)
    sf_expr = transform(subframe)

    run_parity_test(request.getfixturevalue(consumer), ibis_expr, sf_expr)


@pytest.mark.parametrize(
    "consumer",
    [
        "acero_consumer",
        "datafusion_consumer",
        pytest.param(
            "duckdb_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
    ],
)
def test_order_by(consumer, request):

    def transform(module):
        table = _orders(module)
        return table.order_by("fk_customer_id")

    ibis_expr = transform(ibis)
    sf_expr = transform(subframe)

    run_parity_test(request.getfixturevalue(consumer), ibis_expr, sf_expr)


@pytest.mark.parametrize(
    "consumer",
    [
        pytest.param(
            "acero_consumer",
            marks=[
                pytest.mark.xfail(pa.ArrowNotImplementedError, reason="Unimplemented")
            ],
        ),
        "datafusion_consumer",
        pytest.param(
            "duckdb_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
    ],
)
def test_scalar_subquery(consumer, request):

    def transform(module):
        orders = _orders(module)
        stores = _stores(module)

        return orders.select(
            orders["fk_store_id"],
            stores.aggregate(by=[], metrics=[stores["store_id"].max()]).as_scalar(),
        )

    ibis_expr = transform(ibis)
    sf_expr = transform(subframe)

    run_parity_test(request.getfixturevalue(consumer), ibis_expr, sf_expr)


@pytest.mark.parametrize(
    "consumer",
    [
        pytest.param(
            "acero_consumer",
            marks=[
                pytest.mark.xfail(pa.ArrowNotImplementedError, reason="Unimplemented")
            ],
        ),
        "datafusion_consumer",
        pytest.param(
            "duckdb_consumer",
            marks=[pytest.mark.xfail(Exception, reason="Unimplemented")],
        ),
    ],
)
def test_cross_join(consumer, request):

    def transform(module):
        t1 = _orders(module)
        t2 = _stores(module)
        return t1.cross_join(t2)

    ibis_expr = transform(ibis)
    sf_expr = transform(subframe)

    run_parity_test(request.getfixturevalue(consumer), ibis_expr, sf_expr)
