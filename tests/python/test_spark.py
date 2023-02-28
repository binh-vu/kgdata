import tempfile

import pytest, os, orjson, shutil
from kgdata.spark import get_spark_context, left_outer_join, left_outer_join_broadcast
from operator import itemgetter


@pytest.mark.skipif(
    os.environ.get("TEST_SPARK", "false") == "false", reason="TEST_SPARK=false"
)
def test_left_outer_join():
    # use a temporary directory to test our result
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpfile = os.path.join(tmpdir, "tmpfile")
        assert not os.path.exists(tmpfile), tmpfile
        os.environ["spark.master"] = "local[1]"
        sc = get_spark_context()

        rdd1 = sc.parallelize(
            [
                {"department": 1, "employees": [0, 1, 2, 3]},
                {"department": 2, "employees": [3, 5, 4]},
            ]
        )

        rdd2 = sc.parallelize(
            [
                {"id": 0, "name": "Peter"},
                {"id": 1, "name": "Bob"},
                {"id": 2, "name": "John"},
                {"id": 3, "name": "Marry"},
                {"id": 4, "name": "Jessie"},
            ]
        )

        rdd1_keyfn = itemgetter("department")
        rdd1_fk_fn = itemgetter("employees")
        rdd2_keyfn = itemgetter("id")

        def rdd1_join_fn(r, v):
            v = dict(v)
            r["employees"] = [
                v[eid]["name"] for eid in r["employees"] if v[eid] is not None
            ]
            return r

        rdd1_serfn = orjson.dumps
        left_outer_join(
            rdd1,
            rdd2,
            rdd1_keyfn,
            rdd1_fk_fn,
            rdd2_keyfn,
            rdd1_join_fn,
            rdd1_serfn,
            tmpfile,
        )

        result = sc.textFile(tmpfile).map(orjson.loads).collect()
        assert sorted(result, key=rdd1_keyfn) == [
            {"department": 1, "employees": ["Peter", "Bob", "John", "Marry"]},
            {"department": 2, "employees": ["Marry", "Jessie"]},
        ]

        shutil.rmtree(tmpfile)

        left_outer_join_broadcast(
            rdd1, rdd2, rdd1_fk_fn, rdd2_keyfn, rdd1_join_fn, rdd1_serfn, tmpfile
        )

        result = sc.textFile(tmpfile).map(orjson.loads).collect()
        assert sorted(result, key=rdd1_keyfn) == [
            {"department": 1, "employees": ["Peter", "Bob", "John", "Marry"]},
            {"department": 2, "employees": ["Marry", "Jessie"]},
        ]
