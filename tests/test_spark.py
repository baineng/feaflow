import numpy as np
import pandas as pd
from pandas._testing import assert_frame_equal
from pyspark.sql import SparkSession

from feaflow.engine.spark import SparkEngine, SparkEngineSession


def prepare_dataset(spark: SparkSession) -> pd.DataFrame:
    events_dataset = pd.DataFrame(
        {
            "id": np.arange(1001, 1011),
            "title": (
                ["message.send"] * 2
                + ["user.login"] * 3
                + ["user.signup"] * 1
                + ["user.logout"] * 4
            ),
            "published": pd.date_range("2021-09-10", periods=10),
        }
    )
    events_df = spark.createDataFrame(events_dataset)
    events_df.createOrReplaceTempView("events")

    expected_result = pd.DataFrame(
        {
            "title": ["message.send", "user.login", "user.signup", "user.logout"],
            "amount": [2, 3, 1, 4],
        }
    )
    return expected_result


def test_run_job1(example_project, job1, tmpdir):
    engine = example_project.get_engine_by_name("default_spark")
    assert type(engine) == SparkEngine

    with engine.new_session() as engine_session:
        assert isinstance(engine_session, SparkEngineSession)
        spark_session = engine_session.get_or_create_spark_session(
            "test_job1",
            config_overlay={"config": {"spark.sql.warehouse.dir": f"file://{tmpdir}"}},
        )
        expected = prepare_dataset(spark_session)
        engine_session.run(job1)

        sink_df = spark_session.table("test_sink_table")
        sink_df.show()
        real = sink_df.toPandas()
        assert_frame_equal(
            expected.sort_values(by=["title"]).reset_index(drop=True),
            real.sort_values(by=["title"]).reset_index(drop=True),
            check_dtype=False,
        )
