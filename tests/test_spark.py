import random
import time

import numpy as np
import pandas as pd

from feaflow.engine.spark import SparkEngine, SparkEngineSession
from feaflow.job import Job, JobConfig


def test_run_job1(example_project):
    engine = example_project.get_engine_by_name("default_spark")
    assert type(engine) == SparkEngine

    jobs = example_project.scan_jobs()
    test_job1: Job = next(filter(lambda j: j.name == "test_job1", jobs))
    test_job1_config = test_job1.config
    temp_sink_table = f"feaflow_table_sink_test_test_{int(time.time_ns())}_{random.randint(1000, 9999)}"
    patch_conf = test_job1_config.copy(
        update={
            "sinks": [test_job1_config.sinks[0].copy(update={"name": temp_sink_table})]
        }
    )
    patch_job = Job(patch_conf)

    original_data = pd.DataFrame(
        {
            "id": np.arange(1001, 1011),
            "title": np.random.choice(
                ["message.send", "user.login", "user.signup", "user.logout"], 10
            ),
            "published": pd.date_range("2021-09-10", periods=10),
        }
    )

    with engine.new_session() as session:
        assert isinstance(session, SparkEngineSession)
        spark_session = session.get_or_create_spark_session("test_job1")
        origin_df = spark_session.createDataFrame(original_data)
        origin_df.createOrReplaceTempView("events")
        # session.run(test_job1)
        origin_df.printSchema()
        origin_df.show()

        session.run(patch_job)

        sink_df = spark_session.table(temp_sink_table)
        sink_df.printSchema()
        sink_df.show()

        spark_session.sql(f"desc formatted {temp_sink_table}").show(100, False)

        time.sleep(3)
