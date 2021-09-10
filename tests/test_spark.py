import numpy as np
import pandas as pd

from feaflow.engine.spark import SparkEngine, SparkEngineSession
from feaflow.job import Job, JobConfig, scan_jobs_from_project


def test_run_job1(example_project):
    engine = example_project.get_engine("default_spark")
    assert type(engine) == SparkEngine

    job_confs = scan_jobs_from_project(example_project)
    test_job1_conf: JobConfig = next(filter(lambda j: j.name == "test_job1", job_confs))
    test_job1 = Job(example_project, test_job1_conf)

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

        sink_df = spark_session.table("feaflow_table_sink_test")
        sink_df.printSchema()
        sink_df.show()
