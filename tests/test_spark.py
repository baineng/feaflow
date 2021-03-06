from datetime import datetime

import numpy as np
import pandas as pd
import pytest
from pandas._testing import assert_frame_equal
from pyspark.sql import SparkSession

from feaflow.utils import construct_template_context


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


@pytest.mark.integration
def test_run_job1(spark_exec_env, project_misc, job1):
    expected = prepare_dataset(spark_exec_env.spark_session)

    execution_date = datetime.utcnow()
    template_context = construct_template_context(
        project_misc, job1.config, execution_date
    )
    spark_exec_env.engine_session.run(job1, execution_date, template_context)

    sink_df = spark_exec_env.spark_session.table("test_sink_table")
    real = sink_df.toPandas()
    assert_frame_equal(
        expected.sort_values(by=["title"]).reset_index(drop=True)[
            sorted(expected.columns)
        ],
        real.sort_values(by=["title"]).reset_index(drop=True)[sorted(real.columns)],
        check_dtype=False,
    )


@pytest.mark.integration
def test_run_job2(spark_exec_env, project_misc, job2, job2_expect_result):
    execution_date = datetime.utcnow()
    template_context = construct_template_context(
        project_misc, job2.config, execution_date
    )
    spark_exec_env.engine_session.run(job2, execution_date, template_context)

    sink_df = spark_exec_env.spark_session.table("test_sink_table")
    real = sink_df.toPandas()
    assert_frame_equal(
        job2_expect_result.sort_values(by=["title"]).reset_index(drop=True)[
            sorted(job2_expect_result.columns)
        ],
        real.sort_values(by=["title"]).reset_index(drop=True)[sorted(real.columns)],
        check_dtype=False,
    )


@pytest.mark.integration
def test_run_job3(spark_exec_env, project_misc, job2, job3, job2_expect_result):
    execution_date = datetime.utcnow()
    template_context = construct_template_context(
        project_misc, job2.config, execution_date
    )
    spark_exec_env.engine_session.run(job2, execution_date, template_context)
    spark_exec_env.engine_session.run(job3, execution_date, template_context)

    sink_df = spark_exec_env.spark_session.table("test_sink_table2")
    real = sink_df.toPandas()
    assert_frame_equal(
        job2_expect_result.sort_values(by=["title"]).reset_index(drop=True)[
            sorted(job2_expect_result.columns)
        ],
        real.sort_values(by=["title"]).reset_index(drop=True)[sorted(real.columns)],
        check_dtype=False,
    )
