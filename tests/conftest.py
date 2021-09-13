import multiprocessing
from pathlib import Path
from sys import platform

import pandas as pd
import pytest

from feaflow.engine.spark import SparkEngine, SparkEngineRunContext, SparkEngineSession
from feaflow.project import Project


def pytest_configure():
    if platform in ["darwin", "windows"]:
        multiprocessing.set_start_method("spawn")
    else:
        multiprocessing.set_start_method("fork")


@pytest.fixture
def example_project_path():
    return Path(__file__).parent.joinpath("example_project")


@pytest.fixture
def example_project(example_project_path):
    return Project(example_project_path)


@pytest.fixture
def job1(example_project):
    jobs = example_project.scan_jobs()
    return next(filter(lambda j: j.name == "test_job1", jobs))


@pytest.fixture
def job2(example_project):
    jobs = example_project.scan_jobs()
    return next(filter(lambda j: j.name == "test_job2", jobs))


@pytest.fixture()
def job2_expect_result():
    return pd.DataFrame(
        {
            "title": [
                "message.send",
                "user.login",
                "user.signup",
                "user.logout",
                "membership.pay",
                "image.comment",
                "profile.visit",
                "image.upload",
                "image.like",
                "user.poke",
            ],
            "amount": [2, 2, 3, 3, 2, 1, 2, 2, 1, 2],
        }
    )


@pytest.fixture()
def spark_run_context(example_project, tmpdir) -> SparkEngineRunContext:
    engine = example_project.get_engine_by_name("default_spark")
    assert type(engine) == SparkEngine
    with engine.new_session() as engine_session:
        assert isinstance(engine_session, SparkEngineSession)
        spark_session = engine_session.get_or_create_spark_session(
            "test_job1",
            config_overlay={"config": {"spark.sql.warehouse.dir": f"file://{tmpdir}"}},
        )
        yield SparkEngineRunContext(
            engine=engine, engine_session=engine_session, spark_session=spark_session
        )
