import multiprocessing
import os
import shutil
from datetime import datetime
from pathlib import Path
from sys import platform

import pandas as pd
import pytest

from feaflow.engine.spark import (
    SparkEngine,
    SparkEngineSession,
    SparkExecutionEnvironment,
)
from feaflow.job import Job
from feaflow.project import Project
from feaflow.utils import create_random_str


def pytest_configure(config):
    if platform in ["darwin", "windows"]:
        multiprocessing.set_start_method("spawn")
    else:
        multiprocessing.set_start_method("fork")

    # if "not integration" not in config.getoption("-m"):
    #     os.environ["AIRFLOW_HOME"] = tempfile.mkdtemp()


def update_project_warehouse_dir(project: Project, warehouse_dir: str):
    project.get_engine_by_name("default_spark").get_config("config").update(
        {"spark.sql.warehouse.dir": f"file://{warehouse_dir}"}
    )


@pytest.fixture
def project_misc_path():
    return Path(__file__).parent / "test_projects" / "misc"


@pytest.fixture
def project_misc(project_misc_path, tmpdir):
    project = Project(project_misc_path)
    update_project_warehouse_dir(project, tmpdir)
    return project


@pytest.fixture
def project_feast(tmpdir):
    project = Project(Path(__file__).parent / "test_projects" / "feast")
    update_project_warehouse_dir(project, tmpdir)
    return project


@pytest.fixture()
def spark_exec_env(project_misc, tmpdir) -> SparkExecutionEnvironment:
    engine = project_misc.get_engine_by_name("default_spark")
    assert type(engine) == SparkEngine
    with engine.new_session() as engine_session:
        assert isinstance(engine_session, SparkEngineSession)
        spark_session = engine_session._get_or_create_spark_session(
            f"test_spark_job_{create_random_str()}"
        )
        yield SparkExecutionEnvironment(
            engine=engine,
            engine_session=engine_session,
            spark_session=spark_session,
            execution_date=datetime.utcnow(),
        )


@pytest.fixture
def job1(project_misc):
    jobs = project_misc.scan_jobs()
    return Job(next(filter(lambda j: j.name == "test_job1", jobs)))


@pytest.fixture
def job2(project_misc):
    jobs = project_misc.scan_jobs()
    return Job(next(filter(lambda j: j.name == "test_job2", jobs)))


@pytest.fixture
def job3(project_misc):
    jobs = project_misc.scan_jobs()
    return Job(next(filter(lambda j: j.name == "test_job3", jobs)))


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


@pytest.fixture(scope="session")
def airflow_init(tmpdir_factory):
    from airflow.utils import db

    db.initdb()
    yield
    shutil.rmtree(os.environ["AIRFLOW_HOME"], ignore_errors=True)
