import pytest

from feaflow.exceptions import ConfigException
from feaflow.model import Engine
from feaflow.job import JobConfig
from feaflow.sink import RedisSinkConfig
from feaflow.compute import SqlComputeConfig
from feaflow.source import QuerySourceConfig
from feaflow.project import Project


def test_create_project(example_project_path):
    project = Project(example_project_path)
    assert project.name == "Feaflow Test Project"


def test_invalid_project(tmpdir):
    with pytest.raises(FileNotFoundError):
        Project(tmpdir.join("non_existed"))

    with pytest.raises(FileNotFoundError):
        Project(tmpdir)

    invalid_config_file = """
    """
    with open(tmpdir.join("feaflow.yaml"), "w") as f:
        f.write(invalid_config_file)
    with pytest.raises(ConfigException):
        Project(tmpdir)


def test_scan_jobs(example_project):
    jobs = example_project.scan_jobs()
    assert len(jobs) == 1

    test_job1: JobConfig = next(filter(lambda j: j.name == "test_job1", jobs))
    assert test_job1.schedule_interval == "0 6 * * *"
    assert test_job1.engine == Engine.SPARK_SQL
    assert type(test_job1.sources[0]) == QuerySourceConfig
    assert test_job1.sources[0].alias == "daily_events"
    assert type(test_job1.computes[0]) == SqlComputeConfig
    assert type(test_job1.sinks[0]) == RedisSinkConfig
    assert test_job1.sinks[0].host == "127.0.0.1"
    assert test_job1.sinks[0].port == 6380
