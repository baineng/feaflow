import pytest

from feaflow.compute.sql import SqlComputeConfig
from feaflow.exceptions import ConfigLoadError
from feaflow.job import Job
from feaflow.project import Project
from feaflow.sink.table import TableSinkConfig
from feaflow.source.query import QuerySourceConfig


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
    with open(tmpdir.join("feaflow_project.yaml"), "w") as f:
        f.write(invalid_config_file)
    with pytest.raises(ConfigLoadError):
        Project(tmpdir)


def test_scan_jobs(example_project):
    jobs = example_project.scan_jobs()
    assert len(jobs) == 2

    job1: Job = next(filter(lambda j: j.name == "test_job1", jobs))
    job1_config = job1.config
    assert job1_config.scheduler.schedule_interval == "0 6 * * *"
    assert job1_config.engine == "default_spark"
    assert type(job1_config.sources[0]) == QuerySourceConfig
    assert job1_config.sources[0].alias is None
    assert type(job1_config.computes[0]) == SqlComputeConfig
    assert type(job1_config.sinks[0]) == TableSinkConfig
    assert job1_config.sinks[0].name == "test_sink_table"
