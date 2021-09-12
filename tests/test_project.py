import pytest

from feaflow.computes import SqlComputeConfig
from feaflow.exceptions import ConfigLoadError
from feaflow.job import Job, JobConfig
from feaflow.project import Project
from feaflow.sinks import TableSinkConfig
from feaflow.sources import QuerySourceConfig


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
    with pytest.raises(ConfigLoadError):
        Project(tmpdir)


def test_scan_jobs(example_project):
    jobs = example_project.scan_jobs()
    assert len(jobs) == 1

    test_job1: Job = next(filter(lambda j: j.name == "test_job1", jobs))
    test_job1_config = test_job1.config
    assert test_job1_config.scheduler.schedule_interval == "0 6 * * *"
    assert test_job1_config.engine == "default_spark"
    assert type(test_job1_config.sources[0]) == QuerySourceConfig
    assert test_job1_config.sources[0].alias == "daily_events"
    assert type(test_job1_config.computes[0]) == SqlComputeConfig
    assert type(test_job1_config.sinks[0]) == TableSinkConfig
    assert test_job1_config.sinks[0].name == "test_sink_table"
