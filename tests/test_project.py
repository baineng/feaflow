import pytest

from feaflow.compute.sql import SqlComputeConfig
from feaflow.exceptions import ConfigLoadError
from feaflow.project import Project
from feaflow.sink.table import TableSinkConfig
from feaflow.source.query import QuerySourceConfig


def test_create_project(project_misc_path):
    project = Project(project_misc_path)
    assert project.name == "Feaflow_Misc"


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


def test_scan_jobs(project_misc):
    jobs = project_misc.scan_jobs()

    job1_config = next(filter(lambda j: j.name == "test_job1", jobs))
    assert job1_config.scheduler.schedule_interval == "0 6 * * *"
    assert job1_config.engine.use == "default_spark"
    assert type(job1_config.sources[0]) == QuerySourceConfig
    assert job1_config.sources[0].alias is None
    assert type(job1_config.computes[0]) == SqlComputeConfig
    assert type(job1_config.sinks[0]) == TableSinkConfig
    assert job1_config.sinks[0].name == "test_sink_table"
