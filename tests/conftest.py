from pathlib import Path

import pytest

from feaflow.job import Job
from feaflow.project import Project


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
