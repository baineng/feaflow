import multiprocessing
from pathlib import Path
from sys import platform

import pytest

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
