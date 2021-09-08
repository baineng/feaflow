from pathlib import Path

import pytest

from feaflow.project import Project


@pytest.fixture
def example_project_path():
    return Path(__file__).parent.joinpath("data/example_project")


@pytest.fixture
def example_project(example_project_path):
    return Project(example_project_path)
