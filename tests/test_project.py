import pytest

from feaflow.exceptions import ConfigLoadException
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
    with pytest.raises(ConfigLoadException):
        Project(tmpdir)
