from typing import List

from pydantic import BaseModel

from feaflow.exceptions import NotAllowedException
from feaflow.job import Job


class ProjectConfig(BaseModel):
    config_file: StrictStr
    path: StrictStr
    name: StrictStr


def create_project_config(path):
    # check path
    # check feaflow.yaml
    pass


class Project:
    def __init__(self, config: ProjectConfig):
        self._config = config

    @property
    def name(self):
        return self._config.name

    @name.setter
    def name(self, name):
        raise NotAllowedException("Not allowed to change Project name.")

    def scan_jobs(self) -> List[Job]:
        pass
