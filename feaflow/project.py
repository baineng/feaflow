from pathlib import Path
from typing import List, Union

import yaml

from feaflow.exceptions import ConfigException
from feaflow.job import parse_job_config_file
from feaflow.model import JobConfig, ProjectConfig


def create_project_config_from_path(path: Union[str, Path]) -> ProjectConfig:
    root_path = Path(path)
    if not root_path.exists():
        raise FileNotFoundError(f"The project path `{path}` does not exist.")

    config_file_path = root_path.joinpath("feaflow.yaml")
    if not config_file_path.exists():
        raise FileNotFoundError(
            f"The project path `{path}` does not include feaflow.yaml."
        )

    try:
        with open(config_file_path) as f:
            config = yaml.safe_load(f)

            project_config = ProjectConfig(
                name=config["project_name"],
                root_path=root_path,
                config_file_path=config_file_path,
            )
            return project_config
    except Exception as ex:
        raise ConfigException(ex, str(config_file_path.absolute()))


class Project:
    def __init__(self, path: Union[str, Path]):
        self._config = create_project_config_from_path(path)

    @property
    def config(self):
        return self._config

    @property
    def name(self):
        return self.config.name

    @property
    def root_path(self):
        return self.config.root_path

    def scan_jobs(self) -> List[JobConfig]:
        """
        Scan jobs in the project root path,
        any yaml files start or end with "job" will be considered as a job config file.
        """
        jobs_1 = [
            f.resolve()
            for f in self.config.root_path.glob("**/job*.yaml")
            if f.is_file()
        ]
        jobs_2 = [
            f.resolve()
            for f in self.config.root_path.glob("**/*job.yaml")
            if f.is_file()
        ]
        job_conf_files = set(jobs_1 + jobs_2)

        jobs = [parse_job_config_file(f) for f in job_conf_files]
        return jobs
