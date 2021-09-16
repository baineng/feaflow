import re
from pathlib import Path
from typing import Any, List, Optional, Union

import yaml
from pydantic import DirectoryPath, FilePath, constr

from feaflow.abstracts import FeaflowModel, SchedulerConfig
from feaflow.constants import BUILTIN_ENGINES
from feaflow.engine import Engine, EngineConfig
from feaflow.exceptions import ConfigLoadError
from feaflow.job import Job, parse_job_config_file
from feaflow.utils import (
    create_config_from_dict,
    create_instance_from_config,
    create_scheduler_config_from_dict,
)


class ProjectConfig(FeaflowModel):
    name: constr(regex=r"^[^_][\w ]+$", strip_whitespace=True, strict=True)
    root_path: DirectoryPath
    config_file_path: FilePath
    engines: List[EngineConfig]
    scheduler_default: Optional[SchedulerConfig] = None

    def __init__(self, **data: Any):
        if "engines" in data:
            assert type(data["engines"]) == list
            data["engines"] = [
                create_config_from_dict(ec, BUILTIN_ENGINES) for ec in data["engines"]
            ]

        if "scheduler_default" in data and data["scheduler_default"]:
            assert type(data["scheduler_default"]) == dict
            data["scheduler_default"] = create_scheduler_config_from_dict(
                data["scheduler_default"]
            )

        super().__init__(**data)


class Project:
    def __init__(self, path: Union[str, Path]):
        self._config = create_project_config_from_path(path)
        self._engines = None

    @property
    def config(self) -> ProjectConfig:
        return self._config

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def root_path(self) -> Path:
        return self.config.root_path

    @property
    def engines(self) -> List[Engine]:
        if self._engines is None:
            self._engines = [
                create_instance_from_config(c) for c in self._config.engines
            ]
        return self._engines

    def get_engine_by_name(self, engine_name) -> Optional[Engine]:
        for engine in self.engines:
            if engine.config.name == engine_name:
                return engine
        return None

    def scan_jobs(self) -> List[Job]:
        """
        Scan jobs in the project root path,
        any yaml files start or end with "job" will be considered as a job config file.
        """
        job_conf_files = self._find_files(r"\/job.*\.ya?ml$", r"\/.*?job\.ya?ml$")
        job_configs = [parse_job_config_file(f) for f in job_conf_files]
        jobs = [Job(c) for c in job_configs]
        return jobs

    def run_job(self, job: Job):
        engine = self.get_engine_by_name(job.engine_name)
        with engine.new_session() as engine_session:
            engine_session.run(job)

    def _find_files(self, *patterns) -> List:
        def _match_any_pattern(f: Path) -> bool:
            for pat in patterns:
                if re.search(pat, str(f.resolve())):
                    return True
            return False

        return [
            f.resolve()
            for f in self.root_path.glob("**/*")
            if f.is_file() and _match_any_pattern(f)
        ]


def create_project_config_from_path(path: Union[str, Path]) -> ProjectConfig:
    root_path = Path(path)
    if not root_path.exists():
        raise FileNotFoundError(f"The project path `{path}` does not exist.")

    if root_path.joinpath("feaflow_project.yaml").exists():
        config_file_path = root_path.joinpath("feaflow_project.yaml")
    elif root_path.joinpath("feaflow_project.yaml").exists():
        config_file_path = root_path.joinpath("feaflow_project.yaml")
    else:
        raise FileNotFoundError(
            f"The project path `{path}` does not include feaflow_project.yaml."
        )

    try:
        with open(config_file_path) as f:
            config = yaml.safe_load(f)
            config["name"] = config["project_name"]
            del config["project_name"]
            config["root_path"] = root_path
            config["config_file_path"] = config_file_path
            return ProjectConfig(**config)
    except Exception:
        raise ConfigLoadError(str(config_file_path.absolute()))
