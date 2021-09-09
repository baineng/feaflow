from pathlib import Path
from typing import Any, List, Optional, Union

import yaml
from pydantic import DirectoryPath, FilePath, constr

from feaflow.abstracts import (
    Compute,
    ComputeConfig,
    EngineConfig,
    SchedulerConfig,
    Sink,
    SinkConfig,
    Source,
    SourceConfig,
)
from feaflow.constants import (
    BUILTIN_COMPUTES,
    BUILTIN_ENGINES,
    BUILTIN_SCHEDULERS,
    BUILTIN_SINKS,
    BUILTIN_SOURCES,
)
from feaflow.exceptions import ConfigLoadException
from feaflow.model import FeaflowModel
from feaflow.utils import create_config_from_dict


class ProjectConfig(FeaflowModel):
    name: constr(regex=r"^[^_][\w ]+$", strip_whitespace=True, strict=True)
    root_path: DirectoryPath
    config_file_path: FilePath
    engines: List[EngineConfig]

    def __init__(self, **data: Any):
        if "engines" in data:
            assert type(data["engines"]) == list
            data["engines"] = [
                create_config_from_dict(ec, BUILTIN_ENGINES) for ec in data["engines"]
            ]

        super().__init__(**data)


class JobConfig(FeaflowModel):
    name: constr(regex=r"^[^_]\w+$", strip_whitespace=True, strict=True)
    engine: str
    scheduler: SchedulerConfig
    computes: List[ComputeConfig]
    sources: Optional[List[SourceConfig]] = None
    sinks: Optional[List[SinkConfig]] = None

    def __init__(self, **data: Any):
        if "scheduler" in data:
            assert type(data["scheduler"]) == dict
            if "type" not in data["scheduler"]:
                data["scheduler"]["type"] = "airflow"
            data["scheduler"] = create_config_from_dict(
                data["scheduler"], BUILTIN_SCHEDULERS
            )

        if "sources" in data:
            assert type(data["sources"]) == list
            data["sources"] = [
                create_config_from_dict(c, BUILTIN_SOURCES) for c in data["sources"]
            ]
        if "computes" in data:
            assert type(data["computes"]) == list
            data["computes"] = [
                create_config_from_dict(c, BUILTIN_COMPUTES) for c in data["computes"]
            ]

        if "sinks" in data:
            assert type(data["sinks"]) == list
            data["sinks"] = [
                create_config_from_dict(c, BUILTIN_SINKS) for c in data["sinks"]
            ]

        super().__init__(**data)


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
        root_path = self.root_path
        jobs_1 = [f.resolve() for f in root_path.glob("**/job*.yaml") if f.is_file()]
        jobs_2 = [f.resolve() for f in root_path.glob("**/*job.yaml") if f.is_file()]
        job_conf_files = set(jobs_1 + jobs_2)

        jobs = [parse_job_config_file(f) for f in job_conf_files]
        return jobs


class Job:
    def __init__(self, project: Project, config: JobConfig):
        self._project = project
        self._config = config
        self._sources = (
            [c.create_impl_instance() for c in config.sources] if config.sources else []
        )
        self._computes = (
            [c.create_impl_instance() for c in config.computes]
            if config.computes
            else []
        )
        self._sinks = (
            [c.create_impl_instance() for c in config.sinks] if config.sinks else []
        )

    @property
    def project(self) -> Project:
        return self._project

    @property
    def config(self) -> JobConfig:
        return self._config

    @property
    def engine(self) -> str:
        return self._config.engine

    @property
    def sources(self) -> List[Source]:
        return self._sources

    @property
    def computes(self) -> List[Compute]:
        return self._computes

    @property
    def sinks(self) -> List[Sink]:
        return self._sinks


def parse_job_config_file(path: Union[str, Path]) -> JobConfig:
    job_conf_path = Path(path)
    if not job_conf_path.exists():
        raise FileNotFoundError(f"The job path `{path}` does not exist.")

    try:
        with open(job_conf_path) as f:
            config = yaml.safe_load(f)
            return JobConfig(**config)
    except Exception:
        raise ConfigLoadException(str(job_conf_path.absolute()))


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
            config["name"] = config["project_name"]
            del config["project_name"]
            config["root_path"] = root_path
            config["config_file_path"] = config_file_path
            return ProjectConfig(**config)
    except Exception:
        raise ConfigLoadException(str(config_file_path.absolute()))
