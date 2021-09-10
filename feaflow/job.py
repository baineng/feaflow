from pathlib import Path
from typing import Any, List, Optional, Union

import yaml
from pydantic import constr

from feaflow.abstracts import (
    Compute,
    ComputeConfig,
    FeaflowModel,
    SchedulerConfig,
    Sink,
    SinkConfig,
    Source,
    SourceConfig,
)
from feaflow.constants import (
    BUILTIN_COMPUTES,
    BUILTIN_SCHEDULERS,
    BUILTIN_SINKS,
    BUILTIN_SOURCES,
)
from feaflow.exceptions import ConfigLoadError
from feaflow.project import Project
from feaflow.utils import create_config_from_dict, create_instance_from_config


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


class Job:
    def __init__(self, project: Project, config: JobConfig):
        self._project = project
        self._config = config
        self._sources = (
            [create_instance_from_config(c) for c in config.sources]
            if config.sources
            else []
        )
        self._computes = (
            [create_instance_from_config(c) for c in config.computes]
            if config.computes
            else []
        )
        self._sinks = (
            [create_instance_from_config(c) for c in config.sinks]
            if config.sinks
            else []
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

    def __repr__(self):
        return f"Job({self._config.name})"


def scan_jobs_from_project(project: Project) -> List[JobConfig]:
    """
    Scan jobs in the project root path,
    any yaml files start or end with "job" will be considered as a job config file.
    """
    root_path = project.root_path
    jobs_1 = [f.resolve() for f in root_path.glob("**/job*.yaml") if f.is_file()]
    jobs_2 = [f.resolve() for f in root_path.glob("**/*job.yaml") if f.is_file()]
    job_conf_files = set(jobs_1 + jobs_2)

    jobs = [parse_job_config_file(f) for f in job_conf_files]
    return jobs


def parse_job_config_file(path: Union[str, Path]) -> JobConfig:
    job_conf_path = Path(path)
    if not job_conf_path.exists():
        raise FileNotFoundError(f"The job path `{path}` does not exist.")

    try:
        with open(job_conf_path) as f:
            config = yaml.safe_load(f)
            return JobConfig(**config)
    except Exception:
        raise ConfigLoadError(str(job_conf_path.absolute()))
