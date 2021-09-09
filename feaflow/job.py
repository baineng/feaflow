from pathlib import Path
from typing import List, Union

import yaml

from feaflow.compute import BUILTIN_COMPUTES, Compute, create_compute_from_config
from feaflow.exceptions import ConfigLoadException
from feaflow.model import JobConfig
from feaflow.project import Project
from feaflow.sink import BUILTIN_SINKS, Sink, create_sink_from_config
from feaflow.source import BUILTIN_SOURCES, Source, create_source_from_config
from feaflow.utils import create_config_from_dict


def parse_job_config_file(path: Union[str, Path]) -> JobConfig:
    job_conf_path = Path(path)
    if not job_conf_path.exists():
        raise FileNotFoundError(f"The job path `{path}` does not exist.")

    try:
        with open(job_conf_path) as f:
            config = yaml.safe_load(f)
            if "sources" in config:
                assert type(config["sources"]) == list
                config["sources"] = [
                    create_config_from_dict(c, BUILTIN_SOURCES)
                    for c in config["sources"]
                ]
            if "computes" in config:
                assert type(config["computes"]) == list
                config["computes"] = [
                    create_config_from_dict(c, BUILTIN_COMPUTES)
                    for c in config["computes"]
                ]

            if "sinks" in config:
                assert type(config["sinks"]) == list
                config["sinks"] = [
                    create_config_from_dict(c, BUILTIN_SINKS) for c in config["sinks"]
                ]
            return JobConfig(**config)
    except Exception:
        raise ConfigLoadException(str(job_conf_path.absolute()))


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


class Job:
    def __init__(self, project: Project, config: JobConfig):
        self._project = project
        self._config = config
        self._sources = (
            [create_source_from_config(sc) for sc in config.sources]
            if config.sources
            else []
        )
        self._computes = (
            [create_compute_from_config(cc) for cc in config.computes]
            if config.computes
            else []
        )
        self._sinks = (
            [create_sink_from_config(sc) for sc in config.sinks] if config.sinks else []
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
