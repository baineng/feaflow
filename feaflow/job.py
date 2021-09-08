from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from pydantic import constr

from feaflow.compute import Compute, SqlComputeConfig, create_compute_from_config
from feaflow.exceptions import ConfigException
from feaflow.model import FeaflowModel
from feaflow.project import Project
from feaflow.sink import RedisSinkConfig, Sink, create_sink_from_config
from feaflow.source import QuerySourceConfig, Source, create_source_from_config


class JobConfig(FeaflowModel):
    name: constr(regex=r"^[^_]\w+$", strip_whitespace=True, strict=True)
    schedule_interval: str
    computes: List[Union[SqlComputeConfig]]
    engine: str
    depends_on: Optional[str] = None
    airflow_dag_args: Optional[Dict[str, Any]] = None
    sources: Optional[List[Union[QuerySourceConfig]]] = None
    sinks: Optional[List[Union[RedisSinkConfig]]] = None


def parse_job_config_file(path: Union[str, Path]) -> JobConfig:
    job_conf_path = Path(path)
    if not job_conf_path.exists():
        raise FileNotFoundError(f"The job path `{path}` does not exist.")

    try:
        with open(job_conf_path) as f:
            config = yaml.safe_load(f)
            return JobConfig(**config)
    except Exception as ex:
        raise ConfigException(ex, str(job_conf_path.absolute()))


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
