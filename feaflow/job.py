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
    def __init__(self, config: JobConfig):
        self._config = config
        self._sources = None
        self._computes = None
        self._sinks = None

    @property
    def config(self) -> JobConfig:
        return self._config

    @property
    def name(self) -> str:
        return self._config.name

    @property
    def engine_name(self) -> str:
        return self._config.engine

    @property
    def scheduler_config(self) -> SchedulerConfig:
        return self._config.scheduler

    @property
    def sources(self) -> List[Source]:
        if self._sources is None:
            self._sources = (
                [create_instance_from_config(c) for c in self._config.sources]
                if self._config.sources
                else []
            )
        return self._sources

    @property
    def computes(self) -> List[Compute]:
        if self._computes is None:
            self._computes = (
                [create_instance_from_config(c) for c in self._config.computes]
                if self._config.computes
                else []
            )
        return self._computes

    @property
    def sinks(self) -> List[Sink]:
        if self._sinks is None:
            self._sinks = (
                [create_instance_from_config(c) for c in self._config.sinks]
                if self._config.sinks
                else []
            )
        return self._sinks

    def __repr__(self):
        return f"Job({self._config.name})"


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
