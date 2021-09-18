from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
import yaml.parser
from pydantic import FilePath, constr

from feaflow.abstracts import (
    Compute,
    ComputeConfig,
    FeaflowImmutableModel,
    FeaflowModel,
    SchedulerConfig,
    Sink,
    SinkConfig,
    Source,
    SourceConfig,
)
from feaflow.constants import BUILTIN_COMPUTES, BUILTIN_SINKS, BUILTIN_SOURCES
from feaflow.exceptions import ConfigLoadError
from feaflow.utils import (
    construct_config_from_dict,
    construct_impl_from_config,
    construct_scheduler_config_from_dict,
    deep_merge_models,
)


class JobEngineConfig(FeaflowImmutableModel):
    use: str
    config_overlay: Optional[Dict[str, Any]] = None


class JobConfig(FeaflowModel):
    name: constr(regex=r"^[^_]\w+$", strip_whitespace=True, strict=True)
    config_file_path: FilePath
    engine: JobEngineConfig
    scheduler: SchedulerConfig
    computes: List[ComputeConfig]
    sources: Optional[List[SourceConfig]] = None
    sinks: Optional[List[SinkConfig]] = None

    def __init__(self, **data: Any):
        if "engine" in data:
            if type(data["engine"]) == str:
                data["engine"] = {"use": data["engine"]}

        if "scheduler" in data:
            assert type(data["scheduler"]) == dict
            data["scheduler"] = construct_scheduler_config_from_dict(data["scheduler"])

        if "sources" in data:
            assert type(data["sources"]) == list
            data["sources"] = [
                construct_config_from_dict(c, BUILTIN_SOURCES) for c in data["sources"]
            ]

        if "computes" in data:
            assert type(data["computes"]) == list
            data["computes"] = [
                construct_config_from_dict(c, BUILTIN_COMPUTES)
                for c in data["computes"]
            ]

        if "sinks" in data:
            assert type(data["sinks"]) == list
            data["sinks"] = [
                construct_config_from_dict(c, BUILTIN_SINKS) for c in data["sinks"]
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
        return self._config.engine.use

    @property
    def sources(self) -> List[Source]:
        if self._sources is None:
            if self._config.sources is None:
                self._sources = []
            else:
                self._sources = (
                    [construct_impl_from_config(c) for c in self._config.sources]
                    if self._config.sources
                    else []
                )
        return self._sources

    @property
    def computes(self) -> List[Compute]:
        if self._computes is None:
            self._computes = (
                [construct_impl_from_config(c) for c in self._config.computes]
                if self._config.computes
                else []
            )
        return self._computes

    @property
    def sinks(self) -> List[Sink]:
        if self._sinks is None:
            if self._config.sinks is None:
                self._sinks = None
            else:
                self._sinks = (
                    [construct_impl_from_config(c) for c in self._config.sinks]
                    if self._config.sinks
                    else []
                )
        return self._sinks

    @property
    def scheduler_config(self):
        return self._config.scheduler

    def merge_scheduler_config(
        self, default: Optional[SchedulerConfig] = None
    ) -> SchedulerConfig:
        if not default or type(default) != type(self._config.scheduler):
            return self.scheduler_config
        else:
            return deep_merge_models(self.scheduler_config, default)

    def __repr__(self):
        return f"Job({self._config.name})"


def parse_job_config_file(path: Union[str, Path]) -> JobConfig:
    job_conf_path = Path(path)
    if not job_conf_path.exists():
        raise FileNotFoundError(f"The job path `{path}` does not exist.")

    try:
        with open(job_conf_path) as f:
            config = yaml.safe_load(f)
            return JobConfig(config_file_path=job_conf_path, **config)
    except yaml.parser.ParserError:
        raise ConfigLoadError(
            str(job_conf_path.absolute()),
            "if you have used Jinja2 syntax, try to escape by quoting it or use |, > in the config.",
        )
    except Exception as ex:
        raise ConfigLoadError(str(job_conf_path.absolute()), str(ex))
