from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

from pydantic import Field, FilePath
from pydantic.types import StrictStr

from feaflow.abstracts import (
    ComputeConfig,
    FeaflowImmutableModel,
    FeaflowModel,
    SchedulerConfig,
    SinkConfig,
    SourceConfig,
)
from feaflow.constants import BUILTIN_COMPUTES, BUILTIN_SINKS, BUILTIN_SOURCES
from feaflow.utils import (
    construct_config_from_dict,
    construct_scheduler_config_from_dict,
)


class JobEngineConfig(FeaflowImmutableModel):
    _template_attrs: Tuple[str] = ("use", "config_overlay")
    use: StrictStr
    config_overlay: Optional[Dict[str, Any]] = None


class JobConfig(FeaflowModel):
    _template_attrs: Tuple[str] = ("name", "engine", "scheduler")
    name: str
    config_file_path: FilePath
    engine: JobEngineConfig
    scheduler: SchedulerConfig
    computes: List[ComputeConfig]
    sources: Optional[List[SourceConfig]] = None
    sinks: Optional[List[SinkConfig]] = None
    loop_variables: Optional[Dict[str, Any]] = None
    variables: Optional[Dict[str, Any]] = None

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
