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


class FeastEntityConfig(FeaflowImmutableModel):
    name: str
    value_type: str = Field(alias="type", default="UNKNOWN")
    description: str = ""
    join_key: Optional[str] = None
    labels: Optional[Dict[str, str]] = None


class FeastFeatureConfig(FeaflowImmutableModel):
    name: str
    dtype: str = Field(alias="type", default="INT")
    labels: Optional[Dict[str, str]] = None


class FeastBatchSourceConfig(FeaflowImmutableModel):
    class_: str = Field(alias="class")
    args: Dict[str, Any] = {}


class FeatureViewConfig(FeaflowImmutableModel):
    name: StrictStr
    ttl: timedelta = timedelta(seconds=(24 * 60 * 60))  # one day
    batch_source: FeastBatchSourceConfig
    entities: List[FeastEntityConfig]
    features: Optional[List[FeastFeatureConfig]] = None
    tags: Optional[Dict[str, str]] = None


class FeastConfig(FeaflowImmutableModel):
    feature_view: FeatureViewConfig


class JobConfig(FeaflowModel):
    _template_attrs: Tuple[str] = ("name", "engine", "scheduler")
    name: str
    config_file_path: FilePath
    engine: JobEngineConfig
    scheduler: SchedulerConfig
    computes: List[ComputeConfig]
    sources: Optional[List[SourceConfig]] = None
    sinks: Optional[List[SinkConfig]] = None
    loop_params: Optional[Dict[str, Any]] = None
    feast: Optional[FeastConfig] = None

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

        if "feast" in data:
            assert "feature_view" in data["feast"]
            _feature_view_config = data["feast"]["feature_view"]
            assert type(_feature_view_config) == dict
            assert "entities" in _feature_view_config

            if type(_feature_view_config["entities"]) == list:
                _feature_view_config["entities"] = [
                    {"name": _entity} if type(_entity) == str else _entity
                    for _entity in _feature_view_config["entities"]
                ]

            if "features" in _feature_view_config:
                if type(_feature_view_config["features"]) == list:
                    _feature_view_config["features"] = [
                        {"name": _feature} if type(_feature) == str else _feature
                        for _feature in _feature_view_config["features"]
                    ]

        super().__init__(**data)
