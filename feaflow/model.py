from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, DirectoryPath, FilePath, constr

from feaflow.compute import SqlComputeConfig
from feaflow.sink import RedisSinkConfig
from feaflow.source import QuerySourceConfig


class FeaflowModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True


class FeaflowImmutableModel(FeaflowModel):
    class Config:
        allow_mutation = False


class ComponentConfig(FeaflowImmutableModel, ABC):
    @classmethod
    @abstractmethod
    def get_impl_cls(cls):
        raise NotImplementedError


class EngineConfig(ComponentConfig, ABC):
    name: constr(regex=r"^[^_][\w]+$", strip_whitespace=True, strict=True)


class ProjectConfig(FeaflowModel):
    name: constr(regex=r"^[^_][\w ]+$", strip_whitespace=True, strict=True)
    root_path: DirectoryPath
    config_file_path: FilePath
    engines: List[EngineConfig]


class SourceConfig(ComponentConfig, ABC):
    pass


class ComputeConfig(ComponentConfig, ABC):
    pass


class SinkConfig(ComponentConfig, ABC):
    pass


class JobConfig(FeaflowModel):
    name: constr(regex=r"^[^_]\w+$", strip_whitespace=True, strict=True)
    schedule_interval: str
    computes: List[ComputeConfig]
    engine: str
    depends_on: Optional[str] = None
    airflow_dag_args: Optional[Dict[str, Any]] = None
    sources: Optional[List[SourceConfig]] = None
    sinks: Optional[List[SinkConfig]] = None


BUILTIN_ENGINES = {"spark": "feaflow.engine.spark.SparkEngineConfig"}
