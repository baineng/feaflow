from abc import ABC
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, DirectoryPath, FilePath, constr
from typing_extensions import Literal


class FeaflowModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True


class FeaflowImmutableModel(FeaflowModel):
    class Config:
        allow_mutation = False


class ProjectConfig(FeaflowModel):
    name: constr(regex=r"^[^_][\w ]+$", strip_whitespace=True, strict=True)
    root_path: DirectoryPath
    config_file_path: FilePath


class Engine(str, Enum):
    SPARK_SQL = "spark-sql"
    HIVE = "hive"


class SourceConfig(FeaflowModel, ABC):
    pass


class QuerySourceConfig(SourceConfig):
    type: Literal["query"]
    sql: str
    alias: Optional[str] = None


class Source:
    pass


class ComputeConfig(FeaflowModel, ABC):
    pass


class SqlComputeConfig(ComputeConfig):
    type: Literal["sql"]
    sql: str


class Compute:
    pass


class SinkConfig(FeaflowModel, ABC):
    pass


class RedisSinkConfig(SinkConfig):
    type: Literal["redis"]
    host: str
    port: int = 6379
    db: int = 0


class Sink:
    pass


class JobConfig(FeaflowModel):
    name: constr(regex=r"^[^_]\w+$", strip_whitespace=True, strict=True)
    schedule_interval: str
    computes: List[Union[SqlComputeConfig]]
    engine: Engine = Engine.SPARK_SQL
    depends_on: Optional[str] = None
    airflow_dag_args: Optional[Dict[str, Any]] = None
    sources: Optional[List[Union[QuerySourceConfig]]] = None
    sinks: Optional[List[Union[RedisSinkConfig]]] = None
