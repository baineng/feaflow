from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from pydantic import constr

from feaflow.compute import SqlComputeConfig
from feaflow.exceptions import ConfigException
from feaflow.model import Engine, FeaflowModel
from feaflow.sink import RedisSinkConfig
from feaflow.source import QuerySourceConfig


class JobConfig(FeaflowModel):
    name: constr(regex=r"^[^_]\w+$", strip_whitespace=True, strict=True)
    schedule_interval: str
    computes: List[Union[SqlComputeConfig]]
    engine: Engine = Engine.SPARK_SQL
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
    def __init__(self, config: JobConfig):
        self._config = config
