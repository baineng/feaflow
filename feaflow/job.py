from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel


class Engine(Enum):
    SPARK_SQL = "spark_sql"
    HIVE = "hive"


class Source:
    pass


class Compute:
    pass


class Sink:
    pass


class JobConfig(BaseModel):
    name: str
    schedule_interval: str
    computes: List[Compute]
    engine: Optional[Engine] = None
    airflow_dag_args: Optional[Dict[str, str]] = None
    sources: Optional[List[Source]] = None
    sinks: Optional[List[Sink]] = None


class Job:
    def __init__(self, config: JobConfig):
        self._config = config
