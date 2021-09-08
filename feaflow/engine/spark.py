from pydantic.typing import Literal
from pyspark.sql import SparkSession

from feaflow.engine import Engine
from feaflow.job import Job
from feaflow.model import EngineConfig


class SparkEngineConfig(EngineConfig):
    type: Literal["spark"] = "spark"

    @classmethod
    def get_impl_cls(cls):
        return SparkEngine


class SparkEngine(Engine):
    def __init__(self, config: SparkEngineConfig):
        self._config = config

    @property
    def config(self):
        return self._config

    def init(self):
        pass

    def run(self, job: Job):
        assert job.engine == self._config.name
        pass
