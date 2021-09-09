from pydantic.typing import Literal
from pyspark.sql import SparkSession

from feaflow.abstracts import Engine, EngineConfig
from feaflow.job import Job


class SparkEngineConfig(EngineConfig):
    type: Literal["spark"] = "spark"

    def create_impl_instance(self):
        return SparkEngine(self)


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
