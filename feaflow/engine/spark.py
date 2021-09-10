from typing import Dict, List, Optional

from pydantic.typing import Literal
from pyspark.sql import SparkSession

from feaflow.abstracts import Engine, EngineConfig, Source
from feaflow.exceptions import EngineInitError
from feaflow.job import Job
from feaflow.sources import QuerySource


class SparkEngineConfig(EngineConfig):
    type: Literal["spark"]
    master: str = "local"
    enable_hive_support: bool = False
    config: Optional[Dict[str, str]] = None
    job_name_prefix: Optional[str] = None


class SparkEngine(Engine):
    @classmethod
    def create_config(cls, **data):
        return SparkEngineConfig(impl_cls=cls, **data)

    def __init__(self, config: SparkEngineConfig):
        assert isinstance(config, SparkEngineConfig)
        self._config = config

    @property
    def config(self) -> SparkEngineConfig:
        return self._config

    def _create_new_session(self) -> SparkSession:
        try:
            builder = SparkSession.builder.master(self._config.master)
            if self._config.enable_hive_support:
                builder = builder.enableHiveSupport()
            if self._config.config is not None:
                for k, v in self._config.config.items():
                    builder = builder.config(k, v)
            return builder.getOrCreate()
        except Exception:
            raise EngineInitError(self._config.type)

    def _get_session(self) -> SparkSession:
        return self._create_new_session()

    def run(self, job: Job):
        assert (
            job.engine == self._config.name
        ), f"The job '{job}' is not able to be run on engine '{self._config.name}'."

        self.handle_sources(job.sources)

    def handle_sources(self, sources: List[Source]):
        spark = self._get_session()

        for source in sources:
            if isinstance(source, QuerySource):
                df = spark.sql(source.sql)
                if source.alias:
                    df.createOrReplaceTempView(source.alias)
