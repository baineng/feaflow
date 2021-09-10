from typing import Dict, List, Optional

from pydantic.typing import Literal
from pyspark.sql import DataFrame, SparkSession

from feaflow.abstracts import Engine, EngineConfig, Source
from feaflow.computes import SqlCompute
from feaflow.exceptions import EngineInitError
from feaflow.job import Job
from feaflow.sinks import TableSink
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

    def _get_or_create_session(self) -> SparkSession:
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

    def run(self, job: Job):
        assert (
            job.engine == self._config.name
        ), f"The job '{job}' is not able to be run on engine '{self._config.name}'."

        with self._get_or_create_session() as spark:
            for source in job.sources:
                if isinstance(source, QuerySource):
                    df = spark.sql(source.sql)
                    if source.alias:
                        df.createOrReplaceTempView(source.alias)

            computed_df: DataFrame = None
            for compute in job.computes:
                if isinstance(compute, SqlCompute):
                    computed_df = spark.sql(compute.sql)

            for sink in job.sinks:
                if isinstance(sink, TableSink):
                    table_sink_df = computed_df
                    if sink.config.cols:
                        table_sink_df.selectExpr(sink.config.cols)
                    table_sink_df.write.mode(sink.config.mode).saveAsTable(
                        sink.config.name
                    )
