from typing import Dict, Optional

from pydantic.typing import Literal
from pyspark.sql import DataFrame, DataFrameWriter, SparkSession

from feaflow.abstracts import Engine, EngineConfig, EngineSession
from feaflow.computes import SqlCompute
from feaflow.exceptions import EngineInitError
from feaflow.job import Job
from feaflow.sinks import TableSink
from feaflow.sources import QuerySource
from feaflow.utils import split_cols


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

    def new_session(self):
        return SparkEngineSession(self)


class SparkEngineSession(EngineSession):
    def __init__(self, engine: SparkEngine):
        self._engine = engine
        self._spark_session: Optional[SparkSession] = None

    def get_or_create_spark_session(self, job_name: str) -> SparkSession:
        if self._spark_session is None:
            self._spark_session = self._create_spark_session(job_name)
        return self._spark_session

    def run(self, job: Job):
        engine_config = self._engine.config
        assert (
            job.engine_name == engine_config.name
        ), f"The job '{job}' is not able to be run on engine '{engine_config.name}'."

        spark_session = self.get_or_create_spark_session(job.name)

        for source in job.sources:
            if isinstance(source, QuerySource):
                df = spark_session.sql(source.sql)
                if source.alias:
                    df.createOrReplaceTempView(source.alias)

        computed_df: Optional[DataFrame] = None
        for compute in job.computes:
            if isinstance(compute, SqlCompute):
                computed_df = spark_session.sql(compute.sql)

        for sink in job.sinks:
            if isinstance(sink, TableSink):
                table_sink_config = sink.config
                table_sink_df = computed_df
                if sink.config.cols:
                    table_sink_df.selectExpr(*split_cols(sink.config.cols))

                table_sink_writer: DataFrameWriter = (
                    table_sink_df.write.mode(table_sink_config.mode.value).format(
                        table_sink_config.format.value
                    )
                )
                table_sink_writer.saveAsTable(table_sink_config.name)

    def _create_spark_session(self, job_name: str) -> SparkSession:
        engine_config = self._engine.config
        try:
            builder = SparkSession.builder.master(engine_config.master).appName(
                f"{engine_config.job_name_prefix}_{job_name}"
            )
            if engine_config.enable_hive_support:
                builder = builder.enableHiveSupport()
            if engine_config.config is not None:
                for k, v in engine_config.config.items():
                    builder = builder.config(k, v)
            return builder.getOrCreate()
        except Exception:
            raise EngineInitError(engine_config.type)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._spark_session:
            self._spark_session.stop()
