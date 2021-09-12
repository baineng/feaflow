import random
import time
from typing import Any, Dict, Optional

from pydantic.typing import Literal
from pyspark.sql import DataFrame, DataFrameWriter, SparkSession

from feaflow.abstracts import (
    ComputeUnit,
    Engine,
    EngineConfig,
    EngineHandler,
    EngineRunContext,
    EngineSession,
)
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
        self.set_handlers([QuerySourceHandler, SqlComputeHandler, TableSinkHandler])

    def run(self, job: Job):
        engine_config = self._engine.config
        assert (
            job.engine_name == engine_config.name
        ), f"The job '{job}' is not able to be run on engine '{engine_config.name}'."
        spark_session = self.get_or_create_spark_session(job.name)
        context = SparkEngineRunContext(
            engine=self._engine, engine_session=self, spark_session=spark_session
        )
        self.handle(context, job)

    def get_or_create_spark_session(
        self, job_name: str, config_overlay: Optional[Dict[str, Any]] = None
    ) -> SparkSession:
        if self._spark_session is None:
            self._spark_session = self._create_spark_session(job_name, config_overlay)
        return self._spark_session

    def _create_spark_session(
        self, job_name: str, config_overlay: Optional[Dict[str, Any]] = None
    ) -> SparkSession:
        engine_config = self._engine.config
        if config_overlay is not None:
            assert "type" not in config_overlay, "type is not changeable"
            engine_config = engine_config.copy(update=config_overlay)

        try:
            builder = SparkSession.builder.master(engine_config.master).appName(
                f"{engine_config.job_name_prefix}_{job_name}"
            )
            if engine_config.config is not None:
                for k, v in engine_config.config.items():
                    builder = builder.config(k, v)
            if engine_config.enable_hive_support:
                builder = builder.enableHiveSupport()
            return builder.getOrCreate()
        except Exception:
            raise EngineInitError(engine_config.type)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._spark_session:
            self._spark_session.stop()


class SparkEngineRunContext(EngineRunContext):
    engine: SparkEngine
    engine_session: SparkEngineSession
    spark_session: SparkSession
    source_results: Dict[str, DataFrame] = {}
    compute_results: Dict[str, DataFrame] = {}


class QuerySourceHandler(EngineHandler):
    @classmethod
    def can_handle(cls, unit: ComputeUnit) -> bool:
        return isinstance(unit, QuerySource)

    @classmethod
    def handle(cls, context: SparkEngineRunContext, unit: ComputeUnit):
        assert isinstance(unit, QuerySource)
        spark = context.spark_session
        df = spark.sql(unit.sql)
        source_id = (
            unit.alias
            if unit.alias
            else f"source_{unit.config.type}_{int(time.time_ns())}_{random.randint(1000, 9999)}"
        )
        df.createOrReplaceTempView(source_id)
        context.source_results[source_id] = df


class SqlComputeHandler(EngineHandler):
    @classmethod
    def can_handle(cls, unit: ComputeUnit) -> bool:
        return isinstance(unit, SqlCompute)

    @classmethod
    def handle(cls, context: SparkEngineRunContext, unit: ComputeUnit):
        assert isinstance(unit, SqlCompute)
        spark = context.spark_session
        df = spark.sql(unit.sql)
        compute_id = f"compute_{unit.config.type}_{int(time.time_ns())}_{random.randint(1000, 9999)}"
        df.createOrReplaceTempView(compute_id)
        context.compute_results[compute_id] = df


class TableSinkHandler(EngineHandler):
    @classmethod
    def can_handle(cls, unit: ComputeUnit) -> bool:
        return isinstance(unit, TableSink)

    @classmethod
    def handle(cls, context: SparkEngineRunContext, unit: ComputeUnit):
        assert isinstance(unit, TableSink)
        sink_config = unit.config
        assert len(context.compute_results) > 0
        df = None
        for result_id, result_df in context.compute_results.items():
            if not df:
                df = result_df
            else:
                # TODO join by cols
                df = df.join(result_df)
        if sink_config.cols:
            df.selectExpr(*split_cols(sink_config.cols))

        writer: DataFrameWriter = (
            df.write.mode(sink_config.mode.value).format(sink_config.format.value)
        )
        writer.saveAsTable(sink_config.name)
