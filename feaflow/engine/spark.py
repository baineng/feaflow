import copy
from typing import Any, Dict, Optional, Tuple

from pydantic.typing import Literal
from pyspark.sql import DataFrame, DataFrameWriter, SparkSession

from feaflow.abstracts import ComputeUnit
from feaflow.compute.sql import SqlCompute
from feaflow.engine import (
    ComputeUnitHandler,
    Engine,
    EngineConfig,
    EngineRunContext,
    EngineSession,
)
from feaflow.exceptions import EngineInitError
from feaflow.job import Job
from feaflow.sink.table import TableSink
from feaflow.source.pandas import PandasDataFrameSource
from feaflow.source.query import QuerySource
from feaflow.utils import create_random_str, deep_merge_models, split_cols


class SparkEngineConfig(EngineConfig):
    type: Literal["spark"]
    _template_attrs: Tuple[str] = ("master", "job_name_prefix")

    master: str = "local"
    enable_hive_support: bool = False
    config: Dict[str, str] = {}
    job_name_prefix: Optional[str] = None


class SparkEngine(Engine):
    @classmethod
    def create_config(cls, **data):
        return SparkEngineConfig(impl_cls=cls, **data)

    def __init__(self, config: SparkEngineConfig):
        assert isinstance(config, SparkEngineConfig)
        super().__init__(config)

    def new_session(self):
        return SparkEngineSession(self)


class SparkEngineSession(EngineSession):
    def __init__(self, engine: SparkEngine):
        self._engine = engine
        self._spark_session: Optional[SparkSession] = None
        self.set_handlers(
            [
                QuerySourceHandler,
                PandasDataFrameSourceHandler,
                SqlComputeHandler,
                TableSinkHandler,
            ]
        )

    @property
    def engine(self) -> SparkEngine:
        return self._engine

    def run(self, job: Job, upstream_template_context: Optional[Dict[str, Any]] = None):
        template_context = upstream_template_context or {}
        engine_name = self._engine.get_config("name", template_context)
        assert (
            job.engine_name == engine_name
        ), f"The job '{job}' is not able to be run on engine '{engine_name}'."

        spark_session = self._get_or_create_spark_session(
            job.name, job.config.engine.config_overlay
        )
        run_context = SparkEngineRunContext(
            engine=self._engine,
            engine_session=self,
            spark_session=spark_session,
            template_context=template_context,
        )
        self.handle(run_context, job)

    def stop(self):
        if self._spark_session:
            self._spark_session.stop()

    def _get_or_create_spark_session(
        self, job_name: str, config_overlay: Optional[Dict[str, Any]] = None
    ) -> SparkSession:
        if self._spark_session is None:
            self._spark_session = self._create_spark_session(job_name, config_overlay)
        return self._spark_session

    def _create_spark_session(
        self, job_name: str, config_overlay: Optional[Dict[str, Any]] = None
    ) -> SparkSession:
        engine_config: SparkEngineConfig = self._engine.get_config()

        if config_overlay is not None:
            assert "type" not in config_overlay, "type is not changeable"
            engine_config = deep_merge_models(
                engine_config, SparkEngineConfig(type="spark", **config_overlay)
            )

        try:
            builder = SparkSession.builder.master(engine_config.master).appName(
                f"{engine_config.job_name_prefix}_{job_name}"
            )
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
        self.stop()


class SparkEngineRunContext(EngineRunContext):
    engine: SparkEngine
    engine_session: SparkEngineSession
    spark_session: SparkSession
    source_results: Dict[str, DataFrame] = {}
    compute_results: Dict[str, DataFrame] = {}


# === Source Handlers ===


class QuerySourceHandler(ComputeUnitHandler):
    @classmethod
    def can_handle(cls, unit: ComputeUnit) -> bool:
        return isinstance(unit, QuerySource)

    @classmethod
    def handle(cls, run_context: EngineRunContext, unit: ComputeUnit):
        assert isinstance(run_context, SparkEngineRunContext)
        assert isinstance(unit, QuerySource)

        spark = run_context.spark_session
        template_context = run_context.template_context

        df_id = (
            unit.get_alias(template_context)
            or f"source_{unit.type}_{create_random_str()}"
        )
        df = spark.sql(unit.get_sql(template_context))
        df.createOrReplaceTempView(df_id)
        run_context.source_results[df_id] = df


class PandasDataFrameSourceHandler(ComputeUnitHandler):
    @classmethod
    def can_handle(cls, unit: ComputeUnit) -> bool:
        return isinstance(unit, PandasDataFrameSource)

    @classmethod
    def handle(cls, run_context: EngineRunContext, unit: ComputeUnit):
        assert isinstance(run_context, SparkEngineRunContext)
        assert isinstance(unit, PandasDataFrameSource)

        spark = run_context.spark_session
        df_id = f"source_{unit.type}_{create_random_str()}"
        df = spark.createDataFrame(unit.get_dataframe(run_context.template_context))
        df.createOrReplaceTempView(df_id)
        run_context.source_results[df_id] = df


# === Compute Handlers ===


class SqlComputeHandler(ComputeUnitHandler):
    @classmethod
    def can_handle(cls, unit: ComputeUnit) -> bool:
        return isinstance(unit, SqlCompute)

    @classmethod
    def handle(cls, run_context: EngineRunContext, unit: ComputeUnit):
        assert isinstance(run_context, SparkEngineRunContext)
        assert isinstance(unit, SqlCompute)

        spark = run_context.spark_session
        template_context = {
            f"source_{index}": source_df_id
            for index, source_df_id in zip(
                range(len(run_context.source_results)),
                run_context.source_results.keys(),
            )
        }
        template_context.update(run_context.template_context)

        df = spark.sql(unit.get_sql(template_context))
        df_id = f"compute_{unit.type}_{create_random_str()}"
        df.createOrReplaceTempView(df_id)
        run_context.compute_results[df_id] = df


# === Sink Handlers ===


class TableSinkHandler(ComputeUnitHandler):
    @classmethod
    def can_handle(cls, unit: ComputeUnit) -> bool:
        return isinstance(unit, TableSink)

    @classmethod
    def handle(cls, run_context: EngineRunContext, unit: ComputeUnit):
        assert isinstance(run_context, SparkEngineRunContext)
        assert isinstance(unit, TableSink)
        assert len(run_context.compute_results) > 0

        template_context = run_context.template_context
        df = None
        for result_id, result_df in run_context.compute_results.items():
            if not df:
                df = result_df
            else:
                # TODO join by cols
                df = df.join(result_df)

        if unit.get_cols() is not None:
            cols = unit.get_cols(template_context)
            df.selectExpr(*split_cols(cols))

        writer: DataFrameWriter = (
            df.write.mode(unit.get_config("mode").value).format(
                unit.get_config("format").value
            )
        )
        writer.saveAsTable(unit.get_name(template_context))
