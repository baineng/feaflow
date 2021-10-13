from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from pydantic.typing import Literal
from pyspark.sql import DataFrame, DataFrameWriter, SparkSession

from feaflow.abstracts import ComputeUnit
from feaflow.compute.sql import SqlCompute
from feaflow.engine import (
    ComputeExecutionUnit,
    ComputeUnitHandler,
    Engine,
    EngineConfig,
    EngineSession,
    ExecutionEnvironment,
    ExecutionGraph,
    SinkExecutionUnit,
    SourceExecutionUnit,
)
from feaflow.exceptions import EngineExecuteError, EngineInitError
from feaflow.job import Job
from feaflow.sink.table import TableSink
from feaflow.source.pandas import PandasDataFrameSource
from feaflow.source.query import QuerySource
from feaflow.utils import create_random_str, deep_merge_models

logger = logging.getLogger(__name__)


class SparkEngineConfig(EngineConfig):
    type: Literal["spark"] = "spark"
    _template_attrs: Tuple[str] = ("master", "job_name_prefix")

    master: str = "local"
    enable_hive_support: bool = False
    config: Dict[str, str] = {}
    job_name_prefix: Optional[str] = None


class SparkEngine(Engine):
    def __init__(self, config: SparkEngineConfig):
        logger.info("Constructing new SparkEngine with config: %s", config)
        assert isinstance(config, SparkEngineConfig)
        super().__init__(config)

    def new_session(self):
        return SparkEngineSession(self)


class SparkEngineSession(EngineSession):
    def __init__(self, engine: SparkEngine):
        logger.info(
            "Constructing a new session for SparkEngine '%s'", engine.get_config("name")
        )
        self._engine = engine
        self._spark_session: Optional[SparkSession] = None
        handlers = [
            QuerySourceHandler,
            PandasDataFrameSourceHandler,
            SqlComputeHandler,
            TableSinkHandler,
        ]
        logger.info("Setting handlers for the session: %s", handlers)
        self.set_handlers(handlers)

    @property
    def engine(self) -> SparkEngine:
        return self._engine

    def run(
        self,
        job: Job,
        execution_date: datetime,
        upstream_template_context: Optional[Dict[str, Any]] = None,
    ):
        template_context = upstream_template_context or {}
        engine_name = self._engine.get_config("name", template_context)
        logger.info("Running job '%s' on SparkEngine '%s'", job.name, engine_name)

        assert (
            job.engine_name == engine_name
        ), f"The job '{job}' is not able to be run on engine '{engine_name}'."

        exec_graph = self.handle(job, template_context)

        spark_session = self._get_or_create_spark_session(
            job.name, job.config.engine.config_overlay
        )
        exec_env = SparkExecutionEnvironment(
            engine=self._engine,
            execution_date=execution_date,
            engine_session=self,
            spark_session=spark_session,
            template_context=template_context,
        )

        self._execute(exec_env, exec_graph)

    def stop(self):
        if self._spark_session:
            self._spark_session.stop()

    def _execute(
        self, exec_env: SparkExecutionEnvironment, exec_graph: ExecutionGraph,
    ):
        try:
            for _unit in exec_graph.source_execution_units:
                table_id, _df = _unit.execution_func(exec_env)
                for source_id in _unit.ids:
                    exec_env.template_context.update({source_id: table_id})

            for _unit in exec_graph.compute_execution_units:
                table_id = _unit.execution_func(exec_env)
                for compute_id in _unit.ids:
                    exec_env.template_context.update({compute_id: table_id})

            for _unit in exec_graph.sink_execution_units:
                _unit.execution_func(exec_env)
        except Exception as ex:
            raise EngineExecuteError(str(ex), exec_env, exec_graph)

    def _get_or_create_spark_session(
        self, job_name: str, config_overlay: Optional[Dict[str, Any]] = None
    ) -> SparkSession:
        if self._spark_session is None:
            logger.info(
                "Creating a new SparkSession with config overlay: %s", config_overlay,
            )
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

        logger.debug("Creating SparkSession with config %s", engine_config)

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


class SparkExecutionEnvironment(ExecutionEnvironment):
    engine: SparkEngine
    engine_session: SparkEngineSession
    spark_session: SparkSession
    source_results: Dict[str, DataFrame] = {}
    compute_results: Dict[str, DataFrame] = {}


# === Source Handlers ===


class QuerySourceHandler(ComputeUnitHandler):
    @classmethod
    def can_handle(cls, unit: ComputeUnit) -> bool:
        result = isinstance(unit, QuerySource)
        logger.debug(
            "Check if QuerySourceHandler could handle the unit '%s', result is %s",
            unit,
            result,
        )
        return result

    @classmethod
    def handle(cls, source: QuerySource) -> SourceExecutionUnit:
        logger.info("QuerySourceHandler is handling unit '%s'", source.type)
        assert isinstance(source, QuerySource)

        def execution_func(
            exec_env: SparkExecutionEnvironment,
        ) -> Tuple[str, DataFrame]:
            logger.info("Start execute source '%s'", source.type)
            spark = exec_env.spark_session
            table_id = (
                source.get_alias(exec_env.template_context)
                or f"source_{source.type}_{create_random_str()}"
            )
            sql = source.get_sql(exec_env.template_context)

            logger.info("Start creating table '%s' by sql: %s", table_id, sql)
            df = spark.sql(sql)
            df.createOrReplaceTempView(table_id)
            logger.info("Created temp view '%s'", table_id)
            return table_id, df

        return SourceExecutionUnit(execution_func=execution_func)


class PandasDataFrameSourceHandler(ComputeUnitHandler):
    @classmethod
    def can_handle(cls, unit: ComputeUnit) -> bool:
        result = isinstance(unit, PandasDataFrameSource)
        logger.debug(
            "Check if PandasDataFrameSourceHandler could handle the unit '%s', result is %s",
            unit,
            result,
        )
        return result

    @classmethod
    def handle(cls, source: PandasDataFrameSource) -> SourceExecutionUnit:
        logger.info("PandasDataFrameSourceHandler is handling source '%s'", source.type)
        assert isinstance(source, PandasDataFrameSource)

        def execution_func(
            exec_env: SparkExecutionEnvironment,
        ) -> Tuple[str, DataFrame]:
            logger.info("Start execute source '%s'", source.type)
            spark = exec_env.spark_session
            table_id = f"source_{source.type}_{create_random_str()}"

            logger.info("Start creating table '%s' by the Pandas Dataframe", table_id)
            df = spark.createDataFrame(source.get_dataframe(exec_env.template_context))
            df.createOrReplaceTempView(table_id)
            logger.info("Created temp view '%s'", table_id)

            return table_id, df

        return SourceExecutionUnit(execution_func=execution_func)


# === Compute Handlers ===


class SqlComputeHandler(ComputeUnitHandler):
    @classmethod
    def can_handle(cls, unit: ComputeUnit) -> bool:
        result = isinstance(unit, SqlCompute)
        logger.debug(
            "Check if SqlComputeHandler could handle the unit '%s', result is %s",
            unit,
            result,
        )
        return result

    @classmethod
    def handle(cls, compute: SqlCompute) -> ComputeExecutionUnit:
        logger.info("SqlComputeHandler is handling compute '%s'", compute.type)
        assert isinstance(compute, SqlCompute)

        def execution_func(exec_env: SparkExecutionEnvironment) -> str:
            logger.info("Start execute compute '%s'", compute.type)
            spark = exec_env.spark_session

            table_id = f"compute_{compute.type}_{create_random_str()}"

            sql = compute.get_sql(exec_env.template_context)
            logger.info("Start creating table '%s' by sql: %s", table_id, sql)
            df = spark.sql(sql)
            df.createOrReplaceTempView(table_id)
            logger.info("Created temp view '%s'", table_id)

            return table_id

        return ComputeExecutionUnit(execution_func=execution_func)


# === Sink Handlers ===


class TableSinkHandler(ComputeUnitHandler):
    @classmethod
    def can_handle(cls, unit: ComputeUnit) -> bool:
        result = isinstance(unit, TableSink)
        logger.debug(
            "Check if TableSinkHandler could handle the unit '%s', result is %s",
            unit,
            result,
        )
        return result

    @classmethod
    def handle(cls, sink: TableSink) -> SinkExecutionUnit:
        logger.info("TableSinkHandler is handling sink '%s'", sink.type)
        assert isinstance(sink, TableSink)

        def execution_func(exec_env: SparkExecutionEnvironment):
            from_sql = sink.get_from(exec_env.template_context)
            if from_sql is None:
                assert (
                    "compute_0" in exec_env.template_context
                ), "There is no compute result found"
                compute_0 = exec_env.template_context["compute_0"]
                from_sql = f"SELECT * FROM {compute_0}"

            from_df = exec_env.spark_session.sql(from_sql)
            writer: DataFrameWriter = (
                from_df.write.mode(sink.get_config("mode").value).format(
                    sink.get_config("format").value
                )
            )
            sink_table_name = sink.get_name(exec_env.template_context)
            writer.saveAsTable(sink_table_name)

        return SinkExecutionUnit(execution_func=execution_func)
