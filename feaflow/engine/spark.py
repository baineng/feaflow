from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from pydantic.typing import Literal
from pyspark.sql import DataFrame, DataFrameWriter, SparkSession

from feaflow.abstracts import Component
from feaflow.compute.sql import SqlCompute
from feaflow.engine import (
    ComponentHandler,
    ComputeTask,
    Engine,
    EngineConfig,
    EngineSession,
    ExecutionEnvironment,
    FeaflowDAG,
    SinkTask,
    SourceTask,
)
from feaflow.exceptions import EngineExecuteError, EngineInitError
from feaflow.job import Job
from feaflow.sink.feature_view import FeatureViewSink
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
            FeatureViewSinkHandler,
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

        exec_dag = self.handle(job, template_context)

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

        self._execute(exec_env, exec_dag)

    def stop(self):
        if self._spark_session:
            self._spark_session.stop()

    def _execute(
        self,
        exec_env: SparkExecutionEnvironment,
        exec_dag: FeaflowDAG,
    ):
        try:
            for _task in exec_dag.source_tasks:
                table_id, _df = _task.execution_func(exec_env)
                for source_id in _task.ids:
                    exec_env.template_context.update({source_id: table_id})

            for _task in exec_dag.compute_tasks:
                table_id = _task.execution_func(exec_env)
                for compute_id in _task.ids:
                    exec_env.template_context.update({compute_id: table_id})

            for _task in exec_dag.sink_tasks:
                _task.execution_func(exec_env)
        except Exception as ex:
            raise EngineExecuteError(str(ex), exec_env, exec_dag)

    def _get_or_create_spark_session(
        self, job_name: str, config_overlay: Optional[Dict[str, Any]] = None
    ) -> SparkSession:
        if self._spark_session is None:
            logger.info(
                "Creating a new SparkSession with config overlay: %s",
                config_overlay,
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
            app_name = (
                f"{engine_config.job_name_prefix}_{job_name}"
                if engine_config.job_name_prefix
                else job_name
            )
            builder = SparkSession.builder.master(engine_config.master).appName(
                app_name
            )
            for k, v in engine_config.config.items():
                builder = builder.config(k, v)
            if engine_config.enable_hive_support:
                builder = builder.enableHiveSupport()
            spark = builder.getOrCreate()
            self._set_spark_loglevel(spark)
            return spark
        except Exception:
            raise EngineInitError(engine_config.type)

    def _set_spark_loglevel(self, spark: SparkSession):
        curr_root_loglevel = logging.root.level
        if curr_root_loglevel == logging.NOTSET:
            return
        loglevel_mapping = {
            logging.CRITICAL: "FATAL",
            logging.ERROR: "ERROR",
            logging.WARNING: "WARN",
            logging.INFO: "INFO",
            logging.DEBUG: "DEBUG",
        }
        if curr_root_loglevel not in loglevel_mapping:
            return
        new_loglevel = loglevel_mapping[curr_root_loglevel]
        spark.sparkContext.setLogLevel(new_loglevel)
        logger.info("Adjust Spark logging level to '%s'", new_loglevel)

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


class QuerySourceHandler(ComponentHandler):
    @classmethod
    def can_handle(cls, comp: Component) -> bool:
        result = isinstance(comp, QuerySource)
        logger.debug(
            "Check if QuerySourceHandler could handle the component '%s', result is %s",
            comp,
            result,
        )
        return result

    @classmethod
    def handle(cls, source: QuerySource) -> SourceTask:
        logger.info("QuerySourceHandler is handling source '%s'", source.type)
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

            logger.info("Start creating table '%s' by sql: \n%s", table_id, sql)
            df = spark.sql(sql)
            df.createOrReplaceTempView(table_id)
            logger.info("Created temp view '%s'", table_id)
            return table_id, df

        return SourceTask(execution_func=execution_func)


class PandasDataFrameSourceHandler(ComponentHandler):
    @classmethod
    def can_handle(cls, comp: Component) -> bool:
        result = isinstance(comp, PandasDataFrameSource)
        logger.debug(
            "Check if PandasDataFrameSourceHandler could handle the component '%s', result is %s",
            comp,
            result,
        )
        return result

    @classmethod
    def handle(cls, source: PandasDataFrameSource) -> SourceTask:
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

        return SourceTask(execution_func=execution_func)


# === Compute Handlers ===


class SqlComputeHandler(ComponentHandler):
    @classmethod
    def can_handle(cls, comp: Component) -> bool:
        result = isinstance(comp, SqlCompute)
        logger.debug(
            "Check if SqlComputeHandler could handle the component '%s', result is %s",
            comp,
            result,
        )
        return result

    @classmethod
    def handle(cls, compute: SqlCompute) -> ComputeTask:
        logger.info("SqlComputeHandler is handling compute '%s'", compute.type)
        assert isinstance(compute, SqlCompute)

        def execution_func(exec_env: SparkExecutionEnvironment) -> str:
            logger.info("Start execute compute '%s'", compute.type)
            spark = exec_env.spark_session

            table_id = f"compute_{compute.type}_{create_random_str()}"

            sql = compute.get_sql(exec_env.template_context)
            logger.info("Start creating table '%s' by sql: \n%s", table_id, sql)
            df = spark.sql(sql)
            df.createOrReplaceTempView(table_id)
            logger.info("Created temp view '%s'", table_id)

            return table_id

        return ComputeTask(execution_func=execution_func)


# === Sink Handlers ===


class TableSinkHandler(ComponentHandler):
    @classmethod
    def can_handle(cls, comp: Component) -> bool:
        result = isinstance(comp, TableSink)
        logger.debug(
            "Check if TableSinkHandler could handle the component '%s', result is %s",
            comp,
            result,
        )
        return result

    @classmethod
    def handle(cls, sink: TableSink) -> SinkTask:
        logger.info("TableSinkHandler is handling sink '%s'", sink.type)
        assert isinstance(sink, TableSink)

        def execution_func(exec_env: SparkExecutionEnvironment):
            from pyspark.sql.functions import lit

            from_sql = sink.get_from(exec_env.template_context)
            if from_sql is None:
                assert (
                    "compute_0" in exec_env.template_context
                ), "There is no compute result found"
                compute_0 = exec_env.template_context["compute_0"]
                from_sql = f"SELECT * FROM {compute_0}"
            logger.info("Creating TableSink input data by from_sql: \n%s", from_sql)
            from_df = exec_env.spark_session.sql(from_sql)

            partition = sink.get_config("partition", exec_env.template_context)
            partition_cols = []
            if partition:
                for part_spec in partition:
                    part_col, part_val = map(str.strip, part_spec.split("="))
                    logger.info(
                        "Add partition column '%s' and value '%s'", part_col, part_val
                    )
                    from_df = from_df.withColumn(part_col, lit(part_val))
                    partition_cols.append(part_col)

            spark = exec_env.spark_session
            writer: DataFrameWriter = from_df.write.mode(sink.get_config("mode").value)
            sink_table_name = sink.get_name(exec_env.template_context)
            if spark._jsparkSession.catalog().tableExists(sink_table_name):
                logger.info("Inserting into sink table '%s'", sink_table_name)
                # if sink table exists, just inert into there
                spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
                writer.insertInto(sink_table_name)
                # may use spark sql "insert overwrite table ... partion (...)" in the future
            else:
                # otherwise create the sink table
                table_format = sink.get_config("format").value
                writer = writer.format(table_format)
                if len(partition_cols) > 0:
                    writer = writer.partitionBy(partition_cols)
                logger.info(
                    "Sink table '%s' does not exist, creating the table and inserting data into it. "
                    "format: '%s', partition: '%s'",
                    sink_table_name,
                    table_format,
                    partition_cols,
                )
                writer.saveAsTable(sink_table_name)

        return SinkTask(execution_func=execution_func)


class FeatureViewSinkHandler(ComponentHandler):
    @classmethod
    def can_handle(cls, comp: Component) -> bool:
        result = isinstance(comp, FeatureViewSink)
        logger.debug(
            "Check if FeatureViewSink could handle the component '%s', result is %s",
            comp,
            result,
        )
        return result

    @classmethod
    def handle(cls, sink: FeatureViewSink) -> SinkTask:
        logger.info("FeatureViewSinkHandler is handling sink '%s'", sink.type)
        assert isinstance(sink, FeatureViewSink)

        def execution_func(exec_env: SparkExecutionEnvironment):
            ingest_cfg = sink.get_ingest_config(exec_env.template_context)
            datasource_cfg = sink.get_datasource_config(exec_env.template_context)

            select_sql = ingest_cfg.select_sql
            ingest_table = ingest_cfg.store_table
            store_format = ingest_cfg.store_format
            partition_cols = ingest_cfg.partition_columns

            spark = exec_env.spark_session
            logger.info("Ingesting to FeatureViewSink by sql: \n%s", select_sql)
            select_df = spark.sql(select_sql)
            writer: DataFrameWriter = select_df.write.mode(ingest_cfg.store_mode.value)

            if spark._jsparkSession.catalog().tableExists(ingest_table):
                logger.info("Inserting into sink table '%s'", ingest_table)
                # if sink table exists, just inert into there
                spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
                writer.insertInto(ingest_table)
                # may use spark sql "insert overwrite table ... partion (...)" in the future
            else:
                # otherwise create the sink table
                writer = writer.format(store_format)
                if partition_cols:
                    writer = writer.partitionBy(partition_cols)
                logger.info(
                    "Sink table '%s' does not exist, creating the table and inserting data into it. "
                    "format: '%s', partition: '%s'",
                    ingest_table,
                    store_format,
                    partition_cols,
                )
                writer.saveAsTable(ingest_table)

        return SinkTask(execution_func=execution_func)
