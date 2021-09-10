BUILTIN_ENGINES = {"spark": "feaflow.engine.spark.SparkEngine"}
BUILTIN_SCHEDULERS = {"airflow": "feaflow.airflow.AirflowScheduler"}
BUILTIN_SOURCES = {"query": "feaflow.sources.QuerySource"}
BUILTIN_COMPUTES = {"sql": "feaflow.computes.SqlCompute"}

BUILTIN_SINKS = {"table": "feaflow.sinks.TableSink", "redis": "feaflow.sinks.RedisSink"}