BUILTIN_ENGINES = {"spark": "feaflow.engine.spark.SparkEngine"}

BUILTIN_SOURCES = {
    "query": "feaflow.sources.QuerySource",
    "pandas": "feaflow.sources.PandasDataFrameSource",
}

BUILTIN_COMPUTES = {"sql": "feaflow.computes.SqlCompute"}

BUILTIN_SINKS = {"table": "feaflow.sinks.TableSink", "redis": "feaflow.sinks.RedisSink"}
