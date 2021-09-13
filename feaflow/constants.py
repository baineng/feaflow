BUILTIN_ENGINES = {"spark": "feaflow.engine.spark.SparkEngine"}

BUILTIN_SOURCES = {
    "query": "feaflow.source.query.QuerySource",
    "pandas": "feaflow.source.pandas.PandasDataFrameSource",
}

BUILTIN_COMPUTES = {"sql": "feaflow.compute.sql.SqlCompute"}

BUILTIN_SINKS = {
    "table": "feaflow.sink.table.TableSink",
    "redis": "feaflow.sink.redis.RedisSink",
}
