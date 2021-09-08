from enum import Enum


class Engine(str, Enum):
    SPARK_SQL = "spark-sql"
    HIVE = "hive"
