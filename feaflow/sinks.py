from enum import Enum
from typing import Optional

from pydantic.typing import Literal

from feaflow.abstracts import Sink, SinkConfig


class TableSinkMode(str, Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"


class TableSinkFormat(str, Enum):
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"
    ORC = "orc"


class TableSinkConfig(SinkConfig):
    type: Literal["table"]
    name: str
    mode: TableSinkMode = TableSinkMode.APPEND
    format: TableSinkFormat = TableSinkFormat.PARQUET
    cols: Optional[str] = None
    partition_cols: Optional[str] = None


class TableSink(Sink):
    @classmethod
    def create_config(cls, **data):
        return TableSinkConfig(impl_cls=cls, **data)

    def __init__(self, config: TableSinkConfig):
        assert isinstance(config, TableSinkConfig)
        self._config = config

    @property
    def config(self):
        return self._config


class RedisSinkConfig(SinkConfig):
    type: Literal["redis"]
    host: str
    port: int = 6379
    db: int = 0


class RedisSink(Sink):
    @classmethod
    def create_config(cls, **data):
        return RedisSinkConfig(impl_cls=cls, **data)

    def __init__(self, config: RedisSinkConfig):
        assert isinstance(config, RedisSinkConfig)
        self._config = config

    @property
    def config(self):
        return self._config

    @property
    def host(self):
        return self._config.host

    @property
    def port(self):
        return self._config.port

    @property
    def db(self):
        return self._config.db
