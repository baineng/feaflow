from abc import ABC
from typing import Optional

from typing_extensions import Literal

from feaflow.model import ComponentConfig


class Source(ABC):
    pass


class QuerySourceConfig(ComponentConfig):
    type: Literal["query"] = "query"
    sql: str
    alias: Optional[str] = None

    @classmethod
    def get_impl_cls(cls):
        return QuerySource


class QuerySource(Source):
    def __init__(self, config: QuerySourceConfig):
        assert isinstance(config, QuerySourceConfig)
        self._config = config

    @property
    def config(self):
        return self._config

    @property
    def alias(self):
        return self._config.alias

    @property
    def sql(self):
        return self._config.sql
