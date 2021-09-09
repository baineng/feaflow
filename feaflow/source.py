from abc import ABC
from typing import Optional

from pydantic.typing import Literal

from feaflow.model import SourceConfig

BUILTIN_SOURCES = {"query": "feaflow.source.QuerySourceConfig"}


class Source(ABC):
    pass


def create_source_from_config(config: SourceConfig) -> Source:
    impl_class = config.get_impl_cls()
    assert issubclass(impl_class, Source)
    return impl_class(config)


class QuerySourceConfig(SourceConfig):
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
