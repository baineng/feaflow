from typing import Optional

from pydantic.typing import Literal

from feaflow.abstracts import Source, SourceConfig


class QuerySourceConfig(SourceConfig):
    type: Literal["query"]
    sql: str
    alias: Optional[str] = None


class QuerySource(Source):
    @classmethod
    def create_config(cls, **data):
        return QuerySourceConfig(impl_cls=cls, **data)

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
