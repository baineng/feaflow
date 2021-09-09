from typing import Optional

from pydantic.typing import Literal

from feaflow.abstracts import Source, SourceConfig


class QuerySourceConfig(SourceConfig):
    type: Literal["query"] = "query"
    sql: str
    alias: Optional[str] = None

    def create_impl_instance(self):
        return QuerySource(self)


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
