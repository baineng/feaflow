from typing import Any, Dict, Optional

from pydantic.typing import Literal

from feaflow.abstracts import Source, SourceConfig
from feaflow.utils import template_substitute


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
        super().__init__(config)

    def get_alias(
        self, template_context: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        return (
            template_substitute(self._config.alias, template_context)
            if self._config.alias is not None
            else None
        )

    def get_sql(self, template_context: Optional[Dict[str, Any]] = None) -> str:
        return template_substitute(self._config.sql, template_context)
