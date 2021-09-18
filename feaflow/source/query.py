from typing import Any, Dict, Optional, Tuple

from typing_extensions import Literal

from feaflow.abstracts import Source, SourceConfig


class QuerySourceConfig(SourceConfig):
    _template_attrs: Tuple[str] = ("sql", "alias")
    type: Literal["query"]

    sql: str
    alias: Optional[str] = None


class QuerySource(Source):
    @classmethod
    def create_config(cls, **data) -> QuerySourceConfig:
        return QuerySourceConfig(impl_cls=cls, **data)

    def __init__(self, config: QuerySourceConfig):
        assert isinstance(config, QuerySourceConfig)
        super().__init__(config)

    def get_alias(
        self, template_context: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        return self.get_config("alias", template_context)

    def get_sql(self, template_context: Optional[Dict[str, Any]] = None) -> str:
        return self.get_config("sql", template_context)
