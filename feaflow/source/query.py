import logging
from typing import Any, Dict, Optional, Tuple

from typing_extensions import Literal

from feaflow.abstracts import Source, SourceConfig

logger = logging.getLogger(__name__)


class QuerySourceConfig(SourceConfig):
    _template_attrs: Tuple[str] = ("sql", "alias")
    type: Literal["query"] = "query"

    sql: str
    alias: Optional[str] = None


class QuerySource(Source):
    def __init__(self, config: QuerySourceConfig):
        logger.info("Constructing QuerySource")
        logger.debug("With config %s", config)
        assert isinstance(config, QuerySourceConfig)
        super().__init__(config)

    def get_alias(
        self, template_context: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        return self.get_config("alias", template_context)

    def get_sql(self, template_context: Optional[Dict[str, Any]] = None) -> str:
        return self.get_config("sql", template_context)
