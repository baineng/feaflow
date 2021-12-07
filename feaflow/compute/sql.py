import logging
from typing import Any, Dict, Optional, Tuple

from typing_extensions import Literal

from feaflow.abstracts import Compute, ComputeConfig

logger = logging.getLogger(__name__)


class SqlComputeConfig(ComputeConfig):
    _template_attrs: Tuple[str] = ("sql", "desc")
    type: Literal["sql"] = "sql"

    sql: str
    desc: Optional[str] = None


class SqlCompute(Compute):
    def __init__(self, config: SqlComputeConfig):
        logger.info("Constructing SqlCompute")
        logger.debug("With config %s", config)
        assert isinstance(config, SqlComputeConfig)
        super().__init__(config)

    def get_sql(self, template_context: Optional[Dict[str, Any]] = None) -> str:
        return self.get_config("sql", template_context)
