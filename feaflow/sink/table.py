import logging
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from pydantic import Field
from typing_extensions import Literal

from feaflow.abstracts import Sink, SinkConfig

logger = logging.getLogger(__name__)


class TableSinkMode(str, Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"


class TableSinkFormat(str, Enum):
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"
    ORC = "orc"


class TableSinkConfig(SinkConfig):
    _template_attrs: Tuple[str] = ("name", "from_", "partition")
    type: Literal["table"] = "table"

    name: str
    from_: Optional[str] = Field(alias="from", default=None)
    mode: TableSinkMode = TableSinkMode.APPEND
    format: TableSinkFormat = TableSinkFormat.PARQUET
    partition: Optional[List[str]] = None


class TableSink(Sink):
    def __init__(self, config: TableSinkConfig):
        logger.info("Constructing TableSink")
        logger.debug("With config %s", config)
        assert isinstance(config, TableSinkConfig)
        super().__init__(config)

    def get_name(self, template_context: Optional[Dict[str, Any]] = None) -> str:
        return self.get_config("name", template_context)

    def get_from(
        self, template_context: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        return self.get_config("from_", template_context)
