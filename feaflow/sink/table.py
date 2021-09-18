from enum import Enum
from typing import Any, Dict, Optional, Tuple

from typing_extensions import Literal

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
    _template_attrs: Tuple[str] = ("name", "cols", "partition_cols")
    type: Literal["table"] = "table"

    name: str
    cols: Optional[str] = None
    mode: TableSinkMode = TableSinkMode.APPEND
    format: TableSinkFormat = TableSinkFormat.PARQUET
    partition_cols: Optional[str] = None


class TableSink(Sink):
    def __init__(self, config: TableSinkConfig):
        assert isinstance(config, TableSinkConfig)
        super().__init__(config)

    def get_name(self, template_context: Optional[Dict[str, Any]] = None) -> str:
        return self.get_config("name", template_context)

    def get_cols(
        self, template_context: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        return self.get_config("cols", template_context)
