from enum import Enum
from typing import Any, Dict, Optional

from typing_extensions import Literal

from feaflow.abstracts import Sink, SinkConfig
from feaflow.utils import template_substitute


class TableSinkMode(str, Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"


class TableSinkFormat(str, Enum):
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"
    ORC = "orc"


class TableSinkConfig(SinkConfig):
    type: Literal["table"]
    name: str
    cols: Optional[str] = None
    mode: TableSinkMode = TableSinkMode.APPEND
    format: TableSinkFormat = TableSinkFormat.PARQUET
    partition_cols: Optional[str] = None


class TableSink(Sink):
    @classmethod
    def create_config(cls, **data):
        return TableSinkConfig(impl_cls=cls, **data)

    def __init__(self, config: TableSinkConfig):
        assert isinstance(config, TableSinkConfig)
        super().__init__(config)

    def get_name(self, template_context: Optional[Dict[str, Any]] = None) -> str:
        return template_substitute(self._config.name, template_context)

    def get_cols(
        self, template_context: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        return (
            template_substitute(self._config.cols, template_context)
            if self._config.cols is not None
            else None
        )
