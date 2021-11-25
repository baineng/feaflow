import logging
from datetime import timedelta
from typing import Any, Dict, Optional, Tuple

from pydantic import Field
from typing_extensions import Literal

from feaflow.abstracts import FeaflowImmutableModel, Sink, SinkConfig
from feaflow.sink.table import TableSinkFormat

logger = logging.getLogger(__name__)


class FeatureViewIngestConfig(FeaflowImmutableModel):
    _template_attrs: Tuple[str] = ("from_", "into_table")

    from_: str = Field(alias="from")
    into_table: str
    store_format: TableSinkFormat = TableSinkFormat.PARQUET


class FeatureViewSinkConfig(SinkConfig):
    _template_attrs: Tuple[str] = ("name", "batch_source")
    type: Literal["feature_view"] = "feature_view"

    name: str
    ttl: timedelta = timedelta(seconds=(24 * 60 * 60))  # 24 hours
    ingest: FeatureViewIngestConfig
    tags: Optional[Dict[str, str]] = None


class FeatureViewSink(Sink):
    def __init__(self, config: FeatureViewSinkConfig):
        logger.info("Constructing FeatureViewSink")
        logger.debug("With config %s", config)
        assert isinstance(config, FeatureViewSinkConfig)
        super().__init__(config)

    def get_name(self, template_context: Optional[Dict[str, Any]] = None) -> str:
        return self.get_config("name", template_context)

    def get_from(
        self, template_context: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        return self.get_config("from_", template_context)
