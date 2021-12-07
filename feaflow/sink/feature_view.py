import logging
from datetime import timedelta
from typing import Any, Dict, Optional, Tuple

from typing_extensions import Literal

from feaflow.abstracts import FeaflowImmutableModel, Sink, SinkConfig
from feaflow.sink.table import TableSinkFormat, TableSinkMode

logger = logging.getLogger(__name__)


class FeatureViewIngestConfig(FeaflowImmutableModel):
    _template_attrs: Tuple[str] = (
        "select_sql",
        "store_table",
    )

    select_sql: str
    store_table: str
    store_mode: TableSinkMode = TableSinkMode.APPEND
    store_format: TableSinkFormat = TableSinkFormat.PARQUET


class FeatureViewDataSourceConfig(FeaflowImmutableModel):
    _template_attrs: Tuple[str] = (
        "class_name",
        "event_timestamp_column",
        "created_timestamp_column",
        "field_mapping",
        "date_partition_column",
        "other_arguments",
    )

    class_name: str
    event_timestamp_column: str
    created_timestamp_column: Optional[str] = None
    field_mapping: Optional[Dict[str, str]] = None
    date_partition_column: Optional[str] = None
    other_arguments: Optional[Dict[str, Any]] = None

    def __init__(self, **data: Any):
        new_data = {}

        reserved_keys = [
            "class_name",
            "event_timestamp_column",
            "created_timestamp_column",
            "field_mapping",
            "date_partition_column",
        ]
        for rk in reserved_keys:
            if rk in data:
                new_data[rk] = data[rk]
                del data[rk]

        new_data["other_arguments"] = data
        super().__init__(**new_data)


class FeatureViewSinkConfig(SinkConfig):
    _template_attrs: Tuple[str] = ("name", "ttl", "tags", "ingest", "datasource")
    type: Literal["feature_view"] = "feature_view"

    name: str
    ttl: timedelta = timedelta(seconds=(24 * 60 * 60))  # 24 hours
    tags: Optional[Dict[str, str]] = None
    ingest: FeatureViewIngestConfig
    datasource: FeatureViewDataSourceConfig


class FeatureViewSink(Sink):
    def __init__(self, config: FeatureViewSinkConfig):
        logger.info("Constructing FeatureViewSink")
        logger.debug("With config %s", config)
        assert isinstance(config, FeatureViewSinkConfig)
        super().__init__(config)

    def get_name(self, template_context: Optional[Dict[str, Any]] = None) -> str:
        return self.get_config("name", template_context)

    def get_ingest_config(
        self, template_context: Optional[Dict[str, Any]] = None
    ) -> FeatureViewIngestConfig:
        return self.get_config("ingest", template_context)

    def get_datasource_config(
        self, template_context: Optional[Dict[str, Any]] = None
    ) -> FeatureViewDataSourceConfig:
        return self.get_config("datasource", template_context)
