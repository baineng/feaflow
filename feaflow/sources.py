from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Union

import pandas as pd
from pydantic import Field
from pydantic.typing import Literal

from feaflow.abstracts import FeaflowImmutableModel, Source, SourceConfig
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


class PandasDataFrameSourceSupportedFileTypes(str, Enum):
    PICKLE = "pickle"
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    ORC = "orc"


class PandasDataFrameSourceFileConfig(FeaflowImmutableModel):
    type: PandasDataFrameSourceSupportedFileTypes
    path: Union[str, Path]
    args: Dict[str, Any] = {}


class PandasDataFrameSourceConfig(SourceConfig):
    type: Literal["pandas"]
    dict_: Optional[Dict[str, Any]] = Field(alias="dict", default=None)
    file: Optional[PandasDataFrameSourceFileConfig] = None

    def __init__(self, **data):
        assert "dict" in data or "file" in data
        super().__init__(**data)


class PandasDataFrameSource(Source):
    @classmethod
    def create_config(cls, **data):
        return PandasDataFrameSourceConfig(impl_cls=cls, **data)

    def __init__(self, config: PandasDataFrameSourceConfig):
        assert isinstance(config, PandasDataFrameSourceConfig)
        super().__init__(config)

    def get_dataframe(self) -> pd.DataFrame:
        config: PandasDataFrameSourceConfig = self.config
        if config.dict_ is not None:
            return pd.DataFrame(config.dict_)
        elif config.file is not None:
            _mapping = {
                PandasDataFrameSourceSupportedFileTypes.PICKLE: pd.read_pickle,
                PandasDataFrameSourceSupportedFileTypes.CSV: pd.read_csv,
                PandasDataFrameSourceSupportedFileTypes.JSON: pd.read_json,
                PandasDataFrameSourceSupportedFileTypes.PARQUET: pd.read_parquet,
                PandasDataFrameSourceSupportedFileTypes.ORC: pd.read_orc,
            }
            return _mapping[config.file.type](config.file.path, **config.file.args)
