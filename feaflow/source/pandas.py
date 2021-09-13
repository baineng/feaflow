from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Union

import pandas as pd
from pydantic import Field
from typing_extensions import Literal

from feaflow.abstracts import FeaflowImmutableModel, Source, SourceConfig


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
