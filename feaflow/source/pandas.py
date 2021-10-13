import logging
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

import pandas as pd
from pydantic import Field
from typing_extensions import Literal

from feaflow.abstracts import FeaflowImmutableModel, Source, SourceConfig

logger = logging.getLogger(__name__)


class PandasDataFrameSourceSupportedFileTypes(str, Enum):
    PICKLE = "pickle"
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    ORC = "orc"


class PandasDataFrameSourceFileConfig(FeaflowImmutableModel):
    _template_attrs: Tuple[str] = ("path", "args")

    type: PandasDataFrameSourceSupportedFileTypes
    path: Union[str, Path]
    args: Dict[str, Any] = {}


class PandasDataFrameSourceConfig(SourceConfig):
    _template_attrs: Tuple[str] = ("file",)
    type: Literal["pandas"] = "pandas"

    dict_: Optional[Dict[str, Any]] = Field(alias="dict", default=None)
    file: Optional[PandasDataFrameSourceFileConfig] = None

    def __init__(self, **data):
        assert "dict" in data or "file" in data
        super().__init__(**data)


class PandasDataFrameSource(Source):
    def __init__(self, config: PandasDataFrameSourceConfig):
        logger.info("Constructing PandasDataFrameSource")
        logger.debug("With config %s", config)
        assert isinstance(config, PandasDataFrameSourceConfig)
        super().__init__(config)

    def get_dataframe(
        self, template_context: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        config: PandasDataFrameSourceConfig = self.get_config(
            template_context=template_context
        )
        if config.dict_ is not None:
            logger.info("Constructing a Pandas DataFrame from a Dict")
            return pd.DataFrame(config.dict_)
        elif config.file is not None:
            logger.info(
                "Constructing a Pandas DataFrame from a file type: '%s', path: '%s'",
                config.file.type,
                config.file.path,
            )
            _mapping = {
                PandasDataFrameSourceSupportedFileTypes.PICKLE: pd.read_pickle,
                PandasDataFrameSourceSupportedFileTypes.CSV: pd.read_csv,
                PandasDataFrameSourceSupportedFileTypes.JSON: pd.read_json,
                PandasDataFrameSourceSupportedFileTypes.PARQUET: pd.read_parquet,
            }
            try:
                # Since read_orc was added in Pandas 1.0.0, here just ignore the AttributeError for old version
                _mapping[PandasDataFrameSourceSupportedFileTypes.ORC] = pd.read_orc
            except AttributeError:
                pass
            return _mapping[config.file.type](config.file.path, **config.file.args)
