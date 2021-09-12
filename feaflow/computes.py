from typing import Any, Dict, Optional

from pydantic.typing import Literal

from feaflow.abstracts import Compute, ComputeConfig
from feaflow.utils import template_substitute


class SqlComputeConfig(ComputeConfig):
    type: Literal["sql"]
    sql: str


class SqlCompute(Compute):
    @classmethod
    def create_config(cls, **data):
        return SqlComputeConfig(impl_cls=cls, **data)

    def __init__(self, config: SqlComputeConfig):
        assert isinstance(config, SqlComputeConfig)
        super().__init__(config)

    def get_sql(self, template_context: Optional[Dict[str, Any]] = None):
        return template_substitute(self._config.sql, template_context)
