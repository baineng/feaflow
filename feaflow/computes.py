from pydantic.typing import Literal

from feaflow.abstracts import Compute, ComputeConfig


class SqlComputeConfig(ComputeConfig):
    type: Literal["sql"]
    sql: str


class SqlCompute(Compute):
    @classmethod
    def create_config(cls, **data):
        return SqlComputeConfig(impl_cls=cls, **data)

    def __init__(self, config: SqlComputeConfig):
        assert isinstance(config, SqlComputeConfig)
        self._config = config

    @property
    def config(self):
        return self._config

    @property
    def sql(self):
        return self._config.sql
