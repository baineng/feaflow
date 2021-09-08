from abc import ABC

from typing_extensions import Literal

from feaflow.model import ComponentConfig, FeaflowModel


class Compute(ABC):
    pass


class SqlComputeConfig(ComponentConfig):
    type: Literal["sql"] = "sql"
    sql: str

    @classmethod
    def get_impl_cls(cls):
        return SqlCompute


class SqlCompute(Compute):
    def __init__(self, config: SqlComputeConfig):
        assert isinstance(config, SqlComputeConfig)
        self._config = config

    @property
    def config(self):
        return self._config

    @property
    def sql(self):
        return self._config.sql
