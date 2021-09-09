from abc import ABC

from pydantic.typing import Literal

from feaflow.model import ComputeConfig

BUILTIN_COMPUTES = {"sql": "feaflow.compute.SqlComputeConfig"}


class Compute(ABC):
    pass


def create_compute_from_config(config: ComputeConfig) -> Compute:
    impl_class = config.get_impl_cls()
    assert issubclass(impl_class, Compute)
    return impl_class(config)


class SqlComputeConfig(ComputeConfig):
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
