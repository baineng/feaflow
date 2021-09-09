from pydantic.typing import Literal

from feaflow.abstracts import Compute, ComputeConfig


class SqlComputeConfig(ComputeConfig):
    type: Literal["sql"] = "sql"
    sql: str

    def create_impl_instance(self):
        return SqlCompute(self)


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
