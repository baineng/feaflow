from abc import ABC

from typing_extensions import Literal

from feaflow.model import ComponentConfig, FeaflowModel


class Sink(ABC):
    pass


class RedisSinkConfig(ComponentConfig):
    type: Literal["redis"] = "redis"
    host: str
    port: int = 6379
    db: int = 0

    @classmethod
    def get_impl_cls(cls):
        return RedisSink


class RedisSink(Sink):
    def __init__(self, config: RedisSinkConfig):
        assert isinstance(config, RedisSinkConfig)
        self._config = config

    @property
    def config(self):
        return self._config

    @property
    def host(self):
        return self._config.host

    @property
    def port(self):
        return self._config.port

    @property
    def db(self):
        return self._config.db
