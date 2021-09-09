from abc import ABC

from pydantic.typing import Literal

from feaflow.model import SinkConfig

BUILTIN_SINKS = {"redis": "feaflow.sink.RedisSinkConfig"}


class Sink(ABC):
    pass


def create_sink_from_config(config: SinkConfig) -> Sink:
    impl_class = config.get_impl_cls()
    assert issubclass(impl_class, Sink)
    return impl_class(config)


class RedisSinkConfig(SinkConfig):
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
