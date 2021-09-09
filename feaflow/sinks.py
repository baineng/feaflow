from pydantic.typing import Literal

from feaflow.abstracts import Sink, SinkConfig


class RedisSinkConfig(SinkConfig):
    type: Literal["redis"] = "redis"
    host: str
    port: int = 6379
    db: int = 0

    def create_impl_instance(self):
        return RedisSink(self)


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
