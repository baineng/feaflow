from typing import Tuple

from typing_extensions import Literal

from feaflow.abstracts import Sink, SinkConfig


class RedisSinkConfig(SinkConfig):
    _template_attrs: Tuple[str] = ("host", "port", "db")
    type: Literal["redis"] = "redis"

    host: str
    port: int = 6379
    db: int = 0


class RedisSink(Sink):
    def __init__(self, config: RedisSinkConfig):
        assert isinstance(config, RedisSinkConfig)
        super().__init__(config)
