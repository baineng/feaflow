from typing import Tuple

from typing_extensions import Literal

from feaflow.abstracts import Sink, SinkConfig


class RedisSinkConfig(SinkConfig):
    _template_attrs: Tuple[str] = ("host", "port", "db")
    type: Literal["redis"]

    host: str
    port: int = 6379
    db: int = 0


class RedisSink(Sink):
    @classmethod
    def create_config(cls, **data) -> RedisSinkConfig:
        return RedisSinkConfig(impl_cls=cls, **data)

    def __init__(self, config: RedisSinkConfig):
        assert isinstance(config, RedisSinkConfig)
        super().__init__(config)
