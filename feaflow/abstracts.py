from abc import ABC, abstractmethod
from typing import Any, Type

from pydantic import BaseModel, constr


class FeaflowModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True


class FeaflowImmutableModel(FeaflowModel):
    class Config:
        allow_mutation = False


class FeaflowConfig(FeaflowImmutableModel, ABC):
    impl_cls: Any


class FeaflowComponent(ABC):
    @classmethod
    @abstractmethod
    def create_config(cls, **data):
        raise NotImplementedError

    def __init__(self, config: FeaflowConfig):
        raise NotImplementedError


class EngineConfig(FeaflowConfig, ABC):
    name: constr(regex=r"^[^_][\w]+$", strip_whitespace=True, strict=True)


class Engine(FeaflowComponent):
    @abstractmethod
    def run(self, job: "feaflow.job.Job"):
        raise NotImplementedError


class SchedulerConfig(FeaflowConfig, ABC):
    pass


class Scheduler(FeaflowComponent, ABC):
    pass


class SourceConfig(FeaflowConfig, ABC):
    pass


class Source(FeaflowComponent, ABC):
    pass


class ComputeConfig(FeaflowConfig, ABC):
    pass


class Compute(FeaflowComponent, ABC):
    pass


class SinkConfig(FeaflowConfig, ABC):
    pass


class Sink(FeaflowComponent, ABC):
    pass
