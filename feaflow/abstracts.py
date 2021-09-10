from abc import ABC, abstractmethod
from typing import Type

from pydantic import BaseModel, constr


class FeaflowModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True


class FeaflowImmutableModel(FeaflowModel):
    class Config:
        allow_mutation = False


class FeaflowComponent(ABC):
    @classmethod
    @abstractmethod
    def create_config(cls, **data):
        """ :rtype: `feaflow.abstracts.FeaflowConfig` """
        raise NotImplementedError

    @property
    @abstractmethod
    def config(self):
        """ :rtype: `feaflow.abstracts.FeaflowConfig` """
        raise NotImplementedError

    def __init__(self, config):
        raise NotImplementedError


class FeaflowConfig(FeaflowImmutableModel, ABC):
    impl_cls: Type[FeaflowComponent]
    type: str


class EngineConfig(FeaflowConfig, ABC):
    name: constr(regex=r"^[^_][\w]+$", strip_whitespace=True, strict=True)


class SchedulerConfig(FeaflowConfig, ABC):
    pass


class SourceConfig(FeaflowConfig, ABC):
    pass


class ComputeConfig(FeaflowConfig, ABC):
    pass


class SinkConfig(FeaflowConfig, ABC):
    pass


class Engine(FeaflowComponent):
    @abstractmethod
    def new_session(self):
        """ :rtype: `feaflow.abstracts.EngineSession` """
        raise NotImplementedError


class EngineSession(ABC):
    @abstractmethod
    def run(self, job):
        """ :type job: `feaflow.job.Job` """
        raise NotImplementedError

    @abstractmethod
    def __enter__(self):
        """ :rtype: `feaflow.abstracts.EngineSession` """
        raise NotImplementedError

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError


class Scheduler(FeaflowComponent, ABC):
    pass


class Source(FeaflowComponent, ABC):
    pass


class Compute(FeaflowComponent, ABC):
    pass


class Sink(FeaflowComponent, ABC):
    pass
