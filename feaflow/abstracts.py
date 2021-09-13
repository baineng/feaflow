from abc import ABC, abstractmethod
from typing import Type

from pydantic import BaseModel

# === System Level Abstracts Start ===


class FeaflowModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True


class FeaflowImmutableModel(FeaflowModel):
    class Config:
        allow_mutation = False


class FeaflowConfigurableComponent(ABC):
    @classmethod
    @abstractmethod
    def create_config(cls, **data):
        """ :rtype: `feaflow.abstracts.FeaflowConfig` """
        raise NotImplementedError

    def __init__(self, config):
        """ :type config: `feaflow.abstracts.FeaflowConfig` """
        self._config = config

    @property
    def config(self):
        """ :rtype: `feaflow.abstracts.FeaflowConfig` """
        return self._config

    @property
    def type(self):
        """ :rtype: str """
        return self._config.type


class FeaflowConfig(FeaflowImmutableModel, ABC):
    impl_cls: Type[FeaflowConfigurableComponent]
    type: str


# === System Level Abstracts End ===


# === ComputeUnit Abstracts Start ===


class ComputeUnit(FeaflowConfigurableComponent, ABC):
    pass


class Source(ComputeUnit, ABC):
    pass


class Compute(ComputeUnit, ABC):
    pass


class Sink(ComputeUnit, ABC):
    pass


class SourceConfig(FeaflowConfig, ABC):
    impl_cls: Type[Source]


class ComputeConfig(FeaflowConfig, ABC):
    impl_cls: Type[Compute]


class SinkConfig(FeaflowConfig, ABC):
    impl_cls: Type[Sink]


# === ComputeUnit Abstracts End ===

# === Scheduler Abstracts Start ===


class SchedulerConfig(FeaflowImmutableModel, ABC):
    pass


# === Scheduler Abstracts End ===
