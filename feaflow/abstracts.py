from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple, Type

from pydantic import BaseModel


class FeaflowModel(BaseModel):
    # the fields which are going to do template rendering
    _template_attrs: Tuple[str] = ()

    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True


class FeaflowImmutableModel(FeaflowModel):
    class Config:
        allow_mutation = False


class FeaflowConfigurableComponent(ABC):
    @classmethod
    @abstractmethod
    def create_config(cls, **data) -> FeaflowConfig:
        raise NotImplementedError

    def __init__(self, config: FeaflowConfig):
        self._config = config

    @property
    def type(self):
        return self._config.type

    def get_config(
        self,
        name: Optional[str] = None,
        template_context: Optional[Dict[str, Any]] = None,
    ) -> Any:
        if name is None:
            from feaflow.utils import render_template

            return render_template(self._config, template_context)
        elif name in self._config._template_attrs:
            from feaflow.utils import render_template

            return (
                render_template(self._config.__getattribute__(name), template_context)
                if self._config.__getattribute__(name) is not None
                else None
            )
        else:
            return self._config.__getattribute__(name)


class FeaflowConfig(FeaflowImmutableModel, ABC):

    # the impl_cls must have one constructor argument named "config"
    impl_cls: Type[FeaflowConfigurableComponent]

    # the type of the impl of this config, should be a `Literal`
    type: str


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


class SchedulerConfig(FeaflowImmutableModel, ABC):
    pass
