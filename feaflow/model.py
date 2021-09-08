from abc import ABC, abstractmethod

from pydantic import BaseModel, constr


class FeaflowModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True


class FeaflowImmutableModel(FeaflowModel):
    class Config:
        allow_mutation = False


class ComponentConfig(FeaflowImmutableModel, ABC):
    @classmethod
    @abstractmethod
    def get_impl_cls(cls):
        raise NotImplementedError


class Engine(ABC):
    pass


class EngineConfig(ComponentConfig, ABC):
    name: constr(regex=r"^[^_][\w]+$", strip_whitespace=True, strict=True)
