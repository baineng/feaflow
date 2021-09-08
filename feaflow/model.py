from abc import ABC, abstractmethod

from pydantic import BaseModel


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
