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
    @abstractmethod
    def create_impl_instance(self):
        """ create a new implementation instance of the component based on this config """
        raise NotImplementedError
