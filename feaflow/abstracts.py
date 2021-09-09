from abc import ABC, abstractmethod

from pydantic import constr

from feaflow.model import ComponentConfig


class EngineConfig(ComponentConfig, ABC):
    name: constr(regex=r"^[^_][\w]+$", strip_whitespace=True, strict=True)


class Engine(ABC):
    @abstractmethod
    def init(self):
        raise NotImplementedError

    @abstractmethod
    def run(self, job: "feaflow.project.job"):
        raise NotImplementedError


class SchedulerConfig(ComponentConfig, ABC):
    pass


class Scheduler(ABC):
    pass


class SourceConfig(ComponentConfig, ABC):
    pass


class Source(ABC):
    pass


class ComputeConfig(ComponentConfig, ABC):
    pass


class Compute(ABC):
    pass


class SinkConfig(ComponentConfig, ABC):
    pass


class Sink(ABC):
    pass
