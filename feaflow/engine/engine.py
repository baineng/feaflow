from abc import ABC, abstractmethod

from feaflow.job import Job


class Engine(ABC):
    @abstractmethod
    def init(self):
        raise NotImplementedError

    @abstractmethod
    def run(self, job: Job):
        raise NotImplementedError
