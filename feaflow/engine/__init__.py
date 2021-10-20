from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

from pydantic import constr

from feaflow.abstracts import (
    Component,
    FeaflowConfig,
    FeaflowConfigurableComponent,
    FeaflowModel,
)
from feaflow.exceptions import EngineHandleError
from feaflow.job import Job


class Engine(FeaflowConfigurableComponent, ABC):
    @abstractmethod
    def new_session(self):
        """:rtype: `feaflow.engine.EngineSession`"""
        raise NotImplementedError


class ExecutionEnvironment(FeaflowModel, ABC):
    template_context: Dict[str, Any] = {}
    engine: Engine
    engine_session: EngineSession
    execution_date: datetime


class Task(FeaflowModel):
    pass


class SourceTask(Task):
    ids: Tuple[str] = ()
    execution_func: Callable[[ExecutionEnvironment], Any]

    def add_id(self, id: str):
        self.ids = self.ids + (id,)


class ComputeTask(Task):
    ids: Tuple[str] = ()
    execution_func: Callable[[ExecutionEnvironment], None]

    def add_id(self, id: str):
        self.ids = self.ids + (id,)


class SinkTask(Task):
    execution_func: Callable[[ExecutionEnvironment], None]


class FeaflowDAG(FeaflowModel):
    job: Job
    source_tasks: List[SourceTask]
    compute_tasks: List[ComputeTask]
    sink_tasks: List[SinkTask]


class ComponentHandler(ABC):
    @classmethod
    def can_handle(cls, comp: Component) -> bool:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def handle(cls, comp: Component) -> Task:
        raise NotImplementedError


class EngineSession(ABC):
    _handlers: List[Type[ComponentHandler]] = None

    @abstractmethod
    def run(
        self,
        job,
        execution_date: datetime,
        upstream_template_context: Optional[Dict[str, Any]] = None,
    ):
        """
        :type job: `feaflow.job.Job`
        :type execution_date: `datetime.datetime`
        :type upstream_template_context: Optional[Dict[str, Any]]
        """
        raise NotImplementedError

    def get_handlers(self) -> Optional[List[Type[ComponentHandler]]]:
        return self._handlers

    def set_handlers(self, handlers: List[Type[ComponentHandler]]):
        self._handlers = handlers

    def handle(
        self, job, template_context: Optional[Dict[str, Any]] = None
    ) -> FeaflowDAG:
        source_tasks = []
        compute_tasks = []
        sink_tasks = []

        for inx, source in enumerate(job.sources):
            s_task: SourceTask = self._handle_comp(source)
            s_task.add_id(f"source_{inx}")
            source_tasks.append(s_task)

        for inx, compute in enumerate(job.computes):
            c_task: ComputeTask = self._handle_comp(compute)
            c_task.add_id(f"compute_{inx}")
            compute_tasks.append(c_task)

        for sink in job.sinks:
            si_task = self._handle_comp(sink)
            sink_tasks.append(si_task)

        return FeaflowDAG(
            job=job,
            source_tasks=source_tasks,
            compute_tasks=compute_tasks,
            sink_tasks=sink_tasks,
        )

    def _handle_comp(self, comp: Component) -> Task:
        try:
            for handler in self._handlers:
                if handler.can_handle(comp):
                    return handler.handle(comp)
        except Exception as ex:
            raise EngineHandleError(str(ex), type(comp).__name__)

        raise EngineHandleError(f"Not handler found", type(comp).__name__)

    @abstractmethod
    def __enter__(self):
        """:rtype: `feaflow.engine.EngineSession`"""
        raise NotImplementedError

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError


class EngineConfig(FeaflowConfig, ABC):
    name: constr(regex=r"^[^_][\w]+$", strip_whitespace=True, strict=True)
