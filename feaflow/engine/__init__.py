from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

from pydantic import constr

from feaflow.abstracts import (
    ComputeUnit,
    FeaflowConfig,
    FeaflowConfigurableComponent,
    FeaflowModel,
)
from feaflow.exceptions import EngineHandleError
from feaflow.job import Job


class Engine(FeaflowConfigurableComponent, ABC):
    @abstractmethod
    def new_session(self):
        """ :rtype: `feaflow.engine.EngineSession` """
        raise NotImplementedError


class ExecutionEnvironment(FeaflowModel, ABC):
    template_context: Dict[str, Any] = {}
    engine: Engine
    engine_session: EngineSession
    execution_date: datetime


class ExecutionUnit(FeaflowModel):
    pass


class SourceExecutionUnit(ExecutionUnit):
    ids: Tuple[str] = ()
    execution_func: Callable[[ExecutionEnvironment], Any]

    def add_id(self, id: str):
        self.ids = self.ids + (id,)


class ComputeExecutionUnit(ExecutionUnit):
    ids: Tuple[str] = ()
    execution_func: Callable[[ExecutionEnvironment], None]

    def add_id(self, id: str):
        self.ids = self.ids + (id,)


class SinkExecutionUnit(ExecutionUnit):
    execution_func: Callable[[ExecutionEnvironment], None]


class ExecutionGraph(FeaflowModel):
    job: Job
    source_execution_units: List[SourceExecutionUnit]
    compute_execution_units: List[ComputeExecutionUnit]
    sink_execution_units: List[SinkExecutionUnit]


class ComputeUnitHandler(ABC):
    @classmethod
    def can_handle(cls, unit: ComputeUnit) -> bool:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def handle(cls, unit: ComputeUnit) -> ExecutionUnit:
        raise NotImplementedError


class EngineSession(ABC):
    _handlers: List[Type[ComputeUnitHandler]] = None

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

    def get_handlers(self) -> Optional[List[Type[ComputeUnitHandler]]]:
        return self._handlers

    def set_handlers(self, handlers: List[Type[ComputeUnitHandler]]):
        self._handlers = handlers

    def handle(
        self, job, template_context: Optional[Dict[str, Any]] = None
    ) -> ExecutionGraph:
        source_execution_units = []
        compute_execution_units = []
        sink_execution_units = []

        for inx, source in enumerate(job.sources):
            unit: SourceExecutionUnit = self._handle_one_unit(source)
            unit.add_id(f"source_{inx}")
            source_execution_units.append(unit)

        for inx, compute in enumerate(job.computes):
            unit: ComputeExecutionUnit = self._handle_one_unit(compute)
            unit.add_id(f"compute_{inx}")
            compute_execution_units.append(unit)

        for sink in job.sinks:
            unit = self._handle_one_unit(sink)
            sink_execution_units.append(unit)

        return ExecutionGraph(
            job=job,
            source_execution_units=source_execution_units,
            compute_execution_units=compute_execution_units,
            sink_execution_units=sink_execution_units,
        )

    def _handle_one_unit(self, unit: ComputeUnit) -> ExecutionUnit:
        try:
            for handler in self._handlers:
                if handler.can_handle(unit):
                    return handler.handle(unit)
        except Exception as ex:
            raise EngineHandleError(str(ex), type(unit).__name__)

        raise EngineHandleError(f"Not handler found", type(unit).__name__)

    @abstractmethod
    def __enter__(self):
        """ :rtype: `feaflow.engine.EngineSession` """
        raise NotImplementedError

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError


class EngineConfig(FeaflowConfig, ABC):
    name: constr(regex=r"^[^_][\w]+$", strip_whitespace=True, strict=True)
