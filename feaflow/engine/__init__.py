from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Type

from pydantic import constr

from feaflow.abstracts import (
    ComputeUnit,
    FeaflowConfig,
    FeaflowConfigurableComponent,
    FeaflowModel,
)
from feaflow.exceptions import EngineHandleError


class Engine(FeaflowConfigurableComponent, ABC):
    @abstractmethod
    def new_session(self):
        """ :rtype: `feaflow.engine.EngineSession` """
        raise NotImplementedError


class EngineRunContext(FeaflowModel, ABC):
    template_context: Dict[str, Any] = {}
    engine: Engine
    execution_date: datetime


class ComputeUnitHandler(ABC):
    @classmethod
    def can_handle(cls, unit: ComputeUnit) -> bool:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def handle(cls, context: EngineRunContext, unit: ComputeUnit):
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

    def handle(self, run_context: EngineRunContext, job):
        """
        :type run_context: `EngineRunContext`
        :type job: `feaflow.job.Job`
        """
        # Handle Sources, then Computes, then Sinks
        for source in job.sources:
            self._handle_one_unit(run_context, source)
        for compute in job.computes:
            self._handle_one_unit(run_context, compute)
        for sink in job.sinks:
            self._handle_one_unit(run_context, sink)

    def _handle_one_unit(self, run_context: EngineRunContext, unit: ComputeUnit):
        _handled = False

        try:
            for handler in self._handlers:
                if not _handled and handler.can_handle(unit):
                    handler.handle(run_context, unit)
                    _handled = True
        except Exception as ex:
            raise EngineHandleError(str(ex), run_context, type(unit).__name__)

        if not _handled:
            raise EngineHandleError(
                f"Not handler found", run_context, type(unit).__name__
            )

    @abstractmethod
    def __enter__(self):
        """ :rtype: `feaflow.engine.EngineSession` """
        raise NotImplementedError

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError


class EngineConfig(FeaflowConfig, ABC):
    name: constr(regex=r"^[^_][\w]+$", strip_whitespace=True, strict=True)
