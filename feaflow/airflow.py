from typing import Optional

from pydantic.typing import Literal

from feaflow.abstracts import Scheduler, SchedulerConfig


class AirflowSchedulerConfig(SchedulerConfig):
    type: Literal["airflow"] = "airflow"
    schedule_interval: str
    depends_on: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    catchup: bool = False
    retries: int = 1

    def create_impl_instance(self):
        return AirflowScheduler(self)


class AirflowScheduler(Scheduler):
    def __init__(self, config: AirflowSchedulerConfig):
        assert isinstance(config, AirflowSchedulerConfig)
        self._config = config

    @property
    def config(self):
        return self._config
