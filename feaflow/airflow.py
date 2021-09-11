from datetime import datetime, timedelta
from typing import List, Optional, Union

from airflow import DAG
from airflow.operators.python import PythonOperator
from dateutil.relativedelta import relativedelta
from pydantic.typing import Literal

from feaflow.abstracts import Scheduler, SchedulerConfig
from feaflow.job import Job
from feaflow.project import Project


class AirflowSchedulerConfig(SchedulerConfig):
    type: Literal["airflow"]
    schedule_interval: Union[str, timedelta, relativedelta] = timedelta(days=1)
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    catchup: bool = False
    dagrun_timeout: Optional[timedelta] = None
    depends_on_past: bool = False
    retries: int = 1
    retry_delay: Optional[timedelta] = None


class AirflowScheduler(Scheduler):
    @classmethod
    def create_config(cls, **data):
        return AirflowSchedulerConfig(impl_cls=cls, **data)

    def __init__(self, project: Project):
        self._project = project

    def create_project_dags(self) -> List[DAG]:
        jobs = self._project.scan_jobs()
        dags = []
        for job in jobs:
            if not isinstance(job.scheduler_config, AirflowSchedulerConfig):
                # TODO log the case that jobs rely on other scheduler
                continue
            dags.append(self.create_dag_by_job(job))
        return dags

    def create_dag_by_job(self, job: Job) -> DAG:
        airflow_config = job.scheduler_config
        assert isinstance(airflow_config, AirflowSchedulerConfig)

        default_args = {
            "depends_on_past": airflow_config.depends_on_past,
            "retries": airflow_config.retries,
        }
        if airflow_config.retry_delay:
            default_args["retry_delay"] = airflow_config.retry_delay

        with DAG(
            job.name,
            default_args=default_args,
            schedule_interval=airflow_config.schedule_interval,
            start_date=airflow_config.start_date,
            end_date=airflow_config.end_date,
            catchup=airflow_config.catchup,
            dagrun_timeout=airflow_config.dagrun_timeout,
        ) as dag:

            job = PythonOperator()

            return dag
