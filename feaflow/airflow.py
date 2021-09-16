from datetime import datetime, timedelta
from typing import List, Optional, Union

from airflow import DAG
from airflow.operators.python import PythonOperator
from dateutil.relativedelta import relativedelta

from feaflow.abstracts import SchedulerConfig
from feaflow.job import Job
from feaflow.project import Project


class AirflowSchedulerConfig(SchedulerConfig):
    owner: str = "airflow"
    start_date: datetime
    schedule_interval: Union[str, timedelta, relativedelta] = timedelta(days=1)
    end_date: Optional[datetime] = None
    catchup: bool = False
    dagrun_timeout: Optional[timedelta] = None
    depends_on_past: bool = False
    retries: int = 1
    retry_delay: Optional[timedelta] = None


def create_dags_from_project(project: Project) -> List[DAG]:
    jobs = project.scan_jobs()
    dags = []
    for job in jobs:
        if not isinstance(job.scheduler_config, AirflowSchedulerConfig):
            # TODO log the case that jobs rely on other scheduler
            continue
        dags.append(create_dag_from_job(project, job))
    return dags


def create_dag_from_job(project: Project, job: Job) -> DAG:
    airflow_config = job.scheduler_config
    assert isinstance(airflow_config, AirflowSchedulerConfig)

    default_args = {
        "owner": airflow_config.owner,
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
        _ = PythonOperator(
            task_id="run_job",
            python_callable=_run_job,
            op_kwargs={"project": project, "job": job},
        )
        return dag


def _run_job(project: Project, job: Job):
    project.run_job(job)
