from datetime import datetime
from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

from feaflow.airflow_config import AirflowSchedulerConfig
from feaflow.job import Job, JobConfig
from feaflow.project import Project
from feaflow.utils import (
    construct_scheduler_config_from_dict,
    construct_template_context,
    deep_merge_models,
    render_template,
)


def create_dags_from_project(project: Project) -> List[DAG]:
    jobs = project.scan_jobs()
    dags = []
    for job_config in jobs:
        if not isinstance(job_config.scheduler, AirflowSchedulerConfig):
            # TODO log the case that jobs rely on other scheduler
            continue
        dags.append(create_dag_from_job(project, job_config))
    return dags


DEFAULT_TASK_ID = "run_job"


def create_dag_from_job(project: Project, job_config: JobConfig) -> DAG:
    # get scheduler config
    scheduler_config = job_config.scheduler
    assert isinstance(scheduler_config, AirflowSchedulerConfig)
    project_default_scheduler_config = project.config.scheduler_default
    if project_default_scheduler_config is not None:
        _default_scheduler_config = construct_scheduler_config_from_dict(
            project_default_scheduler_config
        )
        if type(_default_scheduler_config) == type(scheduler_config):
            scheduler_config = deep_merge_models(
                scheduler_config, _default_scheduler_config
            )
    scheduler_config = render_template(
        scheduler_config,
        template_context=construct_template_context(project, job_config),
        use_jinja2=False,
    )

    # construct the dag
    dag_args = scheduler_config.dict(
        exclude={"dag_id", "task_id", "docker", "default_args"}
    )
    dag_args = {k: v for k, v in dag_args.items() if v is not None}
    dag_args["dag_id"] = scheduler_config.dag_id or job_config.name
    if scheduler_config.default_args:
        dag_args["default_args"] = {
            k: v
            for k, v in scheduler_config.default_args.dict().items()
            if v is not None
        }
    task_id = scheduler_config.task_id or DEFAULT_TASK_ID

    with DAG(**dag_args) as dag:

        if scheduler_config.docker:
            docker_args = scheduler_config.docker.dict(exclude_none=True)
            _ = DockerOperator(task_id=task_id, **docker_args)

        else:
            _ = PythonOperator(
                task_id=task_id,
                python_callable=_python_run_job,
                op_kwargs={"project": project, "job_config": job_config},
            )

        return dag


def _python_run_job(
    project: Project, job_config: JobConfig, execution_date: datetime, **airflow_context
):
    template_context = construct_template_context(
        project, job_config, execution_date, airflow_context
    )
    job = Job(job_config)
    project.run_job(job, execution_date, template_context)
