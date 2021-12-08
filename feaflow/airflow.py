import logging
from typing import Any, Dict, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

from feaflow.airflow_config import AirflowSchedulerConfig
from feaflow.job import Job
from feaflow.job_config import JobConfig
from feaflow.project import Project
from feaflow.utils import (
    construct_scheduler_config_from_dict,
    construct_template_context,
    deep_merge_models,
    render_template,
)

DEFAULT_TASK_ID = "run_job"

logger = logging.getLogger(__name__)


def create_dags_from_project(
    project: Project,
    dag_args_overlay: Optional[Dict[str, Any]] = None,
) -> List[DAG]:
    """
    Create Airflow DAGs based on Feaflow project
    """
    logger.info("Creating DAGs from project '%s'", project.name)
    jobs = project.scan_jobs()
    logger.info("Scanned %s jobs", len(jobs))
    dags = []
    for job_config in jobs:
        if not isinstance(job_config.scheduler, AirflowSchedulerConfig):
            # TODO log the case that jobs rely on other scheduler
            logger.warning(
                "Detected a scheduler config is not a instance of AirflowSchedulerConfig"
            )
            continue
        dag = create_dag_from_job(project, job_config, dag_args_overlay)
        logger.info("Created a new DAG '%s'", dag.dag_id)
        dags.append(dag)
    return dags


def create_dag_from_job(
    project: Project,
    job_config: JobConfig,
    dag_args_overlay: Optional[Dict[str, Any]] = None,
) -> DAG:
    logger.info(
        "Creating a DAG from project '%s' and job '%s'", project.name, job_config.name
    )
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
    logger.debug("Scheduler config: %s", scheduler_config)

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
    if dag_args_overlay:
        if "default_args" in dag_args_overlay:
            if "default_args" in dag_args:
                dag_args["default_args"].update(dag_args_overlay["default_args"])
            else:
                dag_args["default_args"] = dag_args_overlay["default_args"]
            del dag_args_overlay["default_args"]
        dag_args.update(dag_args_overlay)

    task_id = scheduler_config.task_id or DEFAULT_TASK_ID

    logger.debug("Creating DAG with arguments: %s", dag_args)
    with DAG(**dag_args) as dag:

        if scheduler_config.docker:
            logger.info("Detected docker config, gonna create DockerOperator")
            docker_args = scheduler_config.docker.dict(exclude_none=True)
            logger.debug("Creating DockerOperator with arguments: %s", docker_args)
            _ = DockerOperator(task_id=task_id, **docker_args)

        else:
            logger.info("Haven't detected docker config, gonna create PythonOperator")
            python_args = {"project": project, "job_config": job_config}
            logger.debug("Creating PythonOperator with arguments: %s", python_args)
            _ = PythonOperator(
                task_id=task_id,
                python_callable=_python_run_job,
                op_kwargs=python_args,
            )

        return dag


def _python_run_job(project: Project, job_config: JobConfig, **airflow_context):
    execution_date = airflow_context["execution_date"]

    logger.info(
        "PythonOperator starts, project: '%s', job: '%s', execution_date: '%s', airflow_context: '%s'",
        project.name,
        job_config.name,
        execution_date,
        airflow_context.keys(),
    )
    job = Job(job_config)
    template_context = construct_template_context(
        project, job_config, None, airflow_context
    )
    project.run_job(job, execution_date, template_context)
