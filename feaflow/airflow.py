from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

from airflow import DAG
from airflow.models.baseoperator import TaskStateChangeCallback
from airflow.models.dag import DagStateChangeCallback
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from dateutil.relativedelta import relativedelta
from docker.types import Mount

from feaflow.abstracts import FeaflowImmutableModel, SchedulerConfig
from feaflow.job import Job
from feaflow.project import Project


class DockerOperatorConfig(FeaflowImmutableModel):
    image: str
    api_version: Optional[str] = None
    command: Optional[Union[str, List[str]]] = None
    container_name: Optional[str] = None
    cpus: Optional[float] = None
    docker_url: Optional[str] = None
    environment: Optional[Dict] = None
    private_environment: Optional[Dict] = None
    force_pull: Optional[bool] = None
    mem_limit: Optional[Union[float, str]] = None
    host_tmp_dir: Optional[str] = None
    network_mode: Optional[str] = None
    tls_ca_cert: Optional[str] = None
    tls_client_cert: Optional[str] = None
    tls_client_key: Optional[str] = None
    tls_hostname: Optional[Union[str, bool]] = None
    tls_ssl_version: Optional[str] = None
    mount_tmp_dir: Optional[bool] = None
    tmp_dir: Optional[str] = None
    user: Optional[Union[str, int]] = None
    mounts: Optional[List[Mount]] = None
    entrypoint: Optional[Union[str, List[str]]] = None
    working_dir: Optional[str] = None
    xcom_all: Optional[bool] = None
    docker_conn_id: Optional[str] = None
    dns: Optional[List[str]] = None
    dns_search: Optional[List[str]] = None
    auto_remove: Optional[bool] = None
    shm_size: Optional[int] = None
    tty: Optional[bool] = None
    privileged: Optional[bool] = None
    cap_add: Optional[Iterable[str]] = None
    extra_hosts: Optional[Dict[str, str]] = None


class OperatorDefaultArgs(FeaflowImmutableModel):
    owner: Optional[str] = None
    email: Optional[Union[str, Iterable[str]]] = None
    email_on_retry: Optional[bool] = None
    email_on_failure: Optional[bool] = None
    retries: Optional[int] = None
    retry_delay: Optional[timedelta] = None
    retry_exponential_backoff: Optional[bool] = None
    max_retry_delay: Optional[timedelta] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    depends_on_past: Optional[bool] = None
    wait_for_downstream: Optional[bool] = None
    params: Optional[Dict] = None
    default_args: Optional[Dict] = None
    priority_weight: Optional[int] = None
    weight_rule: Optional[str] = None
    queue: Optional[str] = None
    pool: Optional[str] = None
    pool_slots: Optional[int] = None
    sla: Optional[timedelta] = None
    execution_timeout: Optional[timedelta] = None
    on_execute_callback: Optional[TaskStateChangeCallback] = None
    on_failure_callback: Optional[TaskStateChangeCallback] = None
    on_success_callback: Optional[TaskStateChangeCallback] = None
    on_retry_callback: Optional[TaskStateChangeCallback] = None
    trigger_rule: Optional[str] = None
    resources: Optional[Dict] = None
    run_as_user: Optional[str] = None
    task_concurrency: Optional[int] = None
    executor_config: Optional[Dict] = None
    do_xcom_push: Optional[bool] = None
    inlets: Optional[Any] = None
    outlets: Optional[Any] = None
    task_group: Optional["TaskGroup"] = None
    doc: Optional[str] = None
    doc_md: Optional[str] = None
    doc_json: Optional[str] = None
    doc_yaml: Optional[str] = None
    doc_rst: Optional[str] = None


class AirflowSchedulerConfig(SchedulerConfig):
    dag_id: Optional[str] = None
    description: Optional[str] = None
    schedule_interval: Optional[Union[str, timedelta, relativedelta]] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    full_filepath: Optional[str] = None
    template_searchpath: Optional[Union[str, Iterable[str]]] = None
    user_defined_macros: Optional[Dict] = None
    user_defined_filters: Optional[Dict] = None
    default_args: Optional[OperatorDefaultArgs] = None
    concurrency: Optional[int] = None
    max_active_runs: Optional[int] = None
    dagrun_timeout: Optional[timedelta] = None
    sla_miss_callback: Optional[Callable] = None
    default_view: Optional[str] = None
    orientation: Optional[str] = None
    catchup: Optional[bool] = None
    on_success_callback: Optional[DagStateChangeCallback] = None
    on_failure_callback: Optional[DagStateChangeCallback] = None
    doc_md: Optional[str] = None
    params: Optional[Dict] = None
    access_control: Optional[Dict] = None
    is_paused_upon_creation: Optional[bool] = None
    jinja_environment_kwargs: Optional[Dict] = None
    render_template_as_native_obj: Optional[bool] = None
    tags: Optional[List[str]] = None
    task_id: Optional[str] = None
    docker: Optional[DockerOperatorConfig] = None


def create_dags_from_project(project: Project) -> List[DAG]:
    jobs = project.scan_jobs()
    dags = []
    for job in jobs:
        if not isinstance(job.scheduler_config, AirflowSchedulerConfig):
            # TODO log the case that jobs rely on other scheduler
            continue
        dags.append(create_dag_from_job(project, job))
    return dags


DEFAULT_TASK_ID = "run_job"


def create_dag_from_job(project: Project, job: Job) -> DAG:
    config = job.scheduler_config
    assert isinstance(config, AirflowSchedulerConfig)

    dag_args = config.dict(exclude={"dag_id", "task_id", "docker", "default_args"})
    dag_args = {k: v for k, v in dag_args.items() if v is not None}
    dag_args["dag_id"] = config.dag_id or job.name
    if config.default_args:
        dag_args["default_args"] = {
            k: v for k, v in config.default_args.dict().items() if v is not None
        }
    task_id = config.task_id or DEFAULT_TASK_ID

    with DAG(**dag_args) as dag:

        if config.docker:
            docker_args = config.docker.dict()
            docker_args = {k: v for k, v in docker_args.items() if v is not None}
            _ = DockerOperator(task_id=task_id, **docker_args)

        else:

            def _run_job(project: Project, job: Job):
                project.run_job(job)

            _ = PythonOperator(
                task_id=task_id,
                python_callable=_run_job,
                op_kwargs={"project": project, "job": job},
            )

        return dag
