from datetime import datetime, timedelta

import pytest
from airflow import DAG
from airflow.models import DagBag, TaskInstance
from pytz import utc

from feaflow.airflow import AirflowScheduler


@pytest.fixture
def job1_dag(example_project):
    airflow_scheduler = AirflowScheduler(example_project)
    dags = airflow_scheduler.create_project_dags()
    assert len(dags) == 1
    job1_dag: DAG = next(filter(lambda d: d.dag_id == "test_job1", dags))
    return job1_dag


def test_create_dag(job1_dag):
    assert job1_dag.owner == "feaflow_test"
    assert job1_dag.schedule_interval == "0 6 * * *"
    assert job1_dag.start_date == datetime(2021, 9, 10, tzinfo=utc)
    assert job1_dag.end_date == datetime(2021, 11, 1, tzinfo=utc)
    assert job1_dag.catchup == True
    assert job1_dag.dagrun_timeout == timedelta(seconds=300)
    assert job1_dag.default_args["depends_on_past"] == True
    assert job1_dag.default_args["retries"] == 2
    assert job1_dag.default_args["retry_delay"] == timedelta(seconds=10)


def test_dag_import(example_project):
    dag_bag = DagBag(dag_folder=example_project.root_path, include_examples=False)
    assert len(dag_bag.import_errors) == 0, "No Import Failures"


def test_dag_from_dag_bag(example_project):
    dag_bag = DagBag(dag_folder=example_project.root_path, include_examples=False)
    assert len(dag_bag.dags) == 1
    job1_dag: DAG = dag_bag.dags["test_job1"]
    test_create_dag(job1_dag)


@pytest.mark.skip
def test_run_dag(job1_dag):
    run_job_task = job1_dag.tasks[0]
    ti = TaskInstance(task=run_job_task, execution_date=datetime.now())
    result = run_job_task.execute(ti.get_template_context())
    assert result is True
