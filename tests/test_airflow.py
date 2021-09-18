from datetime import datetime, timedelta

import pytest
from airflow import DAG
from airflow.models import DagBag, TaskInstance
from pytz import utc

from feaflow import airflow

# Refs:
# https://medium.com/@chandukavar/testing-in-airflow-part-1-dag-validation-tests-dag-definition-tests-and-unit-tests-2aa94970570c
# https://www.astronomer.io/guides/testing-airflow


@pytest.fixture
def job1_dag(example_project) -> DAG:
    dags = airflow.create_dags_from_project(example_project)
    job1_dag = next(filter(lambda d: d.dag_id == "test_job1", dags))
    return job1_dag


@pytest.fixture
def job2_dag(example_project) -> DAG:
    dags = airflow.create_dags_from_project(example_project)
    job1_dag = next(filter(lambda d: d.dag_id == "test_job2", dags))
    return job1_dag


def test_create_dag(job1_dag):
    assert job1_dag.schedule_interval == "0 6 * * *"
    assert job1_dag.start_date == datetime(2021, 9, 10, tzinfo=utc)
    assert job1_dag.end_date == datetime(2021, 11, 1, tzinfo=utc)
    assert job1_dag.catchup == False
    assert job1_dag.dagrun_timeout == timedelta(seconds=300)
    assert job1_dag.default_args["depends_on_past"] == True
    assert job1_dag.default_args["retries"] == 2
    assert job1_dag.default_args["retry_delay"] == timedelta(seconds=10)
    assert job1_dag.description == "This is a test dag description"
    assert job1_dag.tags == ["JOB1", "TEST"]

    task = job1_dag.get_task("test_docker")
    assert type(task).__name__ == "DockerOperator"
    assert task.image == "python:3.7"
    assert task.command == 'bash -e "env"'


def test_dag_import(example_project):
    dag_bag = DagBag(dag_folder=example_project.root_dir, include_examples=False)
    assert len(dag_bag.import_errors) == 0, "No Import Failures"


def test_dag_from_dag_bag(example_project):
    dag_bag = DagBag(dag_folder=example_project.root_dir, include_examples=False)
    assert len(dag_bag.dags) == 2
    job1_dag: DAG = dag_bag.dags["test_job1"]
    test_create_dag(job1_dag)


@pytest.mark.integration
def test_run_dag(airflow_init, job2_dag):
    task = job2_dag.get_task(airflow.DEFAULT_TASK_ID)
    task.run()
