from datetime import timedelta

from feaflow.airflow import AirflowSchedulerConfig, OperatorDefaultArgs
from feaflow.compute.sql import SqlCompute
from feaflow.job import Job
from feaflow.sink.table import TableSink
from feaflow.source.query import QuerySource
from feaflow.utils import deep_merge_models


def test_construct_job(example_project):
    jobs = example_project.scan_jobs()
    job1: Job = Job(next(filter(lambda j: j.name == "test_job1", jobs)))
    assert job1.config.name == "test_job1"

    assert len(job1.sources) == 2
    assert isinstance(job1.sources[0], QuerySource)
    assert job1.sources[0].get_alias() is None

    assert len(job1.computes) == 1
    assert isinstance(job1.computes[0], SqlCompute)

    assert len(job1.sinks) == 1
    assert isinstance(job1.sinks[0], TableSink)
    assert job1.sinks[0].get_name() == "test_sink_table"


def test_scheduler_config(job1):
    assert job1.scheduler_config.schedule_interval == "0 6 * * *"
    assert job1.scheduler_config.full_filepath == None

    default_config = AirflowSchedulerConfig(
        schedule_interval="* * * * *",
        full_filepath="/tmp/",
        default_args=OperatorDefaultArgs(
            queue="abc", max_retry_delay=timedelta(seconds=100)
        ),
    )
    assert (
        deep_merge_models(job1.scheduler_config, default_config).schedule_interval
        == "0 6 * * *"
    )
    assert (
        deep_merge_models(job1.scheduler_config, default_config).full_filepath
        == "/tmp/"
    )
    assert (
        deep_merge_models(job1.scheduler_config, default_config).default_args.queue
        == "abc"
    )
    assert deep_merge_models(
        job1.scheduler_config, default_config
    ).default_args.max_retry_delay == timedelta(seconds=100)
    assert (
        deep_merge_models(job1.scheduler_config, default_config).default_args.retries
        == 2
    )
    assert deep_merge_models(
        job1.scheduler_config, default_config
    ).default_args.retry_delay == timedelta(seconds=10)
