import pytest

from feaflow.compute import SqlCompute
from feaflow.job import JobConfig, Job
from feaflow.sink import RedisSink
from feaflow.source import QuerySource


def test_construct_job(example_project):
    job_confs = example_project.scan_jobs()
    test_job1_conf: JobConfig = next(filter(lambda j: j.name == "test_job1", job_confs))

    job = Job(example_project, test_job1_conf)
    assert job.config.name == "test_job1"

    assert len(job.sources) == 1
    assert isinstance(job.sources[0], QuerySource)
    assert job.sources[0].alias == "daily_events"

    assert len(job.computes) == 1
    assert isinstance(job.computes[0], SqlCompute)
    assert "daily_amount" in job.computes[0].sql

    assert len(job.sinks) == 1
    assert isinstance(job.sinks[0], RedisSink)
    assert job.sinks[0].host == "127.0.0.1"
    assert job.sinks[0].port == 6380
