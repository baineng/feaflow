from feaflow.computes import SqlCompute
from feaflow.job import Job, JobConfig
from feaflow.sinks import TableSink
from feaflow.sources import QuerySource


def test_construct_job(example_project):
    jobs = example_project.scan_jobs()
    test_job1: JobConfig = next(filter(lambda j: j.name == "test_job1", jobs))
    assert test_job1.config.name == "test_job1"

    assert len(test_job1.sources) == 1
    assert isinstance(test_job1.sources[0], QuerySource)
    assert test_job1.sources[0].alias == "daily_events"

    assert len(test_job1.computes) == 1
    assert isinstance(test_job1.computes[0], SqlCompute)
    assert "daily_events" in test_job1.computes[0].sql

    assert len(test_job1.sinks) == 1
    assert isinstance(test_job1.sinks[0], TableSink)
    assert test_job1.sinks[0].config.name == "test_sink_table"
