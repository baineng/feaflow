from feaflow.computes import SqlCompute
from feaflow.job import Job, JobConfig
from feaflow.sinks import TableSink
from feaflow.sources import QuerySource


def test_construct_job(example_project):
    jobs = example_project.scan_jobs()
    job1: Job = next(filter(lambda j: j.name == "test_job1", jobs))
    assert job1.config.name == "test_job1"

    assert len(job1.sources) == 2
    assert isinstance(job1.sources[0], QuerySource)
    assert job1.sources[0].get_alias() is None

    assert len(job1.computes) == 1
    assert isinstance(job1.computes[0], SqlCompute)

    assert len(job1.sinks) == 1
    assert isinstance(job1.sinks[0], TableSink)
    assert job1.sinks[0].get_name() == "test_sink_table"
