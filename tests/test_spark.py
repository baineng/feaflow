from feaflow.engine.spark import SparkEngine


def test_run_job1(example_project):
    engine = example_project.get_engine("default_spark")
    assert type(engine) == SparkEngine
