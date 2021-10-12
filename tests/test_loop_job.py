from feaflow.airflow_config import AirflowSchedulerConfig
from feaflow.job import JobConfig
from feaflow.source.pandas import PandasDataFrameSourceConfig


def test_construct_job(example_project):
    jobs = example_project.scan_jobs()
    loop_data = [
        {"name": "l1", "schedule_interval": "0 6 * * *"},
        {"name": "l2", "schedule_interval": "1 7 * * *"},
        {"name": "l3", "schedule_interval": "2 8 * * *"},
    ]
    for loop_params in loop_data:
        job_name = "test_job4_" + loop_params["name"]
        conf: JobConfig = next(filter(lambda j: j.name == job_name, jobs))
        assert conf.name == "test_job4_" + loop_params["name"]
        assert isinstance(conf.scheduler, AirflowSchedulerConfig)
        assert conf.scheduler.schedule_interval == loop_params["schedule_interval"]
        assert len(conf.sources) == 1
        assert isinstance(conf.sources[0], PandasDataFrameSourceConfig)
        assert conf.sources[0].file.path == "{{ project_root }}/../data/pandas_df1.csv"
