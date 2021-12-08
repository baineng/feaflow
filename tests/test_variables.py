from feaflow.airflow_config import AirflowSchedulerConfig
from feaflow.job_config import JobConfig
from feaflow.source.pandas import PandasDataFrameSourceConfig


def test_loop(project_misc):
    jobs = project_misc.scan_jobs()
    loop_data = [
        {"name": "l1", "schedule_interval": "0 6 * * *"},
        {"name": "l2", "schedule_interval": "1 7 * * *"},
        {"name": "l3", "schedule_interval": "2 8 * * *"},
    ]
    for loop_variables in loop_data:
        job_name = "test_job4_" + loop_variables["name"]
        conf: JobConfig = next(filter(lambda j: j.name == job_name, jobs))
        assert conf.name == "test_job4_" + loop_variables["name"]
        assert isinstance(conf.scheduler, AirflowSchedulerConfig)
        assert conf.scheduler.schedule_interval == loop_variables["schedule_interval"]
        assert len(conf.sources) == 1
        assert isinstance(conf.sources[0], PandasDataFrameSourceConfig)
        assert conf.sources[0].file.path == "{{ project_root }}/../data/pandas_df1.csv"


def test_variables(project_misc):
    jobs = project_misc.scan_jobs()
    l1_conf: JobConfig = next(filter(lambda j: j.name == "test_job4_l1", jobs))

    assert l1_conf.variables == {"fields": ["field1", "field2"], "table": "var_table"}
    assert l1_conf.loop_variables == {"name": "l1", "schedule_interval": "0 6 * * *"}
