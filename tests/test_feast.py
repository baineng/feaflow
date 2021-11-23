from datetime import timedelta

from feast.infra.offline_stores.file import FileOfflineStoreConfig
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig

from feaflow import feast
from feaflow.sink.feature_view import FeatureViewSinkConfig


def test_config(project_feast):
    feast_project_config = project_feast.config.feast_project_config
    assert feast_project_config.registry == "data/registry.db"
    assert feast_project_config.provider == "local"

    feast_job = project_feast.get_job("feast_job1")
    assert feast_job is not None

    feature_view_config: FeatureViewSinkConfig = feast_job.config.sinks[0]
    assert feature_view_config
    assert type(feature_view_config) == FeatureViewSinkConfig
    assert feature_view_config.name == "fview_1"
    assert feature_view_config.ttl == timedelta(seconds=3600)
    assert feature_view_config.ingest.into_table == "test_sink_table"


# declar = feast._generate_project_declarations()
# print(declar)

# feast -> feature_view
#          require: table name

# feast materialize

# feast apply

# feast generate project.yml

# feast serve?


def test_init(project_feast):
    with feast.init(project_feast) as feast_project:
        repo_config = feast_project.load_repo_config()

        assert repo_config.project == "Feaflow_Feast"
        assert repo_config.registry == "data/registry.db"
        assert repo_config.provider == "local"
        assert repo_config.repo_path == feast_project.feast_project_dir
        assert type(repo_config.online_store) == SqliteOnlineStoreConfig
        assert type(repo_config.offline_store) == FileOfflineStoreConfig


def test_apply(project_feast):
    with feast.init(project_feast) as feast_project:
        feast_project.apply()
        print(1)
