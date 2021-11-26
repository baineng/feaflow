from datetime import datetime, timedelta

import pytest
from feast.infra.offline_stores.file import FileOfflineStoreConfig
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig

from feaflow import feast
from feaflow.feast import (
    EntityDefinition,
    FeatureDefinition,
    _generate_project_declarations,
)
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


def test_init(project_feast):
    with feast.init(project_feast) as feast_project:
        repo_config = feast_project.load_repo_config()

        assert repo_config.project == "Feaflow_Feast"
        assert repo_config.registry == "data/registry.db"
        assert repo_config.provider == "local"
        assert repo_config.repo_path == feast_project.feast_project_dir
        assert type(repo_config.online_store) == SqliteOnlineStoreConfig
        assert type(repo_config.offline_store) == FileOfflineStoreConfig


@pytest.fixture()
def project_feast_applied(project_feast):
    with feast.init(project_feast) as feast_project:
        feast_project.apply()
        yield feast_project


def test_apply(project_feast_applied):
    pass


def test_materialize(project_feast_applied):
    project_feast_applied.materialize(
        datetime.utcnow() - timedelta(days=1), datetime.utcnow()
    )


def test_generate_project_declarations(project_feast):
    project_defs = _generate_project_declarations(project_feast)
    print(project_defs)


@pytest.mark.parametrize(
    "test_sql, expected_entities, expected_features",
    [
        (
            """
            SELECT id /*entity, type: string*/,
                   send_message_amount/* type: string*/,
                   login_times,
                   logout_times
            FROM compute_0    
            """,
            [EntityDefinition(name="id", join_key="id", value_type="ValueType.STRING")],
            [
                FeatureDefinition(name="send_message_amount", dtype="ValueType.STRING"),
                FeatureDefinition(name="login_times"),
                FeatureDefinition(name="logout_times"),
            ],
        ),
        (
            """
            SELECT id /* entity: user_id, type: string */,
                   send_message_amount as feature_1 /* type: string */,
                   login_times,
                   logout_times /* type: int */
            FROM compute_0    
            """,
            [
                EntityDefinition(
                    name="user_id", join_key="id", value_type="ValueType.STRING"
                )
            ],
            [
                FeatureDefinition(name="feature_1", dtype="ValueType.STRING"),
                FeatureDefinition(name="login_times"),
                FeatureDefinition(name="logout_times", dtype="ValueType.INT"),
            ],
        ),
    ],
)
def test_get_entities_and_features_from_sql(
    test_sql, expected_entities, expected_features
):
    entities, features = feast._get_entities_and_features_from_sql(test_sql)
    assert len(expected_entities) == len(entities)
    assert len(expected_features) == len(features)

    for idx in range(len(entities)):
        assert entities[idx].dict() == expected_entities[idx].dict()

    for idx in range(len(features)):
        assert features[idx].dict() == expected_features[idx].dict()
