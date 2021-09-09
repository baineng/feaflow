import pytest


from feaflow.sources import QuerySourceConfig, QuerySource
from feaflow.abstracts import Source


def test_pydantic_private_attr(capsys):
    config = QuerySourceConfig(sql="...", alias="test_query")
    with capsys.disabled():
        print(config)
        print(config.create_impl_instance())

    query_source = QuerySource(config)
    assert query_source.alias == "test_query"
    with pytest.raises(AttributeError):
        query_source.alias = "new_alias"
