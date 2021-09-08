import pytest


from feaflow.source import QuerySourceConfig, Source, QuerySource


def test_pydantic_private_attr(capsys):
    config = QuerySourceConfig(sql="...", alias="test_query")
    with capsys.disabled():
        print(config)
        print(config.get_impl_cls())

    query_source = QuerySource(config)
    assert query_source.alias == "test_query"
    with pytest.raises(AttributeError):
        query_source.alias = "new_alias"
