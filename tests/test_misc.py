import pytest

from feaflow.sources import QuerySourceConfig, QuerySource


# def test_pydantic_private_attr(capsys):
#     config = QuerySourceConfig(sql="...", alias="test_query")
#     with capsys.disabled():
#         print(config)
#
#     query_source = QuerySource(config)
#     assert query_source.alias == "test_query"
#     with pytest.raises(AttributeError):
#         query_source.alias = "new_alias"
