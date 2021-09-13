import pytest

from feaflow.source.query import QuerySource, QuerySourceConfig
from feaflow.utils import render_template


@pytest.mark.skip
def test_pydantic_private_attr(capsys):
    config = QuerySourceConfig(sql="...", alias="test_query")
    with capsys.disabled():
        print(config)

    query_source = QuerySource(config)
    assert query_source.get_alias == "test_query"
    with pytest.raises(AttributeError):
        query_source.get_alias = "new_alias"


def test_template_substitute():
    context = {"name": "test"}

    template = "name is {{ name }}"
    assert render_template(template, context) == "name is test"

    template = """{% if name == "test" %}1{% else %}2{% endif %}"""
    assert render_template(template, context) == "1"
