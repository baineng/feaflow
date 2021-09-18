import re

from feaflow.compute.sql import SqlCompute, SqlComputeConfig
from feaflow.source.pandas import (
    PandasDataFrameSource,
    PandasDataFrameSourceConfig,
    PandasDataFrameSourceFileConfig,
)
from feaflow.utils import render_template


def test_normal_config():
    sql_config = SqlComputeConfig(
        impl_cls=SqlCompute,
        type="sql",
        sql="""
        SELECT {% for f in fields %}{{ f }}_suffix{% if not loop.last %}, {% endif %}{% endfor %} \
        {% for t_idx in table_amount %}\
        {% if loop.first %}FROM{% else %}JOIN{% endif %} table_{{ t_idx }} \
        {% endfor %}\
        {% if group %}GROUP BY title{% endif %}
        """,
    )
    template_context = {
        "table_amount": range(3),
        "fields": ["f1", "f2", "f3"],
        "group": True,
    }
    result = render_template(sql_config, template_context)
    assert isinstance(result, SqlComputeConfig)
    assert (
        result.sql.strip().replace(" ", "")
        == """SELECTf1_suffix,f2_suffix,f3_suffixFROMtable_0JOINtable_1JOINtable_2GROUPBYtitle"""
    )


def test_nested_config():
    pandas_config = PandasDataFrameSourceConfig(
        impl_cls=PandasDataFrameSource,
        type="pandas",
        file=PandasDataFrameSourceFileConfig(
            type="json",
            path="{{ dir }}/file.ext",
            args={"compression": "{{ comp }}", "numpy": False},
        ),
    )
    template_context = {
        "dir": "/var/log",
        "comp": "gzip",
    }

    result = render_template(pandas_config, template_context)
    assert isinstance(result, PandasDataFrameSourceConfig)
    assert result.file.path == "/var/log/file.ext"
    assert result.file.args == {"compression": "gzip", "numpy": False}
