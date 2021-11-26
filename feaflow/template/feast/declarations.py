import datetime
import importlib

from feast import Entity, Feature, FeatureView, ValueType

def get_class_by_name(class_name):
    module_name, class_name = class_name.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, class_name)

# DataSources
{% for ds in datasource_defs %}
{{ ds.id }} = get_class_by_name("{{ ds.class_name }}")(
    {% if ds.event_timestamp_column %}event_timestamp_column="{{ ds.event_timestamp_column }}",{% endif %}
    {% if ds.created_timestamp_column %}created_timestamp_column="{{ ds.created_timestamp_column }}",{% endif %}
    {% if ds.field_mapping %}field_mapping={{ ds.field_mapping }},{% endif %}
    {% if ds.date_partition_column %}date_partition_column="{{ ds.date_partition_column }}",{% endif %}
    {% if ds.other_arguments %}{% for k, v in ds.other_arguments.items() %}
    {{ k }}={{ v }},
    {% endfor %}{% endif %}
)
{% endfor %}

# Entities
{% for entity in entity_defs %}
entity_{{ loop.index }} = Entity(
    name="{{ entity.name }}",
    value_type={{ entity.value_type }},
    {% if entity.join_key %}join_key="{{ entity.join_key }}",{% endif %}
    {% if entity.description %}description="{{ entity.description }}",{% endif %}
    {% if entity.labels %}labels={{ entity.labels }},{% endif %}
)
{% endfor %}

# FeatureViews
{% for fv in feature_view_defs %}
feature_view_{{ loop.index }} = FeatureView(
    name="{{ fv.name }}",
    entities=[{% if fv.entities %}"{{ '","'.join(fv.entities) }}"{% endif %}],
    ttl={{ fv.ttl }},
    {% if fv.features %}features=[{% for fe in fv.features %}
        Feature(
            name="{{ fe.name }}",
            dtype={{ fe.dtype }},
            {% if fe.labels %}labels={{ fe.labels }},{% endif %}
        ),
    {% endfor %}],{% endif %}
    {% if fv.batch_source %}batch_source={{ fv.batch_source }},{% endif %}
    {% if fv.stream_source %}stream_source={{ fv.stream_source }},{% endif %}
    {% if fv.tags %}tags={{ fv.tags }},{% endif %}
)
{% endfor %}