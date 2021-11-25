import datetime

from feast import Entity, Feature, FeatureView, ValueType

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
            {% if fe.labels %}labels={{ fv.labels }},{% endif %}
        ),
    {% endfor %}]{% endif %}
    {% if fv.tags %}tags={{ fv.tags }},{% endif %}
)
{% endfor %}