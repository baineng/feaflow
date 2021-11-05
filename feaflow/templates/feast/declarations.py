from feast import Entity, Feature, FeatureView, ValueType

# Batch Sources Declarations Starts
{% for _, batch_source_declar in batch_sources.items() %}
{{ batch_source_declar }}
{% endfor %}

# Entities Declarations Starts
{% for _, entity_declar in entities.items() %}
{{ entity_declar }}
{% endfor %}

# FeatureView Declarations Starts
{% for _, feature_view_declar in feature_views.items() %}
{{ feature_view_declar }}
{% endfor %}