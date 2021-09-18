import collections
import importlib
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from jinja2 import Environment, Template
from jinja2.sandbox import SandboxedEnvironment
from pytz import utc

from feaflow import exceptions
from feaflow.abstracts import (
    ComputeUnit,
    FeaflowConfig,
    FeaflowConfigurableComponent,
    FeaflowModel,
    SchedulerConfig,
)


def get_class_from_name(class_name: str):
    if "." not in class_name:
        raise exceptions.ClassImportError(
            class_name,
            "maybe it's a typo or the class name is not a builtin implementation.",
        )
    module_name, class_name = class_name.rsplit(".", 1)
    try:
        module = importlib.import_module(module_name)
        return getattr(module, class_name)
    except Exception:
        raise exceptions.ClassImportError(class_name)


def create_config_from_dict(
    config_dict: Dict[str, Any], builtin_types: Dict[str, str],
) -> FeaflowConfig:
    assert "type" in config_dict
    type_or_class = str(config_dict["type"]).strip().lower()
    the_class_name = (
        builtin_types[type_or_class]
        if type_or_class in builtin_types
        else type_or_class
    )
    the_class = get_class_from_name(the_class_name)
    assert issubclass(the_class, FeaflowConfigurableComponent)
    the_config = the_class.create_config(**config_dict)
    assert isinstance(the_config, FeaflowConfig)
    return the_config


def create_scheduler_config_from_dict(config_dict: Dict[str, Any]) -> SchedulerConfig:
    if (
        "type" not in config_dict
        or str(config_dict["type"]).strip().lower() == "airflow"
    ):
        from feaflow.airflow import AirflowSchedulerConfig

        config_class = AirflowSchedulerConfig
    else:
        raise NotImplementedError

    return config_class(**config_dict)


def create_instance_from_config(config: FeaflowConfig) -> ComputeUnit:
    return config.impl_cls(config=config)


def split_cols(cols: str) -> List[str]:
    return list(map(str.strip, cols.split(",")))


def create_random_str(short: bool = False) -> str:
    # TODO short
    return f"{int(time.time_ns())}_{random.randint(1000, 9999)}"


_jinja2_env: Optional[Environment] = None


def render_template(
    template_source: Any, template_context: Optional[Dict[str, Any]] = None
) -> [FeaflowModel, str]:
    global _jinja2_env

    if _jinja2_env is None:
        _jinja2_env = SandboxedEnvironment(cache_size=0)

    if template_context is None:
        template_context = {}

    if isinstance(template_source, str):
        return _jinja2_env.from_string(template_source).render(**template_context)

    elif isinstance(template_source, FeaflowModel):
        if len(template_source._template_attrs) == 0:
            return template_source
        else:
            new_model = template_source.copy()
            old_allow_mutation = new_model.__config__.allow_mutation
            new_model.__config__.allow_mutation = True
            for tpl_attr in new_model._template_attrs:
                if new_model.__getattribute__(tpl_attr) is not None:
                    new_model.__setattr__(
                        tpl_attr,
                        render_template(
                            new_model.__getattribute__(tpl_attr), template_context
                        ),
                    )
            new_model.__config__.allow_mutation = old_allow_mutation
            return new_model

    elif isinstance(template_source, tuple):
        if type(template_source) is not tuple:
            # Special case for named tuples
            return template_source.__class__(
                *(
                    render_template(element, template_context)
                    for element in template_source
                )
            )
        else:
            return tuple(
                render_template(element, template_context)
                for element in template_source
            )

    elif isinstance(template_source, dict):
        return {
            k: render_template(v, template_context) for k, v in template_source.items()
        }

    elif isinstance(template_source, list):
        return [render_template(v, template_context) for v in template_source]

    elif isinstance(template_source, set):
        return {
            render_template(element, template_context) for element in template_source
        }

    else:
        return template_source


def deep_merge_models(model: FeaflowModel, merge_model: FeaflowModel) -> FeaflowModel:
    assert type(model) == type(merge_model)
    source_dict = merge_model.dict(exclude_defaults=True)
    dest_dict = model.dict(exclude_defaults=True)
    deep_merge_dicts(source_dict, dest_dict)
    return type(model)(**source_dict)


def deep_merge_dicts(_dict: Dict, merge_dict: Dict):
    for k, v in merge_dict.items():
        if (
            k in _dict
            and isinstance(_dict[k], dict)
            and isinstance(merge_dict[k], collections.Mapping)
        ):
            deep_merge_dicts(_dict[k], merge_dict[k])
        else:
            _dict[k] = merge_dict[k]


def make_tzaware(dt: datetime) -> datetime:
    if dt.utcoffset() is None:
        return dt.replace(tzinfo=utc)
    else:
        return dt
