import importlib
from typing import Any, Dict

from feaflow import exceptions
from feaflow.abstracts import FeaflowComponent, FeaflowConfig


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
    assert issubclass(the_class, FeaflowComponent)
    the_config = the_class.create_config(**config_dict)
    assert isinstance(the_config, FeaflowConfig)
    return the_config


def create_instance_from_config(config: FeaflowConfig) -> FeaflowComponent:
    return config.impl_cls(config)
