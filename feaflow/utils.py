import importlib
from typing import Any, Dict

from feaflow import exceptions
from feaflow.model import ComponentConfig


def get_class_from_name(class_name: str):
    module_name, class_name = class_name.rsplit(".", 1)
    try:
        module = importlib.import_module(module_name)
        return getattr(module, class_name)
    except Exception:
        raise exceptions.ClassImportException(class_name)


# TODO generic function
def create_config_from_dict(
    config_dict: Dict[str, Any], builtin_types: Dict[str, str],
) -> ComponentConfig:
    assert "type" in config_dict
    type_or_class = str(config_dict["type"]).strip().lower()
    config_class_name = (
        builtin_types[type_or_class]
        if type_or_class in builtin_types
        else type_or_class
    )
    config_class = get_class_from_name(config_class_name)
    assert issubclass(config_class, ComponentConfig)
    return config_class(**config_dict)
