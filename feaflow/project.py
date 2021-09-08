import importlib
from pathlib import Path
from typing import Any, Dict, List, Union

import yaml
from pydantic import DirectoryPath, FilePath, constr

from feaflow import exceptions
from feaflow.exceptions import ConfigLoadException
from feaflow.model import EngineConfig, FeaflowModel


class ProjectConfig(FeaflowModel):
    name: constr(regex=r"^[^_][\w ]+$", strip_whitespace=True, strict=True)
    root_path: DirectoryPath
    config_file_path: FilePath
    engines: List[EngineConfig]


def create_project_config_from_path(path: Union[str, Path]) -> ProjectConfig:
    root_path = Path(path)
    if not root_path.exists():
        raise FileNotFoundError(f"The project path `{path}` does not exist.")

    config_file_path = root_path.joinpath("feaflow.yaml")
    if not config_file_path.exists():
        raise FileNotFoundError(
            f"The project path `{path}` does not include feaflow.yaml."
        )

    try:
        with open(config_file_path) as f:
            config = yaml.safe_load(f)

            assert "engines" in config and type(config["engines"]) == list
            engines = [parse_engine_config(ec) for ec in config["engines"]]

            project_config = ProjectConfig(
                name=config["project_name"],
                root_path=root_path,
                config_file_path=config_file_path,
                engines=engines,
            )
            return project_config
    except Exception:
        raise ConfigLoadException(str(config_file_path.absolute()))


BUILTIN_ENGINES = {"spark": "feaflow.engine.spark.SparkEngineConfig"}


def parse_engine_config(config: Dict[str, Any]) -> EngineConfig:
    assert "type" in config
    engine_type = str(config["type"]).strip().lower()
    engine_config_class_name = (
        BUILTIN_ENGINES[engine_type] if engine_type in BUILTIN_ENGINES else engine_type
    )

    module_name, class_name = engine_config_class_name.rsplit(".", 1)
    try:
        module = importlib.import_module(module_name)
        engine_config_class = getattr(module, class_name)
    except Exception:
        raise exceptions.EngineImportException(engine_config_class_name)

    assert issubclass(engine_config_class, EngineConfig)
    return engine_config_class(**config)


class Project:
    def __init__(self, path: Union[str, Path]):
        self._config = create_project_config_from_path(path)

    @property
    def config(self):
        return self._config

    @property
    def name(self):
        return self.config.name

    @property
    def root_path(self):
        return self.config.root_path
