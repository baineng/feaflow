from pathlib import Path
from typing import Union

import yaml

from feaflow.exceptions import ConfigLoadException
from feaflow.model import BUILTIN_ENGINES, EngineConfig, ProjectConfig
from feaflow.utils import create_config_from_dict


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
            engines = [
                create_config_from_dict(ec, BUILTIN_ENGINES) for ec in config["engines"]
            ]

            project_config = ProjectConfig(
                name=config["project_name"],
                root_path=root_path,
                config_file_path=config_file_path,
                engines=engines,
            )
            return project_config
    except Exception:
        raise ConfigLoadException(str(config_file_path.absolute()))


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
