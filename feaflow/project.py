from pathlib import Path
from typing import Any, List, Optional, Union

import yaml
from pydantic import DirectoryPath, FilePath, constr

from feaflow.abstracts import Engine, EngineConfig, FeaflowModel
from feaflow.constants import BUILTIN_ENGINES
from feaflow.exceptions import ConfigLoadError
from feaflow.utils import create_config_from_dict, create_instance_from_config


class ProjectConfig(FeaflowModel):
    name: constr(regex=r"^[^_][\w ]+$", strip_whitespace=True, strict=True)
    root_path: DirectoryPath
    config_file_path: FilePath
    engines: List[EngineConfig]

    def __init__(self, **data: Any):
        if "engines" in data:
            assert type(data["engines"]) == list
            data["engines"] = [
                create_config_from_dict(ec, BUILTIN_ENGINES) for ec in data["engines"]
            ]

        super().__init__(**data)


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
            config["name"] = config["project_name"]
            del config["project_name"]
            config["root_path"] = root_path
            config["config_file_path"] = config_file_path
            return ProjectConfig(**config)
    except Exception:
        raise ConfigLoadError(str(config_file_path.absolute()))


class Project:
    def __init__(self, path: Union[str, Path]):
        self._config = create_project_config_from_path(path)
        self._engines = None

    @property
    def config(self) -> ProjectConfig:
        return self._config

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def root_path(self) -> Path:
        return self.config.root_path

    @property
    def engines(self) -> List[Engine]:
        if self._engines is None:
            self._engines = [
                create_instance_from_config(c) for c in self._config.engines
            ]
        return self._engines

    def get_engine(self, engine_name) -> Optional[Engine]:
        for engine in self.engines:
            if engine.config.name == engine_name:
                return engine
        return None
