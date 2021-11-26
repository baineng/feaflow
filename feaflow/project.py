import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from pydantic import DirectoryPath, Field, FilePath, StrictStr, constr

from feaflow.abstracts import FeaflowModel
from feaflow.constants import BUILTIN_ENGINES
from feaflow.engine import Engine, EngineConfig
from feaflow.exceptions import ConfigLoadError
from feaflow.job import Job, parse_job_config_file
from feaflow.job_config import JobConfig
from feaflow.utils import (
    construct_config_from_dict,
    construct_impl_from_config,
    find_files_by_patterns,
)

logger = logging.Logger(__name__)


class FeastProjectConfig(FeaflowModel):
    provider: StrictStr = "local"
    registry: Union[StrictStr, Dict[str, Any]] = "data/registry.db"
    online_store: Any
    offline_store: Any


class ProjectConfig(FeaflowModel):
    name: constr(regex=r"^[^_][\w_]+$", strip_whitespace=True, strict=True)
    root_dir: DirectoryPath
    config_file_path: FilePath
    engines: List[Dict[str, Any]]
    scheduler_default: Optional[Dict[str, Any]] = None
    feast_project_config: Optional[FeastProjectConfig] = Field(
        alias="feast", default=None
    )


class Project:
    def __init__(self, project_dir: Union[str, Path]):
        self._config = create_project_config_from_dir(project_dir)
        self._engine_configs = None
        self._engines = None
        self._feast = None

    @property
    def config(self) -> ProjectConfig:
        return self._config

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def root_dir(self) -> Path:
        return self.config.root_dir

    @property
    def engine_configs(self) -> List[EngineConfig]:
        if self._engine_configs is None:
            self._engine_configs = [
                construct_config_from_dict(ec, BUILTIN_ENGINES)
                for ec in self._config.engines
            ]
        return self._engine_configs

    def get_engine_by_name(self, engine_name) -> Optional[Engine]:
        for engine_cfg in self.engine_configs:
            if engine_cfg.name == engine_name:
                return construct_engine(engine_cfg)
        return None

    def scan_jobs(self) -> List[JobConfig]:
        """
        Scan jobs in the project root path,
        any yaml files start or end with "job" will be considered as a job config file.
        """
        logger.info("Scanning jobs")
        job_conf_files = find_files_by_patterns(
            self.root_dir, r"\/jobs?.*\.ya?ml$", r"\/.*?jobs?\.ya?ml$"
        )
        job_configs = []
        for f in job_conf_files:
            job_configs += parse_job_config_file(f)
        logger.debug("Scanned jobs: %s", job_configs)
        return job_configs

    def get_job(self, job_name: str) -> Optional[Job]:
        jobs = self.scan_jobs()
        for job_config in jobs:
            if job_name == job_config.name:
                return Job(job_config)
        return None

    def run_job(
        self,
        job: Job,
        execution_date: datetime,
        template_context: Optional[Dict[str, Any]] = None,
    ):
        if template_context and job.config.loop_params:
            template_context.update(job.config.loop_params)

        logger.info(
            "Running job '%s' at '%s', with execution_date: '%s', template_context: %s",
            job.name,
            job.engine_name,
            execution_date,
            template_context.keys() if template_context else None,
        )

        engine = self.get_engine_by_name(job.engine_name)
        with engine.new_session() as engine_session:
            engine_session.run(job, execution_date, template_context)

    def support_feast(self) -> bool:
        has_defined_feast = self.config.feast_project_config is not None
        if has_defined_feast:
            try:
                import feast

                return True
            except ImportError:
                logger.warning(
                    f"Project {self.name} has defined 'feast' in config file, "
                    f"but 'feast' is not installed in current execution environment."
                )
        return False

    def get_feast(self) -> "feaflow.feast.Feast":
        if self._feast is None:
            from feast import Feast

            self._feast = Feast(self)
        return self._feast


def create_project_config_from_dir(project_dir: Union[str, Path]) -> ProjectConfig:
    logger.info("Creating project from '%s'", project_dir)
    root_dir = Path(project_dir)
    if not root_dir.exists():
        raise FileNotFoundError(f"The project path `{project_dir}` does not exist.")

    if root_dir.joinpath("feaflow_project.yaml").exists():
        config_file_path = root_dir.joinpath("feaflow_project.yaml")
        logger.info("Detected config file '%s'", config_file_path)
    elif root_dir.joinpath("feaflow_project.yaml").exists():
        config_file_path = root_dir.joinpath("feaflow_project.yaml")
        logger.info("Detected config file '%s'", config_file_path)
    else:
        raise FileNotFoundError(
            f"The project path '{project_dir}' does not include feaflow_project.yaml."
        )

    try:
        with open(config_file_path) as f:
            config = yaml.safe_load(f)
            logger.debug("Parsed config: %s", config)
            project_name = config["project_name"]
            del config["project_name"]
            return ProjectConfig(
                name=project_name,
                root_dir=root_dir,
                config_file_path=config_file_path,
                **config,
            )
    except Exception:
        raise ConfigLoadError(str(config_file_path.absolute()))


def construct_engine(engine_cfg: EngineConfig) -> Engine:
    logger.info("Constructing Engine from config: %s", engine_cfg)
    return construct_impl_from_config(engine_cfg)
