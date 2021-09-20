import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from pydantic import DirectoryPath, FilePath, constr

from feaflow.abstracts import FeaflowModel
from feaflow.constants import BUILTIN_ENGINES
from feaflow.engine import Engine, EngineConfig
from feaflow.exceptions import ConfigLoadError
from feaflow.job import Job, JobConfig, parse_job_config_file
from feaflow.utils import (
    construct_config_from_dict,
    construct_impl_from_config,
    make_tzaware,
)


class ProjectConfig(FeaflowModel):
    name: constr(regex=r"^[^_][\w ]+$", strip_whitespace=True, strict=True)
    root_dir: DirectoryPath
    config_file_path: FilePath
    engines: List[Dict[str, Any]]
    scheduler_default: Optional[Dict[str, Any]] = None


class Project:
    def __init__(self, project_dir: Union[str, Path]):
        self._config = create_project_config_from_dir(project_dir)
        self._engine_configs = None
        self._engines = None

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
        job_conf_files = self._find_files(r"\/job.*\.ya?ml$", r"\/.*?job\.ya?ml$")
        job_configs = [parse_job_config_file(f) for f in job_conf_files]
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
        upstream_template_context: Optional[Dict[str, Any]] = None,
    ):
        # for execution_date with no timezone, just replace to utc
        execution_date = make_tzaware(execution_date)

        engine = self.get_engine_by_name(job.engine_name)
        with engine.new_session() as engine_session:
            template_context = self.construct_template_context(
                job, execution_date, upstream_template_context
            )
            engine_session.run(job, template_context)

    def construct_template_context(
        self,
        job: Job,
        execution_date: datetime,
        upstream_template_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        ds = execution_date.strftime("%Y-%m-%d")
        ts = execution_date.isoformat()
        yesterday_ds = (execution_date - timedelta(1)).strftime("%Y-%m-%d")
        tomorrow_ds = (execution_date + timedelta(1)).strftime("%Y-%m-%d")
        ds_nodash = ds.replace("-", "")
        ts_nodash = execution_date.strftime("%Y%m%dT%H%M%S")
        ts_nodash_with_tz = ts.replace("-", "").replace(":", "")
        yesterday_ds_nodash = yesterday_ds.replace("-", "")
        tomorrow_ds_nodash = tomorrow_ds.replace("-", "")

        context = {
            "project_name": self.name,
            "project_root": self.root_dir.resolve(),
            "job_root": job.config.config_file_path.parent.resolve(),
            "job_name": job.name,
            "engine_name": job.engine_name,
            "execution_date": execution_date,
            "ds": ds,
            "ts": ts,
            "yesterday_ds": yesterday_ds,
            "tomorrow_ds": tomorrow_ds,
            "ds_nodash": ds_nodash,
            "ts_nodash": ts_nodash,
            "ts_nodash_with_tz": ts_nodash_with_tz,
            "yesterday_ds_nodash": yesterday_ds_nodash,
            "tomorrow_ds_nodash": tomorrow_ds_nodash,
        }
        if upstream_template_context:
            context.update(upstream_template_context)
        return context

    def _find_files(self, *patterns) -> List:
        def _match_any_pattern(f: Path) -> bool:
            for pat in patterns:
                if re.search(pat, str(f.resolve())):
                    return True
            return False

        return [
            f.resolve()
            for f in self.root_dir.glob("**/*")
            if f.is_file() and _match_any_pattern(f)
        ]


def create_project_config_from_dir(project_dir: Union[str, Path]) -> ProjectConfig:
    root_dir = Path(project_dir)
    if not root_dir.exists():
        raise FileNotFoundError(f"The project path `{project_dir}` does not exist.")

    if root_dir.joinpath("feaflow_project.yaml").exists():
        config_file_path = root_dir.joinpath("feaflow_project.yaml")
    elif root_dir.joinpath("feaflow_project.yaml").exists():
        config_file_path = root_dir.joinpath("feaflow_project.yaml")
    else:
        raise FileNotFoundError(
            f"The project path `{project_dir}` does not include feaflow_project.yaml."
        )

    try:
        with open(config_file_path) as f:
            config = yaml.safe_load(f)
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
    return construct_impl_from_config(engine_cfg)
