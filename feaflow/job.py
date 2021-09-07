from pathlib import Path
from typing import Union

import yaml

from feaflow.exceptions import ConfigException
from feaflow.model import JobConfig


def parse_job_config_file(path: Union[str, Path]) -> JobConfig:
    job_conf_path = Path(path)
    if not job_conf_path.exists():
        raise FileNotFoundError(f"The job path `{path}` does not exist.")

    try:
        with open(job_conf_path) as f:
            config = yaml.safe_load(f)
            return JobConfig(**config)
    except Exception as ex:
        raise ConfigException(ex, str(job_conf_path.absolute()))


class Job:
    def __init__(self, config: JobConfig):
        self._config = config
