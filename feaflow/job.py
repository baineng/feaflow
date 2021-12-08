import copy
import logging
import re
from pathlib import Path
from typing import List, Union

import yaml
import yaml.parser

from feaflow.abstracts import Compute, Sink, Source
from feaflow.exceptions import ConfigLoadError
from feaflow.job_config import JobConfig
from feaflow.utils import construct_impl_from_config, render_template

logger = logging.getLogger(__name__)


class Job:
    def __init__(self, config: JobConfig):
        self._config = config
        self._sources = None
        self._computes = None
        self._sinks = None

    @property
    def config(self) -> JobConfig:
        return self._config

    @property
    def name(self) -> str:
        return self._config.name

    @property
    def engine_name(self) -> str:
        return self._config.engine.use

    @property
    def sources(self) -> List[Source]:
        if self._sources is None:
            if self._config.sources is None:
                self._sources = []
            else:
                self._sources = (
                    [construct_impl_from_config(c) for c in self._config.sources]
                    if self._config.sources
                    else []
                )
        return self._sources

    @property
    def computes(self) -> List[Compute]:
        if self._computes is None:
            self._computes = (
                [construct_impl_from_config(c) for c in self._config.computes]
                if self._config.computes
                else []
            )
        return self._computes

    @property
    def sinks(self) -> List[Sink]:
        if self._sinks is None:
            if self._config.sinks is None:
                self._sinks = None
            else:
                self._sinks = (
                    [construct_impl_from_config(c) for c in self._config.sinks]
                    if self._config.sinks
                    else []
                )
        return self._sinks

    @property
    def scheduler_config(self):
        return self._config.scheduler

    def __repr__(self):
        return f"Job({self._config.name})"


def parse_job_config_file(path: Union[str, Path]) -> List[JobConfig]:
    logger.info("Parsing Job config file from path '%s'", path)
    job_conf_path = Path(path)
    if not job_conf_path.exists():
        raise FileNotFoundError(f"The job path '{path}' does not exist.")

    try:
        with open(job_conf_path) as f:
            config = yaml.safe_load(f)
            logger.debug("Parsed config: %s", config)
            if "loop" in config:
                logger.debug("Detected loop")
                assert type(config["loop"]) == list
                result = []
                for loop_variables in config["loop"]:
                    logger.debug("Loop params: %s", loop_variables)
                    _config = copy.deepcopy(config)
                    del _config["loop"]
                    _config["loop_variables"] = loop_variables
                    job_config = JobConfig(config_file_path=job_conf_path, **_config)
                    job_config = render_template(
                        job_config, loop_variables, use_jinja2=False
                    )
                    logger.debug("Parsed job config: %s", job_config)
                    assert re.match(
                        r"^[^_]\w+$", job_config.name
                    ), f"Job name needs match regexp '^[^_]\\w+$'"
                    result.append(job_config)
                return result
            else:
                job_config = JobConfig(config_file_path=job_conf_path, **config)
                logger.debug("Parsed job config: %s", job_config)
                return [job_config]
    except yaml.parser.ParserError:
        raise ConfigLoadError(
            str(job_conf_path.absolute()),
            "if you have used Jinja2 syntax, try to escape by quoting it or use |, > in the config.",
        )
    except Exception as ex:
        raise ConfigLoadError(str(job_conf_path.absolute()), str(ex))
