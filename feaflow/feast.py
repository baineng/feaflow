import contextlib
import logging
import os.path
import sys
import tempfile
from pathlib import Path
from typing import Any, ContextManager, Dict, Iterator, Optional, Tuple

import yaml
from feast.errors import FeastProviderLoginError
from feast.repo_config import RepoConfig, load_repo_config
from feast.repo_operations import apply_total

from feaflow.exceptions import NotSupportedFeature
from feaflow.job_config import FeastConfig, JobConfig
from feaflow.project import Project
from feaflow.utils import render_template

logger = logging.getLogger(__name__)

TEMPLATE_DIR = (Path(__file__).parent / "template" / "feast").absolute()


class FeastProject:
    def __init__(self, feaflow_project: Project, feast_project_dir: Path):
        assert feast_project_dir.exists()
        assert (feast_project_dir / "feature_store.yaml").exists()

        self.feaflow_project = feaflow_project
        self.feast_project_dir = feast_project_dir

    def load_repo_config(self) -> RepoConfig:
        return load_repo_config(self.feast_project_dir)

    def apply(self, skip_source_validation=True):
        """Apply Feast infra"""
        try:
            apply_total(
                self.load_repo_config(), self.feast_project_dir, skip_source_validation
            )
        except FeastProviderLoginError as e:
            logger.exception(e)
            raise


@contextlib.contextmanager
def init(feaflow_project: Project) -> ContextManager[FeastProject]:
    """Init a Feast project within a temp folder"""

    if not feaflow_project.support_feast():
        raise NotSupportedFeature("feast")

    with tempfile.TemporaryDirectory(prefix="feaflow_feast_") as feast_project_dir:
        logger.info(f"Initializing a temporary Feast project in '{feast_project_dir}'")

        project_config = _generate_project_config(feaflow_project)
        with open(f"{feast_project_dir}/feature_store.yaml", "xt") as f:
            f.write(project_config)

        project_declarations = _generate_project_declarations(feaflow_project)
        with open(f"{feast_project_dir}/declarations.py", "xt") as f:
            f.write(project_declarations)

        logger.info("Initializing Done")
        feast_project_path = Path(feast_project_dir).resolve()
        sys.path.append(str(feast_project_path))
        yield FeastProject(feaflow_project, feast_project_path)
        sys.path.pop()


def _generate_project_config(feaflow_project: Project) -> str:
    with open(TEMPLATE_DIR / "feature_store.yaml", "r") as tf:
        template_str = tf.read()

    context = feaflow_project.config.feast_project_config.dict()
    context.update({"project": feaflow_project.name})
    if context["online_store"]:
        context["online_store"] = yaml.dump({"online_store": context["online_store"]})
    if context["offline_store"]:
        context["offline_store"] = yaml.dump(
            {"offline_store": context["offline_store"]}
        )
    return render_template(template_str, context)


def _generate_project_declarations(feaflow_project: Project) -> str:
    with open(TEMPLATE_DIR / "declarations.py", "r") as tf:
        template_str = tf.read()

    batch_sources: Dict[str, str] = {}
    entities: Dict[str, str] = {}
    feature_views: Dict[str, str] = {}
    jobs = feaflow_project.scan_jobs()

    for job_config in jobs:
        job_declarations = _generate_declarations_from_job_config(job_config)
        if job_declarations:
            (
                _batch_source,
                _entities,
                _feature_view,
            ) = job_declarations

            if _batch_source[0] in batch_sources:
                logger.warning(
                    f"BatchSource '{_batch_source[0]}' already exists, skipped."
                )
            else:
                batch_sources[_batch_source[0]] = _batch_source[1]

            for ek, ev in _entities.items():
                if ek in entities:
                    logger.warning(f"Entity '{ek}' already exists, skipped.")
                else:
                    entities[ek] = ev

            if _feature_view[0] in feature_views:
                logger.warning(
                    f"FeatureView '{_feature_view[0]}' already exists, skipped."
                )
            else:
                feature_views[_feature_view[0]] = _feature_view[1]

    return str(
        render_template(
            template_str,
            {
                "batch_sources": batch_sources,
                "entities": entities,
                "feature_views": feature_views,
            },
        )
    )


def _generate_declarations_from_job_config(
    job_config: JobConfig,
) -> Optional[Tuple[tuple, dict, tuple]]:
    if job_config.feast is None:
        return None
    feast_config = job_config.feast

    fv_cfg = feast_config.feature_view
    batch_source: Tuple[str, str]
    entities: Dict[str, str] = {}
    features: Dict[str, str] = {}
    feature_view: Tuple[str, str]

    # batch source starts
    bs_key = _generate_hash_key(
        "batch_source",
        {"__class__": fv_cfg.batch_source.class_, **fv_cfg.batch_source.args},
    )
    bs_args = _dict_to_func_args(fv_cfg.batch_source.args)

    (
        bs_module_name,
        bs_class_name,
    ) = fv_cfg.batch_source.class_.rsplit(".", 1)
    bs_declaration = f"""\
from {bs_module_name} import {bs_class_name}
{bs_key} = {bs_class_name}(
    {bs_args}
)\
"""
    batch_source = (bs_key, bs_declaration)

    # entity starts
    for entity_config in fv_cfg.entities:
        entity_key = entity_config.name
        entity_args = entity_config.dict(exclude_none=True)
        if "value_type" in entity_args:
            entity_args["value_type"] = "repr__" + _str_to_feast_value_type(
                entity_args["value_type"]
            )
        entity_args = _dict_to_func_args(entity_args)

        entity_declaration = f"""\
entity_{entity_key} = Entity(
    {entity_args}
)\
"""
        entities[entity_key] = entity_declaration

    # features starts
    if fv_cfg.features:
        for fe_cfg in fv_cfg.features:
            fe_args = fe_cfg.dict(exclude_none=True)
            if "dtype" in fe_args:
                fe_args["dtype"] = "repr__" + _str_to_feast_value_type(fe_args["dtype"])
            fe_args = _dict_to_func_args(fe_args)
            fe_declaration = f"""\
Feature(
    {fe_args}
)\
"""

            features[fe_cfg.name] = fe_declaration

    # feature_view starts
    fv_args = {
        "name": fv_cfg.name,
        "entities": list(entities.keys()),
        "ttl": fv_cfg.ttl,
    }
    if len(features) > 0:
        fv_args["features"] = [f"repr__{f}" for f in list(features.values())]
    if fv_cfg.tags:
        fv_args["tags"] = fv_cfg.tags
    fv_args["batch_source"] = f"repr__{batch_source[0]}"
    fv_args = _dict_to_func_args(fv_args)
    feature_view_declaration = f"""\
feature_view_{fv_cfg.name} = FeatureView(
    {fv_args}
)\
"""
    feature_view = (fv_cfg.name, feature_view_declaration)

    return batch_source, entities, feature_view


def _dict_to_func_args(_dict: dict) -> str:
    def wrap_arg(arg: Any) -> Any:
        if isinstance(arg, str):
            if arg[0:6] == "repr__":
                return arg[6:]
            else:
                return f'"{arg}"'
        elif isinstance(arg, list):
            return "[" + ", \n".join([wrap_arg(a) for a in arg]) + "]"
        return repr(arg)

    return ", \n    ".join([f"{k}={wrap_arg(v)}" for k, v in _dict.items()])


def _generate_hash_key(prefix: str, _dict: dict) -> str:
    """discussion about hashing a dict: https://stackoverflow.com/questions/5884066/hashing-a-dictionary"""
    hash_key = hash(frozenset(_dict))
    hash_key = str(hash_key).replace("-", "_")
    return f"{prefix}_{hash_key}"


VALUE_TYPE_MAPPING = {
    "UNKNOWN": "ValueType.UNKNOWN",
    "BYTES": "ValueType.BYTES",
    "STRING": "ValueType.STRING",
    "INT32": "ValueType.INT32",
    "INT": "ValueType.INT32",
    "INT64": "ValueType.INT64",
    "DOUBLE": "ValueType.DOUBLE",
    "FLOAT": "ValueType.FLOAT",
    "BOOL": "ValueType.BOOL",
    "UNIX_TIMESTAMP": "ValueType.UNIX_TIMESTAMP",
    "BYTES_LIST": "ValueType.BYTES_LIST",
    "STRING_LIST": "ValueType.STRING_LIST",
    "INT32_LIST": "ValueType.INT32_LIST",
    "INT64_LIST": "ValueType.INT64_LIST",
    "DOUBLE_LIST": "ValueType.DOUBLE_LIST",
    "FLOAT_LIST": "ValueType.FLOAT_LIST",
    "BOOL_LIST": "ValueType.BOOL_LIST",
    "UNIX_TIMESTAMP_LIST": "ValueType.UNIX_TIMESTAMP_LIST",
    "NULL": "ValueType.NULL",
}


def _str_to_feast_value_type(type_str: str) -> str:
    type_str = type_str.upper()
    if type_str not in VALUE_TYPE_MAPPING:
        raise ValueError(f"Type {type_str} is not a valid value type in Feast")

    return VALUE_TYPE_MAPPING[type_str]
