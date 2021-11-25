import contextlib
import logging
import re
import sys
import tempfile
from pathlib import Path
from typing import Any, ContextManager, Dict, List, Optional, Tuple, Union

import sqlparse
import yaml
from feast.errors import FeastProviderLoginError
from feast.repo_config import RepoConfig, load_repo_config
from feast.repo_operations import apply_total

from feaflow.abstracts import FeaflowImmutableModel
from feaflow.exceptions import NotSupportedFeature
from feaflow.job_config import JobConfig
from feaflow.project import Project
from feaflow.sink.feature_view import FeatureViewSinkConfig
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


class DataSourceDefinition(FeaflowImmutableModel):
    id: str
    class_name: str
    table_name: str
    event_timestamp_column: Optional[str] = None
    created_timestamp_column: Optional[str] = None
    field_mapping: Optional[str] = None
    date_partition_column: Optional[str] = None


class EntityDefinition(FeaflowImmutableModel):
    name: str
    value_type: str = "ValueType.UNKNOWN"
    description: Optional[str] = None
    join_key: Optional[str] = None
    labels: Optional[str] = None


class FeatureDefinition(FeaflowImmutableModel):
    name: str
    dtype: str = "ValueType.UNKNOWN"
    labels: Optional[str] = None


class FeatureViewDefinition(FeaflowImmutableModel):
    name: str
    entities: List[str]
    ttl: str
    features: Optional[List[FeatureDefinition]] = None
    batch_source: Optional[str] = None
    stream_source: Optional[str] = None
    tags: Optional[str] = None


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

    template_context = {
        "data_source_defs": [],
        "entity_defs": [],
        "feature_view_defs": [],
    }

    jobs = feaflow_project.scan_jobs()

    # TODO batch_source needed

    for job_config in jobs:
        if job_declarations := _get_definitions_from_job_config(
            feaflow_project, job_config
        ):
            (
                job_data_source_defs,
                job_entity_defs,
                job_feature_view_defs,
            ) = job_declarations

            template_context["data_source_defs"] += job_data_source_defs
            template_context["entity_defs"] += job_entity_defs
            template_context["feature_view_defs"] += job_feature_view_defs

    return str(render_template(template_str, template_context))


def _get_definitions_from_job_config(
    feaflow_project: Project,
    job_config: JobConfig,
) -> Optional[
    Tuple[
        List[DataSourceDefinition], List[EntityDefinition], List[FeatureViewDefinition]
    ]
]:
    if job_config.sinks is None:
        return None

    fv_configs = list(
        filter(lambda s: isinstance(s, FeatureViewSinkConfig), job_config.sinks)
    )
    if not fv_configs:
        return None

    job_data_source_defs = []
    job_entity_defs = []
    job_feature_view_defs = []

    for fv_cfg_idx, fv_cfg in enumerate(fv_configs):
        assert isinstance(fv_cfg, FeatureViewSinkConfig)
        ds_cfg = fv_cfg.data_source

        data_source_id = f"data_source_{job_config.name}_{fv_cfg_idx+1}"
        data_source_def = DataSourceDefinition(
            id=data_source_id,
            class_name=feaflow_project.config.feast_project_config.data_source_class,
            table_name=ds_cfg.store_table,
            event_timestamp_column=ds_cfg.event_timestamp_column,
            created_timestamp_column=ds_cfg.created_timestamp_column,
            field_mapping=_dict_to_str(ds_cfg.field_mapping)
            if ds_cfg.field_mapping
            else None,
            date_partition_column=ds_cfg.date_partition_column,
        )

        entity_defs, feature_defs = _get_entities_and_features_from_sql(
            ds_cfg.select_sql
        )
        feature_view_def = FeatureViewDefinition(
            name=fv_cfg.name,
            entities=[ed.name for ed in entity_defs],
            ttl=repr(fv_cfg.ttl),
            features=feature_defs if len(feature_defs) > 0 else None,
            batch_source=data_source_id,
            tags=_dict_to_str(fv_cfg.tags) if fv_cfg.tags else None,
        )

        job_data_source_defs.append(data_source_def)
        job_entity_defs += entity_defs
        job_feature_view_defs.append(feature_view_def)

    return job_data_source_defs, job_entity_defs, job_feature_view_defs


def _get_entities_and_features_from_sql(
    sql: str,
) -> Tuple[List[EntityDefinition], List[FeatureDefinition]]:
    assert sql is not None and sql != ""

    from sqlparse.sql import Comment, Identifier, IdentifierList
    from sqlparse.tokens import Keyword, Name

    entity_defs = []
    feature_defs = []

    def parse_identifier(idt: Identifier) -> Union[EntityDefinition, FeatureDefinition]:
        is_entity = False
        idt_name = ""
        entity_name = None
        idt_type = "ValueType.UNKNOWN"
        for token in idt.tokens:
            if token.match(Name, None):
                idt_name = token.value
            elif isinstance(token, Identifier):
                idt_name = token.value
            elif isinstance(token, Comment):
                comment = token.value
                if _matches := re.search(
                    r"(?:\/\*|,|--) *entity(?:: *?([^ \n\*,]+)|[, \n*$])",
                    comment,
                    re.IGNORECASE,
                ):
                    is_entity = True
                    try:
                        entity_name = _matches.group(1)
                    except IndexError:
                        entity_name = None

                if _matches := re.search(
                    r"(?:\/\*|,|--) *type: *?([^ \n\*]+)", comment, re.IGNORECASE
                ):
                    idt_type = _str_to_feast_value_type(_matches[1])

        if is_entity:
            return EntityDefinition(
                name=entity_name if entity_name else idt_name,
                value_type=idt_type,
                join_key=idt_name,
            )
        else:
            return FeatureDefinition(name=idt_name, dtype=idt_type)

    parsed = sqlparse.parse(sql)[0]
    for token in parsed.tokens:
        if isinstance(token, IdentifierList):
            for sub_token in token.tokens:
                if isinstance(sub_token, Identifier):
                    _def = parse_identifier(sub_token)
                    if isinstance(_def, EntityDefinition):
                        entity_defs.append(_def)
                    else:
                        feature_defs.append(_def)
        elif isinstance(token, Identifier):
            _def = parse_identifier(token)
            if isinstance(_def, EntityDefinition):
                entity_defs.append(_def)
            else:
                feature_defs.append(_def)
        elif token.match(Keyword, "FROM"):
            break

    return entity_defs, feature_defs


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


def _dict_to_str(_dict: Dict[str, str]) -> str:
    return "{" + ", ".join([f'"{k}": "{v}"' for k, v in _dict.items()]) + "}"


def _generate_hash_key(prefix: str, _dict: dict) -> str:
    """discussion about hashing a dict: https://stackoverflow.com/questions/5884066/hashing-a-dictionary"""
    hash_key = hash(frozenset(_dict))
    hash_key = str(hash_key).replace("-", "_")
    return f"{prefix}_{hash_key}"


def _str_to_feast_value_type(type_str: str) -> str:
    """
    available value types:
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
    """

    type_str = type_str.upper()
    if type_str == "INT":
        type_str = "INT32"

    return f"ValueType.{type_str}"
