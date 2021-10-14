import collections
import importlib
import random
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pendulum
from jinja2 import Environment
from jinja2.sandbox import SandboxedEnvironment

from feaflow import exceptions
from feaflow.abstracts import Component, FeaflowConfig, FeaflowModel, SchedulerConfig


def get_class_from_name(class_name: str):
    if "." not in class_name:
        raise exceptions.ClassImportError(
            class_name,
            "maybe it's a typo or the class name is not a builtin implementation.",
        )
    module_name, class_name = class_name.rsplit(".", 1)
    try:
        module = importlib.import_module(module_name)
        return getattr(module, class_name)
    except Exception:
        raise exceptions.ClassImportError(class_name)


def construct_config_from_dict(
    config_dict: Dict[str, Any], builtin_types: Dict[str, str],
) -> FeaflowConfig:
    assert "type" in config_dict
    impl_type = str(config_dict["type"]).strip().lower()

    # get impl class class from type of the dict and builtin_types
    impl_class_name = (
        builtin_types[impl_type] if impl_type in builtin_types else impl_type
    )

    # construct the config object
    impl_config_class_name = f"{impl_class_name}Config"
    impl_config_class = get_class_from_name(impl_config_class_name)
    assert issubclass(impl_config_class, FeaflowConfig)
    return impl_config_class(**config_dict)


def construct_scheduler_config_from_dict(
    config_dict: Dict[str, Any]
) -> SchedulerConfig:
    if (
        "type" not in config_dict
        or str(config_dict["type"]).strip().lower() == "airflow"
    ):
        from feaflow.airflow_config import AirflowSchedulerConfig

        config_class = AirflowSchedulerConfig
    else:
        raise NotImplementedError

    return config_class(**config_dict)


def construct_impl_from_config(config: FeaflowConfig) -> Component:
    impl_class_name = re.sub(r"Config$", "", config.__class__.__qualname__)
    impl_class = get_class_from_name(f"{config.__module__}.{impl_class_name}")
    return impl_class(config=config)


def split_cols(cols: str) -> List[str]:
    return list(map(str.strip, cols.split(",")))


def create_random_str(short: bool = False) -> str:
    # TODO short
    return f"{int(time.time_ns())}_{random.randint(1000, 9999)}"


_jinja2_env: Optional[Environment] = None


def render_template_method_jinja2(
    template_source: str, template_context: Dict[str, Any]
) -> str:
    global _jinja2_env
    if _jinja2_env is None:
        _jinja2_env = SandboxedEnvironment(cache_size=0)
    return _jinja2_env.from_string(template_source).render(**template_context)


def render_template_method_replace(
    template_source: str, template_context: Dict[str, Any]
) -> str:
    for k, v in template_context.items():
        template_source = re.sub(
            r"\{\{ *?" + re.escape(k) + " *?\}\}", str(v), template_source
        )
    return template_source


def render_template(
    template_source: Any,
    template_context: Optional[Dict[str, Any]] = None,
    use_jinja2: bool = True,
) -> [FeaflowModel, str]:
    if template_context is None:
        template_context = {}

    if isinstance(template_source, str):
        render_method = (
            render_template_method_jinja2
            if use_jinja2
            else render_template_method_replace
        )
        return render_method(template_source, template_context)

    elif isinstance(template_source, FeaflowModel):
        if len(template_source._template_attrs) == 0:
            return template_source
        else:
            new_model = template_source.copy()
            old_allow_mutation = new_model.__config__.allow_mutation
            new_model.__config__.allow_mutation = True
            for tpl_attr in new_model._template_attrs:
                if new_model.__getattribute__(tpl_attr) is not None:
                    new_model.__setattr__(
                        tpl_attr,
                        render_template(
                            new_model.__getattribute__(tpl_attr),
                            template_context,
                            use_jinja2,
                        ),
                    )
            new_model.__config__.allow_mutation = old_allow_mutation
            return new_model

    elif isinstance(template_source, tuple):
        if type(template_source) is not tuple:
            # Special case for named tuples
            return template_source.__class__(
                *(
                    render_template(element, template_context, use_jinja2)
                    for element in template_source
                )
            )
        else:
            return tuple(
                render_template(element, template_context, use_jinja2)
                for element in template_source
            )

    elif isinstance(template_source, dict):
        return {
            k: render_template(v, template_context, use_jinja2)
            for k, v in template_source.items()
        }

    elif isinstance(template_source, list):
        return [
            render_template(v, template_context, use_jinja2) for v in template_source
        ]

    elif isinstance(template_source, set):
        return {
            render_template(element, template_context, use_jinja2)
            for element in template_source
        }

    else:
        return template_source


def deep_merge_models(model: FeaflowModel, merge_model: FeaflowModel) -> FeaflowModel:
    assert type(model) == type(merge_model)
    source_dict = merge_model.dict(exclude_defaults=True)
    dest_dict = model.dict(exclude_defaults=True)
    deep_merge_dicts(source_dict, dest_dict)
    return type(model)(**source_dict)


def deep_merge_dicts(_dict: Dict, merge_dict: Dict):
    for k, v in merge_dict.items():
        if (
            k in _dict
            and isinstance(_dict[k], dict)
            and isinstance(merge_dict[k], collections.Mapping)
        ):
            deep_merge_dicts(_dict[k], merge_dict[k])
        else:
            _dict[k] = merge_dict[k]


def make_tzaware(dt: datetime) -> datetime:
    if dt.utcoffset() is None:
        return dt.replace(tzinfo=timezone.utc)
    else:
        return dt


def construct_template_context(
    project: "feaflow.project.Project",
    job_config: "feaflow.job.JobConfig",
    execution_date: Optional[datetime] = None,
    upstream_template_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    context = {
        "project_name": project.name,
        "project_root": project.root_dir.resolve(),
        "job_root": job_config.config_file_path.parent.resolve(),
        "job_name": job_config.name,
        "engine_name": job_config.engine.use,
    }

    if execution_date:
        ds = execution_date.strftime("%Y-%m-%d")
        ts = execution_date.isoformat()
        yesterday_ds = (execution_date - timedelta(1)).strftime("%Y-%m-%d")
        tomorrow_ds = (execution_date + timedelta(1)).strftime("%Y-%m-%d")
        ds_nodash = ds.replace("-", "")
        ts_nodash = execution_date.strftime("%Y%m%dT%H%M%S")
        ts_nodash_with_tz = ts.replace("-", "").replace(":", "")
        yesterday_ds_nodash = yesterday_ds.replace("-", "")
        tomorrow_ds_nodash = tomorrow_ds.replace("-", "")
        context.update(
            {
                "execution_date": pendulum.instance(execution_date),
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
        )

    if upstream_template_context:
        context.update(upstream_template_context)

    return context


def find_files_by_patterns(root_dir, *patterns) -> List:
    def _match_any_pattern(f: Path) -> bool:
        for pat in patterns:
            if re.search(pat, str(f.resolve())):
                return True
        return False

    return [
        f.resolve()
        for f in root_dir.glob("**/*")
        if f.is_file() and _match_any_pattern(f)
    ]
