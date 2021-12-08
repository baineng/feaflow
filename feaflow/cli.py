import contextlib
import logging
import re
import sys
from datetime import datetime
from pathlib import Path, PosixPath
from typing import List

import click
import pkg_resources

from feaflow.exceptions import JobNotFoundError
from feaflow.project import Project
from feaflow.utils import construct_template_context, make_tzaware, render_template


def setup_logging(verbose: int):
    root = logging.getLogger()
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)

    if verbose == 0:
        root.setLevel(logging.WARNING)
    elif verbose == 1:
        root.setLevel(logging.INFO)
    else:
        root.setLevel(logging.DEBUG)

    return handler.stream


DATETIME_TYPE = click.DateTime(
    [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y%m%d",
        "%Y%m%d%H%M%S",
    ]
)


@click.group()
@click.option(
    "--project",
    "-p",
    help="Choose a target project directory for the subcommand (default is current directory).",
)
@click.option("-v", "--verbose", count=True)
@click.pass_context
def cli(ctx: click.Context, project: str, verbose: int):
    """
    Welcome use Feaflow (https://github.com/baineng/feaflow)
    """
    ctx.ensure_object(dict)
    ctx.obj["PROJECT_DIR"] = Path.cwd() if project is None else Path(project).absolute()
    setup_logging(verbose)


@cli.command()
def version():
    """
    Show current version
    """
    print(pkg_resources.get_distribution("feaflow"))


@cli.command("run")
@click.argument("job_name", type=click.STRING)
@click.argument(
    "execution_date",
    type=DATETIME_TYPE,
    metavar="EXECUTION_DATE",
)
@click.pass_context
def run_job(ctx: click.Context, job_name: str, execution_date: datetime):
    """
    Run a job from the project
    """
    project_dir = ctx.obj["PROJECT_DIR"]
    project = Project(project_dir)
    job = project.get_job(job_name)
    if not job:
        raise JobNotFoundError(project_dir, job_name)

    # for execution_date has no timezone, just replace to utc
    execution_date = make_tzaware(execution_date)

    template_context = construct_template_context(project, job.config, execution_date)
    project.run_job(job, execution_date, template_context)


@cli.group(name="dump")
@click.pass_context
def dump_cmd(ctx: click.Context):
    """
    Dump commands
    """
    pass


@dump_cmd.command("job-config")
@click.argument("job_name", type=click.STRING)
@click.argument(
    "execution_date",
    type=DATETIME_TYPE,
    metavar="EXECUTION_DATE",
)
@click.pass_context
def job_config_dump(ctx: click.Context, job_name: str, execution_date: datetime):
    """
    Dump a rendered job config
    """
    import pprint
    from datetime import timedelta

    import yaml

    project_dir = ctx.obj["PROJECT_DIR"]
    project = Project(project_dir)
    job_configs = []
    jobs = project.scan_jobs()
    for _job in jobs:
        if re.search(re.compile(job_name, re.I), _job.name):
            job_configs.append(_job)

    if not job_configs:
        raise JobNotFoundError(project_dir, job_name)

    def path_representer(dumper, data):
        return dumper.represent_scalar("!path", "%s" % data)

    def timedelta_representer(dumper, data):
        return dumper.represent_scalar("!timedelta", "%s" % data)

    yaml.add_representer(Path, path_representer)
    yaml.add_representer(PosixPath, path_representer)
    yaml.add_representer(timedelta, timedelta_representer)

    for job_config in job_configs:
        # for execution_date has no timezone, just replace to utc
        execution_date = make_tzaware(execution_date)

        template_context = construct_template_context(
            project, job_config, execution_date
        )

        rendered_job_config = job_config.dict(
            include={"name", "config_file_path", "loop_variables", "variables"}
        )

        to_render_fields = ["engine", "scheduler", "sources", "computes", "sinks"]
        for field in to_render_fields:
            field_value = job_config.__getattribute__(field)
            if field_value is not None:
                rendered_field_value = render_template(field_value, template_context)

                if type(rendered_field_value) == list:
                    rendered_field_value = [
                        f.dict(by_alias=True, exclude_unset=True)
                        for f in rendered_field_value
                    ]
                else:
                    rendered_field_value = rendered_field_value.dict(
                        by_alias=True, exclude_unset=True
                    )

            rendered_job_config[field] = rendered_field_value

        print(f"============ {job_config.name} ============")
        print(yaml.dump_all([rendered_job_config], indent=2, sort_keys=False))
        # pprint.pprint(rendered_job_config)
        print("")


@cli.group(name="feast")
@click.option(
    "--apply",
    "-a",
    is_flag=True,
    help="Run feast apply before the specify command.",
)
@click.option(
    "--skip-source-validation",
    is_flag=True,
    help="When --apply specified, don't validate the data sources by checking for that the tables exist.",
)
@click.pass_context
def feast_cmd(ctx: click.Context, apply: bool, skip_source_validation: bool):
    """
    Feast commands
    """

    @contextlib.contextmanager
    def init_feast_project(_apply=None, _skip_source_validation=None):
        from feaflow import feast

        _apply = _apply if _apply is not None else apply
        _skip_source_validation = (
            _skip_source_validation
            if _skip_source_validation is not None
            else skip_source_validation
        )

        project_dir = ctx.obj["PROJECT_DIR"]
        project = Project(project_dir)
        try:
            with feast.init(project) as feast_project:
                if _apply:
                    feast_project.apply(_skip_source_validation)

                yield feast_project
        except Exception as e:
            print(str(e))
            exit(1)

    ctx.obj["FEAST_PROJECT"] = init_feast_project


@feast_cmd.command("apply")
@click.option(
    "--skip-source-validation",
    is_flag=True,
    help="Don't validate the data sources by checking for that the tables exist.",
)
@click.pass_context
def apply_total_command(ctx: click.Context, skip_source_validation: bool):
    """
    Create or update a feature store deployment
    """
    with ctx.obj["FEAST_PROJECT"](True, skip_source_validation) as feast_project:
        pass


@feast_cmd.command("materialize")
@click.argument(
    "start_ts",
    type=DATETIME_TYPE,
    metavar="START_TS",
)
@click.argument(
    "end_ts",
    type=DATETIME_TYPE,
    metavar="END_TS",
)
@click.option(
    "--views",
    help="Feature views to materialize",
    multiple=True,
)
@click.pass_context
def materialize_command(
    ctx: click.Context,
    start_ts: datetime,
    end_ts: datetime,
    views: List[str],
):
    """
    Create or update a feature store deployment
    """
    with ctx.obj["FEAST_PROJECT"]() as feast_project:
        feast_project.materialize(
            start_date=make_tzaware(start_ts),
            end_date=make_tzaware(end_ts),
            feature_views=None if not views else views,
        )


@feast_cmd.command("materialize-incremental")
@click.argument(
    "end_ts",
    type=DATETIME_TYPE,
    metavar="END_TS",
)
@click.option(
    "--views",
    help="Feature views to incrementally materialize",
    multiple=True,
)
@click.pass_context
def materialize_incremental_command(
    ctx: click.Context,
    end_ts: datetime,
    views: List[str],
):
    """
    Create or update a feature store deployment
    """
    with ctx.obj["FEAST_PROJECT"]() as feast_project:
        feast_project.materialize_incremental(
            end_date=make_tzaware(end_ts),
            feature_views=None if not views else views,
        )


@feast_cmd.command("serve")
@click.option(
    "--port", "-p", type=click.INT, default=6566, help="Specify a port for the server"
)
@click.pass_context
def serve_command(
    ctx: click.Context,
    port: int,
):
    """Start a the feature consumption server locally on a given port."""
    with ctx.obj["FEAST_PROJECT"]() as feast_project:
        feast_project.serve(port)


@feast_cmd.command("teardown")
@click.pass_context
def teardown_command(ctx: click.Context):
    """
    Tear down deployed feature store infrastructure
    """
    with ctx.obj["FEAST_PROJECT"]() as feast_project:
        feast_project.teardown()


@feast_cmd.command("registry-dump")
@click.pass_context
def registry_dump_command(ctx: click.Context):
    """
    Print contents of the metadata registry
    """
    from feast.registry import Registry

    with ctx.obj["FEAST_PROJECT"]() as feast_project:
        repo_config = feast_project.load_repo_config()
        registry_config = repo_config.get_registry_config()
        project = repo_config.project
        registry = Registry(
            registry_config=registry_config, repo_path=feast_project.feast_project_dir
        )

        for entity in registry.list_entities(project=project):
            print(entity)
        for feature_view in registry.list_feature_views(project=project):
            print(feature_view)


@feast_cmd.group(name="entities")
def entities_cmd():
    """
    Access entities
    """
    pass


@entities_cmd.command("describe")
@click.argument("name", type=click.STRING)
@click.pass_context
def entity_describe(ctx: click.Context, name: str):
    """
    Describe an entity
    """
    import yaml

    with ctx.obj["FEAST_PROJECT"]() as feast_project:
        entity = feast_project.get_entity(name)

        print(
            yaml.dump(
                yaml.safe_load(str(entity)),
                default_flow_style=False,
                sort_keys=False,
            )
        )


@entities_cmd.command(name="list")
@click.pass_context
def entity_list(ctx: click.Context):
    """
    List all entities
    """
    table = []
    with ctx.obj["FEAST_PROJECT"]() as feast_project:
        for entity in feast_project.get_feature_store().list_entities():
            table.append([entity.name, entity.description, entity.value_type])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME", "DESCRIPTION", "TYPE"], tablefmt="plain"))


@feast_cmd.group(name="feature-views")
def feature_views_cmd():
    """
    Access feature views
    """
    pass


@feature_views_cmd.command("describe")
@click.argument("name", type=click.STRING)
@click.pass_context
def feature_view_describe(ctx: click.Context, name: str):
    """
    Describe a feature view
    """
    import yaml

    with ctx.obj["FEAST_PROJECT"]() as feast_project:
        feature_view = feast_project.get_feature_store().get_feature_view(name)
        print(
            yaml.dump(
                yaml.safe_load(str(feature_view)),
                default_flow_style=False,
                sort_keys=False,
            )
        )


@feature_views_cmd.command(name="list")
@click.pass_context
def feature_view_list(ctx: click.Context):
    """
    List all feature views
    """
    from feast.feature_view import FeatureView
    from feast.on_demand_feature_view import OnDemandFeatureView

    table = []
    with ctx.obj["FEAST_PROJECT"]() as feast_project:
        store = feast_project.get_feature_store()
        for feature_view in [
            *store.list_feature_views(),
            *store.list_request_feature_views(),
            *store.list_on_demand_feature_views(),
        ]:
            entities = set()
            if isinstance(feature_view, FeatureView):
                entities.update(feature_view.entities)
            elif isinstance(feature_view, OnDemandFeatureView):
                for backing_fv in feature_view.inputs.values():
                    if isinstance(backing_fv, FeatureView):
                        entities.update(backing_fv.entities)
            table.append(
                [
                    feature_view.name,
                    entities if len(entities) > 0 else "n/a",
                    type(feature_view).__name__,
                ]
            )

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME", "ENTITIES", "TYPE"], tablefmt="plain"))


if __name__ == "__main__":
    cli()
