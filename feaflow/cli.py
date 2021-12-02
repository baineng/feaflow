import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List

import click
import pkg_resources

from feaflow.exceptions import JobNotFoundError
from feaflow.project import Project
from feaflow.utils import construct_template_context, make_tzaware


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
    Welcome use Feaflow (https://github.com/thenetcircle/feaflow)
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


@cli.group(name="feast")
def feast_cmd():
    """
    Feast commands
    """
    pass


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
    from feaflow import feast

    project_dir = ctx.obj["PROJECT_DIR"]
    project = Project(project_dir)
    try:
        with feast.init(project) as feast_project:
            feast_project.apply(skip_source_validation)
    except Exception as e:
        print(str(e))


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
@click.option(
    "--apply",
    "-a",
    is_flag=True,
    help="Run feast apply before materialize.",
)
@click.pass_context
def materialize_command(
    ctx: click.Context,
    start_ts: datetime,
    end_ts: datetime,
    views: List[str],
    apply: bool,
):
    """
    Create or update a feature store deployment
    """
    from feaflow import feast

    project_dir = ctx.obj["PROJECT_DIR"]
    project = Project(project_dir)
    try:
        with feast.init(project) as feast_project:
            if apply:
                feast_project.apply(skip_source_validation=True)

            feast_project.materialize(
                start_date=make_tzaware(start_ts),
                end_date=make_tzaware(end_ts),
                feature_views=None if not views else views,
            )
    except Exception as e:
        print(str(e))


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
@click.option(
    "--apply",
    "-a",
    is_flag=True,
    help="Run feast apply before incrementally materialize.",
)
@click.pass_context
def materialize_incremental_command(
    ctx: click.Context,
    end_ts: datetime,
    views: List[str],
    apply: bool,
):
    """
    Create or update a feature store deployment
    """
    from feaflow import feast

    project_dir = ctx.obj["PROJECT_DIR"]
    project = Project(project_dir)
    try:
        with feast.init(project) as feast_project:
            if apply:
                feast_project.apply(skip_source_validation=True)

            feast_project.materialize_incremental(
                end_date=make_tzaware(end_ts),
                feature_views=None if not views else views,
            )
    except Exception as e:
        print(str(e))


@feast_cmd.command("serve")
@click.option(
    "--port", "-p", type=click.INT, default=6566, help="Specify a port for the server"
)
@click.option(
    "--apply",
    "-a",
    is_flag=True,
    help="Run feast apply before serve.",
)
@click.pass_context
def serve_command(
    ctx: click.Context,
    port: int,
    apply: bool,
):
    """Start a the feature consumption server locally on a given port."""
    from feaflow import feast

    project_dir = ctx.obj["PROJECT_DIR"]
    project = Project(project_dir)
    try:
        with feast.init(project) as feast_project:
            if apply:
                feast_project.apply(skip_source_validation=True)

            feast_project.serve(port)
    except Exception as e:
        print(str(e))


if __name__ == "__main__":
    cli()
