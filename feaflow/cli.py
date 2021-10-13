import logging
import sys
from datetime import datetime
from pathlib import Path

import click
import pkg_resources

from feaflow.exceptions import JobNotFoundError
from feaflow.project import Project
from feaflow.utils import construct_template_context


def setup_logging(verbose: int):
    root = logging.getLogger()
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s {{%(filename)s:%(lineno)d}} %(message)s"
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
    type=click.DateTime(
        [
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%d %H:%M:%S",
            "%Y%m%d",
            "%Y%m%d%H%M%S",
        ]
    ),
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
    template_context = construct_template_context(project, job.config, execution_date)
    project.run_job(job, execution_date, template_context)


if __name__ == "__main__":
    cli()
