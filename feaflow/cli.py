from datetime import datetime
from pathlib import Path

import click
import pkg_resources

from feaflow.exceptions import JobNotFoundError
from feaflow.project import Project
from feaflow.utils import construct_template_context


@click.group()
@click.option(
    "--project",
    "-p",
    help="Choose a target project directory for the subcommand (default is current directory).",
)
@click.pass_context
def cli(ctx: click.Context, project: str):
    """
    Welcome use Feaflow (https://github.com/thenetcircle/feaflow)
    """
    ctx.ensure_object(dict)
    ctx.obj["PROJECT_DIR"] = Path.cwd() if project is None else Path(project).absolute()
    pass


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
