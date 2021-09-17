from datetime import datetime
from pathlib import Path

import click
import pkg_resources

from feaflow.exceptions import JobNotFoundError
from feaflow.project import Project
from feaflow.utils import make_tzaware


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
    ctx.obj["PROJECT_ROOT"] = (
        Path.cwd() if project is None else Path(project).absolute()
    )
    pass


@cli.command()
def version():
    """
    Display current Feaflow version
    """
    print(f'Feaflow Version: "{pkg_resources.get_distribution("feaflow")}"')


@cli.command("run")
@click.argument("job_name", type=click.STRING)
@click.argument("execution_date")
@click.pass_context
def entity_describe(ctx: click.Context, job_name: str, execution_date: str):
    """
    Run a job from the project
    """
    project_root = ctx.obj["PROJECT_ROOT"]
    execution_date = make_tzaware(datetime.fromisoformat(execution_date))

    project = Project(project_root)
    job = project.get_job(job_name)
    if not job:
        raise JobNotFoundError(project_root, job_name)
    project.run_job(job, execution_date)


if __name__ == "__main__":
    cli()
