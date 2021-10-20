import logging
import tempfile
from pathlib import Path

import yaml
from feast.repo_config import RepoConfig

from feaflow.exceptions import NotSupportedFeature
from feaflow.project import Project
from feaflow.utils import render_template

logger = logging.getLogger(__name__)


class Feast:
    def __init__(self, project: Project):
        if not project.support_feast():
            raise NotSupportedFeature("feast")

        self.project = project
        self.feast_project_dir = None

        # self._repo_config = RepoConfig()

    def init(self):
        """Init Feast project in a temp folder,
        by creating configuration files from templates"""
        if self.feast_project_dir:
            return

        template_dir = (Path(__file__).parent / "template" / "feast").absolute()
        feast_project_dir = tempfile.mkdtemp(prefix="feaflow_feast_")

        logger.info(f"Initializing a temporary Feast project in '{feast_project_dir}'")

        with open(template_dir / "feature_store.yaml", "r") as tf:
            context = self.project.config.feast_config.dict()
            context.update({"project": self.project.name})
            if context["online_store"]:
                context["online_store"] = yaml.dump(
                    {"online_store": context["online_store"]}
                )
            if context["offline_store"]:
                context["offline_store"] = yaml.dump(
                    {"offline_store": context["offline_store"]}
                )

            template_content = render_template(tf.read(), context)
            with open(f"{feast_project_dir}/feature_store.yaml", "xt") as f:
                f.write(template_content)

        logger.info("Initializing Done")
        self.feast_project_dir = feast_project_dir

    def apply(self):
        """Apply Feast infra"""
        pass
