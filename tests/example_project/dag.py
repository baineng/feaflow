from pathlib import Path

from feaflow import airflow
from feaflow.project import Project

# Initialize Project
project_root_path = Path(__file__).parent
project = Project(project_root_path)

# Create dags from the project
dags = airflow.create_dags_from_project(project)
for dag in dags:
    # Put the dags into globals, in order to be loaded by Airflow
    globals()[dag.dag_id] = dag
