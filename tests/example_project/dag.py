from pathlib import Path

from feaflow.airflow import AirflowScheduler
from feaflow.project import Project

# Initialize Project
project_root_path = Path(__file__).parent
project = Project(project_root_path)

# Initialize AirflowScheduler
airflow_scheduler = AirflowScheduler(project)

# Create dags from the project
dags = airflow_scheduler.create_project_dags()
for dag in dags:
    # Put the dags into globals, in order to be loaded by Airflow
    globals()[dag.dag_id] = dag
