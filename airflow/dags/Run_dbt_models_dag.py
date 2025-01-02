import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

# Base directory for your models
base_dir = r"C:\Users\Bharath\Desktop\Trainings\capston\models"

# Define the groups in order of execution
model_groups = ["stage", "dim", "fact"]

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# Create the DAG
with DAG(
    dag_id="run_dbt_models",
    default_args=default_args,
    start_date=datetime(2023, 12, 31),
    schedule_interval=None,
    catchup=False,
) as dag:

    previous_group = None  # To manage dependencies

    for group in model_groups:
        group_path = os.path.join(base_dir, group)

        with TaskGroup(group_id=f"run_{group}_models") as task_group:
            # Get all model files in the group folder
            for model_file in os.listdir(group_path):
                if model_file.endswith(".sql"):  # Ensure it's a SQL model file
                    model_name = os.path.splitext(model_file)[0]

                    # Task to run the DBT model
                    BashOperator(
                        task_id=f"dbt_run_{model_name}",
                        bash_command=f"cd {base_dir} && dbt run --models {group}.{model_name}",
                    )

        # Set dependencies between groups
        if previous_group:
            previous_group >> task_group
        previous_group = task_group
