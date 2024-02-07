"""
This is an example of a DAG that performs a transformation on a table called "houses"
and then loads the result into a final table called "dim_houses".
"""

import os
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

DAG_ID = os.path.basename(__file__).replace(".py", "")  # Get the name of the file


@dag(dag_id=DAG_ID, start_date=datetime(2024, 1, 1), schedule=None, catchup=False)
def main_dag():

    # Simulate ingestion job run: This task does nothing
    run_ingestion_job = EmptyOperator(task_id="run_ingestion_job")

    end = EmptyOperator(
        task_id="end"
    )  # End DAG with a DAG that does nothing so it can be referenced in other DAGs

    run_ingestion_job >> end


main_dag()
