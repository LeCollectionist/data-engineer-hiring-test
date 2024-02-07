from datetime import timedelta

from airflow.models import DagRun
from airflow.sensors.external_task import ExternalTaskSensor
from include.custom_operators.duckdb_operators import DuckDBExecuteQueryOperator

CONNECTION = "datawarehouse"
STAGING_SCHEMA = "staging"
CORE_SCHEMA = "core"


def run_tranformation(
    task_id: str, query: str, output_table_id: str, params: dict = None
):
    """
    Execute a SQL query to transform data and store the result in a table.
    Transformation results are always stored in Staging schema.

    :param task_id: Name of the transformation task
    :param query: SQL query to run (or path to the SQL query script to run)
    :param output_table_id: Name of the table that will store the tranformation result
    :param params: Additional params to use in the SQL query (through jinja)
    """

    return DuckDBExecuteQueryOperator(
        task_id=task_id,
        destination_table_name=f"{STAGING_SCHEMA}.{output_table_id}",
        query=query,
        duckdb_conn_id=CONNECTION,
    )


def load_to_final_table(
    task_id: str, input_table_id: str, output_table_id: str, params: dict = None
):
    """
    Execute a SQL query that will store data in the final table that will be used by Analysts in the core schema.

    :param task_id: Name of the transformation task
    :param input_table_id: Name of the table to use to load the data in the final table
    :param output_table_id: Name of the final table
    :param params: Additional params to use in the SQL query (through jinja)
    """

    query = f"SELECT * FROM {STAGING_SCHEMA}.{input_table_id};"

    return DuckDBExecuteQueryOperator(
        task_id=task_id,
        destination_table_name=f"{CORE_SCHEMA}.{output_table_id}",
        query=query,
        duckdb_conn_id=CONNECTION,
    )


def wait_external_dag_ending(task_id: str, external_dag_id: str):
    """
    Wait for the most recent external DAG run ending

    :param task_id: Name of the task
    :param external_dag_id: DAG ID of the external DAG ending to wait for
    """

    def get_most_recent_dag_run(dt):
        dag_runs = DagRun.find(dag_id=external_dag_id)
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
        if dag_runs:
            return dag_runs[0].execution_date

    return ExternalTaskSensor(
        task_id=task_id,
        external_dag_id=external_dag_id,
        external_task_id="end",
        execution_date_fn=get_most_recent_dag_run,
        mode="reschedule",
        poke_interval=1800,  # 30min between
    )
