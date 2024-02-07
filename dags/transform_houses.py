"""
This is an example of a DAG that performs a transformation on a table called "houses"
and then loads the result into a final table called "dim_houses".
"""

import os
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from include.operator_wrappers.elt import (
    load_to_final_table,
    run_tranformation,
    wait_external_dag_ending,
)
from pendulum import datetime

DAG_ID = os.path.basename(__file__).replace(".py", "")  # Get the name of the file
SQL_TRANSFORMATION_DIR_PATH = os.path.join("sql", "transformations")


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=1,  # Run only 1 task at the time because DuckDB can't handle multiple write operations at the same time
)
def main_dag():

    # ====================================================================================
    # WAIT FOR EXTRACT & LOAD DAG
    # ====================================================================================

    wait_ingestion = wait_external_dag_ending(
        task_id="wait_house_ingestion_end",
        external_dag_id="ingest_raw_data",  # Wait for run of ingest_raw_data DAG
    )

    # ====================================================================================
    # TRANSFORMATION TASKS
    # ====================================================================================

    # ########################################################
    # Source: Houses
    # ########################################################

    clean_houses = run_tranformation(
        task_id="clean_houses",
        output_table_id="houses_cleaned",
        query=f"{SQL_TRANSFORMATION_DIR_PATH}/clean_houses.sql",  # Use directly query written in sql/transformations/clean_houses.sql file
    )

    conform_houses = run_tranformation(
        task_id="conform_houses",
        output_table_id="houses_conformed",
        query=f"{SQL_TRANSFORMATION_DIR_PATH}/conform_houses.sql",
    )

    # Define task dependencies
    wait_ingestion >> clean_houses >> conform_houses

    # ########################################################
    # Source: Destinations
    # ########################################################

    clean_destinations = run_tranformation(
        task_id="clean_destinations",
        output_table_id="destinations_cleaned",
        query=f"{SQL_TRANSFORMATION_DIR_PATH}/clean_destinations.sql",
    )

    conform_destinations = run_tranformation(
        task_id="conform_destinations",
        output_table_id="destinations_conformed",
        query=f"{SQL_TRANSFORMATION_DIR_PATH}/conform_destinations.sql",
    )

    # Define task dependancies
    wait_ingestion >> clean_destinations >> conform_destinations

    # ########################################################
    # Combine sources & load to final table
    # ########################################################

    combine_sources = run_tranformation(
        task_id="combine_sources",
        output_table_id="houses_destinations_combined",
        query=f"{SQL_TRANSFORMATION_DIR_PATH}/combine_sources.sql",
    )

    load_final_table_houses = load_to_final_table(
        task_id="load_final_table_houses",
        input_table_id="houses_destinations_combined",  # Just specify the input table that will be loaded into the final table
        output_table_id="dim_houses",  # dim_houses is my final table and is stored in the schema `core` (forced schema for final table)
    )

    end_task = EmptyOperator(
        task_id="end"
    )  # End DAG with a DAG that does nothing so it can be referenced by other DAGs to create dependencies

    # Define final task dependencies

    (
        [conform_houses, conform_destinations]
        >> combine_sources
        >> load_final_table_houses
        >> end_task
    )


main_dag()
