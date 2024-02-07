from typing import TYPE_CHECKING, Sequence

from airflow.models.baseoperator import BaseOperator
from duckdb_provider.hooks.duckdb_hook import DuckDBHook


class DuckDBExecuteQueryOperator(BaseOperator):
    """
    Operator that executes a query on a DuckDB database and store the result in a table.

    :param query: The query to execute.
    :param destination_table_name: The name of the table to create.
    :param write_mode: The write mode to use. Possible values are "OVERWRITE" and "APPEND".
    :param duckdb_conn_id: Airflow connection to duckdb.
    """

    def __init__(
        self,
        query,
        destination_table_name: str = None,
        write_mode: str = "OVERWRITE",
        duckdb_conn_id: str = "duckdb_default",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.destination_table_name = destination_table_name
        self.write_mode = write_mode
        self.duckdb_conn_id = duckdb_conn_id
        self.query = query

    template_ext: Sequence[str] = ".sql"
    template_fields: Sequence[str] = (
        # "destination_table_name",
        "query",
    )

    def execute(self, context):
        duckdb_hook = DuckDBHook(duckdb_conn_id=self.duckdb_conn_id)
        duckdb_conn = duckdb_hook.get_conn()

        query_to_execute = ""

        if self.destination_table_name is not None:
            if self.write_mode == "OVERWRITE":
                query_to_execute = (
                    f"CREATE OR REPLACE TABLE {self.destination_table_name} AS \n"
                    + f"{self.query}"
                )
            elif self.write_mode == "APPEND":
                query_to_execute = (
                    f"INSERT INTO {self.destination_table_name}\n" + f"{self.query}"
                )
        else:
            query_to_execute = f"{self.query}"

        duckdb_conn.execute(query_to_execute)
        self.log.info(f"Executed query: \n{self.query}")

        if self.destination_table_name is not None:
            self.log.info(f"Result stored in table: {self.destination_table_name}")
