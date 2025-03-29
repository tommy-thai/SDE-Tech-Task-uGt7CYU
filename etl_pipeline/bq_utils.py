# bq_utils.py

from datetime import date
import os
from etl_pipeline.utils import log


import pandas as pd
from google.cloud.bigquery import (
    Client,
    SchemaField,
    Table,
    TimePartitioning,
    TimePartitioningType
)

class BigQueryIO:
    """
    Class to interact with BQ.

    This class provides the following methods:
        - `load_pandas_dataframe()`: loads a pandas dataframe to BQ.
        - `insert_rows()`: inserts rows defined as a list of dictionaries to BQ.
    """

    # Upload dataframe to GCP BQ
    def __init__( self,table_name: str,dataset: str,project_id: str) -> None:
        """
        table_name (str): The name of the BQ table in the format "dataset.table_name".
        project_id (str): GCP Project ID.
        job_config (Optional[LoadJobConfig]): Configuration for load jobs.
        """
        self.table_id = f"{project_id}.{dataset}.{table_name}"
        self.client = Client(project=project_id)


    def load_pandas_dataframe(self, df: pd.DataFrame) -> None:
        """Function to load a  pandas dataframe to BQ"""
        if df.empty:
            log(f"No data to write to table: {self.table_id}")
        try:
            # Load DataFrame to BQ
            load_job = self.client.load_table_from_dataframe(
                df, destination=self.table_id
            )
            load_job.result()
            log(f"Loaded {len(df)} rows to {self.table_id}")
        except Exception as e:
            log(f"Bigquery load job failed with error {e}", "error")

    def execute_query(self, query: str) -> None:
        """Function to execute a query"""
        try:
            query_job = self.client.query(query)
            query_job.result()
            log(f"Query executed successfully: {query}")
        except Exception as e:
            log(f"Bigquery query execution failed with error {e}", "error")


    def create_table(
        self, schema: list[SchemaField], 
        time_partitioning_field: str = None
        ) -> Table:
        """Function to create a table in BQ with specified schema"""
        table = Table(self.table_id, schema=schema)
        
        if time_partitioning_field:
            table.time_partitioning = TimePartitioning(
                type_=TimePartitioningType.DAY,
                field=time_partitioning_field,
            )

        table = self.client.create_table(table)
        log(f"Created table: {table} with schema: {schema}")
        return table


if __name__ == "__main__":
    project = os.getenv("PROJECT_ID")
    dataset = 'hn_etl'
    table = 'hn_locations_weather'



    schema = [
        SchemaField("city", "STRING", mode="NULLABLE"),
        SchemaField("state", "STRING", mode="NULLABLE"),
        SchemaField("country", "STRING", mode="NULLABLE"),
        SchemaField("lat", "FLOAT", mode="NULLABLE"),
        SchemaField("lon", "FLOAT", mode="NULLABLE"),
        SchemaField("date", "DATE", mode="REQUIRED"),
        SchemaField(
            "data", 
            "RECORD", 
            mode="REPEATED",  # Because it's an array of structs
            fields=[
                SchemaField("dt", "INTEGER", mode="NULLABLE"),
                SchemaField("sunrise", "INTEGER", mode="NULLABLE"),
                SchemaField("sunset", "INTEGER", mode="NULLABLE"),
                SchemaField("temp", "FLOAT", mode="NULLABLE"),
                SchemaField("feels_like", "FLOAT", mode="NULLABLE"),
                SchemaField("pressure", "INTEGER", mode="NULLABLE"),
                SchemaField("humidity", "INTEGER", mode="NULLABLE"),
                SchemaField("dew_point", "FLOAT", mode="NULLABLE"),
                SchemaField("uvi", "FLOAT", mode="NULLABLE"),
                SchemaField("clouds", "INTEGER", mode="NULLABLE"),
                SchemaField("visibility", "INTEGER", mode="NULLABLE"),
                SchemaField("wind_speed", "FLOAT", mode="NULLABLE"),
                SchemaField("wind_deg", "INTEGER", mode="NULLABLE"),
                SchemaField("wind_gust", "FLOAT", mode="NULLABLE"),
                SchemaField(
                    "weather", 
                    "RECORD", 
                    mode="REPEATED",  # Because it's an array
                    fields=[
                        SchemaField("id", "INTEGER", mode="NULLABLE"),
                        SchemaField("main", "STRING", mode="NULLABLE"),
                        SchemaField("description", "STRING", mode="NULLABLE"),
                        SchemaField("icon", "STRING", mode="NULLABLE"),
                    ],
                ),
            ],
        ),
    ]

    
    client = BigQueryIO(table, dataset, project)
    client.create_table(schema)