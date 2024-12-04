from datetime import timedelta, datetime
import logging
import psycopg2
import json
from google.cloud import bigquery
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago



def custom_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()  # Convert datetime to ISO 8601 string
    return obj  # Default for other types


def extract_data(**kwargs):
    pg_conn = psycopg2.connect(
    host="host.docker.internal",
    user="airflow",
    password="airflow",
    database="lp-db"
    )

    cursor = pg_conn.cursor()
    # Get the last run time from Airflow's prev_execution_date (using context from DAG run)
    last_run_time = kwargs['prev_execution_date']
    
    # Format the SQL query to pull data based on `created_at` or `updated_at`
    query = f"""
    SELECT * 
    FROM public.retail_tx
    WHERE updated_at > '{last_run_time}'
    """
    cursor.execute(query)
    records = cursor.fetchall() 

    logging.info(f'{len(records)} datas successfully fetched')

    # Return the records to the next task (for transformation or further processing)
    return json.dumps(records, default=custom_serializer)

def transform_data(**kwargs):
    extracted_data = kwargs['ti'].xcom_pull(task_ids='extract_data')
    extracted_data = json.loads(extracted_data)

    # transform phase here
    keys = ["id", "customer_id", "pos_origin", "pos_destination", "created_at", "updated_at", "deleted_at", "last_status"]
    # Convert to a list of dictionaries
    data_as_dict = [dict(zip(keys, row)) for row in extracted_data]
    print(f'{len(extracted_data)} data being transformed')
    

    return data_as_dict

def load_staging(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data')
    
     # BigQuery configuration
    project_id = 'loyal-skill-435208-g1'
    dataset_id = 'lp_dataset'
    staging_table_id = 'staging_retail_transaction'
    
    # Initialize BigQuery client
    client = bigquery.Client()

    rows_to_insert = [
    {
        "id": row['id'],
        "customer_id": row['customer_id'],  # Ensure this matches exactly with the schema
        "pos_origin": row['pos_origin'],
        "pos_destination": row['pos_destination'],
        "created_at": row['created_at'],
        "updated_at": row['updated_at'],
        "deleted_at": row['deleted_at'],
        "last_status": row['last_status'],
    }
    for row in transformed_data
    ]
    print("Data to insert:", rows_to_insert)
    # Define the staging table reference
    table_ref = f"{project_id}.{dataset_id}.{staging_table_id}"

    # Load data into the staging table
    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        raise Exception(f"Encountered errors while inserting rows into staging table: {errors}")
    else:
        print(f"Successfully inserted {len(rows_to_insert)} rows into {table_ref}.")

def staging_merger(**kwargs):
    # Initialize BigQuery client
    client = bigquery.Client()

    # Define your query
    query = """
    MERGE INTO `loyal-skill-435208-g1.lp_dataset.retail_transaction` AS target
    USING (
      SELECT *
      FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS row_num
        FROM `loyal-skill-435208-g1.lp_dataset.staging_retail_transaction`
      )
      WHERE row_num = 1
    ) AS source
    ON target.id = source.id
    WHEN MATCHED THEN
      UPDATE SET 
        target.pos_origin = source.pos_origin,
        target.pos_destination = source.pos_destination,
        target.updated_at = source.updated_at,
        target.deleted_at = source.deleted_at,
        target.last_status = source.last_status
    WHEN NOT MATCHED THEN
      INSERT (id, customer_id, pos_origin, pos_destination, created_at, updated_at, deleted_at, last_status)
      VALUES (source.id, source.customer_id, source.pos_origin, source.pos_destination, source.created_at, source.updated_at, source.deleted_at, source.last_status);
    """

    # Execute the query
    query_job = client.query(query)

    # Wait for the query to finish
    query_job.result()

    # Log successful completion
    print("Merge query executed successfully.")


def clear_staging_table():
    client = bigquery.Client(project='loyal-skill-435208-g1')

    # Define the query to clear the staging table
    truncate_query = """
    CREATE OR REPLACE TABLE `loyal-skill-435208-g1.lp_dataset.staging_retail_transaction` AS
    SELECT * FROM `loyal-skill-435208-g1.lp_dataset.staging_retail_transaction`
        WHERE FALSE
    """

    # Execute the query
    query_job = client.query(truncate_query)
    query_job.result()  # Wait for the query to finish

    print("Staging table cleared successfully.")


# DAG args
default_args = {
    'owner': 'ersa',
    'start_date': days_ago(0),  # it means, start date from today
    'email': ['ersa@email.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # retry once if tasks fail when executed
    'retry_delay': timedelta(minutes=1),  # do retry tasks if fail after
}

# define DAG
dag = DAG(
    'ETL_Retail_Tx',
    default_args=default_args,
    description='scheduled etl retail transaction to populate data warehouse (bigquery)',
    schedule_interval=timedelta(hours=1),
)

extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,  # Pass context variables (like prev_execution_date)
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load = PythonOperator(
    task_id='load_staging',
    python_callable=load_staging,
    provide_context=True, 
    dag=dag,
)

merge = PythonOperator(
    task_id='staging_merger',
    python_callable=staging_merger,
    dag=dag,
)

clear_staging = PythonOperator(
    task_id='clear_staging_table',
    python_callable=clear_staging_table,
    dag=dag,
)

extract >> transform >> load >> merge >> clear_staging