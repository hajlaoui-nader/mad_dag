from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from operators.csv_processor import CSVProcessorOperator
import os
import subprocess

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,  # IRL, this would be True and would send an email to the data team
    "email_on_retry": False,  # IRL, this would be True and would send an email to the data team
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "process_csv_dag",
    default_args=default_args,
    description="DAG for processing CSV files",
    schedule_interval=timedelta(days=1),
    catchup=False,  # IRL, this would be True and would catch up on any missing runs
    params={"month": "04", "year": "2021"},
)


process_csv_task = CSVProcessorOperator(
    task_id="process_csv",
    dag=dag,
    s3_bucket=os.environ.get("BUCKET", "work-sample-mk"),
    processing_script="scripts/process_csv.py",
)


def transform():
    pass


def call_clean_redshift_table(**kwargs):
    ti = kwargs["ti"]
    # Pull the file path from the process_csv_task
    file_path = ti.xcom_pull(task_ids="process_csv", key="processed_file_path")
    table_name = "events"
    host = "host.docker.internal"
    redshift_conn = os.environ.get(
        "REDSHIFT_URI", f"postgresql+psycopg2://rshift:rshift@{host}:4333/analytics"
    )
    subprocess.run(
        [
            "python",
            "scripts/clean_redshift_table.py",
            table_name,
            redshift_conn,
            file_path,
        ],
        check=True,
    )


def insert_into_redshift_table(**kwargs):
    ti = kwargs["ti"]
    # Pull the file path from the process_csv_task
    file_path = ti.xcom_pull(task_ids="process_csv", key="processed_file_path")
    table_name = "events"
    host = "host.docker.internal"
    redshift_conn = os.environ.get(
        "REDSHIFT_URI", f"postgresql+psycopg2://rshift:rshift@{host}:4333/analytics"
    )
    subprocess.run(
        [
            "python",
            "scripts/upload_to_redshift.py",
            table_name,
            redshift_conn,
            file_path,
        ],
        check=True,
    )


data_transformation = PythonOperator(
    task_id="data_transformation",
    dag=dag,
    python_callable=transform,
    op_kwargs={},
)

clean_table_task = PythonOperator(
    task_id="clean_redshift_table",
    python_callable=call_clean_redshift_table,
    provide_context=True,
    dag=dag,
)

insert_into_redshift = PythonOperator(
    task_id="insert_into_redshift_table",
    python_callable=insert_into_redshift_table,
    provide_context=True,
    dag=dag,
)


def create_dag():
    process_csv_task >> data_transformation >> clean_table_task >> insert_into_redshift


create_dag()
