import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
REGION = os.environ.get("GCP_REGION", "europe-west1")
BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME", "your-gcs-bucket-name")
CLUSTER_NAME = "ephemeral-spark-cluster"

INPUT_PATH = f"gs://{BUCKET_NAME}/raw/nyc_taxi/"
OUTPUT_PATH = f"gs://{BUCKET_NAME}/processed/nyc_taxi_aggregated/"
PYSPARK_URI = f"gs://{BUCKET_NAME}/scripts/spark_aggregation.py"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_URI,
        "args": [INPUT_PATH, OUTPUT_PATH],
    },
}

with DAG(
    'dataproc_poc_pipeline',
    default_args=default_args,
    description='PoC Data Pipeline with Dataproc and PySpark',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['poc', 'dataproc', 'pyspark'],
) as dag:

    load_raw_data = BashOperator(
        task_id='load_raw_data_to_gcs',
        bash_command=f'echo "Simulating upload to {INPUT_PATH}" && sleep 5'
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done",
    )

    load_raw_data >> create_cluster >> submit_pyspark_job >> delete_cluster
