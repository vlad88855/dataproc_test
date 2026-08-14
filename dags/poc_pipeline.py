from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule


build_cluster_config = {
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "e2-standard-2",
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "e2-standard-2",
            "is_preemptible": True,
        },
        "software_config": {
            "image_version": "2.2-debian12",
        },
    }


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="poc_dataproc_pipeline",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["poc", "dataproc", "spark"],
) as dag:
    upload_raw = LocalFilesystemToGCSOperator(
        task_id="upload_raw_to_gcs",
        src="/opt/airflow/data/UserBehavior.csv",
        dst="raw/UserBehavior.csv",
        bucket="{{ var.value.GCS_BUCKET }}",
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id="{{ var.value.GCP_PROJECT_ID }}",
        region="{{ var.value.GCP_REGION }}",
        cluster_name="{{ var.value.DATAPROC_CLUSTER_NAME }}",
        cluster_config=build_cluster_config,
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        project_id="{{ var.value.GCP_PROJECT_ID }}",
        region="{{ var.value.GCP_REGION }}",
        job={
            "reference": {"job_id": "poc-spark-aggregation-{{ ds_nodash }}"},
            "placement": {"cluster_name": "{{ var.value.DATAPROC_CLUSTER_NAME }}"},
            "pyspark_job": {
                "main_python_file_uri": (
                    "gs://{{ var.value.GCS_BUCKET }}/scripts/spark_aggregation.py"
                ),
                "args": [
                    "--input-path",
                    "gs://{{ var.value.GCS_BUCKET }}/raw/UserBehavior.csv",
                    "--output-path",
                    "gs://{{ var.value.GCS_BUCKET }}/processed/user_behavior_aggregated/",
                ],
            },
        },
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id="{{ var.value.GCP_PROJECT_ID }}",
        region="{{ var.value.GCP_REGION }}",
        cluster_name="{{ var.value.DATAPROC_CLUSTER_NAME }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    upload_raw >> create_cluster >> submit_job >> delete_cluster
