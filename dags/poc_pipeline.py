from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
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
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "software_config": {
        "properties": {
            "spark:spark.driver.memory": "5g",
            "spark:spark.driver.memoryOverhead": "512m",
            
            "spark:spark.executor.memory": "5g",
            "spark:spark.executor.memoryOverhead": "512m",
            
            "spark:spark.executor.cores": "2",
        }
    },
    "gce_cluster_config": {
        "zone_uri": "{{ var.value.DATAPROC_ZONE }}",
        "service_account": "{{ var.value.DATAPROC_SA }}",
        "service_account_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
    }
}

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="poc_dataproc_pipeline",
    default_args=default_args,
    schedule=None,  
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["poc", "dataproc", "spark"],
)
def poc_dataproc_pipeline():
    
    @task
    def stream_kaggle_to_gcs():
        import os
        import base64
        import requests
        from airflow.models import Variable
        from google.cloud import storage
        from stream_unzip import stream_unzip
        
        bucket_name = Variable.get("GCS_BUCKET")
        kaggle_username = Variable.get("KAGGLE_USERNAME")
        kaggle_key = Variable.get("KAGGLE_KEY")
        
        dataset_url = "https://www.kaggle.com/api/v1/datasets/download/marwa80/userbehavior"
        
        auth = (kaggle_username, kaggle_key)
        
        def get_zipped_chunks():
            with requests.get(dataset_url, auth=auth, stream=True) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=65536):
                    yield chunk

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        print("Starting stream from Kaggle -> stream-unzip -> GCS...")
        for file_name, file_size, unzipped_chunks in stream_unzip(get_zipped_chunks()):
            file_name_str = file_name.decode('utf-8')
            
            if file_name_str.endswith('.csv'):
                print(f"Found CSV file in stream: {file_name_str}")
                
                destination_blob_name = f"raw/{file_name_str}"
                blob = bucket.blob(destination_blob_name)
                if blob.exists():
                    print(f"File {destination_blob_name} already exists. Skipping upload.")
                    return f"gs://{bucket_name}/{destination_blob_name}"
                
                print(f"Uploading unzipped stream to gs://{bucket_name}/{destination_blob_name}...")
                
                # Native GCS streaming upload without keeping the whole file in memory
                # blob.open("wb") returns a file-like object that streams chunks directly to GCS via Resumable Upload
                with blob.open("wb") as gcs_file:
                    for chunk in unzipped_chunks:
                        gcs_file.write(chunk)
                
                print("Upload completed successfully.")
                
                # Update the variable to pass it to the Dataproc job
                return f"gs://{bucket_name}/{destination_blob_name}"
                
        raise FileNotFoundError("No CSV file was found in the Kaggle ZIP stream.")

    stream_upload_task = stream_kaggle_to_gcs()

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
            "placement": {"cluster_name": "{{ var.value.DATAPROC_CLUSTER_NAME }}"},
            "pyspark_job": {
                "main_python_file_uri": (
                    "gs://{{ var.value.GCS_BUCKET }}/scripts/spark_aggregation.py"
                ),
                "args": [
                    "--input-path",
                    "{{ ti.xcom_pull(task_ids='stream_kaggle_to_gcs') }}",
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

    # Set dependencies
    stream_upload_task >> create_cluster >> submit_job >> delete_cluster

# Instantiate the DAG
dag = poc_dataproc_pipeline()
