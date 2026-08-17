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
        from google.cloud import secretmanager
        from google.cloud import storage
        from stream_unzip import stream_unzip
        
        project_id = Variable.get("GCP_PROJECT_ID")
        bucket_name = Variable.get("GCS_BUCKET")
        
        # 1. Fetch Kaggle credentials from Secret Manager
        sm_client = secretmanager.SecretManagerServiceClient()
        name_username = f"projects/{project_id}/secrets/KAGGLE_USERNAME/versions/latest"
        name_key = f"projects/{project_id}/secrets/KAGGLE_KEY/versions/latest"
        
        kaggle_username = sm_client.access_secret_version(request={"name": name_username}).payload.data.decode("UTF-8").strip()
        kaggle_key = sm_client.access_secret_version(request={"name": name_key}).payload.data.decode("UTF-8").strip()
        
        # 2. Setup streaming from Kaggle
        # API URL for downloading dataset
        dataset_url = "https://www.kaggle.com/api/v1/datasets/download/marwa80/userbehavior"
        
        # Basic Auth is used by Kaggle API
        auth = (kaggle_username, kaggle_key)
        
        # A generator to fetch chunks from Kaggle
        def get_zipped_chunks():
            with requests.get(dataset_url, auth=auth, stream=True) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=65536):
                    yield chunk

        # 3. Custom file-like object to wrap the unzipped chunks generator for GCS upload
        class IterableStream:
            def __init__(self, iterator):
                self.iterator = iterator
                self.buffer = b''

            def read(self, size=-1):
                if size == -1:
                    res = self.buffer + b''.join(self.iterator)
                    self.buffer = b''
                    return res
                
                while len(self.buffer) < size:
                    try:
                        chunk = next(self.iterator)
                        self.buffer += chunk
                    except StopIteration:
                        break
                
                res = self.buffer[:size]
                self.buffer = self.buffer[size:]
                return res

        # 4. Stream to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        print("Starting stream from Kaggle -> stream-unzip -> GCS...")
        # stream_unzip yields tuples: (file_name, file_size, unzipped_chunks)
        for file_name, file_size, unzipped_chunks in stream_unzip(get_zipped_chunks()):
            file_name_str = file_name.decode('utf-8')
            
            # We are interested in the CSV file
            if file_name_str.endswith('.csv'):
                print(f"Found CSV file in stream: {file_name_str}")
                
                # Wrap the unzipped chunks generator in our file-like object
                stream = IterableStream(unzipped_chunks)
                
                destination_blob_name = f"raw/{file_name_str}"
                blob = bucket.blob(destination_blob_name)
                
                print(f"Uploading unzipped stream to gs://{bucket_name}/{destination_blob_name}...")
                
                # upload_from_file reads from the stream on the fly and uploads in chunks
                blob.upload_from_file(stream, content_type='text/csv')
                
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
            "reference": {"job_id": "poc-spark-aggregation-{{ ds_nodash }}"},
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
