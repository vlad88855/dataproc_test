# PoC: Локальна система пакетної обробки даних

## Архітектура

Цей Proof of Concept реалізує локальний оркестратор Apache Airflow, який керує ефемерним кластером Dataproc та обробкою Parquet-даних у GCS.

Компоненти:
- `docker-compose.yaml` — локальний стек з Airflow Webserver, Scheduler та PostgreSQL для метаданих.
- `dags/poc_pipeline.py` — DAG з чотирма кроками: завантаження raw, створення Dataproc-кластера, запуск PySpark-джоба, видалення кластера.
- `scripts/spark_aggregation.py` — PySpark-скрипт для читання CSV з `GCS`, фільтрації покупок (`behavior_type == 'buy'`), агрегації кількості покупок по `category_id` та запису результату у `processed/`.
- `gcp_infrastucture/` — існуючі Terraform-конфігурації для створення GCP сервісного акаунта, GCS бакету, BigQuery датасету та VM.

## Файлова структура

- `docker-compose.yaml`
- `dags/poc_pipeline.py`
- `scripts/spark_aggregation.py`
- `data/` — локальна директорія для вихідного файлу `UserBehavior.csv`
- `gcp_infrastucture/` — Terraform конфігурації

## Розгортання GCP інфраструктури за допомогою Terraform

1. Перейти до каталогу `gcp_infrastucture`:
   ```bash
   cd gcp_infrastucture
   ```
2. Ініціалізувати Terraform:
   ```bash
   terraform init
   ```
3. Перевірити план:
   ```bash
   terraform plan -var="project_id=<GCP_PROJECT_ID>"
   ```
4. Застосувати зміни:
   ```bash
   terraform apply -var="project_id=<GCP_PROJECT_ID>"
   ```

## Налаштування середовища на VM

Оскільки Airflow буде працювати на створеній через Terraform віртуальній машині (`poc-airflow-vm`), вам не потрібно генерувати JSON-ключ — автентифікація відбувається автоматично через прив'язаний до ВМ Service Account (`poc-pipeline-sa`).

1. Підключіться до віртуальної машини по SSH:
   ```bash
   gcloud compute ssh poc-airflow-vm --zone=us-central1-a
   ```
2. Скопіюйте файли вашого проєкту (DAGs, скрипти, docker-compose) на віртуальну машину (наприклад, через `git clone` або `gcloud compute scp`).
3. Створіть файл `.env` у корені проєкту на ВМ з параметрами:
   ```env
   GCP_PROJECT_ID=<YOUR_PROJECT_ID>
   GCP_REGION=us-central1
   GCS_BUCKET=<YOUR_BUCKET_NAME>
   DATAPROC_CLUSTER_NAME=poc-dataproc-cluster
   DATAPROC_ZONE=us-central1-a
   ```
4. Підготуйте локальний файл `data/nyc_taxi.parquet` (він має знаходитись на ВМ у папці `data/`).

## Запуск Airflow на ВМ

Після того, як Terraform завершить роботу, на ВМ автоматично встановиться Docker та Docker Compose (через startup-скрипт).

1. Перейдіть у директорію проєкту на ВМ та запустіть стек:
   ```bash
   docker compose up -d
   ```
2. Щоб отримати доступ до Airflow UI з вашого локального комп'ютера, прокиньте порт через SSH-тунель:
   ```bash
   gcloud compute ssh poc-airflow-vm --zone=us-central1-a -- -L 8080:localhost:8080
   ```
3. Відкрийте Airflow UI у браузері на своєму комп'ютері:
   ```text
   http://localhost:8080
   ```
4. Логін в Airflow:
   - Username: `admin`
   - Password: `admin`

## Запуск DAG

1. Переконатися, що змінні Airflow налаштовані:
   - `GCP_PROJECT_ID`
   - `GCP_REGION`
   - `GCS_BUCKET`
   - `DATAPROC_CLUSTER_NAME`

2. У Airflow UI знайти DAG `poc_dataproc_pipeline`.
3. Запустити DAG вручну.

## Примітки

- `DataprocCreateClusterOperator` створює ефемерний кластер лише на час виконання Spark-джоба.
- `DataprocDeleteClusterOperator` налаштовано з `trigger_rule="all_done"`, щоб гарантувати видалення кластера незалежно від результату джоба.
- Завантаження файлів з локальної системи (`data/`) в GCS виконується за допомогою нативного `LocalFilesystemToGCSOperator` замість `gsutil`.
- Результат обробки зберігається у GCS під шляхом `gs://<GCS_BUCKET>/processed/nyc_taxi_aggregated/`.

## Додаткові кроки

- За потреби можна додати таск для передачі PySpark-скрипту у GCS перед запуском джоба.
- Для стабільної роботи Airflow рекомендується використовувати окремий `Connection` з `google_cloud_default` та відповідний `Service Account`.
