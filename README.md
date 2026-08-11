# Dataproc Batch Processing PoC

## Архітектура
Proof of Concept (PoC) розподіленої системи пакетної обробки даних з використанням Apache Airflow та Google Cloud Dataproc.

- **Airflow**: Оркестратор розгорнуто локально через `docker-compose` з `LocalExecutor` та PostgreSQL як базою метаданих.
- **Dataproc**: Ефемерний кластер для обчислень, що підіймається тільки на час виконання PySpark-задачі та автоматично видаляється після її завершення.
- **Google Cloud Storage (GCS)**: Сховище для сирих даних, скриптів та результатів обробки.

## Структура проекту
```text
.
├── credentials/                 # Директорія для GCP Service Account Key
├── dags/
│   └── poc_pipeline.py          # DAG для Airflow
├── scripts/
│   └── spark_aggregation.py     # PySpark скрипт для обробки даних
├── docker-compose.yaml          # Конфігурація середовища Airflow
└── README.md
```

## Інструкція з розгортання через Terraform
Переконайтеся, що наявні базові конфігурації Terraform створюють необхідні ресурси:
1. Google Cloud Project.
2. Service Account із правами: `Dataproc Administrator`, `Dataproc Worker`, `Storage Object Admin`.
3. GCS Bucket для зберігання коду і даних.

```bash
terraform init
terraform apply -auto-approve
```

## Конфігурація середовища
1. Створіть директорію для ключів та помістіть туди Service Account JSON-ключ:
   ```bash
   mkdir credentials
   # Помістіть ваш ключ сюди: credentials/gcp-key.json
   ```
2. Завантажте PySpark-скрипт у ваш GCS бакет (шлях, вказаний у Terraform):
   ```bash
   gsutil cp scripts/spark_aggregation.py gs://<YOUR_BUCKET_NAME>/scripts/spark_aggregation.py
   ```
3. Експортуйте змінні середовища (або додайте у Airflow Variables через UI):
   ```bash
   export GCP_PROJECT_ID="ваш-project-id"
   export GCP_REGION="europe-west1"
   export GCP_BUCKET_NAME="ваш-bucket-name"
   ```

## Порядок запуску конвеєра в Airflow
1. Підніміть Airflow-оточення через Docker Compose:
   ```bash
   mkdir -p ./dags ./scripts ./credentials
   echo -e "AIRFLOW_UID=$(id -u)" > .env
   docker-compose up airflow-init
   docker-compose up -d
   ```
2. Відкрийте Airflow UI за адресою [http://localhost:8080](http://localhost:8080) (логін/пароль: `airflow`).
3. Знайдіть у списку DAG `dataproc_poc_pipeline` та увімкніть його (Unpause).
4. Натисніть кнопку **Trigger DAG** для запуску конвеєра.
5. Конвеєр автоматично виконає такі кроки:
   - Створення "raw" директорії/завантаження даних у GCS.
   - Підняття ефемерного Dataproc кластера.
   - Запуск завдання з агрегації на PySpark (фільтрація успішних транзакцій та агрегація сум за VendorID).
   - Збереження результатів у GCS в форматі Parquet.
   - Гарантоване видалення кластера після виконання.
