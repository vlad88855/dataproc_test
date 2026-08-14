from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def main(input_path: str, output_path: str) -> None:
    spark = SparkSession.builder.appName("dataproc-user-behavior-aggregation").getOrCreate()

    # Дані не мають заголовків, тому описуємо схему
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("item_id", IntegerType(), True),
        StructField("category_id", IntegerType(), True),
        StructField("behavior_type", StringType(), True),
        StructField("timestamp", IntegerType(), True)
    ])

    behavior_df = spark.read.csv(input_path, schema=schema, header=False)
    
    # Агрегація: Рахуємо кількість покупок ('buy') по кожній категорії
    result_df = (
        behavior_df.filter(col("behavior_type") == "buy")
        .groupBy("category_id")
        .agg(count("*").alias("total_purchases"))
        .orderBy(col("total_purchases").desc())
    )

    result_df.write.mode("overwrite").parquet(output_path)
    spark.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run Spark aggregation for User Behavior data.")
    parser.add_argument("--input-path", required=True, help="GCS input path for raw CSV data")
    parser.add_argument("--output-path", required=True, help="GCS output path for processed data")
    args = parser.parse_args()

    main(args.input_path, args.output_path)
