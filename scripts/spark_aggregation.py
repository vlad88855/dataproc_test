from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def main(input_path: str, output_path: str) -> None:
    # Конфігуруємо сесію, щоб дозволити Spill (скидання на диск при нестачі пам'яті)
    # та встановити кількість партицій для Shuffle.
    spark = SparkSession.builder \
        .appName("dataproc-stress-test") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    # Схема датасету UserBehavior
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("item_id", IntegerType(), True),
        StructField("category_id", IntegerType(), True),
        StructField("behavior_type", StringType(), True),
        StructField("timestamp", IntegerType(), True)
    ])

    # Читаємо сирі дані
    df = spark.read.csv(input_path, schema=schema, header=False)

    # =======================================================================
    # СТРЕС-ТЕСТ ТРАНСФОРМАЦІЇ
    # =======================================================================

    # А. Unbounded Window Function
    # Змушує Spark тримати всі події однієї категорії в RAM одного екзекутора.
    # Відбувається сильний перекіс (Data Skew), оскільки є дуже популярні категорії.
    window_spec = Window.partitionBy("category_id").orderBy("timestamp") \
                        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    df_windowed = df \
        .withColumn("running_max_item", F.max("item_id").over(window_spec)) \
        .withColumn("dense_rank_ts", F.dense_rank().over(window_spec))

    # Б. Складний Shuffle + Тяжкі агрегації
    # collect_set() збирає всі уникальні ID в один масив у пам'яті (Heavy GC).
    # percentile_approx є обчислювально складною функцією.
    df_aggregated = df_windowed.groupBy("category_id", "behavior_type") \
        .agg(
            F.countDistinct("user_id").alias("unique_users"),
            # Використовуємо collect_set, що спричинить значне навантаження на Heap
            F.collect_set("item_id").alias("all_items_array"),
            F.expr("percentile_approx(timestamp, 0.5)").alias("median_ts"),
            F.sum("running_max_item").alias("sum_max_item")
        )

    # В. Self-Join на перетині категорій
    # Генерує величезну кількість пар та змушує Spark виконувати SortMergeJoin Shuffle.
    # Ми джойнимо агреговані дані самі з собою для різних типів поведінки (наприклад, pv та buy).
    df_final = df_aggregated.alias("a").join(
        df_aggregated.alias("b"),
        on=(F.col("a.category_id") == F.col("b.category_id")) & (F.col("a.behavior_type") != F.col("b.behavior_type")),
        how="inner"
    )
    
    # Додаткове глобальне сортування для фінального навантаження (викликає ще один Shuffle)
    df_result = df_final.orderBy(F.col("a.category_id").desc())

    # Тригер виконання (Action) через збереження
    df_result.write.mode("overwrite").parquet(output_path)
    
    spark.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run Stress-test Spark aggregation for User Behavior data.")
    parser.add_argument("--input-path", required=True, help="GCS input path for raw CSV data")
    parser.add_argument("--output-path", required=True, help="GCS output path for processed data")
    args = parser.parse_args()

    main(args.input_path, args.output_path)
