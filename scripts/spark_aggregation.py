from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def main(input_path: str, output_path: str) -> None:
    spark = SparkSession.builder \
        .appName("dataproc-stress-test") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("item_id", IntegerType(), True),
        StructField("category_id", IntegerType(), True),
        StructField("behavior_type", StringType(), True),
        StructField("timestamp", IntegerType(), True)
    ])

    df = spark.read.csv(input_path, schema=schema, header=False)
    df = df.withColumn("timestamp", F.to_timestamp(F.from_unixtime(F.col("timestamp"))))

    window_spec_unbounded = Window.partitionBy("category_id").orderBy("timestamp") \
                        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    window_spec_rank = Window.partitionBy("category_id").orderBy("timestamp")

    df_windowed = df \
        .withColumn("running_max_item", F.max("item_id").over(window_spec_unbounded)) \
        .withColumn("dense_rank_ts", F.dense_rank().over(window_spec_rank))

    df_aggregated = df_windowed.groupBy("category_id", "behavior_type") \
        .agg(
            F.countDistinct("user_id").alias("unique_users"),
            F.collect_set("item_id").alias("all_items_array"),
            F.expr("percentile_approx(timestamp, 0.5)").alias("median_ts"),
            F.sum("running_max_item").alias("sum_max_item")
        )

    df_final = df_aggregated.alias("a").join(
        df_aggregated.alias("b"),
        on=(F.col("a.category_id") == F.col("b.category_id")) & (F.col("a.behavior_type") != F.col("b.behavior_type")),
        how="inner"
    ).select(
    F.col("a.category_id"),
    F.col("a.behavior_type").alias("behavior_type_a"),
    F.col("b.behavior_type").alias("behavior_type_b"),
    F.col("a.all_items_array").alias("all_items_array_a"),
    F.col("b.all_items_array").alias("all_items_array_b"),
    F.col("a.unique_users").alias("unique_users_a"),
    F.col("b.unique_users").alias("unique_users_b")
)
    
    df_result = df_final.orderBy(F.col("a.category_id").desc())

    df_result.write.mode("overwrite").parquet(output_path)

    # df_result = df_windowed.orderBy(F.col("category_id").desc())

    # df_result.write.mode("overwrite").parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run Stress-test Spark aggregation for User Behavior data.")
    parser.add_argument("--input-path", required=True, help="GCS input path for raw CSV data")
    parser.add_argument("--output-path", required=True, help="GCS output path for processed data")
    args = parser.parse_args()

    main(args.input_path, args.output_path)
