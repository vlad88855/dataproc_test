import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

def main():
    if len(sys.argv) != 3:
        print("Usage: spark_aggregation.py <input_path> <output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder \
        .appName("NYCTaxiAggregation") \
        .getOrCreate()

    df = spark.read.parquet(input_path)

    # Фільтрація успішних транзакцій (payment_type == 1)
    filtered_df = df.filter(df.payment_type == 1)

    # Групування за VendorID та підрахунок суми
    aggregated_df = filtered_df.groupBy("VendorID").agg(
        sum("total_amount").alias("total_sum")
    )

    # Збереження результату
    aggregated_df.write.mode("overwrite").parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    main()
