import argparse
import time
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

def create_spark_session(mode):
    if mode == "local":
        master = "local[1]"
    else:
        master = "spark://spark-master:7077"

    return (SparkSession.builder
        .master(master)
        .appName("FeatureEngineering")
        .config("spark.sql.shuffle.partitions", "20")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "512m")
        .getOrCreate())

def run_pipeline(spark, input_path, output_path, num_partitions):
    start = time.time()

    # 1. Load
    df = spark.read.parquet(input_path + "/*.parquet")
    df = df.repartition(num_partitions)
    log.info(f"Partitions: {df.rdd.getNumPartitions()}")

    # 2. Feature engineering
    df = df.withColumn("log_amount",  F.log1p(F.col("amount")))
    df = df.withColumn("hour_of_day", F.hour(F.col("timestamp")))
    df = df.withColumn("day_of_week", F.dayofweek(F.col("timestamp")))

    user_stats = df.groupBy("user_id").agg(
        F.mean("amount")   .alias("user_avg_amount"),
        F.stddev("amount") .alias("user_std_amount"),
        F.count("*")       .alias("user_event_count"),
    )
    df = df.join(user_stats, on="user_id", how="left")

    indexer = StringIndexer(inputCol="category", outputCol="category_idx", handleInvalid="keep")
    df = indexer.fit(df).transform(df)

    df = df.withColumn("normalized_amount",
        (F.col("amount") - F.col("user_avg_amount")) /
        (F.col("user_std_amount") + F.lit(1e-9))
    )

    # 3. Write
    df.write.mode("overwrite").parquet(output_path)

    elapsed = time.time() - start

    return {
        "runtime_seconds": round(elapsed, 2),
        "partitions_used": num_partitions,
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",      required=True)
    parser.add_argument("--output",     required=True)
    parser.add_argument("--mode",       choices=["local", "distributed"], default="local")
    parser.add_argument("--partitions", type=int, default=20)
    args = parser.parse_args()

    spark   = create_spark_session(args.mode)
    metrics = run_pipeline(spark, args.input, args.output, args.partitions)
    spark.stop()

    print("\n===== METRICS =====")
    for k, v in metrics.items():
        print(f"  {k}: {v}")
    print("===================\n")