import argparse
import time
import logging
import psutil
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

def create_spark_session(mode, workers):
    master = f"local[{workers}]" if mode == "distributed" else "local[1]"
    return (SparkSession.builder
        .master(master)
        .appName("FeatureEngineering")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate())

def run_pipeline(spark, input_path, output_path, num_partitions):
    process = psutil.Process(os.getpid())
    start = time.time()

    # 1. Load data
    df = spark.read.parquet(input_path)
    df = df.repartition(num_partitions)
    log.info(f"Loaded data. Partitions: {df.rdd.getNumPartitions()}")

    # 2. Feature engineering — using only columns that actually exist:
    #    user_id (string), amount (float), timestamp (datetime), category (string)

    # Numeric transforms
    df = df.withColumn("log_amount",  F.log1p(F.col("amount")))
    df = df.withColumn("hour_of_day", F.hour(F.col("timestamp")))
    df = df.withColumn("day_of_week", F.dayofweek(F.col("timestamp")))

    # Aggregations per user (triggers shuffle)
    user_stats = df.groupBy("user_id").agg(
        F.mean("amount")   .alias("user_avg_amount"),
        F.stddev("amount") .alias("user_std_amount"),
        F.count("*")       .alias("user_event_count"),
    )
    df = df.join(user_stats, on="user_id", how="left")

    # Encode category
    indexer = StringIndexer(inputCol="category", outputCol="category_idx", handleInvalid="keep")
    df = indexer.fit(df).transform(df)

    # Normalized amount
    df = df.withColumn("normalized_amount",
        (F.col("amount") - F.col("user_avg_amount")) /
        (F.col("user_std_amount") + F.lit(1e-9))
    )

    # 3. Write output
    df.write.mode("overwrite").parquet(output_path)

    elapsed    = time.time() - start
    mem_gb     = process.memory_info().rss / (1024 ** 3)
    partitions = df.rdd.getNumPartitions()

    return {
        "runtime_seconds": round(elapsed, 2),
        "partitions_used": partitions,
        "peak_memory_gb":  round(mem_gb, 3),
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",      required=True)
    parser.add_argument("--output",     required=True)
    parser.add_argument("--mode",       choices=["local", "distributed"], default="local")
    parser.add_argument("--workers",    type=int, default=4)
    parser.add_argument("--partitions", type=int, default=100)
    args = parser.parse_args()

    spark   = create_spark_session(args.mode, args.workers)
    metrics = run_pipeline(spark, args.input, args.output, args.partitions)
    spark.stop()

    print("\n===== METRICS =====")
    for k, v in metrics.items():
        print(f"  {k}: {v}")
    print("===================\n")