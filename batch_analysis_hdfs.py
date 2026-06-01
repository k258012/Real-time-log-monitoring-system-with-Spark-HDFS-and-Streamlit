import os
import argparse
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# from dotenv import load_dotenv

# load_dotenv()

# HDFS_IP = os.getenv("HDFS_IP")
# HDFS_PORT = os.getenv("HDFS_PORT", "9000")
HDFS_IP = "192.168.0.234"
HDFS_PORT = "9000"

def build_spark():
    return (
        SparkSession.builder
        .appName("LogBatchAnalysis")
        .config("spark.sql.shuffle.partitions", "12")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def q1_service_summary(df):
    res = (
        df.groupBy("service_name")
          .agg(
              F.count("*").alias("total_events"),
              F.sum("is_error").alias("errors"),
              F.round(F.avg("response_time_ms"), 2).alias("avg_resp_ms"),
              F.round(F.expr("percentile_approx(response_time_ms, 0.99)"), 2)
                  .alias("p99_resp_ms"),
          )
          .withColumn("error_rate",
                      F.round(F.col("errors") / F.col("total_events"), 4))
          .orderBy(F.desc("error_rate"))
    )
    print("\n=== Q1. Per-service summary ===")
    res.show(truncate=False)
    return res


def q2_noisy_hosts(df):
    res = (
        df.groupBy("host", "service_name")
          .agg(F.count("*").alias("events"), F.sum("is_error").alias("errors"))
          .orderBy(F.desc("errors"), F.desc("events"))
    )
    print("\n=== Q2. Top 10 hosts by error count ===")
    res.show(10, truncate=False)
    return res.limit(10)


def q3_hourly_trend(df):
    res = (
        df.withColumn("hour", F.date_format("event_time", "yyyy-MM-dd HH:00"))
          .groupBy("hour")
          .agg(
              F.count("*").alias("events"),
              F.sum("is_error").alias("errors"),
              F.round(F.avg("response_time_ms"), 2).alias("avg_resp_ms"),
          )
          .orderBy("hour")
    )
    print("\n=== Q3. Hourly trend ===")
    res.show(50, truncate=False)
    return res


def q4_kmeans_anomalies(df, k: int):
    print(f"\n=== Q4. K-Means anomaly detection (k={k}) ===")
    feat_df = (
        df.select(
            F.col("response_time_ms"),
            F.col("status_code").cast(DoubleType()).alias("status_code_d"),
            "service_name", "log_level", "host", "event_time",
        ).na.drop()
    )

    n = feat_df.count()
    if n < k * 5:
        print(f"  Not enough data yet ({n} rows). Let the stream run longer.")
        return None

    assembler = VectorAssembler(
        inputCols=["response_time_ms", "status_code_d"], outputCol="features_raw")
    vec = assembler.transform(feat_df)

    scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                            withMean=True, withStd=True)
    scaled = scaler.fit(vec).transform(vec)

    model = KMeans(k=k, seed=42, featuresCol="features",
                   predictionCol="cluster").fit(scaled)
    predicted = model.transform(scaled)

    print("Cluster centroids (scaled space):")
    for i, c in enumerate(model.clusterCenters()):
        print(f"  cluster {i}: [{c[0]:+.3f}, {c[1]:+.3f}]")

    sizes = predicted.groupBy("cluster").count().orderBy("count")
    sizes_rows = sizes.collect()
    print("\nCluster sizes (ascending):")
    for row in sizes_rows:
        print(f"  cluster {row['cluster']}: {row['count']} rows")

    outlier_cluster = sizes_rows[0]["cluster"]
    print(f"\nLikely anomaly cluster = {outlier_cluster}. Sample members:")
    anomalies = (
        predicted
        .filter(F.col("cluster") == outlier_cluster)
        .select("event_time", "service_name", "log_level",
                "response_time_ms", "status_code_d", "host", "cluster")
        .orderBy(F.desc("response_time_ms"))
    )
    anomalies.show(15, truncate=False)
    return anomalies


def maybe_write(df_result, report_out, name):
    """Write a result DataFrame back to HDFS as Parquet (overwrite)."""
    if df_result is None or report_out is None:
        return
    path = f"{report_out.rstrip('/')}/{name}"
    (df_result.coalesce(1)
              .write.mode("overwrite")
              .parquet(path))
    print(f"  -> wrote {name} report to {path}")


def main():
    parser = argparse.ArgumentParser(description="Batch analysis on HDFS logs")
    parser.add_argument("--parquet-dir",
                        default=f"hdfs://{HDFS_IP}:{HDFS_PORT}/logs/processed")
    parser.add_argument("--report-out",
                        default=f"hdfs://{HDFS_IP}:{HDFS_PORT}/logs/reports",
                        help="Where to write summary Parquet ('none' to skip)")
    parser.add_argument("--k", type=int, default=3)
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(args.parquet_dir)
    total = df.count()
    print(f"\nLoaded {total} events from {args.parquet_dir}")
    if total == 0:
        print("No data yet. Run the generator + streaming job first.")
        spark.stop()
        return

    report_out = None if args.report_out.lower() == "none" else args.report_out

    r1 = q1_service_summary(df)
    r2 = q2_noisy_hosts(df)
    r3 = q3_hourly_trend(df)
    r4 = q4_kmeans_anomalies(df, args.k)

    maybe_write(r1, report_out, "q1_service_summary")
    maybe_write(r2, report_out, "q2_noisy_hosts")
    maybe_write(r3, report_out, "q3_hourly_trend")
    maybe_write(r4, report_out, "q4_anomalies")

    spark.stop()


if __name__ == "__main__":
    main()
