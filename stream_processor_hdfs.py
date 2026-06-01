"""
stream_processor_hdfs.py
------------------------
PySpark Structured Streaming pipeline — HDFS version.

All paths are hdfs://192.168.0.234:9000/... URIs.

FIX (checkpoint bug): EVERY streaming query — including the console sink —
must have an explicit checkpointLocation on HDFS. If the console sink has no
checkpoint, Spark auto-creates a TEMP checkpoint on the local C: drive and
then tries to use that Windows path inside HDFS, producing an invalid URI like
  hdfs://192.168.0.234:9000/C:/Users/.../Temp/temporary-...
and crashes with "Invalid path name". Giving each query its own HDFS
checkpoint avoids this.

Run (local Spark talking to remote HDFS):
    spark-submit stream_processor_hdfs.py

Run on YARN (if a ResourceManager is reachable):
    spark-submit --master yarn --deploy-mode client stream_processor_hdfs.py
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)


LOG_SCHEMA = StructType([
    StructField("timestamp",        StringType(),  nullable=False),
    StructField("service_name",     StringType(),  nullable=False),
    StructField("log_level",        StringType(),  nullable=False),
    StructField("message",          StringType(),  nullable=True),
    StructField("response_time_ms", DoubleType(),  nullable=False),
    StructField("status_code",      IntegerType(), nullable=False),
    StructField("host",             StringType(),  nullable=False),
])


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "12")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.schemaInference", "false")
        # If any temp checkpoint ever gets made, let Spark delete it cleanly
        # instead of erroring out on Windows.
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )


def main():
    parser = argparse.ArgumentParser(description="PySpark log stream processor (HDFS)")
    parser.add_argument("--input-dir",     default="hdfs://192.168.0.234:9000/logs/raw_logs")
    parser.add_argument("--parquet-out",   default="hdfs://192.168.0.234:9000/logs/processed")
    parser.add_argument("--alerts-out",    default="hdfs://192.168.0.234:9000/logs/alerts")
    parser.add_argument("--checkpoint",    default="hdfs://192.168.0.234:9000/logs/checkpoints")
    parser.add_argument("--error-threshold", type=float, default=0.30)
    parser.add_argument("--trigger-seconds", type=int, default=2)
    args = parser.parse_args()

    spark = build_spark("RealTimeLogMonitor")
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .schema(LOG_SCHEMA)
        .option("maxFilesPerTrigger", 20)
        .json(args.input_dir)
    )

    events = (
        raw
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn("date",       F.to_date("event_time"))
        .withColumn("is_error",
                    F.when(F.col("log_level").isin("ERROR", "CRITICAL"), 1).otherwise(0))
    )

    # ---- SINK 1: persist raw events to Parquet on HDFS, partitioned ----
    parquet_query = (
        events.writeStream
        .format("parquet")
        .option("path", args.parquet_out)
        .option("checkpointLocation", f"{args.checkpoint}/parquet")
        .partitionBy("date", "service_name")
        .outputMode("append")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .queryName("parquet_sink")
        .start()
    )

    windowed = (
        events
        .withWatermark("event_time", "1 minute")
        .groupBy(F.window("event_time", "1 minute"), F.col("service_name"))
        .agg(
            F.count("*").alias("events"),
            F.sum("is_error").alias("errors"),
            F.round(F.avg("response_time_ms"), 2).alias("avg_resp_ms"),
            F.round(F.expr("percentile_approx(response_time_ms, 0.99)"), 2)
                .alias("p99_resp_ms"),
        )
        .withColumn(
            "error_rate",
            F.round(F.col("errors") / F.col("events"), 4)
        )
    )

    # ---- SINK 2: console metrics --------------------------------------
    # FIX: give the console sink its OWN explicit HDFS checkpoint so Spark
    # does not auto-create a local C:\ temp checkpoint (the cause of the
    # "Invalid path name ... hdfs://.../C:/Users/..." crash).
    console_query = (
        windowed
        .select(
            F.col("window.start").alias("window_start"),
            F.col("service_name"),
            F.col("events"),
            F.col("errors"),
            F.col("error_rate"),
            F.col("avg_resp_ms"),
            F.col("p99_resp_ms"),
        )
        .writeStream
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        .option("checkpointLocation", f"{args.checkpoint}/console")
        .outputMode("update")
        .trigger(processingTime="10 seconds")
        .queryName("console_metrics")
        .start()
    )

    # ---- SINK 3: error-rate spike alerts to HDFS ----------------------
    alerts = (
        windowed
        .filter(F.col("error_rate") >= args.error_threshold)
        .select(
            F.lit("error_spike").alias("alert_type"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("service_name"),
            F.col("events"),
            F.col("errors"),
            F.col("error_rate"),
            F.col("avg_resp_ms"),
            F.col("p99_resp_ms"),
            F.current_timestamp().alias("detected_at"),
        )
    )

    alerts_query = (
        alerts.writeStream
        .format("parquet")
        .option("path", args.alerts_out)
        .option("checkpointLocation", f"{args.checkpoint}/alerts")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .queryName("alerts_sink")
        .start()
    )

    print("=" * 70)
    print("Streaming queries started (HDFS). Press Ctrl+C to stop.")
    print(f"  Reading from : {args.input_dir}")
    print(f"  Parquet out  : {args.parquet_out}")
    print(f"  Alerts out   : {args.alerts_out}")
    print(f"  Checkpoints  : {args.checkpoint}")
    print(f"  Alert if error_rate >= {args.error_threshold}")
    print("=" * 70)

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStopping streaming queries...")
        for q in (parquet_query, console_query, alerts_query):
            try:
                q.stop()
            except Exception:
                pass
        spark.stop()


if __name__ == "__main__":
    main()