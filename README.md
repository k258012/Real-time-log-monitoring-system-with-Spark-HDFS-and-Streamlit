# Real-time-log-monitoring-system-with-Spark-HDFS-and-Streamlit
Real-time log monitoring and anomaly detection pipeline using Apache Spark Structured Streaming, HDFS, and Spark MLlib, with a live Streamlit dashboard.


# HDFS + YARN Cluster Mode — Setup & Run Guide

This is the cluster version of the pipeline. The local version
(`log_generator.py`, `stream_processor.py`, `batch_analysis.py`) still works
and is your tested fallback. The `_hdfs` files below are **unverified** — they
have not been run against a real cluster, so test them on your own cluster.

## Prerequisites (on the cluster)

- Hadoop 3.x running: HDFS (NameNode + DataNodes) and YARN (ResourceManager + NodeManagers)
- Spark 3.5.x installed with the YARN client config
- `HADOOP_CONF_DIR` exported (points at the dir with `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`)
- `hdfs` and `spark-submit` on PATH
- Python 3.8+ and `faker` on the node that runs the generator

Verify HDFS and YARN are reachable:

```bash
hdfs dfs -ls /
yarn node -list
```

## 1. Create the HDFS directory layout

```bash
hdfs dfs -mkdir -p /logs/raw_logs
hdfs dfs -mkdir -p /logs/processed
hdfs dfs -mkdir -p /logs/alerts
hdfs dfs -mkdir -p /logs/checkpoints
hdfs dfs -mkdir -p /logs/reports
hdfs dfs -ls /logs
```

## 2. Start the streaming job on YARN (Node 1 / any client node)

```bash
spark-submit .\stream_processor_hdfs.py
```

Leave this running. It prints 1-minute rolling metrics to the console.

## 3. Start the log generator (Node 4 — the proposal's Log Generator node)

In a second terminal:

```bash
python log_generator_hdfs.py --rate 1200 --batch-interval 1
```

For a quick test instead of full load:

```bash
python log_generator_hdfs.py --hdfs-dir hdfs://IP:PORT/logs/raw_logs --rate 3000 --duration 120
```

## 4. Run batch analysis on the cluster (after a few minutes of data)

```bash
spark-submit .\batch_analysis_hdfs.py
```

This prints Q1–Q4 and writes summary Parquet under `hdfs://IP:PORT/logs/reports`.

Check it landed:

```bash
hdfs dfs -ls -R /logs/reports
```

## 5. Dashboard (stays local, pandas)


```bash
python -m streamlit run .\dashboard.py
```

The dashboard's own auto-refresh (every 10s) will then reflect whatever the
sync script has pulled into `./data`.

## Useful operational commands

```bash
# Watch YARN apps
yarn application -list

# Kill a stuck app
yarn application -kill <applicationId>

# Driver logs (if you used --deploy-mode cluster)
yarn logs -applicationId <applicationId>

# How much data has accumulated
hdfs dfs -du -h /logs

# Inspect a few raw log files
hdfs dfs -ls /logs/raw_logs | head
hdfs dfs -cat /logs/raw_logs/$(hdfs dfs -ls /logs/raw_logs | awk 'NR==2{print $8}' | xargs basename) | head
```

## Fault-injection test (matches your proposal's success criteria)

```bash
# kill a Spark executor / NodeManager and confirm the stream recovers
yarn application -list                 # find the app
# (stop a NodeManager on one DataNode, watch the job continue)

# kill and restart the streaming driver — checkpoints on HDFS let it resume
# from where it left off without data loss.
```

## Notes / caveats

- **Checkpoints on HDFS are mandatory** in cluster mode — local checkpoint
  paths won't be reachable from executors on other nodes.
- **Many tiny files** hurt HDFS. The generator's `--batch-interval 1.0` with a
  high `--rate` keeps files reasonably sized. For production you'd batch larger.
- The generator shells out to the `hdfs` CLI for each file (`-put` then `-mv`).
  That's simple and correct but not the fastest possible path; for very high
  rates a real Kafka producer (the original proposal's design) scales better.
- All `_hdfs` files are **untested against a live cluster**. Run a small
  `--rate 3000 --duration 120` smoke test before a full-load demo.
