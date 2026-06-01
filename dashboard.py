"""
dashboard.py — reads DIRECTLY from HDFS via Spark (Option B)
------------------------------------------------------------
Reads the Parquet written by stream_processor_hdfs.py straight from
hdfs://192.168.0.234:9000/logs/... using a Spark session, converts to pandas,
and renders the same Q1-Q4 dashboard. No local sync step needed.

!!! HOW TO RUN — IMPORTANT !!!
Because this uses Spark, you MUST launch it with spark-submit, NOT
`streamlit run`. Launching with `streamlit run` gives a JVM/JavaPackage error
because the Spark gateway isn't initialized.

    spark-submit dashboard.py

That works because Streamlit's own bootstrap is invoked from inside this file
(see the __main__ guard at the bottom). If spark-submit cannot find streamlit,
use:  python -m streamlit run dashboard.py  ONLY if Spark is import-only — but
the supported path here is spark-submit.

Override the HDFS location with env var if needed:
    set HDFS_BASE=hdfs://192.168.0.234:9000/logs   (Windows)
"""

import os
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

HDFS_BASE = os.environ.get("HDFS_BASE", "hdfs://192.168.0.234:9000/logs")
PROCESSED_DIR = f"{HDFS_BASE}/processed"
ALERTS_DIR = f"{HDFS_BASE}/alerts"


# ----------------------------------------------------------------------
# Spark session (cached for the life of the app — expensive to create).
# cache_resource keeps ONE SparkSession across reruns/fragments.
# ----------------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def get_spark():
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName("LogDashboard")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


# ----------------------------------------------------------------------
# Data loading from HDFS. Short TTL so each refresh re-reads HDFS.
# Returns pandas DataFrames (Spark reads HDFS, .toPandas() brings them local).
# ----------------------------------------------------------------------
@st.cache_data(ttl=8, show_spinner=False)
def load_events() -> pd.DataFrame:
    spark = get_spark()
    try:
        sdf = spark.read.parquet(PROCESSED_DIR)
    except Exception:
        return pd.DataFrame()  # path missing / empty
    if len(sdf.columns) == 0:
        return pd.DataFrame()
    df = sdf.toPandas()
    if df.empty:
        return df
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True, errors="coerce")
    df["is_error"] = df["is_error"].astype(int)
    return df


@st.cache_data(ttl=8, show_spinner=False)
def load_alerts() -> pd.DataFrame:
    spark = get_spark()
    try:
        sdf = spark.read.parquet(ALERTS_DIR)
        return sdf.toPandas()
    except Exception:
        return pd.DataFrame()


def run_app():
    st.set_page_config(page_title="Log Monitor — HDFS Dashboard",
                       page_icon="📊", layout="wide")

    st.sidebar.title("⚙️ Controls")
    st.sidebar.caption(f"Source: {HDFS_BASE}")

    auto = st.sidebar.checkbox("Auto-refresh every 10 s", value=True, key="auto_on")

    if st.sidebar.button("🔄 Refresh data now"):
        load_events.clear()
        load_alerts.clear()
        st.rerun()

    _probe = load_events()
    if _probe.empty:
        st.title("📊 Real-Time Log Monitoring — HDFS Dashboard")
        st.warning(
            f"No data found at `{PROCESSED_DIR}`. "
            "Start the generator and `stream_processor_hdfs.py` first, "
            "let them run for a minute, then click **Refresh data now**."
        )
        st.stop()

    _services = sorted(_probe["service_name"].unique().tolist())
    picked = st.sidebar.multiselect("Filter services", _services, default=_services)
    k = st.sidebar.slider("K-Means clusters (k)", 2, 6, 3)

    st.title("📊 Real-Time Log Monitoring — HDFS Dashboard")
    st.caption("Cluster 01 · Big Data Analytics · reads Parquet directly from HDFS")

    # Auto-refresh: set run_every from the checkbox. When the box is toggled,
    # the main script reruns and re-evaluates this decoration with the new value
    # (10 seconds when checked, None when not), so unchecking genuinely stops it.
    # This avoids st.rerun(scope="fragment"), which isn't allowed on the first
    # fragment pass in all Streamlit versions.
    refresh_interval = 10 if st.session_state.get("auto_on") else None

    @st.fragment(run_every=refresh_interval)
    def render_dashboard():
        load_events.clear()
        load_alerts.clear()
        df = load_events()
        alerts = load_alerts()
        df = df[df["service_name"].isin(picked)]
        st.caption(f"Last loaded: {pd.Timestamp.now().strftime('%H:%M:%S')}  ·  "
                   f"auto-refresh: {'ON' if st.session_state.get('auto_on') else 'OFF'}")

        # ---- Top KPIs ----
        total = len(df)
        errors = int(df["is_error"].sum())
        err_rate = errors / total if total else 0.0
        avg_ms = df["response_time_ms"].mean()
        p99_ms = df["response_time_ms"].quantile(0.99)

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Total events", f"{total:,}")
        c2.metric("Errors", f"{errors:,}")
        c3.metric("Error rate", f"{err_rate:.2%}")
        c4.metric("Avg latency", f"{avg_ms:.1f} ms")
        c5.metric("P99 latency", f"{p99_ms:.1f} ms")

        st.divider()

        # ---- Q1 ----
        st.subheader("Q1 · Per-service summary")
        q1 = (
            df.groupby("service_name")
              .agg(total_events=("service_name", "size"),
                   errors=("is_error", "sum"),
                   avg_resp_ms=("response_time_ms", "mean"),
                   p99_resp_ms=("response_time_ms", lambda s: s.quantile(0.99)))
              .reset_index()
        )
        q1["error_rate"] = (q1["errors"] / q1["total_events"]).round(4)
        q1["avg_resp_ms"] = q1["avg_resp_ms"].round(2)
        q1["p99_resp_ms"] = q1["p99_resp_ms"].round(2)
        q1 = q1.sort_values("error_rate", ascending=False)

        left, right = st.columns([1, 1])
        with left:
            st.dataframe(q1, use_container_width=True, hide_index=True)
        with right:
            fig = px.bar(q1, x="service_name", y="error_rate",
                         color="error_rate", color_continuous_scale="Reds",
                         title="Error rate by service")
            st.plotly_chart(fig, use_container_width=True)

        # ---- Q2 ----
        st.subheader("Q2 · Top noisy hosts (by error count)")
        q2 = (
            df.groupby(["host", "service_name"])
              .agg(events=("host", "size"), errors=("is_error", "sum"))
              .reset_index()
              .sort_values(["errors", "events"], ascending=False)
              .head(10)
        )
        st.dataframe(q2, use_container_width=True, hide_index=True)

        # ---- Q3 ----
        st.subheader("Q3 · Traffic & error trend")
        trend = df.set_index("event_time").sort_index()
        if not trend.empty:
            per_min = (
                trend.resample("1min")
                     .agg(events=("is_error", "size"), errors=("is_error", "sum"),
                          avg_resp_ms=("response_time_ms", "mean"))
                     .reset_index()
            )
            fig2 = px.line(per_min, x="event_time", y=["events", "errors"],
                           markers=True, title="Events vs errors per minute")
            st.plotly_chart(fig2, use_container_width=True)
            fig3 = px.line(per_min, x="event_time", y="avg_resp_ms",
                           markers=True, title="Average latency per minute (ms)")
            st.plotly_chart(fig3, use_container_width=True)

        # ---- Q4 ----
        st.subheader(f"Q4 · K-Means anomaly detection (k={k})")
        try:
            from sklearn.cluster import KMeans
            from sklearn.preprocessing import StandardScaler

            feat = df[["response_time_ms", "status_code"]].dropna().astype(float)
            if len(feat) < k * 5:
                st.info(f"Not enough data yet ({len(feat)} rows) for k={k}.")
            else:
                X = StandardScaler().fit_transform(feat.values)
                km = KMeans(n_clusters=k, random_state=42, n_init=10)
                labels = km.fit_predict(X)
                feat = feat.copy()
                feat["cluster"] = labels
                feat["service_name"] = df.loc[feat.index, "service_name"].values
                feat["log_level"] = df.loc[feat.index, "log_level"].values

                sizes = feat["cluster"].value_counts().sort_values()
                outlier_cluster = int(sizes.index[0])

                ca, cb = st.columns([1, 1])
                with ca:
                    scatter = px.scatter(
                        feat, x="response_time_ms", y="status_code",
                        color=feat["cluster"].astype(str), symbol="service_name",
                        title="Clusters — response time vs status code",
                        labels={"color": "cluster"},
                    )
                    st.plotly_chart(scatter, use_container_width=True)
                with cb:
                    st.markdown("**Cluster sizes** (smallest = likely anomalies)")
                    st.dataframe(sizes.rename("rows").reset_index()
                                 .rename(columns={"index": "cluster"}),
                                 use_container_width=True, hide_index=True)
                    st.markdown(f"**Likely anomaly cluster: `{outlier_cluster}`**")

                st.markdown("**Sample anomaly rows** (highest latency in outlier cluster)")
                out = (feat[feat["cluster"] == outlier_cluster]
                       .sort_values("response_time_ms", ascending=False)
                       .head(15))
                st.dataframe(out, use_container_width=True, hide_index=True)
        except ImportError:
            st.error("scikit-learn not installed. Run: pip install scikit-learn")

        # ---- Alerts ----
        st.divider()
        st.subheader("🚨 Error-spike alerts")
        if alerts.empty:
            st.success("No alerts recorded (no service crossed the error-rate threshold).")
        else:
            show = alerts.copy()
            for c in ("window_start", "window_end", "detected_at"):
                if c in show.columns:
                    show[c] = pd.to_datetime(show[c], errors="coerce")
            st.dataframe(show.sort_values("detected_at", ascending=False)
                         if "detected_at" in show.columns else show,
                         use_container_width=True, hide_index=True)

        st.caption("Reading live from HDFS. Leave the generator + "
                   "stream_processor_hdfs.py running.")

    render_dashboard()


# ----------------------------------------------------------------------
# Bootstrap: when launched via `spark-submit dashboard.py`, this file runs as
# a normal script (not under Streamlit's server), so we start Streamlit's
# bootstrap ourselves pointing back at this file. When already running under
# Streamlit (st has a script run context), we just call run_app().
# ----------------------------------------------------------------------
def _running_under_streamlit() -> bool:
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        return get_script_run_ctx() is not None
    except Exception:
        return False


if __name__ == "__main__":
    if _running_under_streamlit():
        run_app()
    else:
        # Re-launch this file under the Streamlit server.
        from streamlit.web import bootstrap
        bootstrap.run(os.path.abspath(__file__), False, [], {})
else:
    # Imported by Streamlit's runner (e.g. `streamlit run dashboard.py`)
    run_app()
