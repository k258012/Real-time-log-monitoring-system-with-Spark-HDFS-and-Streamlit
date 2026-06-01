import argparse
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from dotenv import load_dotenv

load_dotenv()

HDFS_IP = os.getenv("HDFS_IP")
HDFS_PORT = os.getenv("HDFS_PORT", "9000")



SERVICES = {
    "auth-service":     {"mean_ms": 45,  "ok_codes": [200, 200, 200, 401]},
    "api-gateway":      {"mean_ms": 80,  "ok_codes": [200, 200, 200, 404]},
    "database-service": {"mean_ms": 25,  "ok_codes": [200, 200, 200, 200]},
    "payments-service": {"mean_ms": 120, "ok_codes": [200, 200, 402]},
    "orders-service":   {"mean_ms": 60,  "ok_codes": [200, 201, 200]},
}

NORMAL_MESSAGES = [
    "Request processed", "Cache hit", "Query executed",
    "Token validated", "Payment authorized", "Order placed",
]
ANOMALY_MESSAGES = [
    "Upstream timeout", "DB connection refused", "Token verification failed",
    "Card declined", "Out of stock", "Internal server error",
]


# --- locate the hdfs executable in a cross-platform way -------------------
# On Windows the launcher is `hdfs.cmd`; on Linux/Mac it's `hdfs`.
# shutil.which() checks PATHEXT, so it finds hdfs.cmd on Windows too.
def find_hdfs() -> str:
    exe = shutil.which("hdfs") or shutil.which("hdfs.cmd")
    if exe:
        return exe
    # Fall back to $HADOOP_HOME/bin/hdfs[.cmd] if PATH didn't have it.
    hadoop_home = os.environ.get("HADOOP_HOME")
    if hadoop_home:
        for cand in ("hdfs.cmd", "hdfs"):
            p = os.path.join(hadoop_home, "bin", cand)
            if os.path.isfile(p):
                return p
    return ""


HDFS_BIN = None  # set in main() after the preflight check


def hdfs(*args) -> None:
    """Run an `hdfs dfs` command, raising on failure."""
    subprocess.run([HDFS_BIN, "dfs", *args], check=True)


def ensure_hdfs_dir(hdfs_dir: str) -> None:
    subprocess.run([HDFS_BIN, "dfs", "-mkdir", "-p", hdfs_dir], check=True)


def make_event(service: str, faker: Faker, force_anomaly: bool = False) -> dict:
    profile = SERVICES[service]
    if force_anomaly:
        response_time = max(1.0, random.gauss(profile["mean_ms"] * 15, 300))
        status = random.choice([500, 502, 503, 504])
        level = random.choice(["ERROR", "ERROR", "CRITICAL"])
        message = random.choice(ANOMALY_MESSAGES)
    else:
        response_time = max(1.0, random.gauss(profile["mean_ms"], profile["mean_ms"] * 0.3))
        status = random.choice(profile["ok_codes"])
        level = "INFO" if random.random() < 0.95 else "WARN"
        message = random.choice(NORMAL_MESSAGES)

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service_name": service,
        "log_level": level,
        "message": message,
        "response_time_ms": round(response_time, 2),
        "status_code": status,
        "host": f"{service}-{random.randint(1, 3):02d}.local",
    }


def write_batch_hdfs(hdfs_dir: str, events: list) -> str:
    """Stage locally -> put to HDFS as .tmp -> atomic -mv to .jsonl."""
    ts_ms = int(time.time() * 1000)
    base = f"log_{ts_ms}_{uuid.uuid4().hex[:6]}"
    final = f"{hdfs_dir.rstrip('/')}/{base}.jsonl"
    tmp_hdfs = f"{hdfs_dir.rstrip('/')}/{base}.tmp"

    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as fh:
        local_tmp = fh.name
        for e in events:
            fh.write(json.dumps(e) + "\n")

    try:
        hdfs("-put", "-f", local_tmp, tmp_hdfs)
        hdfs("-mv", tmp_hdfs, final)
    finally:
        try:
            os.remove(local_tmp)
        except OSError:
            pass
    return final


def main():
    parser = argparse.ArgumentParser(description="JSON log generator -> HDFS")
    parser.add_argument("--hdfs-dir",
                        default=f"hdfs://{HDFS_IP}:{HDFS_PORT}/logs/raw_logs",
                        help="HDFS directory Spark watches")
    parser.add_argument("--rate", type=int, default=3000,
                        help="Events per minute")
    parser.add_argument("--anomaly-prob", type=float, default=0.03)
    parser.add_argument("--batch-interval", type=float, default=1.0,
                        help="Seconds between files (>= 1.0 recommended)")
    parser.add_argument("--duration", type=int, default=0,
                        help="Stop after N seconds (0 = until Ctrl+C)")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    # ---- PREFLIGHT: make sure hdfs exists, with a clear message ----------
    global HDFS_BIN
    HDFS_BIN = find_hdfs()
    if not HDFS_BIN:
        print("ERROR: the 'hdfs' command was not found on this machine.\n", file=sys.stderr)
        print("not on a laptop without Hadoop installed.\n", file=sys.stderr)
        print("Checklist:", file=sys.stderr)
        print("  1. Are you on the machine that runs HDFS? (prompt should NOT be H:\\...)", file=sys.stderr)
        print("  2. Does 'hdfs version' work in this same terminal?", file=sys.stderr)
        print("  3. If not, add Hadoop's bin folder to PATH, or set HADOOP_HOME.", file=sys.stderr)
        sys.exit(1)

    print(f"Using hdfs at: {HDFS_BIN}")

    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)

    try:
        ensure_hdfs_dir(args.hdfs_dir)
    except subprocess.CalledProcessError as e:
        print(f"\nERROR: 'hdfs dfs -mkdir -p {args.hdfs_dir}' failed (exit {e.returncode}).", file=sys.stderr)
        print("Check that HDFS is running and the address/port is correct:", file=sys.stderr)
        print("  hdfs getconf -confKey fs.defaultFS", file=sys.stderr)
        sys.exit(1)

    faker = Faker()
    rps = args.rate / 60.0
    per_batch = max(1, int(round(rps * args.batch_interval)))
    services = list(SERVICES.keys())

    print(f"Writing logs to HDFS: {args.hdfs_dir}")
    print(f"Rate: {args.rate} events/min  (~{per_batch} per {args.batch_interval}s file)")
    print(f"Anomaly probability: {args.anomaly_prob}")
    print("Press Ctrl+C to stop.\n")

    start = time.time()
    total = 0
    try:
        while True:
            if args.duration and (time.time() - start) >= args.duration:
                break
            batch = []
            for _ in range(per_batch):
                svc = random.choice(services)
                anomaly = random.random() < args.anomaly_prob
                batch.append(make_event(svc, faker, force_anomaly=anomaly))
            final = write_batch_hdfs(args.hdfs_dir, batch)
            total += len(batch)
            print(f"  wrote {final}  ({len(batch)} events, total={total})")
            time.sleep(args.batch_interval)
    except KeyboardInterrupt:
        pass

    elapsed = time.time() - start
    print(f"\nDone. {total} events in {elapsed:.1f}s.")


if __name__ == "__main__":
    main()
