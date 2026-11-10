"""
Reset and Re-seed J.A.R.V.I.S. Demo Database.

1. Wipes any existing jarvis.db (and WAL/SHM files) and cleans up leftover test uploads.
2. Re-initializes clean SQLite schema.
3. Seeds all 4 demo datasets (Microservices, Kubernetes, Streaming, LogHub HDFS).
4. Executes the analysis pipeline on each demo dataset idempotently (yielding exactly 1 incident per dataset = 4 incidents).
5. Trains baseline Isolation Forest models and runs benchmark evaluation against ground truth.
"""

from __future__ import annotations

import glob
import json
import os
import sys
import time

# Ensure backend and repo root are in python path
repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
backend_dir = os.path.join(repo_root, "backend")
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from app.core.config import settings
from app.database.db import db
from app.database.repository import Repository
from app.ingestion.processor import process_dataset
from app.intelligence.orchestrator import AnalysisOrchestrator
from app.intelligence.model_trainer import ModelTrainer


def reset_demo_database():
    print("=" * 70)
    print("J.A.R.V.I.S. - Demo Database Reset and Deterministic Seeding")
    print("=" * 70)

    db_path = settings.DB_PATH
    print(f"Target Database: {db_path}")

    # 1. Remove existing jarvis.db and SQLite temporary files
    for suffix in ["", "-wal", "-shm", "-journal"]:
        f_path = db_path + suffix
        if os.path.exists(f_path):
            try:
                os.remove(f_path)
                print(f"  [DELETED] Removed {os.path.basename(f_path)}")
            except Exception as e:
                print(f"  [WARN] Could not remove {f_path}: {e}")

    # 2. Clean up leftover test uploads
    upload_dir = settings.UPLOAD_DIR
    if os.path.exists(upload_dir):
        leftover_files = glob.glob(os.path.join(upload_dir, "*test_custom_metrics*"))
        for f in leftover_files:
            try:
                os.remove(f)
                print(f"  [CLEANED] Removed leftover upload: {os.path.basename(f)}")
            except Exception as e:
                print(f"  [WARN] Could not remove upload {f}: {e}")

    # 3. Re-initialize database schema
    db.set_db_path(db_path)
    print("  [OK] Database schema initialized with zero records.")

    demo_dir = os.path.join(repo_root, "datasets", "demo")

    # -------------------------------------------------------------
    # 4. Ingest Demo Datasets
    # -------------------------------------------------------------
    # (a) Microservices Domain
    ms_topo_path = os.path.join(demo_dir, "microservices_topology.json")
    ms_logs_path = os.path.join(demo_dir, "microservices_logs.jsonl")
    ms_metrics_path = os.path.join(demo_dir, "microservices_metrics.csv")

    ds_ms = Repository.create_dataset(
        name="Cloud Microservices Telemetry",
        description="Distributed e-commerce microservices with cascading DB connection pool exhaustion",
        source_type="jsonl",
        file_path=ms_logs_path,
        metadata={"domain": "microservices", "services_count": 6},
        is_demo=True,
    )

    if os.path.exists(ms_topo_path):
        with open(ms_topo_path, "r", encoding="utf-8") as f:
            topo = json.load(f)
            for s in topo.get("services", []):
                Repository.upsert_service(s["name"], dataset_id=ds_ms["id"], service_type=s.get("type", "service"), host=s.get("host"), description=s.get("description"))
            for d in topo.get("dependencies", []):
                Repository.upsert_dependency(d["source"], d["target"], dataset_id=ds_ms["id"], latency=d.get("latency_p95", 10.0))
    process_dataset(ds_ms["id"])
    print(f"  [SEEDED] Microservices Logs: {ds_ms['id']}")

    ds_metrics = Repository.create_dataset(
        name="Microservices System Metrics",
        description="CPU, connection pools, and latency metrics for microservices cluster",
        source_type="csv",
        file_path=ms_metrics_path,
        metadata={"domain": "metrics", "frequency": "1m"},
        is_demo=True,
    )
    process_dataset(ds_metrics["id"])
    print(f"  [SEEDED] Microservices Metrics: {ds_metrics['id']}")

    # (b) Kubernetes Domain
    k8s_topo_path = os.path.join(demo_dir, "k8s_infra_topology.json")
    k8s_logs_path = os.path.join(demo_dir, "k8s_infra_logs.jsonl")

    ds_k8s = Repository.create_dataset(
        name="Kubernetes Infrastructure Cluster Logs",
        description="Kubelet node memory leak and CoreDNS crashloop outage",
        source_type="jsonl",
        file_path=k8s_logs_path,
        metadata={"domain": "infrastructure", "platform": "kubernetes"},
        is_demo=True,
    )

    if os.path.exists(k8s_topo_path):
        with open(k8s_topo_path, "r", encoding="utf-8") as f:
            topo = json.load(f)
            for s in topo.get("services", []):
                Repository.upsert_service(s["name"], dataset_id=ds_k8s["id"], service_type=s.get("type", "k8s_node"), host=s.get("host"), description=s.get("description"))
            for d in topo.get("dependencies", []):
                Repository.upsert_dependency(d["source"], d["target"], dataset_id=ds_k8s["id"], latency=d.get("latency_p95", 5.0))
    process_dataset(ds_k8s["id"])
    print(f"  [SEEDED] Kubernetes Cluster Logs: {ds_k8s['id']}")

    # (c) Streaming Pipeline Domain
    stream_topo_path = os.path.join(demo_dir, "streaming_pipeline_topology.json")
    stream_logs_path = os.path.join(demo_dir, "streaming_pipeline_logs.jsonl")

    ds_stream = Repository.create_dataset(
        name="Streaming Video Pipeline Telemetry",
        description="Kafka partition rebalance lag and transcoder frame drop incident",
        source_type="jsonl",
        file_path=stream_logs_path,
        metadata={"domain": "data_streaming", "broker": "kafka"},
        is_demo=True,
    )

    if os.path.exists(stream_topo_path):
        with open(stream_topo_path, "r", encoding="utf-8") as f:
            topo = json.load(f)
            for s in topo.get("services", []):
                Repository.upsert_service(s["name"], dataset_id=ds_stream["id"], service_type=s.get("type", "streaming"), host=s.get("host"), description=s.get("description"))
            for d in topo.get("dependencies", []):
                Repository.upsert_dependency(d["source"], d["target"], dataset_id=ds_stream["id"], latency=d.get("latency_p95", 15.0))
    process_dataset(ds_stream["id"])
    print(f"  [SEEDED] Streaming Video Pipeline: {ds_stream['id']}")

    # (d) LogHub HDFS Domain
    loghub_path = os.path.join(demo_dir, "loghub_hdfs_cluster.log")
    ds_loghub = Repository.create_dataset(
        name="LogHub HDFS Cluster System Logs",
        description="Public LogHub HDFS standard cluster logs with replication monitor failures",
        source_type="log",
        file_path=loghub_path,
        metadata={"domain": "distributed_storage", "source": "LogHub HDFS"},
        is_demo=True,
    )
    process_dataset(ds_loghub["id"])
    print(f"  [SEEDED] LogHub HDFS Logs: {ds_loghub['id']}")

    # -------------------------------------------------------------
    # 5. Run Analysis Pipeline for each demo log dataset
    # -------------------------------------------------------------
    demo_datasets = [
        ("Microservices", ds_ms["id"]),
        ("Kubernetes", ds_k8s["id"]),
        ("Streaming Pipeline", ds_stream["id"]),
        ("LogHub HDFS", ds_loghub["id"]),
    ]

    print("\nRunning Analysis Pipelines (Idempotent RCA + Correlation)...")
    for name, ds_id in demo_datasets:
        job_id = Repository.create_analysis_job(ds_id)
        AnalysisOrchestrator.run_pipeline(ds_id, job_id)
        job = Repository.get_analysis_job(job_id)
        print(f"  [ANALYZED] {name} ({ds_id}) -> Status: {job.get('status')}")

    # -------------------------------------------------------------
    # 6. Train Baseline Model & Run Benchmark
    # -------------------------------------------------------------
    print("\nTraining Baseline Isolation Forest & Evaluating Benchmark...")
    trainer = ModelTrainer()
    train_res = trainer.train_models()
    f1 = train_res["evaluation_metrics"]["f1_score"]
    bench_res = trainer.evaluate_benchmark()
    print(f"  [TRAINED] Isolation Forest Model - F1 Score: {f1}")
    print(f"  [BENCHMARK] Top-1 Accuracy: {bench_res['top1_accuracy'] * 100:.1f}%, Top-3 Accuracy: {bench_res['top3_accuracy'] * 100:.1f}%")

    # -------------------------------------------------------------
    # 7. Verification Summary
    # -------------------------------------------------------------
    summary = Repository.get_dashboard_summary()
    incidents = Repository.list_incidents()
    datasets = Repository.list_datasets()

    print("\n" + "=" * 70)
    print("DEMO DATABASE RE-SEEDED SUCCESSFULLY")
    print("=" * 70)
    print(f"Total Datasets Ingested : {len(datasets)} (4 Logs + 1 Metrics)")
    print(f"Total Incidents Created  : {len(incidents)}")
    print(f"Active Incidents (KPI)   : {summary['active_incidents']}")
    print(f"Total Telemetry Events   : {summary['total_events']}")
    print(f"Services Monitored       : {summary['services_monitored']}")
    print("-" * 70)
    print("Incidents Breakdown:")
    for inc in incidents:
        print(f"  • [{inc['severity'].upper()}] {inc['id']}: {inc['title']}")
        print(f"      Root Cause Service: {inc['root_cause_service']} | Confidence: {inc['confidence_score']:.2f} | Dataset: {inc['dataset_id']}")
    print("=" * 70)


if __name__ == "__main__":
    reset_demo_database()
