"""
Seeds J.A.R.V.I.S. database with multi-domain demo datasets:
  1. Distributed Microservices Cascading Failure
  2. Kubernetes Infrastructure Node Outage
  3. Real-time Streaming Video Pipeline
  4. LogHub HDFS Distributed Cluster Logs
  5. OpenRCA Ground Truth Benchmark
Also executes initial model training and evaluation so benchmark metrics are live.
"""

from __future__ import annotations

import json
import os
import sys

# Ensure backend is in python path
backend_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "backend")
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app.database.repository import Repository
from app.ingestion.processor import process_dataset
from app.intelligence.orchestrator import AnalysisOrchestrator
from app.intelligence.model_trainer import ModelTrainer


def seed_all():
    print("=" * 60)
    print("Seeding J.A.R.V.I.S. with Multi-Domain Operational Datasets")
    print("=" * 60)

    demo_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "demo")

    # 1. Microservices Dataset
    ms_logs_path = os.path.join(demo_dir, "microservices_logs.jsonl")
    ms_metrics_path = os.path.join(demo_dir, "microservices_metrics.csv")
    ms_topo_path = os.path.join(demo_dir, "microservices_topology.json")

    ds_ms = Repository.create_dataset(
        name="Cloud Microservices Telemetry",
        description="Distributed e-commerce microservices with cascading DB connection pool exhaustion",
        source_type="jsonl",
        file_path=ms_logs_path,
        metadata={"domain": "microservices", "services_count": 6},
        is_demo=True,
    )

    # Load topology scoped to ds_ms
    if os.path.exists(ms_topo_path):
        with open(ms_topo_path, "r", encoding="utf-8") as f:
            topo = json.load(f)
            for s in topo.get("services", []):
                Repository.upsert_service(s["name"], dataset_id=ds_ms["id"], service_type=s.get("type", "service"), host=s.get("host"), description=s.get("description"))
            for d in topo.get("dependencies", []):
                Repository.upsert_dependency(d["source"], d["target"], dataset_id=ds_ms["id"], latency=d.get("latency_p95", 10.0))
    print(f"Created dataset: {ds_ms['name']} ({ds_ms['id']})")
    proc_res = process_dataset(ds_ms["id"])
    print(f"  Processed {proc_res.get('valid_records', 0)} records.")

    # Also ingest microservices metrics
    ds_metrics = Repository.create_dataset(
        name="Microservices System Metrics",
        description="CPU, connection pools, and latency metrics for microservices cluster",
        source_type="csv",
        file_path=ms_metrics_path,
        metadata={"domain": "metrics", "frequency": "1m"},
        is_demo=True,
    )
    process_dataset(ds_metrics["id"])

    # 2. Kubernetes Infrastructure Dataset
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

    # 3. Streaming Video Pipeline Dataset
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

    # 4. LogHub HDFS Dataset
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

    # Run Analysis Pipeline on the primary microservices dataset to generate the live incident
    job_id = Repository.create_analysis_job(ds_ms["id"])
    print(f"Running Analysis Pipeline on dataset {ds_ms['id']} (job: {job_id})...")
    AnalysisOrchestrator.run_pipeline(ds_ms["id"], job_id)
    print("  [OK] Analysis pipeline executed.")

    # Also run on K8s dataset
    job_k8s = Repository.create_analysis_job(ds_k8s["id"])
    AnalysisOrchestrator.run_pipeline(ds_k8s["id"], job_k8s)
    print("  [OK] K8s analysis executed.")

    # Train initial models and run benchmark evaluation
    print("Training Isolation Forest and running benchmark evaluation...")
    trainer = ModelTrainer()
    train_res = trainer.train_models()
    print("  [OK] Model trained. F1:", train_res["evaluation_metrics"]["f1_score"])
    bench_res = trainer.evaluate_benchmark()
    print("  [OK] Benchmark evaluated. Top-1 Accuracy:", bench_res["top1_accuracy"], "Top-3:", bench_res["top3_accuracy"])

    summary = Repository.get_dashboard_summary()
    print(f"Summary: {summary['total_incidents']} Incidents, {summary['total_events']} Events, {summary['services_monitored']} Services Monitored.")


if __name__ == "__main__":
    seed_all()
