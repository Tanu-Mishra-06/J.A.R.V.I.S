"""
Multi-Domain Telemetry Dataset Generator for J.A.R.V.I.S.
Generates realistic datasets across diverse domains (not payment-specific):
  1. Distributed Cloud Microservices (Cascading DB Connection Pool Exhaustion)
  2. Kubernetes Infrastructure (Node Memory Leak & CoreDNS CrashLoop)
  3. Streaming Media Pipeline (Kafka Partition Lag & Frame Drops)
  4. LogHub HDFS Cluster (Standard LogHub Syslog format)
  5. OpenRCA Ground-Truth Benchmark (Labeled Evaluation Dataset)
"""

from __future__ import annotations

import csv
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

SEED = 42
random.seed(SEED)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEMO_DIR = os.path.join(BASE_DIR, "datasets", "demo")
os.makedirs(DEMO_DIR, exist_ok=True)

T_BASE = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)


def generate_microservices_dataset():
    """Scenario 1: Distributed Cloud Microservices Cascading Failure."""
    services = [
        {"name": "api-gateway", "type": "gateway", "host": "gw-node-01", "description": "Cloud API Gateway"},
        {"name": "auth-service", "type": "web", "host": "auth-node-01", "description": "OAuth & JWT Token Authority"},
        {"name": "order-service", "type": "web", "host": "order-node-01", "description": "Core Business Order Service"},
        {"name": "inventory-service", "type": "web", "host": "inv-node-01", "description": "Warehouse & Stock Manager"},
        {"name": "database-primary", "type": "database", "host": "db-node-01", "description": "PostgreSQL Primary Cluster"},
        {"name": "redis-cache", "type": "cache", "host": "cache-node-01", "description": "In-memory Redis Cluster"},
    ]

    dependencies = [
        {"source": "api-gateway", "target": "auth-service", "latency_p95": 14.5},
        {"source": "api-gateway", "target": "order-service", "latency_p95": 28.2},
        {"source": "order-service", "target": "inventory-service", "latency_p95": 22.0},
        {"source": "order-service", "target": "database-primary", "latency_p95": 8.5},
        {"source": "auth-service", "target": "redis-cache", "latency_p95": 3.2},
        {"source": "auth-service", "target": "database-primary", "latency_p95": 9.1},
    ]

    # Save services JSON
    with open(os.path.join(DEMO_DIR, "microservices_topology.json"), "w", encoding="utf-8") as f:
        json.dump({"services": services, "dependencies": dependencies}, f, indent=2)

    # Generate Metrics CSV
    metrics_path = os.path.join(DEMO_DIR, "microservices_metrics.csv")
    with open(metrics_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "service_name", "host_name", "metric_name", "metric_value", "unit"])
        for minute in range(30):
            ts = (T_BASE + timedelta(minutes=minute)).isoformat()
            is_incident = minute >= 15
            for s in services:
                srv = s["name"]
                host = s["host"]
                if srv == "database-primary":
                    cpu = random.uniform(85.0, 99.5) if is_incident else random.uniform(25.0, 42.0)
                    conns = random.uniform(920.0, 1000.0) if is_incident else random.uniform(120.0, 250.0)
                    qps = random.uniform(2200.0, 3100.0) if is_incident else random.uniform(1500.0, 1800.0)
                    writer.writerow([ts, srv, host, "cpu_utilization", round(cpu, 1), "%"])
                    writer.writerow([ts, srv, host, "active_connections", int(conns), "connections"])
                    writer.writerow([ts, srv, host, "queries_per_second", int(qps), "qps"])
                elif srv == "order-service":
                    lat = random.uniform(2100.0, 4800.0) if is_incident else random.uniform(20.0, 35.0)
                    err = random.uniform(0.35, 0.72) if is_incident else random.uniform(0.001, 0.005)
                    writer.writerow([ts, srv, host, "latency_p95", round(lat, 1), "ms"])
                    writer.writerow([ts, srv, host, "error_rate", round(err, 3), "ratio"])
                elif srv == "api-gateway":
                    lat = random.uniform(2500.0, 5200.0) if is_incident else random.uniform(30.0, 45.0)
                    err_504 = random.uniform(150.0, 320.0) if is_incident else 0.0
                    writer.writerow([ts, srv, host, "latency_p95", round(lat, 1), "ms"])
                    writer.writerow([ts, srv, host, "http_504_errors", int(err_504), "errors/min"])

    # Generate Logs JSONL
    logs_path = os.path.join(DEMO_DIR, "microservices_logs.jsonl")
    with open(logs_path, "w", encoding="utf-8") as f:
        for minute in range(30):
            ts = T_BASE + timedelta(minutes=minute)
            is_incident = minute >= 15
            for sec in range(0, 60, 15):
                cur_t = (ts + timedelta(seconds=sec)).isoformat()
                trace_id = f"tr-{uuid.uuid4().hex[:12]}"
                if not is_incident:
                    ev = {
                        "timestamp": cur_t,
                        "service_name": "api-gateway",
                        "host_name": "gw-node-01",
                        "severity": "info",
                        "message": "HTTP 200 OK GET /api/v1/orders - latency=24ms",
                        "trace_id": trace_id,
                        "event_type": "http_request",
                    }
                    f.write(json.dumps(ev) + "\n")
                else:
                    # Root cause failure on database-primary
                    ev_db = {
                        "timestamp": cur_t,
                        "service_name": "database-primary",
                        "host_name": "db-node-01",
                        "severity": "critical",
                        "message": "FATAL: remaining connection slots are reserved for non-replication superuser connections (max_connections=1000 saturated)",
                        "trace_id": trace_id,
                        "event_type": "database_pool_exhaustion",
                    }
                    f.write(json.dumps(ev_db) + "\n")

                    # Cascading symptom on order-service
                    ev_ord = {
                        "timestamp": cur_t,
                        "service_name": "order-service",
                        "host_name": "order-node-01",
                        "severity": "error",
                        "message": "SQLException: Connection pool exhausted. HikariPool-1 - Connection is not available, request timed out after 30000ms.",
                        "trace_id": trace_id,
                        "event_type": "pool_timeout",
                    }
                    f.write(json.dumps(ev_ord) + "\n")

                    # Cascading symptom on api-gateway
                    ev_gw = {
                        "timestamp": cur_t,
                        "service_name": "api-gateway",
                        "host_name": "gw-node-01",
                        "severity": "error",
                        "message": "HTTP 504 Gateway Timeout: upstream order-service failed to respond within 30000ms",
                        "trace_id": trace_id,
                        "event_type": "gateway_timeout",
                    }
                    f.write(json.dumps(ev_gw) + "\n")


def generate_k8s_infrastructure_dataset():
    """Scenario 2: Kubernetes Infrastructure & Node Memory Leak."""
    services = [
        {"name": "worker-node-01", "type": "node", "host": "k8s-w01", "description": "Kubelet Worker Node 01"},
        {"name": "coredns", "type": "dns", "host": "k8s-w01", "description": "CoreDNS Cluster Domain Resolver"},
        {"name": "kube-apiserver", "type": "controlplane", "host": "k8s-m01", "description": "Kubernetes API Server"},
        {"name": "ingress-nginx", "type": "ingress", "host": "k8s-w02", "description": "NGINX Ingress Controller"},
    ]

    dependencies = [
        {"source": "coredns", "target": "worker-node-01", "latency_p95": 2.1},
        {"source": "ingress-nginx", "target": "coredns", "latency_p95": 18.0},
        {"source": "ingress-nginx", "target": "kube-apiserver", "latency_p95": 12.0},
    ]

    with open(os.path.join(DEMO_DIR, "k8s_infra_topology.json"), "w", encoding="utf-8") as f:
        json.dump({"services": services, "dependencies": dependencies}, f, indent=2)

    # Logs JSONL
    logs_path = os.path.join(DEMO_DIR, "k8s_infra_logs.jsonl")
    with open(logs_path, "w", encoding="utf-8") as f:
        for minute in range(20):
            ts = T_BASE + timedelta(minutes=minute)
            is_incident = minute >= 10
            for sec in range(0, 60, 20):
                cur_t = (ts + timedelta(seconds=sec)).isoformat()
                trace_id = f"k8s-{uuid.uuid4().hex[:10]}"
                if not is_incident:
                    ev = {
                        "timestamp": cur_t,
                        "service_name": "worker-node-01",
                        "host_name": "k8s-w01",
                        "severity": "info",
                        "message": "kubelet: Node worker-node-01 condition Ready is True. MemoryPressure=False",
                        "trace_id": trace_id,
                        "event_type": "node_heartbeat",
                    }
                    f.write(json.dumps(ev) + "\n")
                else:
                    # Root cause: Kernel OOM on worker-node-01
                    ev_node = {
                        "timestamp": cur_t,
                        "service_name": "worker-node-01",
                        "host_name": "k8s-w01",
                        "severity": "critical",
                        "message": "kernel: [10245.12] Out of memory: Kill process 8841 (coredns) score 950 or sacrifice child. Invoked oom-killer.",
                        "trace_id": trace_id,
                        "event_type": "memory_leak_oom",
                    }
                    f.write(json.dumps(ev_node) + "\n")

                    # Symptom: CoreDNS crashloop & DNS resolution failure
                    ev_dns = {
                        "timestamp": cur_t,
                        "service_name": "coredns",
                        "host_name": "k8s-w01",
                        "severity": "error",
                        "message": "dial tcp: lookup kube-apiserver.default.svc.cluster.local: server misbehaving: DNS timeout after 5000ms",
                        "trace_id": trace_id,
                        "event_type": "dns_resolution_failure",
                    }
                    f.write(json.dumps(ev_dns) + "\n")


def generate_streaming_pipeline_dataset():
    """Scenario 3: Streaming Media & Data Processing Pipeline."""
    services = [
        {"name": "kafka-cluster", "type": "broker", "host": "kafka-01", "description": "Apache Kafka Message Bus"},
        {"name": "transcoder-worker", "type": "worker", "host": "worker-gpu-01", "description": "H.264/AV1 Transcoder Worker"},
        {"name": "video-ingest", "type": "gateway", "host": "ingest-01", "description": "RTMP & WebRTC Ingest Gateway"},
    ]

    dependencies = [
        {"source": "video-ingest", "target": "kafka-cluster", "latency_p95": 5.4},
        {"source": "transcoder-worker", "target": "kafka-cluster", "latency_p95": 45.0},
    ]

    with open(os.path.join(DEMO_DIR, "streaming_pipeline_topology.json"), "w", encoding="utf-8") as f:
        json.dump({"services": services, "dependencies": dependencies}, f, indent=2)

    logs_path = os.path.join(DEMO_DIR, "streaming_pipeline_logs.jsonl")
    with open(logs_path, "w", encoding="utf-8") as f:
        for minute in range(20):
            ts = T_BASE + timedelta(minutes=minute)
            is_incident = minute >= 8
            for sec in range(0, 60, 20):
                cur_t = (ts + timedelta(seconds=sec)).isoformat()
                trace_id = f"stream-{uuid.uuid4().hex[:10]}"
                if not is_incident:
                    ev = {
                        "timestamp": cur_t,
                        "service_name": "transcoder-worker",
                        "host_name": "worker-gpu-01",
                        "severity": "info",
                        "message": "Processed video chunk 48102 - 1080p60 fps=60.0 buffer_lag=12ms",
                        "trace_id": trace_id,
                        "event_type": "chunk_processed",
                    }
                    f.write(json.dumps(ev) + "\n")
                else:
                    # Root cause: Kafka partition rebalance storm
                    ev_kafka = {
                        "timestamp": cur_t,
                        "service_name": "kafka-cluster",
                        "host_name": "kafka-01",
                        "severity": "critical",
                        "message": "org.apache.kafka.clients.consumer.CommitFailedException: Commit cannot be completed since the group has already rebalanced and assigned the partitions to another member",
                        "trace_id": trace_id,
                        "event_type": "kafka_partition_lag_storm",
                    }
                    f.write(json.dumps(ev_kafka) + "\n")

                    # Downstream worker frame drop
                    ev_worker = {
                        "timestamp": cur_t,
                        "service_name": "transcoder-worker",
                        "host_name": "worker-gpu-01",
                        "severity": "error",
                        "message": "Consumer lag exceeded 150000 records. Frame buffer overflow, dropping 450 frames on stream live-912.",
                        "trace_id": trace_id,
                        "event_type": "frame_drop",
                    }
                    f.write(json.dumps(ev_worker) + "\n")


def generate_loghub_hdfs_dataset():
    """Scenario 4: LogHub Public Dataset Ingestion (HDFS System Log format)."""
    log_path = os.path.join(DEMO_DIR, "loghub_hdfs_cluster.log")
    with open(log_path, "w", encoding="utf-8") as f:
        for minute in range(25):
            ts = T_BASE + timedelta(minutes=minute)
            ts_str = ts.strftime("%y%m%d %H%M%S")
            is_incident = minute >= 12
            for sec in range(0, 60, 15):
                blk_id = random.randint(1000000000, 9999999999)
                if not is_incident:
                    line = f"{ts_str} 142 INFO dfs.DataNode$BlockReceiver: Receiving block blk_{blk_id} src: /10.251.43.115:52012 dest: /10.251.43.115:50010\n"
                    f.write(line)
                else:
                    line1 = f"{ts_str} 987 ERROR dfs.DataNode$PacketResponder: PacketResponder 1 for block blk_{blk_id} Interrupted: java.io.IOException: Premature EOF from inputStream\n"
                    line2 = f"{ts_str} 988 FATAL dfs.FSNamesystem: Replication monitor heartbeat expired for DataNode /10.251.43.115:50010. Marking node as DEAD.\n"
                    f.write(line1)
                    f.write(line2)


def generate_openrca_benchmark_dataset():
    """Scenario 5: OpenRCA Ground-Truth Benchmark Dataset."""
    benchmark = {
        "dataset_name": "OpenRCA2-Lite-Evaluation-Suite",
        "description": "Standardized Ground-Truth Root Cause Analysis Benchmark for Distributed Systems",
        "total_incidents": 5,
        "scenarios": [
            {
                "incident_id": "INC-001",
                "domain": "Enterprise Cloud Microservices",
                "ground_truth_root_cause": "database-primary",
                "failure_type": "database_pool_exhaustion",
                "symptom_services": ["order-service", "api-gateway"],
                "evidence_metrics": ["active_connections", "cpu_utilization"],
            },
            {
                "incident_id": "INC-002",
                "domain": "Kubernetes Cloud Infrastructure",
                "ground_truth_root_cause": "worker-node-01",
                "failure_type": "memory_leak_oom",
                "symptom_services": ["coredns", "ingress-nginx"],
                "evidence_metrics": ["memory_usage", "container_restarts"],
            },
            {
                "incident_id": "INC-003",
                "domain": "Real-time Streaming Video Pipeline",
                "ground_truth_root_cause": "kafka-cluster",
                "failure_type": "kafka_partition_lag_storm",
                "symptom_services": ["transcoder-worker", "video-ingest"],
                "evidence_metrics": ["consumer_lag", "frame_drop_rate"],
            },
        ],
    }
    with open(os.path.join(DEMO_DIR, "openrca_ground_truth.json"), "w", encoding="utf-8") as f:
        json.dump(benchmark, f, indent=2)


def main():
    print("=" * 60)
    print("Generating Multi-Domain Datasets (Microservices, K8s, Streaming, LogHub, OpenRCA)")
    print("=" * 60)
    generate_microservices_dataset()
    print("[OK] Microservices cascading failure dataset created.")
    generate_k8s_infrastructure_dataset()
    print("[OK] Kubernetes infrastructure node outage dataset created.")
    generate_streaming_pipeline_dataset()
    print("[OK] Streaming video pipeline dataset created.")
    generate_loghub_hdfs_dataset()
    print("[OK] LogHub HDFS cluster log dataset created.")
    generate_openrca_benchmark_dataset()
    print("[OK] OpenRCA ground truth benchmark created.")
    print("All datasets generated in:", DEMO_DIR)


if __name__ == "__main__":
    main()
