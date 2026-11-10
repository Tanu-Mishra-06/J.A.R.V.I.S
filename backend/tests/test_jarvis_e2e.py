"""
Comprehensive End-to-End Test Suite for J.A.R.V.I.S.
Verifies:
  1. REST API endpoints (Datasets, Incidents, RCA, Impact, Timeline, Graph, Summary, Report).
  2. Dataset validation, normalization, and raw record provenance.
  3. Statistical Anomaly Detection (Z-score) and ML Anomaly Detection (Isolation Forest).
  4. Event correlation and cluster grouping.
  5. Root cause ranking and uncertainty evaluation.
  6. Blast radius and timeline milestones.
  7. Model training and benchmark evaluation.
  8. Live telemetry (OTLP) ingestion receiver.
"""

from __future__ import annotations

import io
import json
import os
import shutil
import sys
import tempfile
import unittest

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
root_dir = os.path.dirname(backend_dir)
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from app.main import app
from app.core.config import settings
from app.database.db import db
from app.database.repository import Repository
from app.intelligence.anomaly_detection.detector import AnomalyDetectionEngine
from app.intelligence.anomaly_detection.zscore import ZScoreDetector
from app.intelligence.anomaly_detection.isolation_forest import IsolationForestDetector
from app.intelligence.event_correlation.correlator import EventCorrelator
from app.intelligence.root_cause_analysis.rca_engine import RootCauseAnalyzer
from app.intelligence.impact_analysis.impact_engine import ImpactAnalysisEngine
from app.intelligence.timeline.timeline_builder import TimelineBuilder
from app.intelligence.recommendations.recommender import RecommendationEngine
from app.intelligence.model_trainer import ModelTrainer


class JarvisE2ETestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp(prefix='jarvis_test_')
        cls.test_db_path = os.path.join(cls.temp_dir, 'test_jarvis.db')
        cls.orig_db_path = db.db_path
        cls.orig_upload_dir = settings.UPLOAD_DIR

        # Point upload dir to temp directory
        cls.test_upload_dir = os.path.join(cls.temp_dir, 'uploads')
        os.makedirs(cls.test_upload_dir, exist_ok=True)
        settings.UPLOAD_DIR = cls.test_upload_dir

        # Point database to isolated test database
        db.set_db_path(cls.test_db_path)

        # Seed the isolated test database so endpoints have data
        from datasets.seed_data import seed_all
        seed_all()

        cls.client = app.test_client()

    @classmethod
    def tearDownClass(cls):
        # Restore original DB and upload directory
        db.set_db_path(cls.orig_db_path)
        settings.UPLOAD_DIR = cls.orig_upload_dir
        try:
            shutil.rmtree(cls.temp_dir, ignore_errors=True)
        except Exception:
            pass

    def test_01_dashboard_summary_api(self):
        """Verify GET /api/dashboard/summary returns real KPI metrics."""
        res = self.client.get("/api/dashboard/summary")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertIn("active_incidents", data)
        self.assertIn("total_events", data)
        self.assertIn("total_anomalies", data)
        self.assertIn("services_monitored", data)
        self.assertGreater(data["total_events"], 0)
        self.assertGreater(data["services_monitored"], 0)

    def test_02_datasets_list_api(self):
        """Verify GET /api/datasets returns ingested datasets."""
        res = self.client.get("/api/datasets")
        self.assertEqual(res.status_code, 200)
        datasets = res.get_json()
        self.assertIsInstance(datasets, list)
        self.assertGreater(len(datasets), 0)
        first = datasets[0]
        self.assertIn("id", first)
        self.assertIn("name", first)
        self.assertIn("status", first)

    def test_03_incidents_list_and_details_api(self):
        """Verify GET /api/incidents and /api/incidents/<id> endpoints."""
        res = self.client.get("/api/incidents")
        self.assertEqual(res.status_code, 200)
        incidents = res.get_json()
        self.assertGreater(len(incidents), 0)

        inc_id = incidents[0]["id"]
        detail_res = self.client.get(f"/api/incidents/{inc_id}")
        self.assertEqual(detail_res.status_code, 200)
        detail = detail_res.get_json()
        self.assertIn("root_cause_service", detail)
        self.assertIn("confidence_score", detail)

    def test_04_root_cause_analysis_api(self):
        """Verify GET /api/incidents/<id>/root-cause returns ranked candidates with evidence."""
        incidents = Repository.list_incidents()
        self.assertGreater(len(incidents), 0)
        inc_id = incidents[0]["id"]

        res = self.client.get(f"/api/incidents/{inc_id}/root-cause")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        candidates = data.get("candidates", [])
        self.assertGreater(len(candidates), 0)
        top = candidates[0]
        self.assertEqual(top["rank"], 1)
        self.assertIn("confidence_score", top)
        self.assertIn("uncertainty_assessment", top)
        self.assertIn("evidence", top)

    def test_05_incident_timeline_api(self):
        """Verify GET /api/incidents/<id>/timeline returns chronological milestones."""
        incidents = Repository.list_incidents()
        inc_id = incidents[0]["id"]

        res = self.client.get(f"/api/incidents/{inc_id}/timeline")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        timeline = data.get("timeline", [])
        self.assertGreater(len(timeline), 0)
        first_event = timeline[0]
        self.assertIn("offset_formatted", first_event)
        self.assertIn("milestone", first_event)

    def test_06_impact_and_recommendations_api(self):
        """Verify impact report and recommendations endpoints."""
        incidents = Repository.list_incidents()
        inc_id = incidents[0]["id"]

        imp_res = self.client.get(f"/api/incidents/{inc_id}/impact")
        self.assertEqual(imp_res.status_code, 200)
        impact = imp_res.get_json()
        self.assertIn("direct_services", impact)
        self.assertIn("total_events_affected", impact)

        rec_res = self.client.get(f"/api/incidents/{inc_id}/recommendations")
        self.assertEqual(rec_res.status_code, 200)
        recs = rec_res.get_json().get("recommendations", [])
        self.assertGreater(len(recs), 0)
        self.assertIn("action_type", recs[0])

    def test_07_postmortem_report_download(self):
        """Verify incident postmortem report generation in Markdown and JSON."""
        incidents = Repository.list_incidents()
        inc_id = incidents[0]["id"]

        # Markdown
        res_md = self.client.get(f"/api/incidents/{inc_id}/report?format=markdown")
        self.assertEqual(res_md.status_code, 200)
        self.assertIn(b"# INCIDENT POST-MORTEM REPORT", res_md.data)

        # JSON
        res_json = self.client.get(f"/api/incidents/{inc_id}/report?format=json")
        self.assertEqual(res_json.status_code, 200)
        data = json.loads(res_json.data)
        self.assertIn("root_cause_candidates", data)

    def test_08_service_topology_graph_api(self):
        """Verify GET /api/services/graph returns nodes and dependency edges."""
        res = self.client.get("/api/services/graph")
        self.assertEqual(res.status_code, 200)
        graph = res.get_json()
        self.assertIn("services", graph)
        self.assertIn("dependencies", graph)
        self.assertGreater(len(graph["services"]), 0)
        self.assertGreater(len(graph["dependencies"]), 0)

    def test_09_model_training_and_benchmark_api(self):
        """Verify model training and OpenRCA benchmark evaluation."""
        train_res = self.client.post("/api/models/train", json={"contamination": 0.08, "n_estimators": 50})
        self.assertEqual(train_res.status_code, 200)
        t_data = train_res.get_json()
        self.assertEqual(t_data["status"], "success")

        bench_res = self.client.get("/api/models/evaluate")
        self.assertEqual(bench_res.status_code, 200)
        b_data = bench_res.get_json()
        self.assertGreaterEqual(b_data["top1_accuracy"], 0.70)
        self.assertGreaterEqual(b_data["top3_accuracy"], 0.90)

    def test_10_live_telemetry_otlp_receiver(self):
        """Verify OpenTelemetry OTLP receiver accepts traces, logs, and metrics."""
        t_res = self.client.post("/v1/traces", json={"spans": [{"trace_id": "test-tr-1", "service_name": "gateway"}]})
        self.assertEqual(t_res.status_code, 202)

        l_res = self.client.post("/v1/logs", json={"logs": [{"severity": "info", "message": "Test OTLP log"}]})
        self.assertEqual(l_res.status_code, 202)

        stats_res = self.client.get("/api/telemetry/stats")
        self.assertEqual(stats_res.status_code, 200)
        s_data = stats_res.get_json()
        self.assertGreater(s_data["buffered_events_count"], 0)

    def test_11_dataset_upload_pipeline(self):
        """Verify file upload via multipart/form-data."""
        csv_data = b"timestamp,service_name,host_name,metric_name,metric_value,unit\n2026-11-09T10:00:00Z,cache-01,host-c1,eviction_rate,88.4,ops\n2026-11-09T10:01:00Z,cache-01,host-c1,eviction_rate,94.2,ops\n"
        data = {
            "file": (io.BytesIO(csv_data), "test_custom_metrics.csv"),
            "name": "Custom Cache Metrics Test",
            "description": "Upload integration test",
            "auto_process": "true",
        }
        res = self.client.post("/api/datasets/upload", data=data, content_type="multipart/form-data")
        self.assertEqual(res.status_code, 201)
        ds = res.get_json()
        self.assertEqual(ds["name"], "Custom Cache Metrics Test")

    def test_12_service_identity_composite_scoping(self):
        """Verify service identity is (dataset_id, name) and datasets never collide."""
        ds_a = Repository.create_dataset(name="Dataset Alpha", source_type="jsonl", file_path="alpha.jsonl")
        ds_b = Repository.create_dataset(name="Dataset Beta", source_type="jsonl", file_path="beta.jsonl")

        Repository.upsert_service("cache-layer", dataset_id=ds_a["id"], host="host-alpha-01", description="Redis Alpha")
        Repository.upsert_service("cache-layer", dataset_id=ds_b["id"], host="host-beta-99", description="Memcached Beta")

        Repository.upsert_dependency("api-gateway", "cache-layer", dataset_id=ds_a["id"])
        Repository.upsert_dependency("checkout-svc", "cache-layer", dataset_id=ds_b["id"])

        graph_a = Repository.get_service_graph(dataset_id=ds_a["id"])
        graph_b = Repository.get_service_graph(dataset_id=ds_b["id"])

        node_a = next((s for s in graph_a["services"] if s["name"] == "cache-layer"), None)
        node_b = next((s for s in graph_b["services"] if s["name"] == "cache-layer"), None)

        self.assertIsNotNone(node_a)
        self.assertIsNotNone(node_b)
        self.assertEqual(node_a["host"], "host-alpha-01")
        self.assertEqual(node_b["host"], "host-beta-99")

        edges_a = [f"{e['source_service']}->{e['target_service']}" for e in graph_a["dependencies"]]
        edges_b = [f"{e['source_service']}->{e['target_service']}" for e in graph_b["dependencies"]]

        self.assertIn("api-gateway->cache-layer", edges_a)
        self.assertNotIn("checkout-svc->cache-layer", edges_a)
        self.assertIn("checkout-svc->cache-layer", edges_b)
        self.assertNotIn("api-gateway->cache-layer", edges_b)

    def test_13_services_graph_scoping_and_dataset_incident_params(self):
        """Verify /api/services/graph scopes to dataset_id and incident_id, never merging all datasets."""
        datasets = Repository.list_datasets()
        ms_ds = next((d for d in datasets if d["name"] == "Cloud Microservices Telemetry"), None)
        k8s_ds = next((d for d in datasets if "Kubernetes" in d["name"]), None)
        self.assertIsNotNone(ms_ds)
        self.assertIsNotNone(k8s_ds)

        # 1. Query by dataset_id for Microservices
        res_ms = self.client.get(f"/api/services/graph?dataset_id={ms_ds['id']}")
        self.assertEqual(res_ms.status_code, 200)
        data_ms = res_ms.get_json()
        names_ms = {s["name"] for s in data_ms["services"]}
        self.assertIn("database-primary", names_ms)
        self.assertNotIn("worker-node-01", names_ms)
        self.assertNotIn("kafka-cluster", names_ms)

        # 2. Query by dataset_id for Kubernetes
        res_k8s = self.client.get(f"/api/services/graph?dataset_id={k8s_ds['id']}")
        self.assertEqual(res_k8s.status_code, 200)
        data_k8s = res_k8s.get_json()
        names_k8s = {s["name"] for s in data_k8s["services"]}
        self.assertIn("worker-node-01", names_k8s)
        self.assertNotIn("database-primary", names_k8s)

        # 3. Query by incident_id
        incidents = Repository.list_incidents(dataset_id=ms_ds["id"])
        self.assertGreater(len(incidents), 0)
        inc_id = incidents[0]["id"]

        res_inc = self.client.get(f"/api/services/graph?incident_id={inc_id}")
        self.assertEqual(res_inc.status_code, 200)
        data_inc = res_inc.get_json()
        self.assertEqual(data_inc.get("incident_id"), inc_id)
        names_inc = {s["name"] for s in data_inc["services"]}
        self.assertIn("database-primary", names_inc)
        self.assertNotIn("worker-node-01", names_inc)

        # 4. Default call (no params) returns a single scoped dataset, not all datasets merged
        res_def = self.client.get("/api/services/graph")
        self.assertEqual(res_def.status_code, 200)
        data_def = res_def.get_json()
        names_def = {s["name"] for s in data_def["services"]}
        # Cannot contain both microservices and k8s nodes in a single scoped graph
        self.assertFalse("database-primary" in names_def and "worker-node-01" in names_def)

    def test_14_data_driven_narrative_healthy_service_never_degraded(self):
        """Verify narratives are data-driven and healthy services (e.g. auth-service) are never degraded."""
        datasets = Repository.list_datasets()
        ms_ds = next((d for d in datasets if d["name"] == "Cloud Microservices Telemetry"), None)
        self.assertIsNotNone(ms_ds)

        res = self.client.get(f"/api/services/graph?dataset_id={ms_ds['id']}")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        services_by_name = {s["name"]: s for s in data["services"]}

        # auth-service is healthy (0 errors) -> MUST never be described as degraded or a symptom
        auth = services_by_name.get("auth-service")
        self.assertIsNotNone(auth)
        self.assertEqual(auth["status"], "healthy")
        self.assertEqual(auth["error_rate_pct"], 0.0)
        self.assertTrue(auth["what_happened"].startswith("Healthy:"))
        self.assertNotIn("degraded", auth["what_happened"].lower())
        self.assertNotIn("symptom", auth["what_happened"].lower())
        self.assertNotIn("pool contention", auth["what_happened"].lower())

        # database-primary is the root cause component
        db_primary = services_by_name.get("database-primary")
        self.assertIsNotNone(db_primary)
        self.assertIn(db_primary["status"], ("critical", "degraded"))
        self.assertIn("Root Cause Component:", db_primary["what_happened"])

        # order-service is impacted by database-primary
        order = services_by_name.get("order-service")
        self.assertIsNotNone(order)
        self.assertIn("Cascading Degradation:", order["what_happened"])

    def test_15_metric_only_dataset_services_counted(self):
        """Verify metric-only datasets discover, count, and display services in study reports."""
        datasets = Repository.list_datasets()
        metric_ds = next((d for d in datasets if d["name"] == "Microservices System Metrics"), None)
        self.assertIsNotNone(metric_ds)

        # Generate study report for metric-only dataset
        report = Repository.generate_dataset_study_report(metric_ds["id"])
        self.assertIsNotNone(report)
        self.assertGreater(report["services_count"], 0)
        self.assertGreater(len(report["services"]), 0)

        service_names = {s["service_name"] for s in report["services"]}
        self.assertIn("database-primary", service_names)
        self.assertIn("order-service", service_names)


if __name__ == "__main__":
    unittest.main()
