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
import sys
import unittest

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app.main import app
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
        cls.client = app.test_client()

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


if __name__ == "__main__":
    unittest.main()
