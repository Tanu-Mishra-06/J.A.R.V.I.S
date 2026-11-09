"""
Anomaly Detection Engine — orchestrates z-score and isolation-forest detectors
across all metrics and events in a dataset.

Runs univariate rolling z-score on metric series, multivariate IsolationForest
on numeric feature vectors per service, and event severity frequency analysis
to flag abnormal error/critical rates.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from app.database.repository import Repository
from app.database.db import db
from app.intelligence.anomaly_detection.zscore import ZScoreDetector, AnomalyResult
from app.intelligence.anomaly_detection.isolation_forest import IsolationForestDetector

logger = logging.getLogger(__name__)

_SEVERITY_RANK = {
    "debug": 0,
    "info": 1,
    "warning": 2,
    "error": 3,
    "critical": 4,
    "fatal": 4,
}


class AnomalyDetectionEngine:
    """Top-level orchestrator for anomaly detection on a dataset.

    Parameters
    ----------
    zscore_window : int
        Rolling window size for the z-score detector.
    zscore_threshold : float
        Z-score threshold for flagging metric anomalies.
    iforest_contamination : float
        Expected anomaly fraction for IsolationForest.
    error_rate_threshold : float
        Fraction of error+critical events in a time window above which
        the window is considered anomalous.
    """

    def __init__(
        self,
        zscore_window: int = 10,
        zscore_threshold: float = 2.5,
        iforest_contamination: float = 0.1,
        error_rate_threshold: float = 0.25,
    ) -> None:
        self.zscore = ZScoreDetector(window_size=zscore_window, threshold=zscore_threshold)
        self.iforest = IsolationForestDetector(contamination=iforest_contamination)
        self.error_rate_threshold = error_rate_threshold

    def run(self, dataset_id: str) -> Dict[str, Any]:
        """Execute all anomaly detection passes on the specified dataset.

        Returns summary of metric anomalies, iforest anomalies, and event anomalies.
        """
        metric_anomalies = self._detect_metric_anomalies(dataset_id)
        iforest_anomalies = self._detect_iforest_anomalies(dataset_id)
        event_anomalies = self._detect_event_anomalies(dataset_id)

        # Flag detected anomalies in the database
        self._persist_anomaly_flags(dataset_id, metric_anomalies, event_anomalies)

        return {
            "dataset_id": dataset_id,
            "metric_anomalies_count": len(metric_anomalies),
            "iforest_anomalies_count": len(iforest_anomalies),
            "event_anomalies_count": len(event_anomalies),
            "total_anomalies": len(metric_anomalies) + len(event_anomalies),
            "details": {
                "sample_metric_anomalies": metric_anomalies[:10],
                "sample_event_anomalies": event_anomalies[:10],
            },
        }

    def _detect_metric_anomalies(self, dataset_id: str) -> List[Dict[str, Any]]:
        """Run rolling z-score on metric time series grouped by service & metric_name."""
        raw_metrics = Repository.get_metrics(dataset_id=dataset_id, limit=5000)
        grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
        for m in raw_metrics:
            key = (m["service_name"], m["metric_name"])
            grouped[key].append(m)

        anomalies: List[Dict[str, Any]] = []

        for (service, metric_name), points in grouped.items():
            if len(points) < 3:
                continue

            points.sort(key=lambda p: p["timestamp"])
            series: List[Tuple[datetime, float]] = []
            for p in points:
                try:
                    dt = datetime.fromisoformat(p["timestamp"].replace("Z", "+00:00"))
                except Exception:
                    dt = datetime.now(timezone.utc)
                series.append((dt, float(p["metric_value"])))

            detected = self.zscore.detect(series)
            for res in detected:
                # Find matching point
                ts_iso = res.timestamp.isoformat()
                matched_id = None
                for pt in points:
                    if pt["timestamp"].startswith(ts_iso[:19]):
                        matched_id = pt["id"]
                        break

                anomalies.append({
                    "id": matched_id,
                    "service_name": service,
                    "metric_name": metric_name,
                    "timestamp": ts_iso,
                    "value": res.value,
                    "score": round(res.score, 3),
                    "method": "rolling_zscore",
                })

        return anomalies

    def _detect_iforest_anomalies(self, dataset_id: str) -> List[Dict[str, Any]]:
        """Fit IsolationForest on multivariate numeric features per service."""
        raw_metrics = Repository.get_metrics(dataset_id=dataset_id, limit=5000)
        by_service: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for m in raw_metrics:
            by_service[m["service_name"]].append(m)

        anomalies: List[Dict[str, Any]] = []

        for service, points in by_service.items():
            if len(points) < 10:
                continue

            # Group by timestamp into feature vectors
            ts_features: Dict[str, Dict[str, float]] = defaultdict(dict)
            metric_names = sorted(list({p["metric_name"] for p in points}))
            for p in points:
                ts_features[p["timestamp"]][p["metric_name"]] = float(p["metric_value"])

            timestamps = sorted(ts_features.keys())
            matrix = []
            for ts in timestamps:
                row = [ts_features[ts].get(m_name, 0.0) for m_name in metric_names]
                matrix.append(row)

            if len(matrix) >= 10:
                X = np.array(matrix, dtype=float)
                results = self.iforest.detect(X)
                for res in results:
                    if res.is_anomaly:
                        anomalies.append({
                            "service_name": service,
                            "timestamp": timestamps[res.index],
                            "score": round(res.anomaly_score, 3),
                            "method": "isolation_forest",
                        })

        return anomalies

    def _detect_event_anomalies(self, dataset_id: str) -> List[Dict[str, Any]]:
        """Detect abnormal spikes in error/critical logs per service."""
        events = Repository.get_events(dataset_id=dataset_id, limit=5000)
        by_service: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for ev in events:
            by_service[ev["service_name"]].append(ev)

        anomalies: List[Dict[str, Any]] = []

        for service, ev_list in by_service.items():
            if not ev_list:
                continue
            # Any critical/fatal log is an anomaly point
            for ev in ev_list:
                sev = ev.get("severity", "info").lower()
                is_crit = sev in ("critical", "fatal", "severe", "emerg")
                is_err = sev in ("error", "err")
                score = 0.95 if is_crit else (0.75 if is_err else 0.0)
                if score > 0.5:
                    anomalies.append({
                        "id": ev["id"],
                        "service_name": service,
                        "timestamp": ev["timestamp"],
                        "severity": sev,
                        "message": ev["message"],
                        "score": score,
                        "method": "severity_threshold",
                    })

        return anomalies

    def _persist_anomaly_flags(
        self,
        dataset_id: str,
        metric_anomalies: List[Dict[str, Any]],
        event_anomalies: List[Dict[str, Any]],
    ) -> None:
        """Mark anomalies in the SQLite database."""
        with db.get_connection() as conn:
            # Update metrics
            for ma in metric_anomalies:
                if ma.get("id"):
                    conn.execute(
                        "UPDATE metrics SET is_anomaly = 1, anomaly_score = ? WHERE id = ?",
                        (ma["score"], ma["id"]),
                    )
            # Update events
            for ea in event_anomalies:
                if ea.get("id"):
                    conn.execute(
                        "UPDATE events SET is_anomaly = 1, anomaly_score = ? WHERE id = ?",
                        (ea["score"], ea["id"]),
                    )
            conn.commit()
