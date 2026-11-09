"""
Repository operations for J.A.R.V.I.S.
Provides strongly typed helper methods for all data access across tables.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.database.db import db, utc_now_iso


def _format_ts(val: Any, default_iso: str) -> str:
    if val is None:
        return default_iso
    if isinstance(val, datetime):
        return val.isoformat()
    s = str(val).strip()
    return s if s else default_iso


class Repository:
    """Data Access Repository implementing CRUD for all J.A.R.V.I.S. entities."""

    # -------------------------------------------------------------------------
    # Datasets
    # -------------------------------------------------------------------------
    @staticmethod
    def create_dataset(
        name: str,
        description: Optional[str] = None,
        source_type: Optional[str] = "custom",
        file_path: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        is_demo: bool = False,
        dataset_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        d_id = dataset_id or str(uuid.uuid4())
        now = utc_now_iso()
        meta_str = json.dumps(metadata or {})
        with db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO datasets (
                    id, name, description, source_type, file_path,
                    record_count, valid_records, invalid_records,
                    status, metadata_json, is_demo, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 0, 0, 0, 'uploaded', ?, ?, ?, ?)
                """,
                (d_id, name, description, source_type, file_path, meta_str, 1 if is_demo else 0, now, now),
            )
            conn.commit()
        return Repository.get_dataset(d_id)

    @staticmethod
    def get_dataset(dataset_id: str) -> Optional[Dict[str, Any]]:
        with db.get_connection() as conn:
            cur = conn.execute("SELECT * FROM datasets WHERE id = ?", (dataset_id,))
            row = cur.fetchone()
            if not row:
                return None
            data = dict(row)
            data["metadata"] = json.loads(data.get("metadata_json") or "{}")
            data["is_demo"] = bool(data.get("is_demo"))
            return data

    @staticmethod
    def list_datasets() -> List[Dict[str, Any]]:
        with db.get_connection() as conn:
            cur = conn.execute("SELECT * FROM datasets ORDER BY created_at DESC")
            items = []
            for row in cur.fetchall():
                d = dict(row)
                d["metadata"] = json.loads(d.get("metadata_json") or "{}")
                d["is_demo"] = bool(d.get("is_demo"))
                items.append(d)
            return items

    @staticmethod
    def update_dataset_stats(
        dataset_id: str,
        record_count: int,
        valid_records: int,
        invalid_records: int,
        status: str = "processed",
        error_message: Optional[str] = None,
    ) -> None:
        now = utc_now_iso()
        with db.get_connection() as conn:
            conn.execute(
                """
                UPDATE datasets
                SET record_count = ?, valid_records = ?, invalid_records = ?,
                    status = ?, error_message = ?, updated_at = ?
                WHERE id = ?
                """,
                (record_count, valid_records, invalid_records, status, error_message, now, dataset_id),
            )
            conn.commit()

    @staticmethod
    def delete_dataset(dataset_id: str) -> bool:
        with db.get_connection() as conn:
            cur = conn.execute("DELETE FROM datasets WHERE id = ?", (dataset_id,))
            conn.commit()
            return cur.rowcount > 0

    # -------------------------------------------------------------------------
    # Raw Records
    # -------------------------------------------------------------------------
    @staticmethod
    def insert_raw_records(records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        now = utc_now_iso()
        rows = [
            (
                r.get("id") or str(uuid.uuid4()),
                r["dataset_id"],
                r.get("record_type", "event"),
                r.get("line_number", 0),
                json.dumps(r.get("raw_data", {})),
                now,
            )
            for r in records
        ]
        with db.get_connection() as conn:
            conn.executemany(
                """
                INSERT INTO raw_records (id, dataset_id, record_type, line_number, raw_data, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()

    # -------------------------------------------------------------------------
    # Events
    # -------------------------------------------------------------------------
    @staticmethod
    def insert_events(events: List[Dict[str, Any]]) -> None:
        if not events:
            return
        now = utc_now_iso()
        rows = [
            (
                e.get("id") or str(uuid.uuid4()),
                e["dataset_id"],
                _format_ts(e.get("timestamp"), now),
                e.get("source", "system"),
                e.get("service_name", "unknown"),
                e.get("host_name", "host-01"),
                e.get("event_type", "log"),
                e.get("severity", "info").lower() if e.get("severity") else "info",
                e.get("message", "") or "",
                e.get("trace_id"),
                e.get("span_id"),
                e.get("parent_span_id"),
                json.dumps(e.get("attributes", {})),
                e.get("raw_record_id"),
                1 if e.get("is_anomaly") else 0,
                float(e.get("anomaly_score", 0.0) or 0.0),
                now,
            )
            for e in events
        ]
        with db.get_connection() as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    id, dataset_id, timestamp, source, service_name, host_name,
                    event_type, severity, message, trace_id, span_id, parent_span_id,
                    attributes_json, raw_record_id, is_anomaly, anomaly_score, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()

    @staticmethod
    def get_events(
        dataset_id: Optional[str] = None,
        service_name: Optional[str] = None,
        severity: Optional[str] = None,
        search_query: Optional[str] = None,
        is_anomaly_only: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        query = "SELECT * FROM events WHERE 1=1"
        params: List[Any] = []

        if dataset_id:
            query += " AND dataset_id = ?"
            params.append(dataset_id)
        if service_name:
            query += " AND service_name = ?"
            params.append(service_name)
        if severity:
            query += " AND LOWER(severity) = ?"
            params.append(severity.lower())
        if is_anomaly_only:
            query += " AND is_anomaly = 1"
        if search_query:
            query += " AND (message LIKE ? OR service_name LIKE ? OR event_type LIKE ?)"
            pat = f"%{search_query}%"
            params.extend([pat, pat, pat])

        query += " ORDER BY timestamp ASC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with db.get_connection() as conn:
            cur = conn.execute(query, params)
            results = []
            for r in cur.fetchall():
                d = dict(r)
                d["attributes"] = json.loads(d.get("attributes_json") or "{}")
                d["is_anomaly"] = bool(d.get("is_anomaly"))
                results.append(d)
            return results

    @staticmethod
    def count_events(
        dataset_id: Optional[str] = None,
        service_name: Optional[str] = None,
        severity: Optional[str] = None,
        search_query: Optional[str] = None,
    ) -> int:
        query = "SELECT COUNT(*) FROM events WHERE 1=1"
        params: List[Any] = []
        if dataset_id:
            query += " AND dataset_id = ?"
            params.append(dataset_id)
        if service_name:
            query += " AND service_name = ?"
            params.append(service_name)
        if severity:
            query += " AND LOWER(severity) = ?"
            params.append(severity.lower())
        if search_query:
            query += " AND (message LIKE ? OR service_name LIKE ? OR event_type LIKE ?)"
            pat = f"%{search_query}%"
            params.extend([pat, pat, pat])

        with db.get_connection() as conn:
            cur = conn.execute(query, params)
            return cur.fetchone()[0]

    # -------------------------------------------------------------------------
    # Metrics
    # -------------------------------------------------------------------------
    @staticmethod
    def insert_metrics(metrics: List[Dict[str, Any]]) -> None:
        if not metrics:
            return
        now = utc_now_iso()
        rows = [
            (
                m.get("id") or str(uuid.uuid4()),
                m["dataset_id"],
                _format_ts(m.get("timestamp"), now),
                m.get("service_name", "unknown") or "unknown",
                m.get("host_name", "host-01") or "host-01",
                m.get("metric_name", "gauge") or "gauge",
                float(m.get("metric_value", 0.0) or 0.0),
                m.get("unit", "") or "",
                json.dumps(m.get("attributes", {})),
                1 if m.get("is_anomaly") else 0,
                float(m.get("anomaly_score", 0.0) or 0.0),
                now,
            )
            for m in metrics
        ]
        with db.get_connection() as conn:
            conn.executemany(
                """
                INSERT INTO metrics (
                    id, dataset_id, timestamp, service_name, host_name,
                    metric_name, metric_value, unit, attributes_json,
                    is_anomaly, anomaly_score, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()

    @staticmethod
    def get_metrics(
        dataset_id: Optional[str] = None,
        service_name: Optional[str] = None,
        metric_name: Optional[str] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        query = "SELECT * FROM metrics WHERE 1=1"
        params: List[Any] = []
        if dataset_id:
            query += " AND dataset_id = ?"
            params.append(dataset_id)
        if service_name:
            query += " AND service_name = ?"
            params.append(service_name)
        if metric_name:
            query += " AND metric_name = ?"
            params.append(metric_name)

        query += " ORDER BY timestamp ASC LIMIT ?"
        params.append(limit)

        with db.get_connection() as conn:
            cur = conn.execute(query, params)
            res = []
            for r in cur.fetchall():
                d = dict(r)
                d["attributes"] = json.loads(d.get("attributes_json") or "{}")
                d["is_anomaly"] = bool(d.get("is_anomaly"))
                res.append(d)
            return res

    # -------------------------------------------------------------------------
    # Traces
    # -------------------------------------------------------------------------
    @staticmethod
    def insert_traces(traces: List[Dict[str, Any]]) -> None:
        if not traces:
            return
        now = utc_now_iso()
        rows = [
            (
                t.get("id") or str(uuid.uuid4()),
                t["dataset_id"],
                t.get("trace_id") or str(uuid.uuid4()),
                t.get("span_id") or str(uuid.uuid4()),
                t.get("parent_span_id"),
                t.get("service_name", "unknown") or "unknown",
                t.get("operation_name", "request") or "request",
                _format_ts(t.get("start_time"), now),
                _format_ts(t.get("end_time"), now) if t.get("end_time") else None,
                float(t.get("duration_ms", 0.0) or 0.0),
                str(t.get("status_code", "200")),
                json.dumps(t.get("attributes", {})),
                now,
            )
            for t in traces
        ]
        with db.get_connection() as conn:
            conn.executemany(
                """
                INSERT INTO traces (
                    id, dataset_id, trace_id, span_id, parent_span_id,
                    service_name, operation_name, start_time, end_time,
                    duration_ms, status_code, attributes_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()

    @staticmethod
    def get_traces(dataset_id: Optional[str] = None, trace_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        query = "SELECT * FROM traces WHERE 1=1"
        params: List[Any] = []
        if dataset_id:
            query += " AND dataset_id = ?"
            params.append(dataset_id)
        if trace_id:
            query += " AND trace_id = ?"
            params.append(trace_id)
        query += " ORDER BY start_time ASC LIMIT ?"
        params.append(limit)

        with db.get_connection() as conn:
            cur = conn.execute(query, params)
            res = []
            for r in cur.fetchall():
                d = dict(r)
                d["attributes"] = json.loads(d.get("attributes_json") or "{}")
                res.append(d)
            return res

    # -------------------------------------------------------------------------
    # Services & Dependencies (Topology)
    # -------------------------------------------------------------------------
    @staticmethod
    def upsert_service(name: str, service_type: str = "service", host: Optional[str] = None, description: Optional[str] = None) -> None:
        now = utc_now_iso()
        with db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO services (name, service_type, host, description, created_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    service_type = excluded.service_type,
                    host = excluded.host,
                    description = excluded.description
                """,
                (name, service_type, host or f"{name}-node", description or f"{name} service", now),
            )
            conn.commit()

    @staticmethod
    def upsert_dependency(source: str, target: str, dep_type: str = "sync_http", latency: float = 0.0, error_rate: float = 0.0) -> None:
        now = utc_now_iso()
        dep_id = f"{source}->{target}"
        with db.get_connection() as conn:
            # Ensure services exist first
            Repository.upsert_service(source)
            Repository.upsert_service(target)
            conn.execute(
                """
                INSERT INTO service_dependencies (id, source_service, target_service, dependency_type, latency_p95, error_rate, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    dependency_type = excluded.dependency_type,
                    latency_p95 = excluded.latency_p95,
                    error_rate = excluded.error_rate
                """,
                (dep_id, source, target, dep_type, latency, error_rate, now),
            )
            conn.commit()

    @staticmethod
    def get_service_graph() -> Dict[str, Any]:
        with db.get_connection() as conn:
            # Get services with live health / anomaly metrics
            cur_s = conn.execute("""
                SELECT s.*,
                    COALESCE(e.error_count, 0) as error_count,
                    COALESCE(e.anomaly_count, 0) as anomaly_count,
                    COALESCE(m.avg_latency, 0.0) as avg_latency
                FROM services s
                LEFT JOIN (
                    SELECT service_name,
                           SUM(CASE WHEN LOWER(severity) IN ('error', 'critical', 'fatal') THEN 1 ELSE 0 END) as error_count,
                           SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) as anomaly_count
                    FROM events
                    GROUP BY service_name
                ) e ON s.name = e.service_name
                LEFT JOIN (
                    SELECT service_name, AVG(metric_value) as avg_latency
                    FROM metrics
                    WHERE metric_name LIKE '%latency%'
                    GROUP BY service_name
                ) m ON s.name = m.service_name
            """)
            nodes = []
            for r in cur_s.fetchall():
                node = dict(r)
                # Compute health status
                if node["anomaly_count"] > 10 or node["error_count"] > 15:
                    status = "critical"
                elif node["anomaly_count"] > 0 or node["error_count"] > 0:
                    status = "degraded"
                else:
                    status = "healthy"
                node["status"] = status
                nodes.append(node)

            cur_e = conn.execute("SELECT * FROM service_dependencies")
            edges = [dict(r) for r in cur_e.fetchall()]

            return {"services": nodes, "dependencies": edges}

    # -------------------------------------------------------------------------
    # Incidents & Investigation
    # -------------------------------------------------------------------------
    @staticmethod
    def create_incident(
        dataset_id: str,
        title: str,
        summary: str,
        severity: str = "critical",
        status: str = "active",
        root_cause_service: Optional[str] = None,
        root_cause_type: Optional[str] = None,
        confidence_score: float = 0.0,
        uncertainty_level: str = "likely",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        duration_seconds: float = 0.0,
        affected_service_count: int = 1,
        incident_id: Optional[str] = None,
    ) -> str:
        inc_id = incident_id or str(uuid.uuid4())
        now = utc_now_iso()
        st = start_time or now
        with db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO incidents (
                    id, dataset_id, title, summary, severity, status,
                    root_cause_service, root_cause_type, confidence_score,
                    uncertainty_level, start_time, end_time, duration_seconds,
                    affected_service_count, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    inc_id, dataset_id, title, summary, severity, status,
                    root_cause_service, root_cause_type, confidence_score,
                    uncertainty_level, st, end_time or now, duration_seconds,
                    affected_service_count, now, now
                ),
            )
            conn.commit()
        return inc_id

    @staticmethod
    def get_incident(incident_id: str) -> Optional[Dict[str, Any]]:
        with db.get_connection() as conn:
            cur = conn.execute("SELECT * FROM incidents WHERE id = ?", (incident_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    @staticmethod
    def list_incidents(dataset_id: Optional[str] = None, status: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        query = "SELECT * FROM incidents WHERE 1=1"
        params: List[Any] = []
        if dataset_id:
            query += " AND dataset_id = ?"
            params.append(dataset_id)
        if status:
            query += " AND status = ?"
            params.append(status)
        query += " ORDER BY start_time DESC LIMIT ?"
        params.append(limit)

        with db.get_connection() as conn:
            cur = conn.execute(query, params)
            return [dict(r) for r in cur.fetchall()]

    @staticmethod
    def link_incident_events(incident_id: str, event_links: List[Dict[str, Any]]) -> None:
        if not event_links:
            return
        now = utc_now_iso()
        rows = [
            (
                str(uuid.uuid4()),
                incident_id,
                l["event_id"],
                float(l.get("correlation_score", 0.8)),
                l.get("correlation_reason", "Correlated event"),
                now,
            )
            for l in event_links
        ]
        with db.get_connection() as conn:
            conn.executemany(
                """
                INSERT INTO incident_events (id, incident_id, event_id, correlation_score, correlation_reason, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()

    @staticmethod
    def get_incident_events(incident_id: str) -> List[Dict[str, Any]]:
        with db.get_connection() as conn:
            cur = conn.execute(
                """
                SELECT e.*, ie.correlation_score, ie.correlation_reason
                FROM incident_events ie
                JOIN events e ON ie.event_id = e.id
                WHERE ie.incident_id = ?
                ORDER BY e.timestamp ASC
                """,
                (incident_id,),
            )
            res = []
            for r in cur.fetchall():
                d = dict(r)
                d["attributes"] = json.loads(d.get("attributes_json") or "{}")
                d["is_anomaly"] = bool(d.get("is_anomaly"))
                res.append(d)
            return res

    # -------------------------------------------------------------------------
    # RCA Candidates
    # -------------------------------------------------------------------------
    @staticmethod
    def insert_root_cause_candidates(candidates: List[Dict[str, Any]]) -> None:
        if not candidates:
            return
        now = utc_now_iso()
        rows = [
            (
                c.get("id") or str(uuid.uuid4()),
                c["incident_id"],
                int(c.get("rank", 1)),
                c.get("service_name", "unknown"),
                c.get("failure_type", "unknown"),
                float(c.get("confidence_score", 0.5)),
                c.get("uncertainty_assessment", "Moderate confidence based on temporal precedence"),
                json.dumps(c.get("evidence", [])),
                json.dumps(c.get("alternative_explanations", [])),
                1 if c.get("is_selected") else 0,
                now,
            )
            for c in candidates
        ]
        with db.get_connection() as conn:
            conn.executemany(
                """
                INSERT INTO root_cause_candidates (
                    id, incident_id, rank, service_name, failure_type,
                    confidence_score, uncertainty_assessment, evidence_json,
                    alternative_explanations_json, is_selected, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()

    @staticmethod
    def get_root_cause_candidates(incident_id: str) -> List[Dict[str, Any]]:
        with db.get_connection() as conn:
            cur = conn.execute(
                "SELECT * FROM root_cause_candidates WHERE incident_id = ? ORDER BY rank ASC",
                (incident_id,),
            )
            res = []
            for r in cur.fetchall():
                d = dict(r)
                d["evidence"] = json.loads(d.get("evidence_json") or "[]")
                d["alternative_explanations"] = json.loads(d.get("alternative_explanations_json") or "[]")
                d["is_selected"] = bool(d.get("is_selected"))
                res.append(d)
            return res

    # -------------------------------------------------------------------------
    # Impact Reports
    # -------------------------------------------------------------------------
    @staticmethod
    def upsert_impact_report(
        incident_id: str,
        direct_services: List[str],
        downstream_services: List[str],
        total_events_affected: int,
        error_count: int,
        avg_latency_increase_pct: float,
        user_impact_summary: str,
    ) -> None:
        now = utc_now_iso()
        rep_id = str(uuid.uuid4())
        with db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO impact_reports (
                    id, incident_id, direct_services_json, downstream_services_json,
                    total_events_affected, error_count, avg_latency_increase_pct,
                    user_impact_summary, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(incident_id) DO UPDATE SET
                    direct_services_json = excluded.direct_services_json,
                    downstream_services_json = excluded.downstream_services_json,
                    total_events_affected = excluded.total_events_affected,
                    error_count = excluded.error_count,
                    avg_latency_increase_pct = excluded.avg_latency_increase_pct,
                    user_impact_summary = excluded.user_impact_summary
                """,
                (
                    rep_id,
                    incident_id,
                    json.dumps(direct_services),
                    json.dumps(downstream_services),
                    total_events_affected,
                    error_count,
                    avg_latency_increase_pct,
                    user_impact_summary,
                    now,
                ),
            )
            conn.commit()

    @staticmethod
    def get_impact_report(incident_id: str) -> Optional[Dict[str, Any]]:
        with db.get_connection() as conn:
            cur = conn.execute("SELECT * FROM impact_reports WHERE incident_id = ?", (incident_id,))
            row = cur.fetchone()
            if not row:
                return None
            d = dict(row)
            d["direct_services"] = json.loads(d.get("direct_services_json") or "[]")
            d["downstream_services"] = json.loads(d.get("downstream_services_json") or "[]")
            return d

    # -------------------------------------------------------------------------
    # Recommendations
    # -------------------------------------------------------------------------
    @staticmethod
    def insert_recommendations(recommendations: List[Dict[str, Any]]) -> None:
        if not recommendations:
            return
        now = utc_now_iso()
        rows = [
            (
                r.get("id") or str(uuid.uuid4()),
                r["incident_id"],
                r.get("priority", "high"),
                r["title"],
                r["description"],
                r.get("category", "investigation"),
                r.get("action_type", "manual_verification"),
                json.dumps(r.get("evidence_citations", [])),
                now,
            )
            for r in recommendations
        ]
        with db.get_connection() as conn:
            conn.executemany(
                """
                INSERT INTO recommendations (
                    id, incident_id, priority, title, description,
                    category, action_type, evidence_citations_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()

    @staticmethod
    def get_recommendations(incident_id: str) -> List[Dict[str, Any]]:
        with db.get_connection() as conn:
            cur = conn.execute(
                "SELECT * FROM recommendations WHERE incident_id = ? ORDER BY priority ASC, created_at ASC",
                (incident_id,),
            )
            res = []
            for r in cur.fetchall():
                d = dict(r)
                d["evidence_citations"] = json.loads(d.get("evidence_citations_json") or "[]")
                res.append(d)
            return res

    # -------------------------------------------------------------------------
    # Analysis Jobs
    # -------------------------------------------------------------------------
    @staticmethod
    def create_analysis_job(dataset_id: str) -> str:
        job_id = str(uuid.uuid4())
        now = utc_now_iso()
        with db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO analysis_jobs (id, dataset_id, status, progress_pct, current_step, started_at, created_at)
                VALUES (?, ?, 'pending', 0, 'Job queued', ?, ?)
                """,
                (job_id, dataset_id, now, now),
            )
            conn.commit()
        return job_id

    @staticmethod
    def update_analysis_job(
        job_id: str,
        status: str,
        progress_pct: int,
        current_step: str,
        result: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
    ) -> None:
        now = utc_now_iso()
        with db.get_connection() as conn:
            conn.execute(
                """
                UPDATE analysis_jobs
                SET status = ?, progress_pct = ?, current_step = ?,
                    result_json = COALESCE(?, result_json),
                    error_message = ?,
                    completed_at = CASE WHEN ? IN ('completed', 'failed') THEN ? ELSE completed_at END
                WHERE id = ?
                """,
                (
                    status,
                    progress_pct,
                    current_step,
                    json.dumps(result) if result is not None else None,
                    error_message,
                    status,
                    now,
                    job_id,
                ),
            )
            conn.commit()

    @staticmethod
    def get_analysis_job(job_id: str) -> Optional[Dict[str, Any]]:
        with db.get_connection() as conn:
            cur = conn.execute("SELECT * FROM analysis_jobs WHERE id = ?", (job_id,))
            row = cur.fetchone()
            if not row:
                return None
            d = dict(row)
            d["result"] = json.loads(d.get("result_json") or "{}")
            return d

    # -------------------------------------------------------------------------
    # Model Checkpoints & Benchmark
    # -------------------------------------------------------------------------
    @staticmethod
    def save_model_checkpoint(
        model_name: str,
        model_type: str,
        trained_on_dataset_ids: List[str],
        hyperparameters: Dict[str, Any],
        evaluation_metrics: Dict[str, Any],
        file_path: Optional[str] = None,
    ) -> str:
        checkpoint_id = str(uuid.uuid4())
        now = utc_now_iso()
        with db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO model_checkpoints (
                    id, model_name, model_type, trained_on_dataset_ids,
                    hyperparameters_json, evaluation_metrics_json, file_path, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'ready', ?)
                """,
                (
                    checkpoint_id,
                    model_name,
                    model_type,
                    json.dumps(trained_on_dataset_ids),
                    json.dumps(hyperparameters),
                    json.dumps(evaluation_metrics),
                    file_path,
                    now,
                ),
            )
            conn.commit()
        return checkpoint_id

    @staticmethod
    def get_latest_model_checkpoint(model_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        query = "SELECT * FROM model_checkpoints"
        params: List[Any] = []
        if model_type:
            query += " WHERE model_type = ?"
            params.append(model_type)
        query += " ORDER BY created_at DESC LIMIT 1"

        with db.get_connection() as conn:
            cur = conn.execute(query, params)
            row = cur.fetchone()
            if not row:
                return None
            d = dict(row)
            d["hyperparameters"] = json.loads(d.get("hyperparameters_json") or "{}")
            d["evaluation_metrics"] = json.loads(d.get("evaluation_metrics_json") or "{}")
            d["trained_on_dataset_ids"] = json.loads(d.get("trained_on_dataset_ids") or "[]")
            return d

    # -------------------------------------------------------------------------
    # Dashboard Summary KPI Aggregations
    # -------------------------------------------------------------------------
    @staticmethod
    def get_dashboard_summary() -> Dict[str, Any]:
        with db.get_connection() as conn:
            # Active and total incidents
            cur_inc = conn.execute("""
                SELECT
                    COUNT(*) as total_incidents,
                    SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) as active_incidents,
                    SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) as critical_incidents,
                    AVG(duration_seconds) as avg_duration
                FROM incidents
            """).fetchone()

            # Events & anomalies
            cur_evt = conn.execute("""
                SELECT
                    COUNT(*) as total_events,
                    SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) as total_anomalies,
                    SUM(CASE WHEN LOWER(severity) IN ('error', 'critical', 'fatal') THEN 1 ELSE 0 END) as total_errors
                FROM events
            """).fetchone()

            # Total services
            cur_srv = conn.execute("SELECT COUNT(*) FROM services").fetchone()

            # Datasets count
            cur_ds = conn.execute("SELECT COUNT(*) FROM datasets").fetchone()

            # Recent incidents
            cur_rec = conn.execute("""
                SELECT id, title, severity, status, root_cause_service, confidence_score, start_time
                FROM incidents
                ORDER BY start_time DESC LIMIT 5
            """)
            recent_incidents = [dict(r) for r in cur_rec.fetchall()]

            # Severity distribution
            cur_sev = conn.execute("""
                SELECT severity, COUNT(*) as count
                FROM incidents
                GROUP BY severity
            """)
            sev_dist = {r["severity"]: r["count"] for r in cur_sev.fetchall()}

            return {
                "active_incidents": cur_inc["active_incidents"] or 0,
                "total_incidents": cur_inc["total_incidents"] or 0,
                "critical_incidents": cur_inc["critical_incidents"] or 0,
                "avg_resolution_seconds": round(cur_inc["avg_duration"] or 0.0, 1),
                "total_events": cur_evt["total_events"] or 0,
                "total_anomalies": cur_evt["total_anomalies"] or 0,
                "total_errors": cur_evt["total_errors"] or 0,
                "services_monitored": cur_srv[0] or 0,
                "datasets_count": cur_ds[0] or 0,
                "severity_distribution": sev_dist,
                "recent_incidents": recent_incidents,
            }

    # -------------------------------------------------------------------------
    # Dataset Telemetry Study Report Generator
    # -------------------------------------------------------------------------
    @staticmethod
    def generate_dataset_study_report(dataset_id: str) -> Optional[Dict[str, Any]]:
        ds = Repository.get_dataset(dataset_id)
        if not ds:
            return None

        with db.get_connection() as conn:
            # Events breakdown
            cur_sev = conn.execute("""
                SELECT severity, COUNT(*) as count,
                       SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) as anom_count
                FROM events WHERE dataset_id = ?
                GROUP BY severity
            """, (dataset_id,)).fetchall()
            sev_breakdown = {r["severity"]: r["count"] for r in cur_sev}
            sev_anoms = sum(r["anom_count"] or 0 for r in cur_sev)

            # Services in dataset
            cur_srv = conn.execute("""
                SELECT service_name, COUNT(*) as count,
                       SUM(CASE WHEN LOWER(severity) IN ('error', 'critical', 'fatal') THEN 1 ELSE 0 END) as errs
                FROM events WHERE dataset_id = ?
                GROUP BY service_name ORDER BY errs DESC, count DESC
            """, (dataset_id,)).fetchall()
            services_found = [dict(r) for r in cur_srv]

            # Metrics profile
            cur_met = conn.execute("""
                SELECT metric_name, service_name,
                       COUNT(*) as points_count,
                       MIN(metric_value) as min_val,
                       MAX(metric_value) as max_val,
                       AVG(metric_value) as avg_val,
                       SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) as anoms
                FROM metrics WHERE dataset_id = ?
                GROUP BY metric_name, service_name
                ORDER BY anoms DESC, points_count DESC
            """, (dataset_id,)).fetchall()
            metric_profiles = [dict(r) for r in cur_met]
            metric_anoms = sum(r["anoms"] or 0 for r in cur_met)

            # Traces count
            cur_tr = conn.execute("SELECT COUNT(*) as count, AVG(duration_ms) as avg_lat FROM traces WHERE dataset_id = ?", (dataset_id,)).fetchone()
            trace_count = cur_tr["count"] or 0
            avg_trace_lat = round(cur_tr["avg_lat"] or 0.0, 1)

            # Associated incidents
            cur_inc = conn.execute("SELECT * FROM incidents WHERE dataset_id = ? ORDER BY start_time DESC", (dataset_id,)).fetchall()
            incidents = [dict(r) for r in cur_inc]

            # Time range
            cur_time = conn.execute("""
                SELECT MIN(timestamp) as min_ts, MAX(timestamp) as max_ts
                FROM (
                    SELECT timestamp FROM events WHERE dataset_id = ?
                    UNION ALL
                    SELECT timestamp FROM metrics WHERE dataset_id = ?
                )
            """, (dataset_id, dataset_id)).fetchone()
            time_start = cur_time["min_ts"] or ds["created_at"]
            time_end = cur_time["max_ts"] or ds["updated_at"]

            total_anomalies = sev_anoms + metric_anoms

            # Generate automated forensic insights
            findings = []
            if incidents:
                for inc in incidents:
                    findings.append(f"Incident Flagged: '{inc['title']}' with {round(inc['confidence_score'] * 100, 1)}% confidence isolated to root cause '{inc['root_cause_service']}'.")
            elif total_anomalies > 0:
                findings.append(f"Detected {total_anomalies} telemetry anomalies requiring operational review.")
            else:
                findings.append(f"All {ds.get('valid_records', 0)} telemetry records evaluated within healthy operational thresholds. No anomalies detected.")

            if services_found:
                top_err_srv = services_found[0]
                if top_err_srv.get("errs", 0) > 0:
                    findings.append(f"Service '{top_err_srv['service_name']}' generated the highest error volume ({top_err_srv['errs']} errors).")

            if metric_profiles:
                top_m = metric_profiles[0]
                findings.append(f"Metric '{top_m['metric_name']}' on {top_m['service_name']} peaked at {round(top_m['max_val'], 1)} (baseline avg: {round(top_m['avg_val'], 1)}).")

            # Assemble study report object
            report = {
                "dataset_id": dataset_id,
                "dataset_name": ds["name"],
                "description": ds["description"],
                "source_type": ds["source_type"],
                "file_path": ds["file_path"],
                "total_records": ds["record_count"],
                "valid_records": ds["valid_records"],
                "invalid_records": ds["invalid_records"],
                "time_window": {"start": time_start, "end": time_end},
                "total_anomalies": total_anomalies,
                "events_count": sum(sev_breakdown.values()),
                "severity_breakdown": sev_breakdown,
                "services_count": len(services_found),
                "services": services_found,
                "metrics_count": len(metric_profiles),
                "metric_profiles": metric_profiles,
                "traces_count": trace_count,
                "avg_trace_latency_ms": avg_trace_lat,
                "incidents_count": len(incidents),
                "incidents": incidents,
                "findings": findings,
                "generated_at": utc_now_iso(),
            }

            # Generate formatted markdown
            md = f"""# TELEMETRY DATASET STUDY REPORT
**Dataset**: `{ds['name']}`  
**Dataset ID**: `{dataset_id}`  
**Format**: `{ds['source_type'].upper()}` | **Status**: `{ds['status'].upper()}`  
**Analyzed Records**: {ds['valid_records']} valid / {ds['invalid_records']} invalid  
**Generated At**: {report['generated_at']}  

---

## 1. Executive Summary & Forensic Findings
"""
            for f in findings:
                md += f"- {f}\n"

            md += f"""
---

## 2. Telemetry Volume & Severity Distribution
- **Total Correlated Anomalies**: `{total_anomalies}`
- **Events Ingested**: {report['events_count']}
"""
            for sev, cnt in sev_breakdown.items():
                md += f"  - `{sev.upper()}`: {cnt}\n"

            md += f"""- **Distributed Traces Ingested**: {trace_count} (Avg duration: {avg_trace_lat} ms)

---

## 3. Discovered Services & Error Profiles
| Service Name | Total Events | Errors Recorded | Status |
|:---|:---|:---|:---|
"""
            for s in services_found[:10]:
                st = "CRITICAL" if s["errs"] > 10 else ("DEGRADED" if s["errs"] > 0 else "HEALTHY")
                md += f"| `{s['service_name']}` | {s['count']} | {s['errs']} | `{st}` |\n"

            if metric_profiles:
                md += """
---

## 4. Key Metric Time-Series Profiles
| Metric Name | Service | Min | Avg | Max | Anomalies |
|:---|:---|:---|:---|:---|:---|
"""
                for m in metric_profiles[:10]:
                    md += f"| `{m['metric_name']}` | `{m['service_name']}` | {round(m['min_val'], 1)} | {round(m['avg_val'], 1)} | {round(m['max_val'], 1)} | `{m['anoms']}` |\n"

            if incidents:
                md += """
---

## 5. Correlated Operational Incidents
"""
                for inc in incidents:
                    md += f"""### [{inc['severity'].upper()}] {inc['title']}
- **Root Cause**: `{inc['root_cause_service']}` ({inc.get('root_cause_type', 'N/A')})
- **Diagnostic Confidence**: {round(inc['confidence_score'] * 100, 1)}%
- **Summary**: {inc['summary']}
"""

            md += "\n---\n*Report generated by J.A.R.V.I.S. AIOps Diagnostic Engine*\n"
            report["markdown_report"] = md

            return report
