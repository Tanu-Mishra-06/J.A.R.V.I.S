"""
SQLite Database Layer for J.A.R.V.I.S.
Provides zero-dependency, robust relational storage with ACID transactions,
JSON attributes support, foreign keys, and dictionary-like row factories.
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


class Database:
    """Encapsulates SQLite connection management and repository methods."""

    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            # Default to jarvis.db in the backend root
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            db_path = os.path.join(base_dir, "jarvis.db")
        self.db_path = db_path
        self._init_tables()

    def get_connection(self) -> sqlite3.Connection:
        """Create and return a configured SQLite connection."""
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        return conn

    def _init_tables(self) -> None:
        """Create all required schema tables and indexes if they do not exist."""
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        with self.get_connection() as conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS datasets (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                source_type TEXT,
                file_path TEXT,
                record_count INTEGER DEFAULT 0,
                valid_records INTEGER DEFAULT 0,
                invalid_records INTEGER DEFAULT 0,
                status TEXT DEFAULT 'uploaded',
                error_message TEXT,
                metadata_json TEXT,
                is_demo INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS raw_records (
                id TEXT PRIMARY KEY,
                dataset_id TEXT NOT NULL,
                record_type TEXT NOT NULL,
                line_number INTEGER,
                raw_data TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                dataset_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                source TEXT,
                service_name TEXT NOT NULL,
                host_name TEXT,
                event_type TEXT,
                severity TEXT DEFAULT 'info',
                message TEXT NOT NULL,
                trace_id TEXT,
                span_id TEXT,
                parent_span_id TEXT,
                attributes_json TEXT,
                raw_record_id TEXT,
                is_anomaly INTEGER DEFAULT 0,
                anomaly_score REAL DEFAULT 0.0,
                created_at TEXT NOT NULL,
                FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS metrics (
                id TEXT PRIMARY KEY,
                dataset_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                service_name TEXT NOT NULL,
                host_name TEXT,
                metric_name TEXT NOT NULL,
                metric_value REAL NOT NULL,
                unit TEXT,
                attributes_json TEXT,
                is_anomaly INTEGER DEFAULT 0,
                anomaly_score REAL DEFAULT 0.0,
                created_at TEXT NOT NULL,
                FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS traces (
                id TEXT PRIMARY KEY,
                dataset_id TEXT NOT NULL,
                trace_id TEXT NOT NULL,
                span_id TEXT NOT NULL,
                parent_span_id TEXT,
                service_name TEXT NOT NULL,
                operation_name TEXT,
                start_time TEXT NOT NULL,
                end_time TEXT,
                duration_ms REAL DEFAULT 0.0,
                status_code TEXT DEFAULT '200',
                attributes_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS services (
                name TEXT PRIMARY KEY,
                service_type TEXT,
                host TEXT,
                description TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS service_dependencies (
                id TEXT PRIMARY KEY,
                source_service TEXT NOT NULL,
                target_service TEXT NOT NULL,
                dependency_type TEXT DEFAULT 'sync_http',
                latency_p95 REAL DEFAULT 0.0,
                error_rate REAL DEFAULT 0.0,
                created_at TEXT NOT NULL,
                FOREIGN KEY (source_service) REFERENCES services(name) ON DELETE CASCADE,
                FOREIGN KEY (target_service) REFERENCES services(name) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS incidents (
                id TEXT PRIMARY KEY,
                dataset_id TEXT NOT NULL,
                title TEXT NOT NULL,
                summary TEXT,
                severity TEXT DEFAULT 'medium',
                status TEXT DEFAULT 'active',
                root_cause_service TEXT,
                root_cause_type TEXT,
                confidence_score REAL DEFAULT 0.0,
                uncertainty_level TEXT DEFAULT 'likely',
                start_time TEXT NOT NULL,
                end_time TEXT,
                duration_seconds REAL DEFAULT 0.0,
                affected_service_count INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS incident_events (
                id TEXT PRIMARY KEY,
                incident_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                correlation_score REAL DEFAULT 0.0,
                correlation_reason TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (incident_id) REFERENCES incidents(id) ON DELETE CASCADE,
                FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS root_cause_candidates (
                id TEXT PRIMARY KEY,
                incident_id TEXT NOT NULL,
                rank INTEGER NOT NULL,
                service_name TEXT NOT NULL,
                failure_type TEXT NOT NULL,
                confidence_score REAL NOT NULL,
                uncertainty_assessment TEXT,
                evidence_json TEXT,
                alternative_explanations_json TEXT,
                is_selected INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                FOREIGN KEY (incident_id) REFERENCES incidents(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS impact_reports (
                id TEXT PRIMARY KEY,
                incident_id TEXT NOT NULL UNIQUE,
                direct_services_json TEXT,
                downstream_services_json TEXT,
                total_events_affected INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0,
                avg_latency_increase_pct REAL DEFAULT 0.0,
                user_impact_summary TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (incident_id) REFERENCES incidents(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS recommendations (
                id TEXT PRIMARY KEY,
                incident_id TEXT NOT NULL,
                priority TEXT DEFAULT 'high',
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                category TEXT DEFAULT 'investigation',
                action_type TEXT DEFAULT 'manual_verification',
                evidence_citations_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (incident_id) REFERENCES incidents(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS analysis_jobs (
                id TEXT PRIMARY KEY,
                dataset_id TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                progress_pct INTEGER DEFAULT 0,
                current_step TEXT,
                result_json TEXT,
                error_message TEXT,
                started_at TEXT,
                completed_at TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS model_checkpoints (
                id TEXT PRIMARY KEY,
                model_name TEXT NOT NULL,
                model_type TEXT NOT NULL,
                trained_on_dataset_ids TEXT,
                hyperparameters_json TEXT,
                evaluation_metrics_json TEXT,
                file_path TEXT,
                status TEXT DEFAULT 'ready',
                created_at TEXT NOT NULL
            );

            -- Indexes for fast queries
            CREATE INDEX IF NOT EXISTS idx_events_dataset ON events(dataset_id);
            CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
            CREATE INDEX IF NOT EXISTS idx_events_service ON events(service_name);
            CREATE INDEX IF NOT EXISTS idx_events_severity ON events(severity);
            CREATE INDEX IF NOT EXISTS idx_metrics_dataset ON metrics(dataset_id);
            CREATE INDEX IF NOT EXISTS idx_metrics_ts_srv ON metrics(service_name, metric_name, timestamp);
            CREATE INDEX IF NOT EXISTS idx_traces_dataset ON traces(dataset_id);
            CREATE INDEX IF NOT EXISTS idx_traces_trace_id ON traces(trace_id);
            CREATE INDEX IF NOT EXISTS idx_incidents_dataset ON incidents(dataset_id);
            CREATE INDEX IF NOT EXISTS idx_incident_events_incident ON incident_events(incident_id);
            CREATE INDEX IF NOT EXISTS idx_rca_incident ON root_cause_candidates(incident_id);
            CREATE INDEX IF NOT EXISTS idx_jobs_dataset ON analysis_jobs(dataset_id);
            """)


# Global singleton instance
db = Database()
