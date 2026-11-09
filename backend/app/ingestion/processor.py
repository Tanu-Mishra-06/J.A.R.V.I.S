"""Dataset processing pipeline — detects format, parses, and persists normalised records."""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.database.repository import Repository
from app.ingestion.adapters.auto_detect import detect_adapter, detect_source_type

logger = logging.getLogger(__name__)


def process_dataset(dataset_id: str) -> Dict[str, Any]:
    """Main entry point: reads, parses, normalizes, and persists data for a dataset.

    Updates the Dataset record with status and counts throughout the process.
    Preserves raw records for audit provenance.
    """
    dataset = Repository.get_dataset(dataset_id)
    if not dataset:
        logger.error("Dataset %s not found", dataset_id)
        return {"status": "error", "error": f"Dataset {dataset_id} not found"}

    file_path = dataset.get("file_path")
    if not file_path or not os.path.isfile(file_path):
        Repository.update_dataset_stats(
            dataset_id=dataset_id,
            record_count=0,
            valid_records=0,
            invalid_records=0,
            status="error",
            error_message=f"File not found: {file_path}",
        )
        return {"status": "error", "error": f"File not found: {file_path}"}

    # Mark as processing
    Repository.update_dataset_stats(
        dataset_id=dataset_id,
        record_count=dataset.get("record_count", 0),
        valid_records=dataset.get("valid_records", 0),
        invalid_records=dataset.get("invalid_records", 0),
        status="processing",
    )

    try:
        adapter = detect_adapter(file_path)
        if adapter is None:
            Repository.update_dataset_stats(
                dataset_id=dataset_id,
                record_count=0,
                valid_records=0,
                invalid_records=0,
                status="error",
                error_message="Could not detect file format. Supported: CSV, JSON, JSONL, log/txt.",
            )
            return {"status": "error", "error": "Could not detect file format."}

        source_type = detect_source_type(file_path)

        # Parse records
        result = adapter.parse(file_path, dataset_id)

        # Insert events
        if result.events:
            Repository.insert_events(result.events)
            # Register discovered services
            for ev in result.events:
                srv = ev.get("service_name")
                if srv and srv != "unknown":
                    Repository.upsert_service(srv, host=ev.get("host_name"))

        # Insert metrics
        if result.metrics:
            Repository.insert_metrics(result.metrics)
            for m in result.metrics:
                srv = m.get("service_name")
                if srv and srv != "unknown":
                    Repository.upsert_service(srv, host=m.get("host_name"))

        # Insert traces
        if result.traces:
            Repository.insert_traces(result.traces)
            # Build service dependencies from trace parent-child relationships
            span_map = {t["span_id"]: t for t in result.traces if t.get("span_id")}
            for t in result.traces:
                parent_id = t.get("parent_span_id")
                if parent_id and parent_id in span_map:
                    parent_span = span_map[parent_id]
                    src_srv = parent_span.get("service_name")
                    tgt_srv = t.get("service_name")
                    if src_srv and tgt_srv and src_srv != tgt_srv:
                        latency = float(t.get("duration_ms", 10.0))
                        is_err = 1.0 if str(t.get("status_code", "200")).startswith(("4", "5")) else 0.0
                        Repository.upsert_dependency(src_srv, tgt_srv, dep_type="sync_rpc", latency=latency, error_rate=is_err)

        total_valid = len(result.events) + len(result.metrics) + len(result.traces)
        total_invalid = len(result.errors)
        total_records = total_valid + total_invalid

        # Store raw records sample for provenance
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                raw_sample = []
                for idx, line in enumerate(f):
                    if idx >= 50:  # store first 50 lines for fast audit
                        break
                    raw_sample.append({
                        "dataset_id": dataset_id,
                        "record_type": source_type,
                        "line_number": idx + 1,
                        "raw_data": {"line": line.strip()},
                    })
                Repository.insert_raw_records(raw_sample)
        except Exception as e:
            logger.warning("Could not persist raw sample: %s", e)

        Repository.update_dataset_stats(
            dataset_id=dataset_id,
            record_count=total_records,
            valid_records=total_valid,
            invalid_records=total_invalid,
            status="processed",
        )

        return {
            "status": "success",
            "dataset_id": dataset_id,
            "source_type": source_type,
            "total_records": total_records,
            "valid_records": total_valid,
            "invalid_records": total_invalid,
            "events_count": len(result.events),
            "metrics_count": len(result.metrics),
            "traces_count": len(result.traces),
            "errors": result.errors[:10],
        }

    except Exception as exc:
        logger.exception("Error processing dataset %s: %s", dataset_id, exc)
        Repository.update_dataset_stats(
            dataset_id=dataset_id,
            record_count=0,
            valid_records=0,
            invalid_records=0,
            status="error",
            error_message=str(exc),
        )
        return {"status": "error", "error": str(exc)}
