"""CSV ingestion adapter — handles metric-style and log-style CSV files."""

import csv
import json
import uuid
from datetime import datetime, timezone
from typing import Optional

from app.ingestion.adapters.base import BaseAdapter, ParseResult

# Maps common column names (lowercased) to our normalized field names
_TIMESTAMP_ALIASES = {"timestamp", "time", "date", "datetime", "ts", "@timestamp", "event_time", "log_time"}
_SEVERITY_ALIASES = {"severity", "level", "priority", "log_level", "loglevel", "sev"}
_SERVICE_ALIASES = {"service", "service_name", "component", "app", "application", "source_service"}
_HOST_ALIASES = {"host", "host_name", "hostname", "node", "server", "machine"}
_MESSAGE_ALIASES = {"message", "msg", "log", "content", "body", "text", "log_message", "description"}
_EVENT_TYPE_ALIASES = {"event_type", "type", "category", "event", "action"}
_METRIC_NAME_ALIASES = {"metric", "metric_name", "name", "measurement"}
_METRIC_VALUE_ALIASES = {"value", "metric_value", "val", "reading", "amount"}
_TRACE_ID_ALIASES = {"trace_id", "traceid", "trace"}
_SPAN_ID_ALIASES = {"span_id", "spanid", "span"}


def _find_column(headers: list[str], aliases: set[str]) -> Optional[str]:
    """Return the first header that matches any alias, or None."""
    lower_map = {h.lower().strip(): h for h in headers}
    for alias in aliases:
        if alias in lower_map:
            return lower_map[alias]
    return None


def _parse_timestamp(value: str) -> Optional[datetime]:
    """Try several common timestamp formats."""
    if not value:
        return None
    formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%d/%b/%Y:%H:%M:%S",
        "%b %d %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(value.strip(), fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    # Try numeric epoch (seconds)
    try:
        epoch = float(value)
        return datetime.fromtimestamp(epoch, tz=timezone.utc)
    except (ValueError, OSError):
        pass
    return None


def _normalize_severity(raw: Optional[str]) -> str:
    if not raw:
        return "info"
    raw_lower = raw.strip().lower()
    mapping = {
        "debug": "debug", "trace": "debug",
        "info": "info", "information": "info", "notice": "info",
        "warn": "warning", "warning": "warning",
        "error": "error", "err": "error", "severe": "error", "fatal": "critical",
        "critical": "critical", "crit": "critical", "alert": "critical", "emergency": "critical", "emerg": "critical",
    }
    return mapping.get(raw_lower, "info")


def _is_numeric(value: str) -> bool:
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


class CsvAdapter(BaseAdapter):
    """Parses CSV files, auto-detecting columns and distinguishing metric vs log data."""

    def detect(self, file_path: str, sample: str) -> bool:
        if file_path.lower().endswith(".csv"):
            return True
        # Sniff — if comma-separated with at least 2 columns, treat as CSV
        try:
            dialect = csv.Sniffer().sniff(sample)
            return dialect.delimiter == ","
        except csv.Error:
            return False

    def parse(self, file_path: str, dataset_id: str) -> ParseResult:
        result = ParseResult()

        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as fh:
                reader = csv.DictReader(fh)
                if reader.fieldnames is None:
                    result.errors.append("CSV file has no header row")
                    return result

                headers = list(reader.fieldnames)

                # Discover column mapping
                ts_col = _find_column(headers, _TIMESTAMP_ALIASES)
                sev_col = _find_column(headers, _SEVERITY_ALIASES)
                svc_col = _find_column(headers, _SERVICE_ALIASES)
                host_col = _find_column(headers, _HOST_ALIASES)
                msg_col = _find_column(headers, _MESSAGE_ALIASES)
                etype_col = _find_column(headers, _EVENT_TYPE_ALIASES)
                mname_col = _find_column(headers, _METRIC_NAME_ALIASES)
                mval_col = _find_column(headers, _METRIC_VALUE_ALIASES)
                trace_col = _find_column(headers, _TRACE_ID_ALIASES)
                span_col = _find_column(headers, _SPAN_ID_ALIASES)

                # Heuristic: if metric_name + metric_value columns exist, treat as metrics
                is_metric_csv = (mname_col is not None and mval_col is not None)

                for line_no, row in enumerate(reader, start=2):
                    try:
                        ts = _parse_timestamp(row.get(ts_col, "")) if ts_col else None

                        if is_metric_csv:
                            raw_val = row.get(mval_col, "")
                            if not _is_numeric(raw_val):
                                result.errors.append(f"Line {line_no}: non-numeric metric value '{raw_val}'")
                                continue
                            result.metrics.append({
                                "id": str(uuid.uuid4()),
                                "dataset_id": dataset_id,
                                "timestamp": ts,
                                "service_name": row.get(svc_col, "") if svc_col else None,
                                "host_name": row.get(host_col, "") if host_col else None,
                                "metric_name": row.get(mname_col, "unknown"),
                                "metric_value": float(raw_val),
                                "unit": row.get("unit") or row.get("Unit"),
                                "attributes_json": json.dumps({k: v for k, v in row.items()
                                                               if k not in {ts_col, mname_col, mval_col, svc_col, host_col}}),
                            })
                        else:
                            # Treat as event / log row
                            raw_json = json.dumps(row)
                            severity_raw = row.get(sev_col, "") if sev_col else ""
                            result.events.append({
                                "id": str(uuid.uuid4()),
                                "dataset_id": dataset_id,
                                "timestamp": ts,
                                "service_name": row.get(svc_col, "") if svc_col else None,
                                "host_name": row.get(host_col, "") if host_col else None,
                                "event_type": row.get(etype_col, "") if etype_col else None,
                                "severity": _normalize_severity(severity_raw),
                                "message": row.get(msg_col, "") if msg_col else None,
                                "trace_id": row.get(trace_col, "") if trace_col else None,
                                "span_id": row.get(span_col, "") if span_col else None,
                                "raw_record_json": raw_json,
                            })
                    except Exception as exc:
                        result.errors.append(f"Line {line_no}: {exc}")

        except Exception as exc:
            result.errors.append(f"Failed to read CSV: {exc}")

        return result
