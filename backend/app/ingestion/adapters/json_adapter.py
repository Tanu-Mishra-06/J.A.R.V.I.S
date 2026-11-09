"""JSON / JSONL ingestion adapter — handles single objects, arrays, and line-delimited JSON."""

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from app.ingestion.adapters.base import BaseAdapter, ParseResult

# Field-name aliases (same normalisation philosophy as the CSV adapter)
_TS_KEYS = {"timestamp", "time", "date", "datetime", "ts", "@timestamp", "event_time", "log_time", "start_time"}
_SEV_KEYS = {"severity", "level", "priority", "log_level", "loglevel", "sev"}
_SVC_KEYS = {"service", "service_name", "component", "app", "application", "source_service"}
_HOST_KEYS = {"host", "host_name", "hostname", "node", "server"}
_MSG_KEYS = {"message", "msg", "log", "content", "body", "text", "log_message"}
_ETYPE_KEYS = {"event_type", "type", "category", "event", "action"}
_MNAME_KEYS = {"metric", "metric_name", "name", "measurement"}
_MVAL_KEYS = {"value", "metric_value", "val", "reading"}
_TRACE_KEYS = {"trace_id", "traceid", "trace"}
_SPAN_KEYS = {"span_id", "spanid", "span"}
_PARENT_SPAN_KEYS = {"parent_span_id", "parent_spanid", "parent_span"}
_OP_KEYS = {"operation", "operation_name", "op", "method", "endpoint"}
_DUR_KEYS = {"duration", "duration_ms", "elapsed", "latency", "response_time"}
_STATUS_KEYS = {"status", "status_code", "http_status"}


def _get(obj: dict, aliases: set[str]) -> Optional[Any]:
    """Return the value of the first key that matches any alias."""
    lower_map = {k.lower(): k for k in obj}
    for alias in aliases:
        if alias in lower_map:
            val = obj[lower_map[alias]]
            if val is not None and str(val).strip():
                return val
    return None


def _parse_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except (OSError, ValueError):
            return None
    s = str(value).strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        except ValueError:
            continue
    try:
        return datetime.fromtimestamp(float(s), tz=timezone.utc)
    except (ValueError, OSError):
        return None


def _norm_sev(raw: Any) -> str:
    if not raw:
        return "info"
    s = str(raw).strip().lower()
    m = {
        "debug": "debug", "trace": "debug",
        "info": "info", "information": "info", "notice": "info",
        "warn": "warning", "warning": "warning",
        "error": "error", "err": "error", "severe": "error",
        "fatal": "critical", "critical": "critical", "crit": "critical",
        "alert": "critical", "emergency": "critical",
    }
    return m.get(s, "info")


def _is_trace_record(obj: dict) -> bool:
    """Heuristic: it's a trace/span if it has both trace_id and span_id."""
    return _get(obj, _TRACE_KEYS) is not None and _get(obj, _SPAN_KEYS) is not None and _get(obj, _OP_KEYS) is not None


def _is_metric_record(obj: dict) -> bool:
    """Heuristic: it's a metric if it has a metric_name and a numeric value."""
    mname = _get(obj, _MNAME_KEYS)
    mval = _get(obj, _MVAL_KEYS)
    if mname is None or mval is None:
        return False
    try:
        float(mval)
        return True
    except (ValueError, TypeError):
        return False


class JsonAdapter(BaseAdapter):
    """Handles .json (single object or array) and .jsonl (one JSON object per line)."""

    def detect(self, file_path: str, sample: str) -> bool:
        lower = file_path.lower()
        if lower.endswith(".json") or lower.endswith(".jsonl"):
            return True
        stripped = sample.strip()
        return stripped.startswith("{") or stripped.startswith("[")

    def parse(self, file_path: str, dataset_id: str) -> ParseResult:
        result = ParseResult()
        records: list[dict] = []

        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as fh:
                content = fh.read().strip()

            # Try full JSON parse first (array or single object)
            try:
                data = json.loads(content)
                if isinstance(data, list):
                    records = [r for r in data if isinstance(r, dict)]
                elif isinstance(data, dict):
                    records = [data]
            except json.JSONDecodeError:
                # Fall back to line-delimited JSON
                for line_no, line in enumerate(content.splitlines(), start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        if isinstance(obj, dict):
                            records.append(obj)
                    except json.JSONDecodeError as exc:
                        result.errors.append(f"Line {line_no}: invalid JSON — {exc}")
        except Exception as exc:
            result.errors.append(f"Failed to read file: {exc}")
            return result

        # Classify each record
        for idx, obj in enumerate(records):
            try:
                ts = _parse_ts(_get(obj, _TS_KEYS))

                if _is_trace_record(obj):
                    end_ts = _parse_ts(_get(obj, {"end_time", "end", "finish_time"}))
                    dur = _get(obj, _DUR_KEYS)
                    result.traces.append({
                        "id": str(uuid.uuid4()),
                        "dataset_id": dataset_id,
                        "trace_id": str(_get(obj, _TRACE_KEYS)),
                        "span_id": str(_get(obj, _SPAN_KEYS)),
                        "parent_span_id": str(v) if (v := _get(obj, _PARENT_SPAN_KEYS)) else None,
                        "service_name": str(v) if (v := _get(obj, _SVC_KEYS)) else None,
                        "operation_name": str(v) if (v := _get(obj, _OP_KEYS)) else None,
                        "start_time": ts,
                        "end_time": end_ts,
                        "duration_ms": float(dur) if dur is not None else None,
                        "status_code": str(v).lower() if (v := _get(obj, _STATUS_KEYS)) else None,
                        "attributes_json": json.dumps(obj),
                    })
                elif _is_metric_record(obj):
                    result.metrics.append({
                        "id": str(uuid.uuid4()),
                        "dataset_id": dataset_id,
                        "timestamp": ts,
                        "service_name": str(v) if (v := _get(obj, _SVC_KEYS)) else None,
                        "host_name": str(v) if (v := _get(obj, _HOST_KEYS)) else None,
                        "metric_name": str(_get(obj, _MNAME_KEYS)),
                        "metric_value": float(_get(obj, _MVAL_KEYS)),
                        "unit": str(v) if (v := _get(obj, {"unit", "units"})) else None,
                        "attributes_json": json.dumps(obj),
                    })
                else:
                    result.events.append({
                        "id": str(uuid.uuid4()),
                        "dataset_id": dataset_id,
                        "timestamp": ts,
                        "service_name": str(v) if (v := _get(obj, _SVC_KEYS)) else None,
                        "host_name": str(v) if (v := _get(obj, _HOST_KEYS)) else None,
                        "event_type": str(v) if (v := _get(obj, _ETYPE_KEYS)) else None,
                        "severity": _norm_sev(_get(obj, _SEV_KEYS)),
                        "message": str(v) if (v := _get(obj, _MSG_KEYS)) else None,
                        "trace_id": str(v) if (v := _get(obj, _TRACE_KEYS)) else None,
                        "span_id": str(v) if (v := _get(obj, _SPAN_KEYS)) else None,
                        "parent_span_id": str(v) if (v := _get(obj, _PARENT_SPAN_KEYS)) else None,
                        "raw_record_json": json.dumps(obj),
                    })
            except Exception as exc:
                result.errors.append(f"Record {idx}: {exc}")

        return result
