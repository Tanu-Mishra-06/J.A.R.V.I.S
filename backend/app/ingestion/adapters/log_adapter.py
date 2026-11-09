"""Plain-text log file adapter — parses syslog, Apache/Nginx, and generic log formats."""

import json
import re
import uuid
from datetime import datetime, timezone
from typing import Optional

from app.ingestion.adapters.base import BaseAdapter, ParseResult

# ─── Regex patterns for common log formats ───────────────────────────────────

# Syslog: "Nov  9 12:34:56 myhost sshd[1234]: Failed password ..."
_SYSLOG_RE = re.compile(
    r"^(?P<ts>[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+"
    r"(?P<host>\S+)\s+"
    r"(?P<service>\S+?)(?:\[\d+\])?:\s+"
    r"(?P<message>.+)$"
)

# ISO-prefixed: "2024-03-15T10:23:01.123Z [ERROR] auth-service - Connection refused"
_ISO_LEVEL_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)\s+"
    r"\[?(?P<level>DEBUG|TRACE|INFO|NOTICE|WARN(?:ING)?|ERROR|ERR|SEVERE|FATAL|CRITICAL|CRIT|ALERT|EMERG(?:ENCY)?)\]?\s+"
    r"(?:(?P<service>\S+)\s+[-–—]\s+)?"
    r"(?P<message>.+)$",
    re.IGNORECASE,
)

# Generic bracketed level: "[ERROR] something happened"
_BRACKET_LEVEL_RE = re.compile(
    r"^\[?(?P<level>DEBUG|TRACE|INFO|NOTICE|WARN(?:ING)?|ERROR|ERR|SEVERE|FATAL|CRITICAL|CRIT|ALERT|EMERG(?:ENCY)?)\]?\s+"
    r"(?P<message>.+)$",
    re.IGNORECASE,
)

# Apache / Nginx combined log format
_APACHE_RE = re.compile(
    r'^(?P<host>\S+)\s+\S+\s+\S+\s+'
    r'\[(?P<ts>[^\]]+)\]\s+'
    r'"(?P<method>\S+)\s+(?P<path>\S+)\s+\S+"\s+'
    r'(?P<status>\d{3})\s+(?P<size>\d+|-)'
)


def _parse_ts(raw: str) -> Optional[datetime]:
    """Attempt to parse a timestamp from common formats."""
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%d/%b/%Y:%H:%M:%S %z",
        "%d/%b/%Y:%H:%M:%S",
        "%b %d %H:%M:%S",
    ):
        try:
            dt = datetime.strptime(raw.strip(), fmt)
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        except ValueError:
            continue
    return None


_SEV_MAP = {
    "debug": "debug", "trace": "debug",
    "info": "info", "information": "info", "notice": "info",
    "warn": "warning", "warning": "warning",
    "error": "error", "err": "error", "severe": "error",
    "fatal": "critical", "critical": "critical", "crit": "critical",
    "alert": "critical", "emergency": "critical", "emerg": "critical",
}


def _norm_sev(raw: Optional[str]) -> str:
    if not raw:
        return "info"
    return _SEV_MAP.get(raw.strip().lower(), "info")


def _http_status_severity(status: str) -> str:
    """Map HTTP status code to a severity level."""
    code = int(status)
    if code < 400:
        return "info"
    if code < 500:
        return "warning"
    return "error"


class LogAdapter(BaseAdapter):
    """Parses plain-text log files using pattern matching."""

    def detect(self, file_path: str, sample: str) -> bool:
        if file_path.lower().endswith(".log") or file_path.lower().endswith(".txt"):
            return True
        # Heuristic: if sample lines match known patterns
        lines = sample.strip().splitlines()[:10]
        matched = sum(1 for line in lines if _ISO_LEVEL_RE.match(line) or _SYSLOG_RE.match(line) or _APACHE_RE.match(line))
        return matched >= len(lines) * 0.5 if lines else False

    def parse(self, file_path: str, dataset_id: str) -> ParseResult:
        result = ParseResult()

        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as fh:
                lines = fh.readlines()
        except Exception as exc:
            result.errors.append(f"Failed to read log file: {exc}")
            return result

        for line_no, raw_line in enumerate(lines, start=1):
            line = raw_line.rstrip("\n\r")
            if not line.strip():
                continue

            try:
                event = self._parse_line(line, dataset_id)
                if event:
                    result.events.append(event)
                else:
                    # Couldn't match any pattern — store as raw info event
                    result.events.append({
                        "id": str(uuid.uuid4()),
                        "dataset_id": dataset_id,
                        "timestamp": None,
                        "severity": "info",
                        "message": line,
                        "raw_record_json": json.dumps({"raw": line, "line": line_no}),
                    })
            except Exception as exc:
                result.errors.append(f"Line {line_no}: {exc}")

        return result

    def _parse_line(self, line: str, dataset_id: str) -> Optional[dict]:
        """Try each regex pattern in priority order."""

        # 1. ISO-prefixed with level
        m = _ISO_LEVEL_RE.match(line)
        if m:
            return {
                "id": str(uuid.uuid4()),
                "dataset_id": dataset_id,
                "timestamp": _parse_ts(m.group("ts")),
                "severity": _norm_sev(m.group("level")),
                "service_name": m.group("service"),
                "message": m.group("message"),
                "raw_record_json": json.dumps({"raw": line}),
            }

        # 2. Syslog
        m = _SYSLOG_RE.match(line)
        if m:
            return {
                "id": str(uuid.uuid4()),
                "dataset_id": dataset_id,
                "timestamp": _parse_ts(m.group("ts")),
                "severity": self._infer_severity_from_message(m.group("message")),
                "host_name": m.group("host"),
                "service_name": m.group("service"),
                "message": m.group("message"),
                "raw_record_json": json.dumps({"raw": line}),
            }

        # 3. Apache / Nginx
        m = _APACHE_RE.match(line)
        if m:
            status = m.group("status")
            return {
                "id": str(uuid.uuid4()),
                "dataset_id": dataset_id,
                "timestamp": _parse_ts(m.group("ts")),
                "severity": _http_status_severity(status),
                "host_name": m.group("host"),
                "service_name": "httpd",
                "event_type": f"HTTP {m.group('method')}",
                "message": f"{m.group('method')} {m.group('path')} {status}",
                "attributes_json": json.dumps({
                    "method": m.group("method"),
                    "path": m.group("path"),
                    "status": status,
                    "size": m.group("size"),
                }),
                "raw_record_json": json.dumps({"raw": line}),
            }

        # 4. Generic bracketed level
        m = _BRACKET_LEVEL_RE.match(line)
        if m:
            return {
                "id": str(uuid.uuid4()),
                "dataset_id": dataset_id,
                "timestamp": None,
                "severity": _norm_sev(m.group("level")),
                "message": m.group("message"),
                "raw_record_json": json.dumps({"raw": line}),
            }

        return None

    @staticmethod
    def _infer_severity_from_message(msg: str) -> str:
        """Guess severity from keywords when no explicit level is provided."""
        lower = msg.lower()
        if any(kw in lower for kw in ("fatal", "critical", "panic", "emergency")):
            return "critical"
        if any(kw in lower for kw in ("error", "fail", "exception", "refused", "denied")):
            return "error"
        if any(kw in lower for kw in ("warn", "timeout", "retry", "slow", "degraded")):
            return "warning"
        if any(kw in lower for kw in ("debug", "trace")):
            return "debug"
        return "info"
