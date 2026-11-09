"""
Timeline Builder for J.A.R.V.I.S.
Generates an audit-grade chronological timeline of observed failure milestones
from real telemetry events without fabricating missing data.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class TimelineBuilder:
    """Builds chronological incident timeline from actual observed telemetry."""

    @staticmethod
    def build_timeline(incident_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not incident_events:
            return []

        # Sort strictly by timestamp
        sorted_events = sorted(incident_events, key=lambda x: x["timestamp"])

        try:
            t0 = datetime.fromisoformat(sorted_events[0]["timestamp"].replace("Z", "+00:00"))
        except Exception:
            t0 = datetime.now(timezone.utc)

        timeline: List[Dict[str, Any]] = []
        services_seen = set()

        for idx, ev in enumerate(sorted_events):
            try:
                t = datetime.fromisoformat(ev["timestamp"].replace("Z", "+00:00"))
            except Exception:
                t = t0

            delta_sec = int((t - t0).total_seconds())
            hours, remainder = divmod(abs(delta_sec), 3600)
            minutes, seconds = divmod(remainder, 60)
            offset_str = f"T+{hours:02d}:{minutes:02d}:{seconds:02d}"

            srv = ev.get("service_name", "unknown")
            sev = ev.get("severity", "info").lower()

            # Determine milestone tag
            if idx == 0:
                milestone = "INCIDENT_ONSET"
            elif srv not in services_seen and sev in ("error", "critical", "fatal"):
                milestone = "CASCADE_SPREAD"
            elif sev in ("critical", "fatal"):
                milestone = "CRITICAL_FAILURE"
            elif ev.get("is_anomaly"):
                milestone = "ANOMALY_SPIKE"
            else:
                milestone = "SYMPTOM_LOG"

            services_seen.add(srv)

            timeline.append({
                "timestamp": ev["timestamp"],
                "offset_seconds": delta_sec,
                "offset_formatted": offset_str,
                "milestone": milestone,
                "service_name": srv,
                "severity": sev,
                "message": ev["message"],
                "trace_id": ev.get("trace_id"),
                "event_id": ev.get("id"),
            })

        return timeline
