"""
Root Cause Analysis (RCA) Engine for J.A.R.V.I.S.
Hybrid evidence-based diagnostic ranking using:
  1. Temporal precedence (root causes trigger earlier than cascading symptoms)
  2. Dependency graph upstream centrality (causes propagate downstream)
  3. Metric deviation severity & saturation
  4. Error log signature matching & stack trace severity
Provides candidate ranks, confidence scores, supporting record citations,
alternative explanations, and explicit uncertainty assessments.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import networkx as nx

from app.database.repository import Repository

logger = logging.getLogger(__name__)

# Known failure patterns and signature regexes
KNOWN_FAILURE_PATTERNS = [
    {
        "type": "database_pool_exhaustion",
        "keywords": ["connection pool", "pool exhausted", "too many connections", "connection timeout", "deadlock", "lock wait"],
        "service_types": ["database", "postgres", "mysql", "db"],
        "category": "resource_exhaustion",
    },
    {
        "type": "memory_leak_oom",
        "keywords": ["out of memory", "oom-killer", "java heap space", "memory allocation failed", "killed process", "cgroup oom"],
        "service_types": ["worker", "node", "jvm", "container"],
        "category": "infrastructure_saturation",
    },
    {
        "type": "dns_resolution_failure",
        "keywords": ["dial tcp: lookup", "server misbehaving", "nxdomain", "dns timeout", "no such host", "temporary failure in name resolution"],
        "service_types": ["coredns", "kube-dns", "dns", "ingress"],
        "category": "network_partition",
    },
    {
        "type": "kafka_partition_lag_storm",
        "keywords": ["partition rebalance", "commit failed", "offset out of range", "consumer lag", "broker unreachable", "heartbeat poll"],
        "service_types": ["kafka", "queue", "streaming", "messaging"],
        "category": "message_broker_lag",
    },
    {
        "type": "upstream_http_timeout",
        "keywords": ["504 gateway timeout", "upstream timed out", "connection reset by peer", "read timeout", "context deadline exceeded"],
        "service_types": ["gateway", "proxy", "ingress"],
        "category": "cascading_timeout",
    },
]


class RootCauseAnalyzer:
    """Ranks likely root causes with explicit evidence citations and uncertainty."""

    def __init__(self, topo_graph: Optional[nx.DiGraph] = None):
        self.topo_graph = topo_graph or self._build_graph()

    def _build_graph(self) -> nx.DiGraph:
        graph_data = Repository.get_service_graph()
        g = nx.DiGraph()
        for s in graph_data.get("services", []):
            g.add_node(s["name"], **s)
        for dep in graph_data.get("dependencies", []):
            g.add_edge(dep["source_service"], dep["target_service"])
        return g

    @staticmethod
    def _parse_iso(ts_str: str) -> datetime:
        try:
            return datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
        except Exception:
            return datetime.now(timezone.utc)

    def analyze_incident(
        self,
        incident_events: List[Dict[str, Any]],
        dataset_id: str,
    ) -> List[Dict[str, Any]]:
        """Evaluate evidence across incident events & metrics to rank root cause candidates."""
        if not incident_events:
            return [{
                "rank": 1,
                "service_name": "unknown",
                "failure_type": "inconclusive",
                "confidence_score": 0.0,
                "uncertainty_assessment": "Inconclusive: No anomalous events linked to this incident window.",
                "evidence": [],
                "alternative_explanations": ["Insufficient telemetry data collected during failure period."],
                "is_selected": True,
            }]

        # Sort events by timestamp
        sorted_events = sorted(incident_events, key=lambda x: x["timestamp"])
        earliest_time = self._parse_iso(sorted_events[0]["timestamp"])

        # 1. Group events and metrics by service
        services_in_incident = list({e["service_name"] for e in sorted_events})
        service_stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "first_anomaly_offset_sec": 999999.0,
            "error_count": 0,
            "critical_count": 0,
            "metric_anomaly_count": 0,
            "max_metric_score": 0.0,
            "matched_patterns": [],
            "evidence_events": [],
            "evidence_metrics": [],
        })

        for ev in sorted_events:
            srv = ev["service_name"]
            st = service_stats[srv]
            t = self._parse_iso(ev["timestamp"])
            offset = (t - earliest_time).total_seconds()
            if offset < st["first_anomaly_offset_sec"]:
                st["first_anomaly_offset_sec"] = offset

            sev = ev.get("severity", "info").lower()
            if sev in ("critical", "fatal", "severe"):
                st["critical_count"] += 1
                st["evidence_events"].append({
                    "event_id": ev["id"],
                    "timestamp": ev["timestamp"],
                    "message": ev["message"],
                    "severity": sev,
                })
            elif sev in ("error", "err"):
                st["error_count"] += 1
                if len(st["evidence_events"]) < 5:
                    st["evidence_events"].append({
                        "event_id": ev["id"],
                        "timestamp": ev["timestamp"],
                        "message": ev["message"],
                        "severity": sev,
                    })

            # Check known failure patterns in log messages
            msg_lower = ev.get("message", "").lower()
            for pattern in KNOWN_FAILURE_PATTERNS:
                for kw in pattern["keywords"]:
                    if kw in msg_lower:
                        st["matched_patterns"].append(pattern["type"])
                        break

        # Check metric anomalies for each service
        for srv in services_in_incident:
            m_list = Repository.get_metrics(dataset_id=dataset_id, service_name=srv, limit=100)
            st = service_stats[srv]
            for m in m_list:
                if m.get("is_anomaly"):
                    st["metric_anomaly_count"] += 1
                    score = float(m.get("anomaly_score", 1.0))
                    if score > st["max_metric_score"]:
                        st["max_metric_score"] = score
                    if len(st["evidence_metrics"]) < 4:
                        st["evidence_metrics"].append({
                            "metric_name": m["metric_name"],
                            "value": m["metric_value"],
                            "timestamp": m["timestamp"],
                            "anomaly_score": score,
                        })

        # 2. Score candidates using multi-factor ranking
        candidates: List[Dict[str, Any]] = []

        # Graph upstreamness (nodes that have more out-edges to other services are upstream)
        for srv in services_in_incident:
            st = service_stats[srv]

            # A. Temporal Precedence Factor (0.0 to 1.0)
            # Earliest offset gets highest precedence
            offset = st["first_anomaly_offset_sec"]
            s_temporal = math.exp(-max(0.0, offset) / 120.0)  # decays over 2 minutes

            # B. Topological Upstream Position Factor (0.0 to 1.0)
            # Upstream components (databases, internal brokers, primary storage) propagate downstream
            s_topo = 0.5
            if self.topo_graph.has_node(srv):
                # Out-degree in dependency graph means services that this service feeds or is called by
                # In our dependency representation: (caller -> callee)
                # Callee failure cascades up to caller! (database failure impacts auth, auth impacts gateway)
                # Therefore, sinks in the caller->callee graph (i.e. high in-degree) are the root providers
                in_deg = self.topo_graph.in_degree(srv)
                out_deg = self.topo_graph.out_degree(srv)
                s_topo = min(1.0, 0.4 + in_deg * 0.2 - out_deg * 0.05)

            # C. Anomaly Severity Factor
            s_severity = min(1.0, (st["critical_count"] * 0.25) + (st["error_count"] * 0.08) + (st["max_metric_score"] * 0.15))

            # D. Pattern Match Factor
            s_pattern = 0.85 if st["matched_patterns"] else 0.40

            # Composite weighted confidence
            confidence = (
                0.35 * s_temporal
                + 0.25 * s_topo
                + 0.25 * s_severity
                + 0.15 * s_pattern
            )
            confidence = round(min(0.96, max(0.15, confidence)), 2)

            # Determine failure type
            if st["matched_patterns"]:
                failure_type = st["matched_patterns"][0]
            elif "database" in srv or "db" in srv:
                failure_type = "database_pool_exhaustion"
            elif "dns" in srv:
                failure_type = "dns_resolution_failure"
            elif "kafka" in srv:
                failure_type = "kafka_partition_lag_storm"
            elif st["metric_anomaly_count"] > 0:
                failure_type = "resource_saturation"
            else:
                failure_type = "cascading_service_degradation"

            # Uncertainty assessment
            if confidence >= 0.75:
                uncertainty = f"High confidence: {srv} exhibited the earliest anomalies and matches known root causes."
            elif confidence >= 0.50:
                uncertainty = f"Likely root cause: {srv} is strongly correlated temporally and topographically."
            else:
                uncertainty = f"Inconclusive / Secondary symptom: {srv} showed degradation, but could be a downstream cascade."

            # Assemble evidence citations
            evidence = []
            if st["first_anomaly_offset_sec"] <= 10.0:
                evidence.append(f"Onset detected at T+{int(st['first_anomaly_offset_sec'])}s prior to other service alerts.")
            if st["critical_count"] > 0:
                evidence.append(f"Recorded {st['critical_count']} critical/fatal event logs.")
            if st["evidence_metrics"]:
                top_m = st["evidence_metrics"][0]
                evidence.append(f"Metric '{top_m['metric_name']}' spiked to {top_m['value']} (anomaly score {top_m['anomaly_score']}).")
            for ev_sample in st["evidence_events"][:2]:
                evidence.append(f"Log citation [{ev_sample['severity'].upper()}]: \"{ev_sample['message'][:90]}\"")

            candidates.append({
                "service_name": srv,
                "failure_type": failure_type,
                "confidence_score": confidence,
                "uncertainty_assessment": uncertainty,
                "evidence": evidence,
                "raw_stats": st,
            })

        # Rank candidates by confidence descending
        candidates.sort(key=lambda x: x["confidence_score"], reverse=True)

        # Build alternative explanations for lower-ranked candidates
        top_service = candidates[0]["service_name"]
        for idx, c in enumerate(candidates):
            c["rank"] = idx + 1
            c["is_selected"] = (idx == 0)
            if idx == 0:
                alt = [
                    f"Alternative: Network jitter between {top_service} and caller services.",
                    f"Alternative: Hardware degradation or noisy neighbor VM on host node.",
                ]
            else:
                alt = [
                    f"Likely downstream symptom: Degradation caused by backpressure from primary failure at {top_service}.",
                ]
            c["alternative_explanations"] = alt

        return candidates
