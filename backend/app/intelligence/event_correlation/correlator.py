"""
Event Correlation Engine for J.A.R.V.I.S.
Correlates anomalous events and metrics across temporal proximity,
dependency graph topology (networkx), shared trace/request IDs, and common hosts.
Groups related events into Incident Candidates with explicit link justifications.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import networkx as nx

from app.database.repository import Repository

logger = logging.getLogger(__name__)


class EventCorrelator:
    """Configurable multi-dimensional event correlation engine.

    Weights:
      w_temporal: weight for time delta proximity
      w_topology: weight for service graph dependency distance
      w_trace: weight for shared distributed trace ID
      w_host: weight for shared physical/virtual host node
    """

    def __init__(
        self,
        time_window_seconds: float = 900.0,  # 15 minutes window
        w_temporal: float = 0.35,
        w_topology: float = 0.30,
        w_trace: float = 0.25,
        w_host: float = 0.10,
        min_correlation_threshold: float = 0.45,
    ) -> None:
        self.time_window_seconds = time_window_seconds
        self.w_temporal = w_temporal
        self.w_topology = w_topology
        self.w_trace = w_trace
        self.w_host = w_host
        self.min_correlation_threshold = min_correlation_threshold

    def build_topology_graph(self) -> nx.DiGraph:
        """Construct a NetworkX directed graph from known service dependencies."""
        graph_data = Repository.get_service_graph()
        g = nx.DiGraph()
        for s in graph_data.get("services", []):
            g.add_node(s["name"], **s)
        for dep in graph_data.get("dependencies", []):
            g.add_edge(dep["source_service"], dep["target_service"], weight=1.0)
        return g

    def calculate_correlation(
        self,
        e1: Dict[str, Any],
        e2: Dict[str, Any],
        topo_graph: nx.DiGraph,
    ) -> Tuple[float, str]:
        """Compute weighted correlation score between two events and return explanation."""
        # 1. Temporal score (exponential decay over time delta)
        t1 = self._parse_iso(e1["timestamp"])
        t2 = self._parse_iso(e2["timestamp"])
        delta_sec = abs((t1 - t2).total_seconds())

        if delta_sec > self.time_window_seconds:
            return 0.0, "Time delta exceeds correlation window"

        s_temporal = math.exp(-delta_sec / (self.time_window_seconds / 2.0))

        # 2. Topology distance score
        s1 = e1.get("service_name")
        s2 = e2.get("service_name")
        reasons = []

        if s1 == s2:
            s_topo = 1.0
            reasons.append(f"Occurred on the same service ({s1})")
        elif topo_graph.has_node(s1) and topo_graph.has_node(s2):
            # Check shortest path in undirected version of topology
            undirected = topo_graph.to_undirected()
            if nx.has_path(undirected, s1, s2):
                dist = nx.shortest_path_length(undirected, s1, s2)
                s_topo = max(0.0, 1.0 - (dist - 1) * 0.35)
                reasons.append(f"Graph distance {dist} between {s1} and {s2}")
            else:
                s_topo = 0.1
        else:
            s_topo = 0.2

        # 3. Trace ID score
        tr1 = e1.get("trace_id")
        tr2 = e2.get("trace_id")
        if tr1 and tr2 and tr1 == tr2:
            s_trace = 1.0
            reasons.append(f"Shared trace_id {tr1}")
        else:
            s_trace = 0.0

        # 4. Host score
        h1 = e1.get("host_name")
        h2 = e2.get("host_name")
        if h1 and h2 and h1 == h2:
            s_host = 1.0
            reasons.append(f"Co-located on host {h1}")
        else:
            s_host = 0.0

        # Weighted total
        total_score = (
            self.w_temporal * s_temporal
            + self.w_topology * s_topo
            + self.w_trace * s_trace
            + self.w_host * s_host
        )

        explanation = (
            f"Correlation: {total_score:.2f} (Time delta: {int(delta_sec)}s, "
            + (", ".join(reasons) if reasons else "weak topology link")
            + ")"
        )

        return total_score, explanation

    def cluster_events_into_incidents(
        self,
        dataset_id: str,
    ) -> List[Dict[str, Any]]:
        """Cluster anomalous and severe events in the dataset into incident candidate groups."""
        # Fetch anomalous or error events
        events = Repository.get_events(dataset_id=dataset_id, limit=3000)
        anomalous_events = [
            e for e in events
            if e.get("is_anomaly") or e.get("severity", "info").lower() in ("error", "critical", "fatal")
        ]

        if not anomalous_events:
            logger.info("No anomalous events found for correlation in dataset %s", dataset_id)
            return []

        # Sort by timestamp
        anomalous_events.sort(key=lambda x: x["timestamp"])

        topo = self.build_topology_graph()

        # Build correlation graph where edge exists if correlation score >= threshold
        cg = nx.Graph()
        for idx, ev in enumerate(anomalous_events):
            cg.add_node(ev["id"], event=ev)

        for i in range(len(anomalous_events)):
            e1 = anomalous_events[i]
            for j in range(i + 1, min(i + 25, len(anomalous_events))):
                e2 = anomalous_events[j]
                score, reason = self.calculate_correlation(e1, e2, topo)
                if score >= self.min_correlation_threshold:
                    cg.add_edge(e1["id"], e2["id"], weight=score, reason=reason)

        # Connected components represent distinct incident candidates
        clusters: List[Dict[str, Any]] = []
        for comp in nx.connected_components(cg):
            if len(comp) < 1:
                continue

            cluster_events = [cg.nodes[node_id]["event"] for node_id in comp]
            cluster_events.sort(key=lambda x: x["timestamp"])

            # Calculate cluster metrics
            t_start = cluster_events[0]["timestamp"]
            t_end = cluster_events[-1]["timestamp"]
            dt_start = self._parse_iso(t_start)
            dt_end = self._parse_iso(t_end)
            duration = max(1.0, (dt_end - dt_start).total_seconds())

            services_involved = sorted(list({e["service_name"] for e in cluster_events}))
            severities = [e.get("severity", "info").lower() for e in cluster_events]

            if any(s in ("critical", "fatal") for s in severities):
                cluster_severity = "critical"
            elif any(s in ("error", "err") for s in severities):
                cluster_severity = "high"
            else:
                cluster_severity = "medium"

            # Create event links with reasons
            event_links = []
            for ev in cluster_events:
                # Find best edge reason
                reason = "Primary anomalous event"
                best_score = 0.8
                for neighbor in cg.neighbors(ev["id"]):
                    edge_data = cg.get_edge_data(ev["id"], neighbor)
                    if edge_data and edge_data.get("weight", 0) > best_score:
                        best_score = edge_data["weight"]
                        reason = edge_data.get("reason", reason)

                event_links.append({
                    "event_id": ev["id"],
                    "correlation_score": round(best_score, 2),
                    "correlation_reason": reason,
                })

            clusters.append({
                "dataset_id": dataset_id,
                "start_time": t_start,
                "end_time": t_end,
                "duration_seconds": duration,
                "severity": cluster_severity,
                "services": services_involved,
                "affected_service_count": len(services_involved),
                "events": cluster_events,
                "event_links": event_links,
            })

        return clusters

    @staticmethod
    def _parse_iso(ts_str: str) -> datetime:
        try:
            return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except Exception:
            return datetime.now(timezone.utc)
