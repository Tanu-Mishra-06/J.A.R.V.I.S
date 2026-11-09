"""
Impact Analysis Engine for J.A.R.V.I.S.
Calculates blast radius, categorizes direct vs downstream affected services,
and quantifies latency degradation and error counts using NetworkX.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

import networkx as nx

from app.database.repository import Repository

logger = logging.getLogger(__name__)


class ImpactAnalysisEngine:
    """Computes dependency blast radius and service impact metrics."""

    def __init__(self, topo_graph: Optional[nx.DiGraph] = None):
        self.topo_graph = topo_graph or self._build_graph()

    def _build_graph(self) -> nx.DiGraph:
        graph_data = Repository.get_service_graph()
        g = nx.DiGraph()
        for s in graph_data.get("services", []):
            g.add_node(s["name"], **s)
        for dep in graph_data.get("dependencies", []):
            # Dependency: caller -> callee
            g.add_edge(dep["source_service"], dep["target_service"])
        return g

    def evaluate_impact(
        self,
        root_cause_service: str,
        incident_events: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Determine direct and downstream affected services and aggregate metrics."""
        direct_services: List[str] = [root_cause_service] if root_cause_service else []
        downstream_services: List[str] = []

        # Find callers that depend on root_cause_service
        # In a caller->callee graph, callers are predecessors of callee
        if self.topo_graph.has_node(root_cause_service):
            # Immediate callers (direct impact)
            callers = list(self.topo_graph.predecessors(root_cause_service))
            for c in callers:
                if c not in direct_services:
                    direct_services.append(c)

            # Transitive callers (downstream impact)
            # Reverse graph to traverse from callee to all upstream callers
            rg = self.topo_graph.reverse()
            try:
                all_impacted = list(nx.descendants(rg, root_cause_service))
                for s in all_impacted:
                    if s not in direct_services and s not in downstream_services:
                        downstream_services.append(s)
            except Exception as e:
                logger.warning("Could not compute descendants: %s", e)

        # Include any other services seen with errors during the incident
        for ev in incident_events:
            srv = ev.get("service_name")
            if srv and srv not in direct_services and srv not in downstream_services:
                downstream_services.append(srv)

        # Aggregate counts from incident events
        error_count = sum(
            1 for e in incident_events
            if e.get("severity", "info").lower() in ("error", "critical", "fatal")
        )
        total_events = len(incident_events)

        # Estimate latency increase based on severity of errors
        avg_latency_increase_pct = round(min(450.0, max(25.0, error_count * 4.5)), 1)

        user_impact_summary = (
            f"{len(direct_services)} core services degraded, "
            f"{len(downstream_services)} downstream services affected. "
            f"Observed {error_count} error events with estimated {avg_latency_increase_pct}% latency degradation."
        )

        return {
            "direct_services": direct_services,
            "downstream_services": downstream_services,
            "total_events_affected": total_events,
            "error_count": error_count,
            "avg_latency_increase_pct": avg_latency_increase_pct,
            "user_impact_summary": user_impact_summary,
        }
