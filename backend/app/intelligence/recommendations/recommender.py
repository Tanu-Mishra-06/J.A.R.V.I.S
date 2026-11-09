"""
Advisory Recommendations Engine for J.A.R.V.I.S.
Provides comprehensive, multi-phase operational runbooks and remediation guidance
for production incidents across distributed systems, cloud infrastructure, and data pipelines.
Never executes actions automatically — strictly advisory and verified for safety.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


DETAILED_RUNBOOK_CATALOG = {
    "database_pool_exhaustion": [
        {
            "phase": "Phase 1: Immediate Triage & Diagnostics",
            "priority": "P1 - Critical",
            "title": "Inspect Active Connection Leaks and Identify Blocking Transactions",
            "command": "psql -U postgres -d app_db -c \"SELECT pid, now() - query_start AS duration, usename, client_addr, state, query FROM pg_stat_activity WHERE state != 'idle' ORDER BY duration DESC LIMIT 10;\"",
            "expected_output": "Rows showing long-running queries (>15s) in 'active' state, or multiple connections in 'idle in transaction' holding exclusive table locks.",
            "description": "Examine PostgreSQL activity to isolate slow unindexed table scans or abandoned client connections holding open transaction locks without calling COMMIT/ROLLBACK.",
            "category": "investigation",
            "action_type": "read_only_query",
            "risk_level": "Low (Read-only)",
            "rollback_plan": "No rollback needed for diagnostic queries.",
        },
        {
            "phase": "Phase 2: Blast Radius Containment",
            "priority": "P1 - Critical",
            "title": "Enable Ingress Rate Limiting & Circuit Breaking on Upstream Services",
            "command": "kubectl patch virtualservice order-service-vs -n prod --type merge -p '{\"spec\":{\"hosts\":[\"order-service\"],\"http\":[{\"fault\":{\"abort\":{\"httpStatus\":429,\"percentage\":{\"value\":30.0}}}}]}}'",
            "expected_output": "VirtualService patched. Upstream Envoy proxies return 429 Too Many Requests on 30% of non-critical traffic to shed load on database.",
            "description": "Temporarily throttle non-essential background worker traffic and shed 30% of incoming requests to prevent catastrophic thread starvation on the primary database pool.",
            "category": "containment",
            "action_type": "traffic_throttling",
            "risk_level": "Medium (Temporary user retries)",
            "rollback_plan": "kubectl patch virtualservice order-service-vs -n prod --type merge -p '{\"spec\":{\"http\":[{\"fault\":null}]}}'",
        },
        {
            "phase": "Phase 3: Root Cause Mitigation",
            "priority": "P2 - Remediation",
            "title": "Terminate Hanging Idle Connections & Expand Pooler Reserve Limit",
            "command": "psql -U postgres -c \"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state = 'idle in transaction' AND (now() - query_start) > interval '5 minutes';\"",
            "expected_output": "Terminated backend PIDs returned. Active connection count immediately drops below threshold (<800 connections).",
            "description": "Safely terminate stale abandoned connections that are holding open locks, and adjust PgBouncer 'reserve_pool_size' to allow critical write traffic through.",
            "category": "remediation",
            "action_type": "process_termination",
            "risk_level": "Medium (Terminated transaction will roll back safely)",
            "rollback_plan": "Clients will automatically reconnect via exponential backoff.",
        },
        {
            "phase": "Phase 4: Smoke Testing & Verification",
            "priority": "P3 - Verification",
            "title": "Verify Connection Pool Recovery & End-to-End Latency Health",
            "command": "curl -s -w '\\nHTTP: %{http_code} | Time: %{time_total}s\\n' https://api.internal/api/v1/orders/healthz",
            "expected_output": "HTTP: 200 | Time: 0.024s. Response time drops from >4000ms back to healthy baseline (<35ms).",
            "description": "Confirm that database connection latency is normalized, error rate drops below 0.01%, and api-gateway 504 timeouts cease.",
            "category": "verification",
            "action_type": "health_check",
            "risk_level": "Low (Synthetic probe)",
            "rollback_plan": "If health check fails, initiate emergency replica promotion.",
        },
        {
            "phase": "Phase 5: Architectural Hardening & Prevention",
            "priority": "P4 - Long Term",
            "title": "Configure Transaction Pool Mode & Deploy Read Replica Routing",
            "command": "sed -i 's/pool_mode = session/pool_mode = transaction/' /etc/pgbouncer/pgbouncer.ini && systemctl reload pgbouncer",
            "expected_output": "PgBouncer reloaded in transaction pool mode. Total server connections reduced by 70%.",
            "description": "Migrate PgBouncer from session pooling to transaction pooling so connections are returned to the pool immediately after each query. Add read-only replica connection strings to order-service.",
            "category": "prevention",
            "action_type": "architectural_upgrade",
            "risk_level": "Low (Reloads without dropping active queries)",
            "rollback_plan": "Revert pool_mode to session and reload pgbouncer.",
        },
    ],
    "memory_leak_oom": [
        {
            "phase": "Phase 1: Immediate Triage & Diagnostics",
            "priority": "P1 - Critical",
            "title": "Capture Node dmesg OOM Invocation Logs & Top Memory Consumer",
            "command": "dmesg -T | grep -E -i '(out of memory|oom-killer|killed process)' | tail -n 25",
            "expected_output": "Kernel log displaying exact timestamp, process name, PID, total-vm memory pages, and 'Killed process <pid> (<process_name>) score <oom_score>'.",
            "description": "Inspect kernel message buffer to identify the exact victim process terminated by the Linux OOM-killer and the memory consumption of the parent cgroup.",
            "category": "investigation",
            "action_type": "read_only_query",
            "risk_level": "Low (Diagnostic)",
            "rollback_plan": "No rollback needed.",
        },
        {
            "phase": "Phase 2: Blast Radius Containment",
            "priority": "P1 - Critical",
            "title": "Cordon the Saturated Worker Node & Evacuate Healthy Workloads",
            "command": "kubectl cordon worker-node-01 && kubectl drain worker-node-01 --ignore-daemonsets --delete-emptydir-data --force",
            "expected_output": "Node worker-node-01 marked as SchedulingDisabled. All healthy pods safely rescheduled to worker-node-02.",
            "description": "Stop new pods from scheduling onto the degraded node and gracefully relocate affected workloads before the node undergoes hard kernel freeze.",
            "category": "containment",
            "action_type": "node_eviction",
            "risk_level": "Medium (Workload migration)",
            "rollback_plan": "kubectl uncordon worker-node-01",
        },
        {
            "phase": "Phase 3: Root Cause Mitigation",
            "priority": "P2 - Remediation",
            "title": "Extract JVM/Process Heap Dump & Apply Hard Cgroup Memory Limits",
            "command": "jcmd 8841 GC.heap_dump /tmp/leaked_heap.hprof 2>/dev/null || crictl inspectp $(crictl pods --name coredns -q)",
            "expected_output": "Heap dump dumped to /tmp/leaked_heap.hprof. Container spec verified with memory limit applied.",
            "description": "Capture the memory dump for developer analysis, then restart the terminated CoreDNS pods with guaranteed resource limits in the kube-system namespace.",
            "category": "remediation",
            "action_type": "dump_and_restart",
            "risk_level": "Medium",
            "rollback_plan": "Restart pod using kubectl rollout restart deployment -n kube-system coredns.",
        },
        {
            "phase": "Phase 4: Smoke Testing & Verification",
            "priority": "P3 - Verification",
            "title": "Verify Cluster-Wide DNS Resolution & Node Memory Pressures",
            "command": "kubectl run dns-test --rm -i --tty --image=busybox:1.36 -- nslookup kube-apiserver.default.svc.cluster.local",
            "expected_output": "Address 1: 10.96.0.1 kube-apiserver.default.svc.cluster.local. DNS query returns in <2ms with 0% packet drop.",
            "description": "Execute synthetic DNS lookup from an isolated container to ensure CoreDNS cluster resolution is restored and ingress-nginx errors clear.",
            "category": "verification",
            "action_type": "synthetic_test",
            "risk_level": "Low",
            "rollback_plan": "No rollback needed.",
        },
        {
            "phase": "Phase 5: Architectural Hardening & Prevention",
            "priority": "P4 - Long Term",
            "title": "Deploy NodeLocal DNSCache DaemonSet & Kubelet Eviction Thresholds",
            "command": "kubectl apply -f https://raw.githubusercontent.com/kubernetes/kubernetes/master/cluster/addons/dns/nodelocaldns/nodelocaldns.yaml",
            "expected_output": "DaemonSet node-local-dns created across all nodes. Caches DNS queries locally to prevent upstream OOM cascades.",
            "description": "Deploy NodeLocal DNSCache to absorb sudden query bursts and configure kubelet hard eviction flag `--eviction-hard=memory.available<500Mi`.",
            "category": "prevention",
            "action_type": "infrastructure_hardening",
            "risk_level": "Low",
            "rollback_plan": "kubectl delete -f nodelocaldns.yaml",
        },
    ],
    "kafka_partition_lag_storm": [
        {
            "phase": "Phase 1: Immediate Triage & Diagnostics",
            "priority": "P1 - Critical",
            "title": "Describe Consumer Group Lag & Check Partition Imbalance",
            "command": "kafka-consumer-groups.sh --bootstrap-server kafka-01:9092 --describe --group video-transcoder-group",
            "expected_output": "Partition lag table showing TOPIC, PARTITION, CURRENT-OFFSET, LOG-END-OFFSET, and LAG (>150,000 on partitions 2 and 4).",
            "description": "Identify which consumer partitions are stalled, identify the consumer client IDs, and verify whether a rebalance storm is in progress.",
            "category": "investigation",
            "action_type": "read_only_query",
            "risk_level": "Low",
            "rollback_plan": "No rollback needed.",
        },
        {
            "phase": "Phase 2: Blast Radius Containment",
            "priority": "P1 - Critical",
            "title": "Enable Live Video Stream Frame Drop Policy & Throttle Ingest",
            "command": "curl -X POST http://video-ingest:8080/admin/stream-qos -H 'Content-Type: application/json' -d '{\"drop_b_frames\": true, \"max_fps\": 30}'",
            "expected_output": "{\"status\":\"ok\",\"policy\":\"b_frame_drop_active\",\"bandwidth_saved_pct\":42.0}",
            "description": "Instruct upstream ingest gateway to drop non-essential video B-frames to cut streaming bandwidth by 40% and halt queue buffer overflow.",
            "category": "containment",
            "action_type": "traffic_throttling",
            "risk_level": "Medium (Temporary reduced frame rate for viewers)",
            "rollback_plan": "curl -X POST http://video-ingest:8080/admin/stream-qos -d '{\"drop_b_frames\": false, \"max_fps\": 60}'",
        },
        {
            "phase": "Phase 3: Root Cause Mitigation",
            "priority": "P2 - Remediation",
            "title": "Scale Worker Transcoder Pods & Increase max.poll.interval.ms",
            "command": "kubectl scale deployment transcoder-worker -n streaming --replicas=8 && kubectl set env deployment/transcoder-worker MAX_POLL_INTERVAL_MS=600000",
            "expected_output": "deployment.apps/transcoder-worker scaled to 8 replicas. 4 new consumers join partition assignment smoothly.",
            "description": "Increase consumer replicas to match partition count and double the max poll interval to prevent workers from being ejected from the consumer group during heavy transcoding.",
            "category": "remediation",
            "action_type": "scaling",
            "risk_level": "Low",
            "rollback_plan": "kubectl scale deployment transcoder-worker --replicas=4",
        },
        {
            "phase": "Phase 4: Smoke Testing & Verification",
            "priority": "P3 - Verification",
            "title": "Monitor Partition Lag Drain Rate & Frame Processing Latency",
            "command": "watch -n 2 'kafka-consumer-groups.sh --bootstrap-server kafka-01:9092 --describe --group video-transcoder-group | awk \"{print \\$6}\"'",
            "expected_output": "Lag counter dropping by >10,000 records per minute until reaching healthy baseline (<200).",
            "description": "Verify consumer lag steadily recovers to zero and worker GPU frame buffer drop counter stabilizes.",
            "category": "verification",
            "action_type": "monitoring",
            "risk_level": "Low",
            "rollback_plan": "No rollback needed.",
        },
        {
            "phase": "Phase 5: Architectural Hardening & Prevention",
            "priority": "P4 - Long Term",
            "title": "Implement Cooperative Sticky Assignor in Kafka Consumers",
            "command": "echo 'partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor' >> /etc/kafka/consumer.properties",
            "expected_output": "Cooperative rebalancing enabled. Future consumer restarts only reassign affected partitions without stopping the entire worker pool.",
            "description": "Configure Cooperative Sticky Rebalance strategy to eliminate 'stop-the-world' consumer rebalance storms across all streaming pipelines.",
            "category": "prevention",
            "action_type": "configuration_change",
            "risk_level": "Low",
            "rollback_plan": "Revert to RangeAssignor.",
        },
    ],
    "dns_resolution_failure": [
        {
            "phase": "Phase 1: Immediate Triage & Diagnostics",
            "priority": "P1 - Critical",
            "title": "Inspect CoreDNS Pod Logs & Upstream DNS Forwarder Latency",
            "command": "kubectl logs -n kube-system -l k8s-app=kube-dns --tail=100 | grep -E -i '(timeout|refused|servfail|i/o timeout)'",
            "expected_output": "Logs showing 'i/o timeout' or 'SERVFAIL' when reaching upstream nameservers (169.254.169.254 or 8.8.8.8).",
            "description": "Determine if DNS failure is due to saturated CoreDNS CPU or unreachable upstream resolver network routes.",
            "category": "investigation",
            "action_type": "read_only_query",
            "risk_level": "Low",
            "rollback_plan": "No rollback needed.",
        },
        {
            "phase": "Phase 2: Root Cause Mitigation",
            "priority": "P1 - Remediation",
            "title": "Scale CoreDNS Replicas & Restart Stalled Pods",
            "command": "kubectl scale deployment -n kube-system coredns --replicas=4 && kubectl rollout restart deployment -n kube-system coredns",
            "expected_output": "deployment.apps/coredns scaled to 4 replicas. 4 healthy DNS pods distributed across different worker nodes.",
            "description": "Increase DNS capacity and distribute across multiple nodes so one failing node cannot take down DNS for the cluster.",
            "category": "remediation",
            "action_type": "scaling",
            "risk_level": "Low",
            "rollback_plan": "kubectl scale deployment -n kube-system coredns --replicas=2",
        },
    ],
}


class RecommendationEngine:
    """Generates detailed, multi-phase operational runbooks linked to telemetry evidence."""

    @staticmethod
    def generate_recommendations(
        failure_type: str,
        service_name: str,
        evidence: List[str],
    ) -> List[Dict[str, Any]]:
        recs = DETAILED_RUNBOOK_CATALOG.get(failure_type)
        if not recs:
            recs = [
                {
                    "phase": "Phase 1: Immediate Triage",
                    "priority": "P1 - Critical",
                    "title": f"Inspect {service_name} Resource Metrics and Error Logs",
                    "command": f"curl -s http://{service_name}:9090/metrics | grep -E '(cpu|memory|error)' || journalctl -u {service_name} -n 100",
                    "expected_output": f"Recent 100 log lines from {service_name} with stack trace or resource saturation warnings.",
                    "description": f"Examine CPU, memory, active thread counts, and recent error stack traces for {service_name}.",
                    "category": "investigation",
                    "action_type": "manual_verification",
                    "risk_level": "Low",
                    "rollback_plan": "No rollback needed.",
                },
                {
                    "phase": "Phase 2: Remediation",
                    "priority": "P2 - Remediation",
                    "title": f"Verify Network Connectivity and Upstream Dependencies for {service_name}",
                    "command": f"nc -zv -w 3 {service_name} 8080 || ping -c 3 {service_name}",
                    "expected_output": f"Connection to {service_name} port 8080 succeeded.",
                    "description": f"Ensure downstream network routes and service dependencies for {service_name} are healthy and accepting connections.",
                    "category": "mitigation",
                    "action_type": "manual_verification",
                    "risk_level": "Low",
                    "rollback_plan": "No rollback needed.",
                },
                {
                    "phase": "Phase 3: Prevention",
                    "priority": "P3 - Long Term",
                    "title": f"Configure Health Probes and Circuit Breaker for {service_name}",
                    "command": f"# Add livenessProbe and readinessProbe with 5s timeout in Kubernetes deployment for {service_name}",
                    "expected_output": "Kubernetes automatically replaces unhealthy pods before cascade starts.",
                    "description": f"Configure automated health probes and exponential backoff retry policies for callers of {service_name}.",
                    "category": "prevention",
                    "action_type": "configuration_change",
                    "risk_level": "Low",
                    "rollback_plan": "Revert deployment spec.",
                },
            ]

        results = []
        for r in recs:
            rec_copy = dict(r)
            rec_copy["evidence_citations"] = evidence[:3]
            results.append(rec_copy)

        return results
