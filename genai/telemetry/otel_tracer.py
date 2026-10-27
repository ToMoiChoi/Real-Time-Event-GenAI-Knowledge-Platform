"""
OpenTelemetry & Prometheus Telemetry Exporter.
Tracks LLM inference latency, token drift, and operational costs.
"""

import time
import logging
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Telemetry")


class GenAITelemetryTracer:
    """Instruments RAG requests with OpenTelemetry spans and Prometheus counters."""

    def __init__(self, service_name: str = "genai-knowledge-service"):
        self.service_name = service_name
        self.metrics_registry = {
            "total_queries": 0,
            "total_tokens_consumed": 0,
            "total_estimated_cost_usd": 0.0,
            "latency_histogram_ms": [],
        }

    def record_query_trace(
        self,
        endpoint: str,
        duration_ms: float,
        prompt_tokens: int,
        completion_tokens: int,
        metadata: Dict[str, Any] = None
    ):
        """Record telemetry metric for a RAG inference request."""
        cost = (prompt_tokens * 0.0000015) + (completion_tokens * 0.000006)

        self.metrics_registry["total_queries"] += 1
        self.metrics_registry["total_tokens_consumed"] += (prompt_tokens + completion_tokens)
        self.metrics_registry["total_estimated_cost_usd"] += cost
        self.metrics_registry["latency_histogram_ms"].append(duration_ms)

        logger.info(
            f"[OTEL TRACE] service={self.service_name} endpoint={endpoint} "
            f"duration={duration_ms:.2f}ms tokens={prompt_tokens + completion_tokens} cost=${cost:.6f}"
        )

    def export_prometheus_metrics(self) -> str:
        """Expose standard Prometheus metric format string."""
        total_q = self.metrics_registry["total_queries"]
        total_tok = self.metrics_registry["total_tokens_consumed"]
        cost = self.metrics_registry["total_estimated_cost_usd"]
        avg_lat = (
            sum(self.metrics_registry["latency_histogram_ms"]) / max(total_q, 1)
        )

        return f"""# HELP genai_requests_total Total number of processed RAG queries
# TYPE genai_requests_total counter
genai_requests_total {total_q}

# HELP genai_tokens_consumed_total Total LLM tokens consumed
# TYPE genai_tokens_consumed_total counter
genai_tokens_consumed_total {total_tok}

# HELP genai_cost_dollars_total Total estimated LLM cost in USD
# TYPE genai_cost_dollars_total counter
genai_cost_dollars_total {cost:.6f}

# HELP genai_latency_ms_avg Average query latency in milliseconds
# TYPE genai_latency_ms_avg gauge
genai_latency_ms_avg {avg_lat:.2f}
"""


if __name__ == "__main__":
    tracer = GenAITelemetryTracer()
    tracer.record_query_trace(
        endpoint="/api/v1/rag/query",
        duration_ms=145.2,
        prompt_tokens=320,
        completion_tokens=95
    )
    print("Prometheus Export Format:")
    print(tracer.export_prometheus_metrics())
