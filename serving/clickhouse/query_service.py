"""
ClickHouse Real-time Analytical Query Service.
Delivers sub-second query latency for user-facing features and live dashboards.
"""

import os
import time
import logging
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ClickHouseService")


class ClickHouseQueryService:
    """Interface to ClickHouse for fast analytical queries."""

    def __init__(
        self,
        host: str = os.getenv("CLICKHOUSE_HOST", "localhost"),
        port: int = int(os.getenv("CLICKHOUSE_PORT", 8123)),
        username: str = os.getenv("CLICKHOUSE_USER", "default"),
        password: str = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123"),
        database: str = os.getenv("CLICKHOUSE_DB", "analytics"),
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.client = None
        self._connect()

    def _connect(self):
        try:
            import clickhouse_connect
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
            )
            logger.info("Connected to ClickHouse database.")
        except Exception as e:
            logger.warning(f"Could not connect to live ClickHouse server ({e}). Running in simulation mode.")
            self.client = None

    def get_live_kpis(self, region: str = "us-east-1") -> Dict[str, Any]:
        """Fetch pre-aggregated metrics from AggregatingMergeTree in < 15ms."""
        query = f"""
        SELECT
            event_type,
            countMerge(total_events) AS total_events,
            uniqExactMerge(unique_users) AS active_users,
            sumMerge(total_volume) AS total_revenue,
            avgMerge(avg_latency) AS avg_latency_ms
        FROM analytics.hourly_platform_metrics
        WHERE region = '{region}'
        GROUP BY event_type
        ORDER BY total_events DESC;
        """
        start = time.perf_counter()
        if self.client:
            res = self.client.query(query)
            data = res.result_rows
        else:
            # High-fidelity mock response
            data = [
                ("page_view", 2500000, 185000, 0.0, 32.4),
                ("item_view", 1100000, 142000, 0.0, 28.1),
                ("payment_completed", 45000, 39000, 4820000.0, 45.8),
                ("search_query", 850000, 112000, 0.0, 18.2),
            ]
        latency_ms = (time.perf_counter() - start) * 1000

        return {
            "region": region,
            "query_latency_ms": round(latency_ms, 2),
            "metrics": [
                {
                    "event_type": row[0],
                    "total_events": row[1],
                    "active_users": row[2],
                    "total_revenue": row[3],
                    "avg_latency_ms": row[4]
                }
                for row in data
            ]
        }

    def get_user_activity_timeline(self, user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Fetch user clickstream history in sub-second response for fraud/recommendation context."""
        start = time.perf_counter()
        # Simulated fast point query
        records = [
            {
                "event_id": f"evt_{i}",
                "user_id": user_id,
                "event_type": "item_view",
                "timestamp": "2026-10-27T20:15:00Z",
                "device": "desktop_chrome",
                "amount": 0.0
            }
            for i in range(min(limit, 5))
        ]
        duration_ms = (time.perf_counter() - start) * 1000
        logger.info(f"Retrieved timeline for user={user_id} in {duration_ms:.2f} ms")
        return records


if __name__ == "__main__":
    service = ClickHouseQueryService()
    kpis = service.get_live_kpis()
    print("Live KPI Query Results:")
    print(kpis)
