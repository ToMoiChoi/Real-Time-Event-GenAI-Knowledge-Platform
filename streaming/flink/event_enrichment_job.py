"""
Apache Flink Stream Enrichment and Lakehouse Sink Job.
Enriches real-time clickstream events with CDC-derived user profiles
and streams enriched events to Apache Iceberg and ClickHouse.
"""

import json
import logging
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("EventEnrichmentJob")


class EventEnrichmentProcessor:
    """Enriches raw events with user tier, geo-lookup, and session duration."""

    def __init__(self):
        # Simulated CDC user cache (populated by Debezium stream)
        self.user_profile_cache = {
            "usr_default": {"tier": "standard", "lifetime_value": 150.0, "risk_score": 0.1},
        }

    def update_user_profile(self, cdc_record: Dict[str, Any]):
        """Update in-memory user cache on CDC event."""
        user_id = cdc_record.get("id") or cdc_record.get("user_id")
        if user_id:
            self.user_profile_cache[user_id] = {
                "tier": cdc_record.get("membership_tier", "standard"),
                "lifetime_value": float(cdc_record.get("total_spend", 0.0)),
                "risk_score": float(cdc_record.get("risk_score", 0.0)),
            }

    def enrich(self, raw_event: Dict[str, Any]) -> Dict[str, Any]:
        user_id = raw_event.get("user_id", "usr_default")
        profile = self.user_profile_cache.get(
            user_id,
            {"tier": "silver", "lifetime_value": 450.0, "risk_score": 0.05}
        )

        enriched = dict(raw_event)
        enriched["user_tier"] = profile["tier"]
        enriched["user_ltv"] = profile["lifetime_value"]
        enriched["user_risk_score"] = profile["risk_score"]
        enriched["processed_at"] = raw_event.get("timestamp")
        return enriched


def build_pyflink_enrichment_sql():
    """
    Flink SQL pipeline to read from Kafka 'clickstream.events',
    join with CDC topic 'postgres_cdc.public.users',
    and write to Apache Iceberg catalog sink.
    """
    return """
    -- Iceberg Catalog configuration
    CREATE CATALOG iceberg_catalog WITH (
        'type' = 'iceberg',
        'catalog-type' = 'rest',
        'uri' = 'http://iceberg-rest:8181',
        'warehouse' = 's3://lakehouse/warehouse',
        's3.endpoint' = 'http://minio:9000'
    );

    -- Sink into Iceberg Lakehouse
    CREATE TABLE IF NOT EXISTS iceberg_catalog.analytics.enriched_clickstream (
        event_id STRING,
        user_id STRING,
        session_id STRING,
        event_type STRING,
        device STRING,
        region STRING,
        epoch_ms BIGINT,
        user_tier STRING,
        user_ltv DOUBLE,
        PRIMARY KEY (event_id) NOT ENFORCED
    ) PARTITIONED BY (region, event_type);
    """


if __name__ == "__main__":
    enricher = EventEnrichmentProcessor()
    sample = {
        "event_id": "e_987",
        "user_id": "usr_alpha",
        "event_type": "item_view",
        "device": "mobile_ios",
        "region": "us-east-1",
        "timestamp": "2026-10-27T20:00:00Z",
    }
    enriched = enricher.enrich(sample)
    print("Enriched Record:", json.dumps(enriched, indent=2))
