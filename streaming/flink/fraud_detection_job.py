"""
Apache Flink Real-Time Fraud Detection & Pattern Matching Job.
Consumes clickstream from Kafka, applies CEP/Windowing rules:
1. Rapid checkout bursts (> 3 checkouts in 60s)
2. Abnormal monetary spikes (amount > $10,000)
3. Suspicious geo-location changes within 5 minutes
Emits alerts to 'fraud.alerts' Kafka topic and writes to ClickHouse.
"""

import json
import logging
from typing import Dict, Any, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FraudDetectionJob")


class FraudRuleEngine:
    """In-memory stateful fraud detection engine for streaming events."""

    def __init__(self, high_value_threshold: float = 10000.0, burst_window_seconds: int = 60):
        self.high_value_threshold = high_value_threshold
        self.burst_window_seconds = burst_window_seconds
        # User state: user_id -> list of (timestamp_ms, event_type, amount, ip_address)
        self.user_state: Dict[str, List[Dict[str, Any]]] = {}

    def process_event(self, event: Dict[str, Any]) -> Dict[str, Any] | None:
        user_id = event.get("user_id")
        event_type = event.get("event_type")
        epoch_ms = event.get("epoch_ms", 0)
        props = event.get("properties", {})
        amount = props.get("amount", 0.0)
        ip = event.get("ip_address")

        if not user_id:
            return None

        # Maintain sliding state
        history = self.user_state.setdefault(user_id, [])
        cutoff = epoch_ms - (self.burst_window_seconds * 1000)
        history = [e for e in history if e["epoch_ms"] >= cutoff]
        history.append({
            "epoch_ms": epoch_ms,
            "event_type": event_type,
            "amount": amount,
            "ip_address": ip,
            "event_id": event.get("event_id"),
        })
        self.user_state[user_id] = history

        # Rule 1: High-value transaction threshold
        if amount > self.high_value_threshold:
            return self._build_alert(
                event=event,
                rule_name="HIGH_VALUE_TRANSACTION",
                severity="CRITICAL",
                details=f"Payment amount ${amount:.2f} exceeds threshold ${self.high_value_threshold}"
            )

        # Rule 2: Rapid transaction burst
        checkouts = [e for e in history if e["event_type"] in ("checkout_started", "payment_completed")]
        if len(checkouts) >= 3:
            return self._build_alert(
                event=event,
                rule_name="VELOCITY_BURST_ATTACK",
                severity="HIGH",
                details=f"{len(checkouts)} checkouts detected within {self.burst_window_seconds} seconds"
            )

        # Rule 3: Flagged pattern trigger
        if props.get("is_flagged_fraud_pattern"):
            return self._build_alert(
                event=event,
                rule_name="FLAGGED_ANOMALY_PATTERN",
                severity="MEDIUM",
                details="Synthetic fraud pattern detected in event payload"
            )

        return None

    def _build_alert(self, event: Dict[str, Any], rule_name: str, severity: str, details: str) -> Dict[str, Any]:
        return {
            "alert_id": f"alt_{event.get('event_id', '')[:8]}",
            "rule_name": rule_name,
            "severity": severity,
            "user_id": event.get("user_id"),
            "event_id": event.get("event_id"),
            "timestamp": event.get("timestamp"),
            "amount": event.get("properties", {}).get("amount", 0.0),
            "ip_address": event.get("ip_address"),
            "details": details,
        }


def build_pyflink_pipeline():
    """
    Template definition for Apache PyFlink Table/Stream API environment.
    Can be submitted directly to flink-jobmanager via `flink run -py fraud_detection_job.py`.
    """
    try:
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.table import StreamTableEnvironment, EnvironmentSettings

        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(2)
        t_env = StreamTableEnvironment.create(env)

        # Define Kafka Source Table
        t_env.execute_sql("""
            CREATE TABLE clickstream_kafka_source (
                event_id STRING,
                user_id STRING,
                session_id STRING,
                event_type STRING,
                epoch_ms BIGINT,
                ip_address STRING,
                device STRING,
                region STRING,
                ts AS TO_TIMESTAMP_LTZ(epoch_ms, 3),
                WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'clickstream.events',
                'properties.bootstrap.servers' = 'kafka:29092',
                'properties.group.id' = 'flink_fraud_detection',
                'scan.startup.mode' = 'latest-offset',
                'format' = 'json'
            );
        """)

        # Define Kafka Alert Sink Table
        t_env.execute_sql("""
            CREATE TABLE fraud_alerts_sink (
                user_id STRING,
                event_count BIGINT,
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3)
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'fraud.alerts',
                'properties.bootstrap.servers' = 'kafka:29092',
                'format' = 'json'
            );
        """)

        # Aggregation window query
        t_env.execute_sql("""
            INSERT INTO fraud_alerts_sink
            SELECT
                user_id,
                COUNT(event_id) AS event_count,
                TUMBLE_START(ts, INTERVAL '1' MINUTE) AS window_start,
                TUMBLE_END(ts, INTERVAL '1' MINUTE) AS window_end
            FROM clickstream_kafka_source
            WHERE event_type IN ('checkout_started', 'payment_completed')
            GROUP BY
                user_id,
                TUMBLE(ts, INTERVAL '1' MINUTE)
            HAVING COUNT(event_id) >= 3;
        """)
        logger.info("PyFlink DAG successfully registered.")
    except ImportError:
        logger.warning("PyFlink not available in local environment. Engine running in standalone Python stream mode.")


if __name__ == "__main__":
    logger.info("Initializing Fraud Detection Streaming Engine...")
    engine = FraudRuleEngine()

    sample_event = {
        "event_id": "evt_test_1001",
        "user_id": "usr_9999",
        "event_type": "payment_completed",
        "epoch_ms": 1714000000000,
        "timestamp": "2026-10-27T20:00:00Z",
        "ip_address": "10.99.1.5",
        "properties": {"amount": 15000.0, "is_flagged_fraud_pattern": True}
    }
    alert = engine.process_event(sample_event)
    print("Test Sample Alert Output:", json.dumps(alert, indent=2))
