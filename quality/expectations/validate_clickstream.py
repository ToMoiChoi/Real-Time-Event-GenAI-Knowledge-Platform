"""
Automated Data Quality & Schema Drift Alerting Engine.
Built with Great Expectations rules and Slack webhook integration.
Validates clickstream events, CDC schemas, and detects anomalies.
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any
try:
    import requests
except ImportError:
    requests = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DataQuality")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


class ClickstreamQualitySuite:
    """Quality validation suite checking schema integrity and statistical boundaries."""

    EXPECTED_COLUMNS = {
        "event_id", "user_id", "session_id", "event_type",
        "timestamp", "epoch_ms", "device", "region", "ip_address", "properties"
    }

    VALID_EVENT_TYPES = {
        "page_view", "search_query", "item_view", "add_to_cart",
        "remove_from_cart", "checkout_started", "payment_completed",
        "login_attempt", "review_submitted"
    }

    def validate_batch(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Runs Great Expectations-style checks across batch."""
        total_records = len(events)
        if total_records == 0:
            return {"status": "EMPTY", "failed_checks": []}

        failures = []
        schema_drift_detected = False
        null_id_count = 0
        invalid_type_count = 0
        negative_amount_count = 0

        first_record_keys = set(events[0].keys())
        missing_keys = self.EXPECTED_COLUMNS - first_record_keys
        extra_keys = first_record_keys - self.EXPECTED_COLUMNS

        if missing_keys or extra_keys:
            schema_drift_detected = True
            failures.append({
                "check": "expect_table_columns_to_match_ordered_schema",
                "missing_columns": list(missing_keys),
                "unexpected_columns": list(extra_keys),
                "severity": "CRITICAL"
            })

        for event in events:
            # Check 1: Non-null primary keys
            if not event.get("event_id") or not event.get("user_id"):
                null_id_count += 1

            # Check 2: Valid event types
            if event.get("event_type") not in self.VALID_EVENT_TYPES:
                invalid_type_count += 1

            # Check 3: Non-negative amount
            amount = event.get("properties", {}).get("amount", 0.0)
            if amount < 0:
                negative_amount_count += 1

        if null_id_count > 0:
            failures.append({
                "check": "expect_column_values_to_not_be_null(event_id, user_id)",
                "failed_count": null_id_count,
                "percentage": (null_id_count / total_records) * 100,
                "severity": "CRITICAL"
            })

        if invalid_type_count > 0:
            failures.append({
                "check": "expect_column_values_to_be_in_set(event_type)",
                "failed_count": invalid_type_count,
                "percentage": (invalid_type_count / total_records) * 100,
                "severity": "HIGH"
            })

        if negative_amount_count > 0:
            failures.append({
                "check": "expect_column_values_to_be_between(amount, min=0)",
                "failed_count": negative_amount_count,
                "severity": "HIGH"
            })

        passed = len(failures) == 0
        result = {
            "batch_timestamp": datetime.now(timezone.utc).isoformat(),
            "total_records_checked": total_records,
            "passed": passed,
            "schema_drift_detected": schema_drift_detected,
            "failures": failures,
        }

        if not passed:
            self.send_slack_alert(result)

        return result

    def send_slack_alert(self, validation_result: Dict[str, Any]):
        """Dispatches automated Slack notification for quality failures."""
        if not SLACK_WEBHOOK_URL:
            logger.warning("[Slack Alert] No SLACK_WEBHOOK_URL set. Notification logged to console.")
            logger.warning(f"[Slack Alert Payload]: {json.dumps(validation_result, indent=2)}")
            return

        payload = {
            "text": ":rotating_light: *Data Quality & Schema Drift Alert*",
            "attachments": [
                {
                    "color": "#FF0000",
                    "fields": [
                        {"title": "Total Records", "value": str(validation_result["total_records_checked"]), "short": True},
                        {"title": "Schema Drift", "value": str(validation_result["schema_drift_detected"]), "short": True},
                        {"title": "Failure Details", "value": json.dumps(validation_result["failures"]), "short": False}
                    ]
                }
            ]
        }
        try:
            if requests:
                requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=5)
            else:
                import urllib.request
                req = urllib.request.Request(
                    SLACK_WEBHOOK_URL,
                    data=json.dumps(payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"}
                )
                urllib.request.urlopen(req, timeout=5)
            logger.info("Dispatched alert to Slack successfully.")
        except Exception as e:
            logger.error(f"Failed to deliver Slack webhook: {e}")


if __name__ == "__main__":
    suite = ClickstreamQualitySuite()
    clean_sample = [{
        "event_id": "evt_1",
        "user_id": "usr_1",
        "session_id": "sess_1",
        "event_type": "page_view",
        "timestamp": "2026-10-27T20:00:00Z",
        "epoch_ms": 1714000000000,
        "device": "desktop_chrome",
        "region": "us-east-1",
        "ip_address": "192.168.1.1",
        "properties": {"amount": 0.0}
    }]
    report = suite.validate_batch(clean_sample)
    print("Clean Sample Check:", report["passed"])

    drift_sample = [{
        "event_id": "evt_2",
        "user_id": None,  # intentional null error
        "event_type": "unknown_action",  # invalid
        "properties": {"amount": -50.0}
    }]
    report_bad = suite.validate_batch(drift_sample)
    print("Drift/Failure Report:", json.dumps(report_bad, indent=2))
