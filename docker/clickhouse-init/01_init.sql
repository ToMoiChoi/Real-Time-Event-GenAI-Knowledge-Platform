-- ClickHouse Real-time Analytics Engine Initialization Script
-- Designed for sub-second query latency on 50,000+ events/sec

CREATE DATABASE IF NOT EXISTS analytics;

USE analytics;

-- 1. Raw / Deduplicated Clickstream Table
CREATE TABLE IF NOT EXISTS analytics.user_clickstream_events (
    event_id UUID,
    user_id LowCardinality(String),
    session_id String,
    event_type LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    device LowCardinality(String),
    region LowCardinality(String),
    ip_address IPv4,
    amount Float64 DEFAULT 0.0,
    search_keyword Nullable(String),
    latency_ms UInt32,
    is_fraud UInt8 DEFAULT 0,
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (region, event_type, toDate(timestamp), user_id, event_id)
TTL timestamp + INTERVAL 90 DAY;

-- 2. Real-time Fraud Alerts Table
CREATE TABLE IF NOT EXISTS analytics.fraud_alerts (
    alert_id String,
    rule_name LowCardinality(String),
    severity LowCardinality(String),
    user_id String,
    event_id String,
    amount Float64,
    ip_address String,
    details String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (severity, created_at, user_id);

-- 3. Aggregating Table for Live Metric Dashboards (Sub-10ms response)
CREATE TABLE IF NOT EXISTS analytics.hourly_platform_metrics (
    hour_bucket DateTime,
    region LowCardinality(String),
    event_type LowCardinality(String),
    total_events AggregateFunction(count, UInt64),
    unique_users AggregateFunction(uniqExact, String),
    total_volume AggregateFunction(sum, Float64),
    avg_latency AggregateFunction(avg, UInt32)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour_bucket)
ORDER BY (region, event_type, hour_bucket);

-- 4. Materialized View continuous aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.hourly_platform_metrics_mv
TO analytics.hourly_platform_metrics AS
SELECT
    toStartOfHour(timestamp) AS hour_bucket,
    region,
    event_type,
    countState(toUInt64(1)) AS total_events,
    uniqExactState(user_id) AS unique_users,
    sumState(amount) AS total_volume,
    avgState(latency_ms) AS avg_latency
FROM analytics.user_clickstream_events
GROUP BY hour_bucket, region, event_type;
