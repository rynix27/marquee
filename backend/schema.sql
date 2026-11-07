-- Marquee ClickHouse schema
-- Run with: clickhouse-client --host <host> --secure --password <pw> < schema.sql

CREATE TABLE IF NOT EXISTS ticket_sales (
    event_time    DateTime DEFAULT now(),
    theater_id    String,
    city          String,
    region        String,          -- e.g. "West", "Midwest", "Northeast", "South"
    screen_type   String,          -- "Standard", "IMAX", "Premium"
    tickets_sold  UInt32,
    seats_total   UInt32,
    revenue       Float64,
    showtime      DateTime
) ENGINE = MergeTree()
ORDER BY (event_time, region, theater_id);

CREATE TABLE IF NOT EXISTS sentiment_events (
    event_time      DateTime DEFAULT now(),
    platform        String,        -- "X", "Reddit", "Letterboxd", "TikTok"
    post_id         String,
    sentiment_score Float32,       -- -1.0 (very negative) to 1.0 (very positive)
    topic           String,        -- e.g. "third_act", "performances", "pacing", "general"
    text_snippet    String,
    region          String DEFAULT ''  -- best-effort geo tag ("West"/"Midwest"/"Northeast"/"South"/""),
                                        -- lets sentiment be correlated against region-level sales/streaming data.
                                        -- '' means "not geo-resolved" (most real social posts aren't) —
                                        -- treat as unknown, not as a fifth region.
) ENGINE = MergeTree()
ORDER BY event_time;

CREATE TABLE IF NOT EXISTS streaming_health (
    event_time          DateTime DEFAULT now(),
    region              String,
    cdn_node             String,
    buffer_rate          Float32,  -- fraction of sessions buffering, 0.0-1.0
    error_rate            Float32, -- fraction of sessions erroring, 0.0-1.0
    concurrent_viewers    UInt32
) ENGINE = MergeTree()
ORDER BY (event_time, region);

CREATE TABLE IF NOT EXISTS alerts_log (
    event_time   DateTime DEFAULT now(),
    alert_type   String,           -- "sales_drop", "sentiment_shift", "streaming_degradation"
    severity     String,           -- "info", "warning", "critical"
    message      String
) ENGINE = MergeTree()
ORDER BY event_time;

-- Observability: every tool call the agent makes, so the "Agent activity"
-- panel can show judges/operators exactly what the agent queried and when,
-- rather than asking them to trust an opaque black-box answer.
CREATE TABLE IF NOT EXISTS agent_activity (
    event_time  DateTime DEFAULT now(),
    session_id  String,
    tool_name   String,
    arguments   String,   -- JSON-encoded args, best-effort
    duration_ms UInt32 DEFAULT 0  -- not currently populated (ADK's event stream
                                    -- doesn't cleanly expose per-tool-call timing
                                    -- at this integration point) — always 0 for now,
                                    -- reserved for when that instrumentation is added
) ENGINE = MergeTree()
ORDER BY event_time;

-- Convenience view: rolling 15-minute sales by region
CREATE VIEW IF NOT EXISTS sales_rolling_15m AS
SELECT
    region,
    toStartOfFifteenMinutes(event_time) AS bucket,
    sum(tickets_sold) AS tickets,
    sum(revenue) AS revenue
FROM ticket_sales
GROUP BY region, bucket
ORDER BY bucket DESC;
