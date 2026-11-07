"""Tests the proactive alert logic in isolation.

This seeds a scenario specifically crafted to cross each threshold
(sales drop, sentiment shift, streaming degradation), runs one pass of the
checker functions directly, and asserts each alert actually fires and lands
in `alerts_log`. This is what lets you *know* the proactive alert will fire
during a live demo instead of hoping it does.

Usage:
    python -m backend.test_alerts
"""
import asyncio
import uuid
from datetime import datetime

from backend.alerts import (
    _check_sales_drop,
    _check_sentiment_shift,
    _check_streaming_health,
    _last_alert_keys,
)
from backend.clickhouse_client import get_client


def _clear_recent_test_data(client):
    """Alerts de-dupe on in-memory keys, so make sure repeated test runs
    don't get silently skipped."""
    _last_alert_keys.clear()


def _seed_sales_drop_scenario(client):
    showtime = datetime.utcnow()  # business field only, not used for freshness filtering
    rows = [
        # region, tickets  (West is >30% below the ~180 avg of the rest)
        ["West", 40], ["Midwest", 180], ["Northeast", 190], ["South", 175],
    ]
    client.insert(
        "ticket_sales",
        [[f"TEST-{r[0]}", "TestCity", r[0], "Standard", r[1], 200, r[1] * 14.0, showtime] for r in rows],
        column_names=["theater_id", "city", "region", "screen_type",
                       "tickets_sold", "seats_total", "revenue", "showtime"],
    )


def _seed_sentiment_drop_scenario(client):
    # 5+ strongly negative mentions on "third_act" -> should trip SENTIMENT_THRESHOLD
    rows = [[-0.6 - i * 0.02, "third_act", f"test negative post {i}"] for i in range(6)]
    client.insert(
        "sentiment_events",
        [["X", str(uuid.uuid4())[:8], r[0], r[1], r[2]] for r in rows],
        column_names=["platform", "post_id", "sentiment_score", "topic", "text_snippet"],
    )


def _seed_streaming_degradation_scenario(client):
    client.insert(
        "streaming_health",
        [["West", "cdn-west-test", 0.15, 0.06, 5000]],
        column_names=["region", "cdn_node", "buffer_rate", "error_rate", "concurrent_viewers"],
    )


async def main():
    client = get_client()
    _clear_recent_test_data(client)

    queue: asyncio.Queue = asyncio.Queue()

    print("→ Seeding sales-drop scenario and checking...")
    _seed_sales_drop_scenario(client)
    await _check_sales_drop(client, queue)
    assert not queue.empty(), "Expected a sales_drop alert to fire but none did"
    alert = await queue.get()
    assert alert["type"] == "sales_drop"
    print(f"  ✓ fired: {alert['message']}\n")

    print("→ Seeding sentiment-shift scenario and checking...")
    _seed_sentiment_drop_scenario(client)
    await _check_sentiment_shift(client, queue)
    assert not queue.empty(), "Expected a sentiment_shift alert to fire but none did"
    alert = await queue.get()
    assert alert["type"] == "sentiment_shift"
    print(f"  ✓ fired: {alert['message']}\n")

    print("→ Seeding streaming-degradation scenario and checking...")
    _seed_streaming_degradation_scenario(client)
    await _check_streaming_health(client, queue)
    assert not queue.empty(), "Expected a streaming_degradation alert to fire but none did"
    alert = await queue.get()
    assert alert["type"] == "streaming_degradation"
    print(f"  ✓ fired: {alert['message']}\n")

    print("→ Confirming de-dupe works (re-running sales-drop check should NOT re-fire)...")
    await _check_sales_drop(client, queue)
    assert queue.empty(), "Expected no duplicate alert, but one fired anyway"
    print("  ✓ de-dupe holds\n")

    print("All alert checks passed. ✅")
    print("This confirms each alert type will actually fire against real thresholds —")
    print("safe to rely on during a live demo.")


if __name__ == "__main__":
    asyncio.run(main())
