"""Proactive alert checker.

Runs on a timer, independent of any chat request, and pushes alerts onto a
shared asyncio.Queue that the SSE endpoint (/stream/alerts) drains. This is
what makes the agent "agentic" rather than purely reactive: it notices
problems and speaks up before anyone asks.
"""
import asyncio
from datetime import datetime

from backend.clickhouse_client import get_client

# Thresholds — tune these for your demo / real deployment
SALES_DROP_THRESHOLD = 0.30       # 30% below the all-region average = flag
SENTIMENT_THRESHOLD = -0.15       # rolling avg sentiment below this = flag
STREAMING_ERROR_THRESHOLD = 0.03  # error rate above this = flag

# How often the proactive checker re-evaluates every threshold. This is the
# real, system-level "time to detect" bound: because the checker re-runs
# these queries on this cadence and _fire() de-dupes on first breach, any
# condition that crosses a threshold is caught within one interval of it
# happening — not a marketing number, just how the loop actually works.
# Surfaced to the frontend so the UI can show this honestly rather than
# asserting an unverified "vs. manual review" comparison.
CHECK_INTERVAL_SECONDS = 8.0

_last_alert_keys: set[str] = set()  # simple de-dupe so we don't spam the same alert


def reset_dedupe():
    """Clears the alert de-dupe cache. Used by the instant scenario-replay
    endpoint so a manually triggered demo scenario always fires fresh
    alerts, even if the same condition already fired once this run."""
    _last_alert_keys.clear()


async def run_alert_checker(alert_queue: "asyncio.Queue", stop_event: asyncio.Event, interval_seconds: float = CHECK_INTERVAL_SECONDS):
    client = get_client()
    while not stop_event.is_set():
        await _check_sales_drop(client, alert_queue)
        await _check_sentiment_shift(client, alert_queue)
        await _check_streaming_health(client, alert_queue)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except asyncio.TimeoutError:
            pass


async def _check_sales_drop(client, alert_queue):
    query = """
        SELECT region, sum(tickets_sold) AS tickets
        FROM ticket_sales
        WHERE event_time > now() - INTERVAL 5 MINUTE
        GROUP BY region
    """
    result = await asyncio.to_thread(client.query, query)
    rows = result.result_rows
    if not rows or len(rows) < 2:
        return

    avg_tickets = sum(r[1] for r in rows) / len(rows)
    if avg_tickets == 0:
        return

    for region, tickets in rows:
        drop = 1 - (tickets / avg_tickets)
        if drop > SALES_DROP_THRESHOLD:
            await _fire(
                client, alert_queue,
                key=f"sales_drop:{region}",
                alert_type="sales_drop",
                severity="critical",
                message=(
                    f"{region} region ticket sales are {drop*100:.0f}% below "
                    f"the cross-region average in the last 5 minutes "
                    f"({tickets} vs. ~{avg_tickets:.0f} tickets)."
                ),
            )


async def _check_sentiment_shift(client, alert_queue):
    query = """
        SELECT topic, avg(sentiment_score) AS avg_score, count() AS n
        FROM sentiment_events
        WHERE event_time > now() - INTERVAL 5 MINUTE
        GROUP BY topic
        HAVING n >= 5
    """
    result = await asyncio.to_thread(client.query, query)
    for topic, avg_score, n in result.result_rows:
        if avg_score < SENTIMENT_THRESHOLD:
            await _fire(
                client, alert_queue,
                key=f"sentiment:{topic}",
                alert_type="sentiment_shift",
                severity="warning",
                message=(
                    f"Sentiment on \"{topic}\" just turned negative "
                    f"(avg {avg_score:.2f} across {n} mentions in the last 5 min)."
                ),
            )


async def _check_streaming_health(client, alert_queue):
    query = """
        SELECT region, avg(error_rate) AS avg_error
        FROM streaming_health
        WHERE event_time > now() - INTERVAL 5 MINUTE
        GROUP BY region
    """
    result = await asyncio.to_thread(client.query, query)
    for region, avg_error in result.result_rows:
        if avg_error > STREAMING_ERROR_THRESHOLD:
            await _fire(
                client, alert_queue,
                key=f"streaming:{region}",
                alert_type="streaming_degradation",
                severity="warning",
                message=(
                    f"Streaming error rate in {region} is elevated "
                    f"({avg_error*100:.1f}% of sessions) over the last 5 minutes."
                ),
            )


async def _fire(client, alert_queue, key, alert_type, severity, message):
    if key in _last_alert_keys:
        return  # already alerted on this condition; avoid spamming
    _last_alert_keys.add(key)

    await asyncio.to_thread(
        client.insert,
        "alerts_log",
        [[alert_type, severity, message]],
        column_names=["alert_type", "severity", "message"],
    )

    audio_base64 = None
    if severity == "critical":
        # Best-effort: a spoken announcement for the highest-severity alerts
        # only, so routine warnings don't talk over each other. Runs in a
        # thread so a slow TTS call doesn't block the alert-checking loop.
        from backend.tts import synthesize_alert_audio
        audio_base64 = await asyncio.to_thread(synthesize_alert_audio, message)

    await alert_queue.put({
        "type": alert_type,
        "severity": severity,
        "message": message,
        "time": datetime.utcnow().isoformat(),
        "audio_base64": audio_base64,
        "detection_interval_seconds": CHECK_INTERVAL_SECONDS,
    })
