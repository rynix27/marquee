"""Simulates a live release-day event stream into ClickHouse.

This stands in for real feeds (box-office reporting, streaming telemetry,
social listening) so the project is fully runnable without third-party API
keys. Everything downstream of this file — the schema, the agent's queries,
the MCP wiring, the alert logic — is real and would work unmodified against
a genuine data feed.

We deliberately script in a "West Coast evening dip" around the 40-60%
mark of a simulated run so the proactive alert has something real to catch
during a demo, rather than relying on pure randomness to trigger it.

NOTE ON TIMESTAMPS: event_time is intentionally left out of every insert
below and allowed to fall back to the schema's `DEFAULT now()`. That means
ClickHouse stamps each row with its own server clock rather than trusting
whatever clock the machine running this code happens to have — this avoids
an entire class of "no rows found in the last N minutes" bugs caused by a
skewed or misconfigured local/container clock (client clocks, especially on
dev machines, are not something to rely on for freshness filtering).

REALISM NOTES:
- Theater names are fictional (no real chain names/trademarks) but follow a
  realistic naming pattern: "<City> <Descriptor> <Screen count>".
- Ticket sales follow a simulated day/night curve (quiet mornings, a lunch
  bump, a big evening peak) layered on top of the scripted West-region dip,
  so the numbers rise and fall the way a real release day actually does
  instead of flat random noise.
"""
import asyncio
import math
import random
import uuid
from datetime import datetime

from backend.clickhouse_client import get_client

REGIONS = ["West", "Midwest", "Northeast", "South"]
CITIES = {
    "West": ["Los Angeles", "San Francisco", "Seattle", "Portland"],
    "Midwest": ["Chicago", "Detroit", "Minneapolis", "St. Louis"],
    "Northeast": ["New York", "Boston", "Philadelphia"],
    "South": ["Atlanta", "Dallas", "Houston", "Miami"],
}
SCREEN_TYPES = ["Standard", "IMAX", "Premium"]
PLATFORMS = ["X", "Reddit", "Letterboxd", "TikTok"]
TOPICS = ["third_act", "performances", "pacing", "visuals", "general"]

# Fictional theater descriptors used to build realistic-sounding names like
# "Chicago Riverside 14" or "Atlanta Grand Vista 9" — deliberately generic,
# no real theater chain names or trademarks.
THEATER_DESCRIPTORS = [
    "Riverside", "Grand Vista", "Starlight", "Metro Plaza", "Uptown",
    "Cascade", "Harborview", "Summit", "Meridian", "Northgate",
]

# Index into this run's tick count where the West region deliberately
# underperforms, so the alert checker has something concrete to fire on.
DIP_START_TICK = 12
DIP_END_TICK = 30

# One full simulated "release day" (open to close) plays out over this many
# ticks, looping if the simulator runs longer than that.
TICKS_PER_SIMULATED_DAY = 48


def _theater_name(city: str, screen_type: str) -> tuple[str, str]:
    """Returns (display_name, theater_id) for a fictional theater — a
    realistic-sounding name plus a short stable-looking ID."""
    descriptor = random.choice(THEATER_DESCRIPTORS)
    screens = random.choice([9, 12, 14, 16, 18])
    name = f"{city} {descriptor} {screens}"
    theater_id = f"{city[:3].upper()}-{descriptor[:3].upper()}-{screen_type[:2]}"
    return name, theater_id


def _day_night_multiplier(tick: int) -> float:
    """Maps a tick number onto a simulated 24-hour release-day curve: quiet
    in the morning, a modest lunch bump, and a big evening peak (most movie
    tickets sell for evening showtimes) — using a smooth sinusoidal shape
    rather than a flat random baseline, so a chart of sales over time looks
    like an actual day instead of noise.
    """
    hour_of_day = (tick % TICKS_PER_SIMULATED_DAY) / TICKS_PER_SIMULATED_DAY * 24
    # Peak centered around hour 19 (7pm), trough around hour 6 (6am).
    curve = math.sin((hour_of_day - 6) / 24 * 2 * math.pi - math.pi / 2)
    # Rescale from [-1, 1] to a [0.35, 1.15] multiplier range so it's never
    # fully dead, just quieter overnight.
    return 0.35 + (curve + 1) / 2 * 0.80


def seed_dip_scenario(client, uuid_module=uuid):
    """Inserts a small, deterministic 'West region dip' scenario directly —
    used by the smoke test and by the instant scenario-replay endpoint
    (POST /replay-scenario) so a demo doesn't have to wait real minutes for
    the background simulator's scripted dip tick to come around.
    """
    showtime = datetime.utcnow()  # a business field, not used for freshness filtering

    sales_rows = [
        # region, tickets_sold, seats_total, revenue, city
        ["West",      45, 200, 45 * 14.0, "Los Angeles"],
        ["West",      38, 200, 38 * 14.0, "San Francisco"],
        ["Midwest",  180, 200, 180 * 14.0, "Chicago"],
        ["Northeast",190, 200, 190 * 14.0, "New York"],
        ["South",    175, 200, 175 * 14.0, "Atlanta"],
    ]
    rows = []
    for region, tickets, seats, revenue, city in sales_rows:
        _, theater_id = _theater_name(city, "Standard")
        rows.append([theater_id, city, region, "Standard", tickets, seats, round(revenue, 2), showtime])

    client.insert(
        "ticket_sales",
        rows,
        column_names=["theater_id", "city", "region", "screen_type",
                       "tickets_sold", "seats_total", "revenue", "showtime"],
    )

    sentiment_rows = [
        # score, topic, text, region (negative third_act mentions tagged West
        # to match the scripted West dip; positive general chatter left
        # unresolved ('') since most real social posts aren't geo-taggable)
        [-0.6, "third_act", "the ending just didn't land for me", "West"],
        [-0.7, "third_act", "felt rushed and confusing at the end", "West"],
        [-0.5, "third_act", "third act pacing killed the momentum", "West"],
        [-0.65, "third_act", "wanted more from the finale", "West"],
        [-0.55, "third_act", "ending felt unearned", "West"],
        [0.6, "performances", "the lead actor was incredible", ""],
        [0.7, "visuals", "cinematography was stunning", ""],
    ]
    client.insert(
        "sentiment_events",
        [["X", str(uuid_module.uuid4())[:8], r[0], r[1], r[2], r[3]] for r in sentiment_rows],
        column_names=["platform", "post_id", "sentiment_score", "topic", "text_snippet", "region"],
    )

    streaming_rows = [
        ["West", "cdn-west-1", 0.12, 0.05, 8000],
        ["Midwest", "cdn-midwest-1", 0.02, 0.005, 12000],
        ["Northeast", "cdn-northeast-1", 0.015, 0.004, 15000],
        ["South", "cdn-south-1", 0.02, 0.006, 9000],
    ]
    client.insert(
        "streaming_health",
        streaming_rows,
        column_names=["region", "cdn_node", "buffer_rate", "error_rate", "concurrent_viewers"],
    )


async def run_simulator(stop_event: asyncio.Event, tick_seconds: float = 5.0):
    """Runs forever (until stop_event is set), inserting a batch of events
    into ClickHouse every `tick_seconds`."""
    client = get_client()
    tick = 0

    while not stop_event.is_set():
        tick += 1
        showtime = datetime.utcnow()  # business field only, not a freshness filter field
        in_dip = DIP_START_TICK <= tick <= DIP_END_TICK
        day_multiplier = _day_night_multiplier(tick)

        # Each insert function stays synchronous (they're reused directly by
        # test_seed.py and test_alerts.py, which have no event loop), so the
        # asyncio.to_thread wrapping happens here at the async call site —
        # without it, these blocking network calls would stall the whole
        # event loop every tick, freezing chat/SSE/every other request for
        # the duration of each insert.
        await asyncio.to_thread(_insert_ticket_sales, client, showtime, in_dip, day_multiplier)
        await asyncio.to_thread(_insert_sentiment, client, in_dip)
        await asyncio.to_thread(_insert_streaming_health, client, in_dip)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=tick_seconds)
        except asyncio.TimeoutError:
            pass


def _insert_ticket_sales(client, showtime, in_dip, day_multiplier):
    rows = []
    for region in REGIONS:
        for city in CITIES[region]:
            for screen_type in random.sample(SCREEN_TYPES, k=random.randint(1, 3)):
                baseline = {"Standard": 120, "IMAX": 220, "Premium": 90}[screen_type]
                multiplier = day_multiplier
                if in_dip and region == "West":
                    multiplier *= 0.45  # the scripted dip layers on top of the day curve
                sold = max(0, int(baseline * multiplier * random.uniform(0.85, 1.1)))
                seats = int(baseline * 1.3)
                sold = min(sold, seats)
                price = {"Standard": 14.0, "IMAX": 19.5, "Premium": 22.0}[screen_type]

                _, theater_id = _theater_name(city, screen_type)
                rows.append([
                    theater_id, city, region, screen_type, sold, seats,
                    round(sold * price, 2), showtime,
                ])

    client.insert(
        "ticket_sales",
        rows,
        column_names=[
            "theater_id", "city", "region", "screen_type",
            "tickets_sold", "seats_total", "revenue", "showtime",
        ],
    )


def _insert_sentiment(client, in_dip):
    rows = []
    n = random.randint(15, 40)
    for _ in range(n):
        topic = random.choice(TOPICS)
        score = random.uniform(0.1, 0.9)
        region = ""  # most simulated posts are left geo-unresolved, like real social data
        if in_dip and topic == "third_act":
            score = random.uniform(-0.9, -0.2)  # scripted negative swing
            # ~70% of the scripted negative third_act chatter gets geo-resolved
            # to West, giving the correlation tool a genuine region-tagged
            # signal to find instead of an unregioned pile of negativity.
            region = "West" if random.random() < 0.7 else ""
        rows.append([
            random.choice(PLATFORMS), str(uuid.uuid4())[:8],
            round(score, 3), topic, f"[simulated post about {topic}]", region,
        ])

    client.insert(
        "sentiment_events",
        rows,
        column_names=["platform", "post_id", "sentiment_score", "topic", "text_snippet", "region"],
    )


def _insert_streaming_health(client, in_dip):
    rows = []
    for region in REGIONS:
        buffer_rate = random.uniform(0.01, 0.04)
        error_rate = random.uniform(0.001, 0.01)
        if in_dip and region == "West":
            buffer_rate = random.uniform(0.08, 0.15)
            error_rate = random.uniform(0.03, 0.07)

        rows.append([
            region, f"cdn-{region.lower()}-{random.randint(1,3)}",
            round(buffer_rate, 4), round(error_rate, 4),
            random.randint(2000, 25000),
        ])

    client.insert(
        "streaming_health",
        rows,
        column_names=["region", "cdn_node", "buffer_rate", "error_rate", "concurrent_viewers"],
    )
