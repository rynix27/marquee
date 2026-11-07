"""Marquee agent: Gemini + ADK, grounded in ClickHouse via the
official ClickHouse MCP server (mcp-clickhouse).

ARCHITECTURE — multi-agent, not a single monolithic agent:
  marquee_agent (coordinator)
    - sales_agent       — owns ticket_sales questions (compare_regions, MCP SQL)
    - sentiment_agent    — owns sentiment/topic questions + the cross-signal
                            correlation tool (does negative sentiment line up
                            with underperforming regions?)
    - streaming_agent    — owns streaming-health questions
  Each sub-agent gets the same underlying ClickHouse MCP toolset (so any of
  them can run ad-hoc SQL in their domain) plus tools scoped to their
  specialty. The coordinator delegates based on the question and can call
  more than one sub-agent for a diagnostic question that spans domains
  (e.g. "why is West underperforming" touches sales AND sentiment).
  `recent_alerts` and `draft_status_update` stay on the coordinator since
  they're cross-cutting (alerts span all three domains; a status update
  summarizes across all of them).

The agent's data tool is the actual `mcp-clickhouse` MCP server, run as a
subprocess via `uvx` and connected over stdio, not a hand-rolled wrapper.

NOTE: ADK's exact import paths have moved around between versions. This
file targets `google-adk` as installed via
`google-cloud-aiplatform[agent_engines,adk]>=1.101.0`. If your installed
version differs, check
`python -c "import google.adk; print(google.adk.__file__)"` and adjust the
imports below — the *shape* of the agent (MCPToolset + FunctionTool +
sub_agents) will stay the same. In particular, if your installed ADK
version's `Agent` doesn't accept `sub_agents=[...]` directly, check
`google.adk.agents.LlmAgent`, which is the class `Agent` typically aliases —
the delegation pattern is unchanged either way.
"""
import asyncio
import os

from google.adk.agents import Agent
from google.adk.tools.mcp_tool.mcp_toolset import MCPToolset, StdioServerParameters
from google.adk.tools import FunctionTool

FILM_TITLE = os.environ.get("FILM_TITLE", "Midnight Fracture")

SYSTEM_INSTRUCTION = f"""You are Marquee, the release-day war room coordinator
for the studio distribution team behind "{FILM_TITLE}". You have three
specialist sub-agents: sales_agent (ticket sales), sentiment_agent (social
sentiment, including cross-signal correlation against sales), and
streaming_agent (streaming health). You also have direct access to
recent_alerts (what the proactive checker has already flagged),
draft_status_update (composes a forwardable exec summary), and
whats_changed (a cross-domain diff vs. N minutes ago — use this for
general "catch me up" / "what's changed" questions instead of asking each
sub-agent separately, since it's a single pre-joined comparison across all
three domains).

Rules:
- Delegate domain-specific questions to the matching sub-agent rather than
  guessing at their data yourself.
- For diagnostic questions that span domains ("why is X underperforming"),
  consult more than one sub-agent (e.g. sales_agent AND sentiment_agent)
  before answering, and connect their findings into one coherent answer.
- Always ground answers in numbers actually queried — cite the specific
  figures (region, timeframe, values) pulled, never vague terms like
  "sales seem down."
- If the user asks for a status update, summary for the team, or something
  to forward/share, call draft_status_update rather than composing one from
  memory.
- Keep answers tight: 2-4 sentences unless the user asks for detail.
- If you don't have enough data to answer confidently, say so plainly instead
  of guessing.
- The three ClickHouse tables in play: ticket_sales (event_time, theater_id,
  city, region, screen_type, tickets_sold, seats_total, revenue, showtime),
  sentiment_events (event_time, platform, post_id, sentiment_score [-1 to 1],
  topic, text_snippet, region [may be '' if not geo-resolved]),
  streaming_health (event_time, region, cdn_node, buffer_rate, error_rate,
  concurrent_viewers). There's also a rolling view `sales_rolling_15m`
  (region, bucket, tickets, revenue).
"""


def _build_clickhouse_mcp_toolset() -> MCPToolset:
    """Connects to the official ClickHouse MCP server as a subprocess.

    mcp-clickhouse reads its ClickHouse connection details straight from
    environment variables (CLICKHOUSE_HOST, CLICKHOUSE_PORT, etc.), which
    is why we just pass through the current environment.
    """
    return MCPToolset(
        connection_params=StdioServerParameters(
            command="uvx",
            args=["mcp-clickhouse"],
            env={
                "CLICKHOUSE_HOST": os.environ["CLICKHOUSE_HOST"],
                "CLICKHOUSE_PORT": os.environ.get("CLICKHOUSE_PORT", "8443"),
                "CLICKHOUSE_USER": os.environ.get("CLICKHOUSE_USER", "default"),
                "CLICKHOUSE_PASSWORD": os.environ.get("CLICKHOUSE_PASSWORD", ""),
                "CLICKHOUSE_DATABASE": os.environ.get("CLICKHOUSE_DATABASE", "default"),
                "CLICKHOUSE_SECURE": os.environ.get("CLICKHOUSE_SECURE", "true"),
            },
        ),
    )


async def recent_alerts(limit: int = 5) -> dict:
    """Returns the most recent alerts the proactive checker has already
    fired, so the agent can reference them instead of re-deriving them.

    Args:
        limit: max number of recent alerts to return.
    """
    from backend.clickhouse_client import get_client

    client = get_client()
    result = await asyncio.to_thread(
        client.query,
        """
        SELECT event_time, alert_type, severity, message
        FROM alerts_log
        ORDER BY event_time DESC
        LIMIT {limit:UInt32}
        """,
        parameters={"limit": int(limit)},
    )
    return {
        "alerts": [
            {
                "time": str(row[0]),
                "type": row[1],
                "severity": row[2],
                "message": row[3],
            }
            for row in result.result_rows
        ]
    }


async def compare_regions(window_minutes: int = 5) -> dict:
    """Compares each region's ticket sales in the current window against the
    immediately preceding window of the same length, in one ClickHouse query
    using conditional aggregation. This is the tool to reach for on any
    "why is X behind" or "how does this compare" question — it shows the
    trend, not just a snapshot.

    Args:
        window_minutes: length of each comparison window in minutes (default 5,
            so this compares "last 5 min" vs. "the 5 min before that").
    """
    from backend.clickhouse_client import get_client

    client = get_client()
    w = int(window_minutes)
    query = """
        SELECT
            region,
            sumIf(tickets_sold, event_time > now() - INTERVAL {w:UInt32} MINUTE) AS current_tickets,
            sumIf(tickets_sold, event_time <= now() - INTERVAL {w:UInt32} MINUTE
                               AND event_time > now() - INTERVAL {w2:UInt32} MINUTE) AS previous_tickets,
            sumIf(revenue, event_time > now() - INTERVAL {w:UInt32} MINUTE) AS current_revenue,
            sumIf(revenue, event_time <= now() - INTERVAL {w:UInt32} MINUTE
                          AND event_time > now() - INTERVAL {w2:UInt32} MINUTE) AS previous_revenue
        FROM ticket_sales
        WHERE event_time > now() - INTERVAL {w2:UInt32} MINUTE
        GROUP BY region
        ORDER BY current_tickets ASC
    """
    result = await asyncio.to_thread(client.query, query, parameters={"w": w, "w2": 2 * w})

    comparisons = []
    for region, cur_tix, prev_tix, cur_rev, prev_rev in result.result_rows:
        pct_change = None
        if prev_tix and prev_tix > 0:
            pct_change = round((cur_tix - prev_tix) / prev_tix * 100, 1)
        comparisons.append({
            "region": region,
            "current_window_tickets": cur_tix,
            "previous_window_tickets": prev_tix,
            "pct_change": pct_change,
            "current_window_revenue": round(cur_rev, 2) if cur_rev else 0,
            "previous_window_revenue": round(prev_rev, 2) if prev_rev else 0,
        })

    return {"window_minutes": w, "comparisons": comparisons}


async def correlate_sentiment_sales(window_minutes: int = 10) -> dict:
    """Finds regions where negative sentiment and underperforming sales are
    happening at the same time — the cross-signal check a human would have
    to do manually by tabbing between a sentiment dashboard and a box-office
    dashboard. Only sentiment events that have been geo-resolved to a region
    (see `region` column) can be correlated; unresolved posts are excluded
    from this specific tool but still visible via recent_alerts/raw SQL.

    Args:
        window_minutes: how far back to look for both signals (default 10).
    """
    from backend.clickhouse_client import get_client

    client = get_client()
    w = int(window_minutes)
    # Correctly excludes the region itself from "average of other regions" —
    # computed via a grand-total CTE rather than a naive subquery, since a
    # naive (non-excluding) version would compare a region against an
    # average that includes its own number, understating real divergence.
    query = """
        WITH region_totals AS (
            SELECT region, sum(tickets_sold) AS tickets
            FROM ticket_sales
            WHERE event_time > now() - INTERVAL {w:UInt32} MINUTE
            GROUP BY region
        ),
        totals AS (
            SELECT sum(tickets) AS grand_total, count() AS region_count FROM region_totals
        )
        SELECT
            s.region AS region,
            avg(s.sentiment_score) AS avg_sentiment,
            count() AS mention_count,
            any(s.topic) AS sample_topic,
            rt.tickets AS tickets,
            (t.grand_total - rt.tickets) / greatest(t.region_count - 1, 1) AS avg_other_regions
        FROM
            (SELECT region, sentiment_score, topic
             FROM sentiment_events
             WHERE event_time > now() - INTERVAL {w:UInt32} MINUTE
               AND region != '') AS s
        INNER JOIN region_totals AS rt ON s.region = rt.region
        CROSS JOIN totals AS t
        GROUP BY s.region, rt.tickets, t.grand_total, t.region_count
        HAVING avg_sentiment < 0 AND rt.tickets < avg_other_regions
        ORDER BY avg_sentiment ASC
    """
    result = await asyncio.to_thread(client.query, query, parameters={"w": w})

    findings = []
    for region, avg_sentiment, mention_count, sample_topic, tickets, avg_other in result.result_rows:
        findings.append({
            "region": region,
            "avg_sentiment": round(avg_sentiment, 2),
            "geo_tagged_mention_count": mention_count,
            "sample_topic": sample_topic,
            "tickets_this_region": tickets,
            "avg_tickets_other_regions": round(avg_other, 1) if avg_other else None,
        })

    return {
        "window_minutes": w,
        "correlated_regions": findings,
        "note": (
            "Only includes sentiment already geo-resolved to a region — "
            "real deployments would need a geolocation/NLP step upstream "
            "for posts that don't self-report location."
        ),
    }


async def draft_status_update() -> dict:
    """Composes a short, copy-pasteable exec status update by pulling
    together the most recent alerts and a regional sales snapshot — turns
    a diagnosis into an artifact the war room team can actually forward,
    instead of leaving the user to manually transcribe the chat answer.
    Call this when the user asks for a "status update", "summary for the
    team", or similar — not for routine Q&A.
    """
    alerts = (await recent_alerts(limit=3))["alerts"]
    regions = (await compare_regions(window_minutes=5))["comparisons"]

    lines = [f"MARQUEE STATUS UPDATE — {FILM_TITLE}", ""]
    if alerts:
        lines.append("Active/recent alerts:")
        for a in alerts:
            lines.append(f"  [{a['severity'].upper()}] {a['message']}")
    else:
        lines.append("No active alerts in the last check window.")

    lines.append("")
    lines.append("Regional sales trend (current vs. previous 5-min window):")
    for r in regions:
        trend = f"{r['pct_change']:+.1f}%" if r["pct_change"] is not None else "n/a"
        lines.append(f"  {r['region']}: {r['current_window_tickets']} tickets ({trend})")

    return {"status_update_text": "\n".join(lines)}


async def whats_changed(minutes_ago: int = 15) -> dict:
    """Cross-domain 'what's different from N minutes ago' diff — sales,
    sentiment, and streaming health each compared: current window vs. the
    equivalent prior window. Useful for general situational awareness
    ('catch me up') even when nothing has crossed an alert threshold yet —
    alerts only fire on hard thresholds, this shows drift before that.

    Args:
        minutes_ago: size of each comparison window in minutes (default 15).
    """
    from backend.clickhouse_client import get_client

    client = get_client()
    w = int(minutes_ago)

    # The three queries are independent (different tables, no shared state),
    # so they run concurrently via asyncio.gather rather than one after
    # another — this genuinely halves-to-thirds the tool's latency compared
    # to sequential awaits, since each is a separate network round trip.
    sales, sentiment, streaming = await asyncio.gather(
        asyncio.to_thread(
            client.query,
            """
            SELECT region,
                sumIf(tickets_sold, event_time > now() - INTERVAL {w:UInt32} MINUTE) AS recent,
                sumIf(tickets_sold, event_time <= now() - INTERVAL {w:UInt32} MINUTE
                                   AND event_time > now() - INTERVAL {w2:UInt32} MINUTE) AS prior
            FROM ticket_sales
            WHERE event_time > now() - INTERVAL {w2:UInt32} MINUTE
            GROUP BY region
            """,
            parameters={"w": w, "w2": 2 * w},
        ),
        asyncio.to_thread(
            client.query,
            """
            SELECT topic,
                avgIf(sentiment_score, event_time > now() - INTERVAL {w:UInt32} MINUTE) AS recent,
                avgIf(sentiment_score, event_time <= now() - INTERVAL {w:UInt32} MINUTE
                                       AND event_time > now() - INTERVAL {w2:UInt32} MINUTE) AS prior
            FROM sentiment_events
            WHERE event_time > now() - INTERVAL {w2:UInt32} MINUTE
            GROUP BY topic
            """,
            parameters={"w": w, "w2": 2 * w},
        ),
        asyncio.to_thread(
            client.query,
            """
            SELECT region,
                avgIf(error_rate, event_time > now() - INTERVAL {w:UInt32} MINUTE) AS recent,
                avgIf(error_rate, event_time <= now() - INTERVAL {w:UInt32} MINUTE
                                  AND event_time > now() - INTERVAL {w2:UInt32} MINUTE) AS prior
            FROM streaming_health
            WHERE event_time > now() - INTERVAL {w2:UInt32} MINUTE
            GROUP BY region
            """,
            parameters={"w": w, "w2": 2 * w},
        ),
    )

    def _pct_delta(recent, prior):
        if not prior:
            return None
        return round((recent - prior) / prior * 100, 1)

    movers = []
    for region, recent, prior in sales.result_rows:
        d = _pct_delta(recent, prior)
        if d is not None:
            movers.append({"domain": "sales", "dimension": region, "metric": "tickets_sold", "pct_change": d})
    for topic, recent, prior in sentiment.result_rows:
        if prior is not None:
            movers.append({"domain": "sentiment", "dimension": topic, "metric": "avg_score",
                            "pct_change": round(recent - prior, 3) if recent is not None else None})
    for region, recent, prior in streaming.result_rows:
        d = _pct_delta(recent, prior)
        if d is not None:
            movers.append({"domain": "streaming", "dimension": region, "metric": "error_rate", "pct_change": d})

    # Biggest absolute movers first — that's what a "catch me up" question wants to see.
    movers.sort(key=lambda m: abs(m["pct_change"]) if m["pct_change"] is not None else 0, reverse=True)

    return {"window_minutes": w, "biggest_movers": movers[:8]}


def build_sales_agent(clickhouse_toolset: MCPToolset) -> Agent:
    return Agent(
        name="sales_agent",
        model="gemini-2.0-flash",
        description="Answers ticket-sales and box-office questions using ClickHouse.",
        instruction=(
            "You specialize in ticket_sales data. Use compare_regions for any "
            "trend/comparison question; use the ClickHouse SQL tools for anything "
            "else. Always cite the actual figures you queried."
        ),
        tools=[clickhouse_toolset, FunctionTool(compare_regions)],
    )


def build_sentiment_agent(clickhouse_toolset: MCPToolset) -> Agent:
    return Agent(
        name="sentiment_agent",
        model="gemini-2.0-flash",
        description=(
            "Answers social-sentiment questions and checks whether negative "
            "sentiment is coinciding with underperforming regions."
        ),
        instruction=(
            "You specialize in sentiment_events data. For diagnostic questions "
            "('is sentiment hurting sales anywhere'), prefer correlate_sentiment_sales "
            "over eyeballing sentiment alone — it's the tool that actually joins "
            "sentiment against regional sales. Otherwise use the ClickHouse SQL tools."
        ),
        tools=[clickhouse_toolset, FunctionTool(correlate_sentiment_sales)],
    )


def build_streaming_agent(clickhouse_toolset: MCPToolset) -> Agent:
    return Agent(
        name="streaming_agent",
        model="gemini-2.0-flash",
        description="Answers streaming-health questions (buffering, errors, concurrent viewers) using ClickHouse.",
        instruction=(
            "You specialize in streaming_health data. Use the ClickHouse SQL "
            "tools to answer buffering/error-rate/concurrency questions."
        ),
        tools=[clickhouse_toolset],
    )


def build_agent() -> Agent:
    """Builds the coordinator agent with three domain sub-agents. The
    coordinator itself keeps the cross-cutting tools (recent_alerts,
    draft_status_update) since those summarize across all three domains,
    and delegates domain-specific questions to the relevant sub-agent.

    Each sub-agent gets its OWN MCPToolset instance (its own `uvx
    mcp-clickhouse` subprocess) rather than sharing one across all three —
    a shared stdio connection would risk interleaved/contended requests if
    the coordinator ever invokes sub-agents concurrently for one question
    (e.g. a cross-domain "why is X down" that consults sales_agent AND
    sentiment_agent). Three subprocesses cost a bit more memory than one;
    that's a reasonable trade for not having a shared-connection race.
    """
    sub_agents = [
        build_sales_agent(_build_clickhouse_mcp_toolset()),
        build_sentiment_agent(_build_clickhouse_mcp_toolset()),
        build_streaming_agent(_build_clickhouse_mcp_toolset()),
    ]

    return Agent(
        name="marquee_agent",
        model="gemini-2.0-flash",
        description="Release-day war room coordinator grounded in live ClickHouse data.",
        instruction=SYSTEM_INSTRUCTION,
        tools=[
            FunctionTool(recent_alerts),
            FunctionTool(draft_status_update),
            FunctionTool(whats_changed),
        ],
        sub_agents=sub_agents,
    )
