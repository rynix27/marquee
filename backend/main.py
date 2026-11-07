"""Marquee backend.

Endpoints:
  POST /chat                 -> ask the agent a question, get a grounded answer
  POST /replay-scenario       -> instantly seed the demo dip scenario
  GET  /region/{region}/theaters -> per-theater drill-down for a region
  GET  /stream/sales          -> SSE stream of rolling ticket-sales totals (chart)
  GET  /stream/alerts         -> SSE stream of proactive alerts as they fire
  GET  /stream/activity       -> SSE stream of agent tool-call activity (observability)
  GET  /healthz               -> liveness check

On startup this also boots two background tasks:
  - the simulator (writes fake-but-realistic release-day events to ClickHouse)
  - the alert checker (reads thresholds, pushes to the alerts queue)

AUTH: if MARQUEE_API_KEY is set in the environment, /chat and
/replay-scenario require a matching `X-API-Key` header. If it's unset
(the default), those endpoints are open — this matches the original
hackathon-demo posture but now makes the tradeoff an explicit opt-in
rather than a silent gap.

RATE LIMITING: a simple in-memory sliding-window limiter (no extra
dependency) caps /chat and /replay-scenario per client IP. This is
process-local and resets on restart — fine for a single Cloud Run
instance, not a substitute for a real distributed limiter at scale.
"""
import asyncio
import json
import os
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types as genai_types

from backend.agent import build_agent
from backend.alerts import run_alert_checker, reset_dedupe, _check_sales_drop, _check_sentiment_shift, _check_streaming_health
from backend.clickhouse_client import get_client
from backend.simulator import run_simulator, seed_dip_scenario

load_dotenv()

APP_NAME = "marquee"
alert_queue: asyncio.Queue = asyncio.Queue()
activity_queue: asyncio.Queue = asyncio.Queue(maxsize=500)  # agent tool-call activity, for the observability panel; bounded so it can't grow unbounded if nobody's watching the panel
_stop_event = asyncio.Event()

session_service = InMemorySessionService()
_agent = None
_runner: Runner | None = None


# ---- Auth (opt-in) ----
def require_api_key(x_api_key: str | None = Header(default=None)):
    """No-op if MARQUEE_API_KEY isn't set (open, demo-friendly default).
    If it is set, requires a matching X-API-Key header."""
    expected = os.environ.get("MARQUEE_API_KEY")
    if not expected:
        return
    if x_api_key != expected:
        raise HTTPException(status_code=401, detail="Missing or invalid X-API-Key header.")


# ---- Rate limiting (simple in-memory sliding window, no extra dependency) ----
_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("MARQUEE_RATE_LIMIT_PER_MINUTE", "30"))
_rate_limit_hits: dict[str, deque] = {}


def enforce_rate_limit(request: Request):
    client_ip = request.client.host if request.client else "unknown"
    now = time.monotonic()
    hits = _rate_limit_hits.setdefault(client_ip, deque())

    while hits and now - hits[0] > 60:
        hits.popleft()

    if len(hits) >= _RATE_LIMIT_MAX_REQUESTS:
        raise HTTPException(status_code=429, detail="Rate limit exceeded — try again shortly.")

    hits.append(now)


async def _log_tool_activity_best_effort(session_id: str, event) -> None:
    """Best-effort extraction of tool-call info from an ADK event: pushed to
    the in-memory activity queue for the live /stream/activity SSE panel,
    and persisted to the `agent_activity` ClickHouse table so activity
    survives a server restart and can be queried/audited later.

    ADK's event schema for function/tool calls has shifted across versions
    (see the note in agent.py), so this deliberately uses getattr() with
    fallbacks and is wrapped in try/except at the call site — a failure
    here must never break the chat response, it just means one activity
    entry doesn't show up in the panel or the table.

    The ClickHouse insert runs via asyncio.to_thread — clickhouse-connect's
    client is synchronous, so calling it directly inside an async function
    would block the event loop (and therefore every other concurrent
    request: other chat sessions, the live chart's SSE poll, etc.) for the
    duration of the network round trip.
    """
    calls = getattr(event, "get_function_calls", None)
    if not callable(calls):
        return

    for call in event.get_function_calls() or []:
        tool_name = getattr(call, "name", "unknown_tool")
        arguments = str(getattr(call, "args", {}))[:300]
        timestamp = datetime_utcnow_iso()

        activity_queue.put_nowait({
            "session_id": session_id,
            "tool_name": tool_name,
            "arguments": arguments,
            "time": timestamp,
        })

        try:
            await asyncio.to_thread(
                get_client().insert,
                "agent_activity",
                [[session_id, tool_name, arguments]],
                column_names=["session_id", "tool_name", "arguments"],
            )
        except Exception as e:  # noqa: BLE001 — persistence is best-effort, never breaks chat
            print(f"[activity] failed to persist tool call to ClickHouse: {e}")


def datetime_utcnow_iso() -> str:
    from datetime import datetime
    return datetime.utcnow().isoformat()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _agent, _runner

    _agent = build_agent()
    _runner = Runner(agent=_agent, app_name=APP_NAME, session_service=session_service)

    tasks = [asyncio.create_task(run_alert_checker(alert_queue, _stop_event))]

    # DISABLE_SIMULATOR=true skips the continuous random-data background task
    # so a deterministic scenario (from test_seed.py or /replay-scenario) isn't
    # drowned out by simulator noise while doing controlled testing. Leave
    # this unset (or false) for normal/demo use — you want the live-ticking
    # data then.
    if os.environ.get("DISABLE_SIMULATOR", "false").lower() != "true":
        tasks.append(asyncio.create_task(run_simulator(_stop_event)))

    yield

    _stop_event.set()
    for t in tasks:
        t.cancel()


app = FastAPI(title="Marquee", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChatRequest(BaseModel):
    message: str
    session_id: str | None = None


class ChatResponse(BaseModel):
    reply: str
    session_id: str
    tool_call_count: int = 0  # how many tool calls backed this answer — surfaced in the UI as a grounding signal


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.post("/chat", response_model=ChatResponse, dependencies=[Depends(require_api_key), Depends(enforce_rate_limit)])
async def chat(req: ChatRequest):
    session_id = req.session_id or str(uuid.uuid4())
    user_id = "studio-exec"

    existing = await session_service.get_session(
        app_name=APP_NAME, user_id=user_id, session_id=session_id
    )
    if existing is None:
        await session_service.create_session(
            app_name=APP_NAME, user_id=user_id, session_id=session_id
        )

    content = genai_types.Content(role="user", parts=[genai_types.Part(text=req.message)])

    final_text = ""
    tool_call_count = 0
    try:
        async for event in _runner.run_async(
            user_id=user_id, session_id=session_id, new_message=content
        ):
            try:
                calls_fn = getattr(event, "get_function_calls", None)
                if callable(calls_fn):
                    n = len(calls_fn() or [])
                    tool_call_count += n
                await _log_tool_activity_best_effort(session_id, event)
            except Exception:  # noqa: BLE001 — observability must never break chat
                pass
            if event.is_final_response() and event.content and event.content.parts:
                final_text = "".join(p.text or "" for p in event.content.parts)
    except Exception as e:  # noqa: BLE001 — the agent call must never take the API down
        print(f"[chat] agent run failed for session {session_id}: {e}")
        return ChatResponse(
            reply="I hit an error reaching the agent just now — please try again in a moment.",
            session_id=session_id,
        )

    return ChatResponse(
        reply=final_text or "I couldn't generate a response — try rephrasing.",
        session_id=session_id,
        tool_call_count=tool_call_count,
    )


@app.post("/replay-scenario", dependencies=[Depends(require_api_key), Depends(enforce_rate_limit)])
async def replay_scenario():
    """Instantly seeds the deterministic 'West region dip' scenario and runs
    one alert-check pass immediately, so a demo doesn't have to wait real
    minutes for the background simulator's scripted dip tick to come
    around. Pushes any resulting alerts onto the same queue the frontend's
    SSE stream is already listening on.
    """
    client = get_client()
    await asyncio.to_thread(seed_dip_scenario, client)
    reset_dedupe()

    fired = []
    for check in (_check_sales_drop, _check_sentiment_shift, _check_streaming_health):
        before = alert_queue.qsize()
        await check(client, alert_queue)
        if alert_queue.qsize() > before:
            fired.append(check.__name__)

    return {"status": "seeded", "alerts_fired": fired}


@app.get("/stream/alerts")
async def stream_alerts():
    async def event_generator():
        while True:
            alert = await alert_queue.get()
            yield {"event": "alert", "data": json.dumps(alert)}

    return EventSourceResponse(event_generator())


@app.get("/region/{region}/theaters")
async def region_theaters(region: str):
    """Per-theater breakdown for a region, for the frontend's click-to-drill-down
    interaction on the region list."""
    client = get_client()
    result = await asyncio.to_thread(
        client.query,
        """
        SELECT theater_id, any(city) AS city, any(screen_type) AS screen_type,
               sum(tickets_sold) AS tickets, sum(seats_total) AS seats, sum(revenue) AS revenue
        FROM ticket_sales
        WHERE region = {region:String} AND event_time > now() - INTERVAL 15 MINUTE
        GROUP BY theater_id
        ORDER BY tickets ASC
        LIMIT 20
        """,
        parameters={"region": region},
    )
    theaters = [
        {
            "theater_id": row[0],
            "city": row[1],
            "screen_type": row[2],
            "tickets": row[3],
            "seats": row[4],
            "occupancy_pct": round(row[3] / row[4] * 100, 1) if row[4] else 0,
            "revenue": round(row[5], 2),
        }
        for row in result.result_rows
    ]
    return {"region": region, "theaters": theaters}


@app.get("/stream/activity")
async def stream_activity():
    """SSE stream of agent tool-call activity, powering the frontend's
    'Agent activity' observability panel — lets a user watch which tools
    the agent actually invoked to answer a question, rather than trusting
    an opaque final answer."""

    async def event_generator():
        while True:
            entry = await activity_queue.get()
            yield {"event": "activity", "data": json.dumps(entry)}

    return EventSourceResponse(event_generator())


@app.get("/stream/sales")
async def stream_sales():
    """Polls ClickHouse for rolling sales totals every few seconds and pushes
    them to the frontend chart as an SSE stream."""

    async def event_generator():
        client = get_client()
        while True:
            result = await asyncio.to_thread(
                client.query,
                """
                SELECT region, sum(tickets_sold) AS tickets, sum(revenue) AS revenue
                FROM ticket_sales
                WHERE event_time > now() - INTERVAL 5 MINUTE
                GROUP BY region
                ORDER BY region
                """
            )
            payload = [
                {"region": row[0], "tickets": row[1], "revenue": round(row[2], 2)}
                for row in result.result_rows
            ]
            yield {"event": "sales", "data": json.dumps(payload)}
            await asyncio.sleep(4)

    return EventSourceResponse(event_generator())


# Serve the frontend as static files (useful for a single-service Cloud Run deploy)
if os.path.isdir("frontend"):
    app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
