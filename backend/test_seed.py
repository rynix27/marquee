"""Smoke test for Marquee.

Run this before wiring up the full simulator to confirm each layer actually
works end to end:

  1. ClickHouse connectivity (can we connect + write + read?)
  2. A deterministic "West region dip" scenario seeded directly (so you get
     a predictable answer to sanity-check the agent against, instead of
     waiting on random simulator data)
  3. (optional) the live /chat endpoint, asking it a few questions and
     printing the answers so you can eyeball whether they're grounded in
     the real numbers you just inserted

Usage:
    python -m backend.test_seed                    # seed + ClickHouse checks only
    python -m backend.test_seed --with-api          # also hit the running API
    python -m backend.test_seed --with-api --api-url http://localhost:8080
"""
import argparse
import sys

import requests

from backend.clickhouse_client import get_client
from backend.simulator import seed_dip_scenario


def check_connection(client):
    print("→ Checking ClickHouse connection...")
    result = client.query("SELECT 1")
    assert result.result_rows[0][0] == 1
    print("  ✓ connected\n")


def seed_deterministic_scenario(client):
    print("→ Seeding deterministic test scenario (West region dip)...")
    seed_dip_scenario(client)
    print("  ✓ seeded 5 sales rows, 7 sentiment rows, 4 streaming rows\n")

    print("  ✓ seeded 5 sales rows, 7 sentiment rows, 4 streaming rows\n")


def verify_scenario(client):
    print("→ Verifying seeded data is queryable...")
    result = client.query("""
        SELECT region, sum(tickets_sold) AS tickets
        FROM ticket_sales
        WHERE event_time > now() - INTERVAL 2 MINUTE
        GROUP BY region ORDER BY tickets ASC
    """)
    rows = result.result_rows
    assert rows, "No rows found — check your CLICKHOUSE_* env vars"
    lowest_region, lowest_tickets = rows[0]
    print(f"  ✓ lowest-selling region right now: {lowest_region} ({lowest_tickets} tickets)")
    assert lowest_region == "West", "Expected West to be the underperforming region — check simulator/seed logic"
    print("  ✓ matches expected scenario (West underperforming)\n")


def test_chat_api(api_url):
    print(f"→ Testing live /chat endpoint at {api_url} ...")
    questions = [
        "Which region has the lowest ticket sales right now?",
        "Why might the West region be underperforming?",
        "What's the sentiment on the third act?",
    ]
    session_id = None
    for q in questions:
        print(f"\n  Q: {q}")
        try:
            resp = requests.post(
                f"{api_url}/chat",
                json={"message": q, "session_id": session_id},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            session_id = data["session_id"]
            print(f"  A: {data['reply']}")
        except Exception as e:
            print(f"  ✗ request failed: {e}")
            print("    (is `uvicorn backend.main:app --port 8080` running?)")
            return False
    print("\n  ✓ chat endpoint responded to all questions\n")
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--with-api", action="store_true", help="also test the live /chat endpoint")
    parser.add_argument("--api-url", default="http://localhost:8080")
    args = parser.parse_args()

    client = get_client()
    check_connection(client)
    seed_deterministic_scenario(client)
    verify_scenario(client)

    if args.with_api:
        ok = test_chat_api(args.api_url)
        if not ok:
            sys.exit(1)

    print("All checks passed. ✅")
    if not args.with_api:
        print("Run again with --with-api once `uvicorn backend.main:app --port 8080` is up,")
        print("to confirm the agent's answers are actually grounded in this data.")


if __name__ == "__main__":
    main()
