"""
Simulate traffic by sending events to the ingestion API.

Usage:
    python -m examples.simulate_traffic --url http://localhost:8000 --events 100 --rate 10
"""

import argparse
import asyncio
import json
import random
import time

import httpx

EVENT_TYPES = ["transaction", "click", "pageview", "login", "logout", "purchase", "refund"]
SOURCES = ["web-app", "mobile-app", "api-gateway", "admin-panel", "external-service"]


def generate_event():
    event_type = random.choice(EVENT_TYPES)
    source = random.choice(SOURCES)

    data = {}
    if event_type in ("transaction", "purchase", "refund"):
        data["amount"] = round(random.uniform(1.0, 50000.0), 2)
        data["currency"] = random.choice(["USD", "EUR", "GBP"])
    elif event_type == "click":
        data["element"] = random.choice(["button", "link", "image", "form"])
        data["page"] = random.choice(["/home", "/pricing", "/login", "/dashboard"])
    elif event_type == "pageview":
        data["url"] = random.choice(["/home", "/about", "/products", "/contact"])
        data["referrer"] = random.choice(["google.com", "direct", "twitter.com", "linkedin.com"])
    elif event_type == "login":
        data["method"] = random.choice(["password", "oauth", "sso"])
        data["success"] = random.choice([True, False])

    return {
        "event_type": event_type,
        "source": source,
        "data": data,
    }


async def send_event(client: httpx.AsyncClient, url: str, event: dict) -> bool:
    try:
        response = await client.post(f"{url}/events", json=event, timeout=5.0)
        if response.status_code == 202:
            return True
        else:
            print(f"  Rejected ({response.status_code}): {response.text[:100]}")
            return False
    except Exception as e:
        print(f"  Error: {e}")
        return False


async def main():
    parser = argparse.ArgumentParser(description="Simulate traffic to the ingestion API")
    parser.add_argument("--url", default="http://localhost:8000", help="Ingestion API URL")
    parser.add_argument("--events", type=int, default=100, help="Number of events to send")
    parser.add_argument("--rate", type=int, default=10, help="Events per second")
    args = parser.parse_args()

    async with httpx.AsyncClient() as client:
        accepted = 0
        rejected = 0
        start = time.time()

        for i in range(args.events):
            event = generate_event()
            success = await send_event(client, args.url, event)
            if success:
                accepted += 1
            else:
                rejected += 1

            if i > 0 and i % args.rate == 0:
                elapsed = time.time() - start
                print(f"Progress: {i}/{args.events} ({accepted} accepted, {rejected} rejected) in {elapsed:.1f}s")

            await asyncio.sleep(1.0 / args.rate)

        elapsed = time.time() - start
        print(f"\nDone: {accepted} accepted, {rejected} rejected in {elapsed:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())
