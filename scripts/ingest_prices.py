"""Manual price ingest for the card tracker trial.

Usage:  python -m scripts.ingest_prices

Ensures the schema, syncs watchlist.py into tracked_cards, then pulls current
JustTCG pricing into price_snapshots. Safe to re-run; per-card failures are
logged and skipped. Needs DATABASE_URL and JUSTTCG_API_KEY in the environment
(reads .env via python-dotenv, same as the app).
"""

import asyncio
import logging
import os
import sys

import asyncpg
from dotenv import load_dotenv

from card_tracker import ensure_card_tracker_schema, sync_watchlist, run_ingest

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("scripts.ingest_prices")


async def main() -> int:
    load_dotenv()
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        logger.error("DATABASE_URL is not set")
        return 1
    if not os.getenv("JUSTTCG_API_KEY"):
        logger.error("JUSTTCG_API_KEY is not set")
        return 1

    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
    try:
        await ensure_card_tracker_schema(pool)
        added = await sync_watchlist(pool)
        summary = await run_ingest(pool)
    finally:
        await pool.close()

    print(f"\nWatchlist: +{added} new card(s)")
    print(f"Snapshots inserted: {summary['snapshots']} / {summary['cards']} tracked")
    print(f"History rows backfilled (first-fetch cards): {summary.get('backfilled', 0)}")
    print(f"Newly resolved JustTCG ids: {summary['resolved']}")
    if summary["failed"]:
        print(f"Failures ({len(summary['failed'])}):")
        for f in summary["failed"]:
            print(f"  - {f}")
    return 0 if summary["snapshots"] > 0 or summary["cards"] == 0 else 2


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
