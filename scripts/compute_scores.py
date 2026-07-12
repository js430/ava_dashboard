"""Manual score computation for the card tracker trial.

Usage:  python -m scripts.compute_scores

Recomputes potential scores for every tracked card from stored snapshots and
inserts a new card_scores row each. Run after (or any time between) ingests.
Weights/knobs live in card_scoring.py.
"""

import asyncio
import logging
import os
import sys

import asyncpg
from dotenv import load_dotenv

from card_tracker import ensure_card_tracker_schema, run_scoring

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("scripts.compute_scores")


async def main() -> int:
    load_dotenv()
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        logger.error("DATABASE_URL is not set")
        return 1
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
    try:
        await ensure_card_tracker_schema(pool)
        summary = await run_scoring(pool)
    finally:
        await pool.close()
    print(f"\nScored: {summary['scored']}  ·  Skipped (no snapshots): {summary['skipped']}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
