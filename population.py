"""GemRate grading-population cache, keyed by TCGplayer id.

Owned by ava_dashboard: `population_cache`. Created idempotently at app
startup, matching the `catalog_cards` / `card_tracker` convention, since
neither repo has a migration system.

WHY A CACHE AT ALL: PokemonPriceTracker's /population endpoint costs 2
credits per card and is Business-plan only — the two most expensive/scarce
things this app spends. A card looked up repeatedly in the Grading
Calculator (by the same person re-checking, or different people looking at
the same popular card) would otherwise pay full price every time, even
though population counts move slowly (PSA doesn't publish new numbers every
day). Caching for a week turns "every lookup" into "at most once a week per
card".

Nothing here fetches. `main.py` owns the network call
(price_sources.fetch_ppt_population); this module owns the schema, the
cache read, and the cache write.

The response body has open-ended per-grader keys — BGS carries
g9_5/pristine/perfect that PSA and CGC don't, and PPT could add a new grader
or grade bucket at any time — so it's stored as JSONB rather than forced
into fixed columns, and served back exactly as PPT returned it.
"""

import json
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger("dashboard.population")

# "More than a week old" per the caching rule — after this, a lookup pays
# for a fresh call instead of trusting the stored numbers.
CACHE_MAX_AGE = timedelta(days=7)

POPULATION_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS population_cache (
        id           SERIAL PRIMARY KEY,
        tcgplayer_id TEXT NOT NULL,
        language     TEXT NOT NULL DEFAULT 'english',
        data         JSONB NOT NULL,
        fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    # One row per printing — a card looked up in English and Japanese is a
    # different product with different population, so language is part of
    # the identity, same reasoning as catalog_cards.
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_population_cache_identity
        ON population_cache (tcgplayer_id, language)
    """,
]


async def ensure_population_schema(pool) -> None:
    """Create the population cache table if absent (idempotent)."""
    async with pool.acquire() as conn:
        for ddl in POPULATION_SCHEMA:
            await conn.execute(ddl)


async def get_cached(pool, tcgplayer_id: str, language: str = "english"):
    """(data, is_fresh) for a cached lookup, or (None, False) if nothing's
    stored yet. `is_fresh` is False once the row is older than
    CACHE_MAX_AGE — the caller decides what to do with a stale-but-present
    row (this module never deletes one, so a vendor outage still leaves the
    last known numbers on hand rather than nothing)."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT data, fetched_at FROM population_cache "
            "WHERE tcgplayer_id = $1 AND language = $2",
            tcgplayer_id, language)
    if not row:
        return None, False
    data = row["data"]
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except (TypeError, ValueError):
            return None, False
    fresh = (datetime.now(timezone.utc) - row["fetched_at"]) < CACHE_MAX_AGE
    return data, fresh


async def upsert(pool, tcgplayer_id: str, language: str, data: dict) -> None:
    """Store (or refresh) one card's population data."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO population_cache (tcgplayer_id, language, data, fetched_at)
            VALUES ($1, $2, $3::jsonb, NOW())
            ON CONFLICT (tcgplayer_id, language) DO UPDATE
            SET data = EXCLUDED.data, fetched_at = NOW()
            """,
            tcgplayer_id, language, json.dumps(data))
