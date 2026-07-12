"""Card profit-potential tracker (trial) — schema + DB plumbing.

Owned by ava_dashboard (see data-system.md): tracked_cards, price_snapshots,
card_scores. Tables are created idempotently at app startup and by the ingest
script — matching ava_bot's CREATE TABLE IF NOT EXISTS convention, since
neither repo has a migration system.
"""

import logging

from watchlist import WATCHLIST

logger = logging.getLogger("dashboard.card_tracker")

VALID_GAMES = ("pokemon", "one_piece")

# set_name / card_number are NOT NULL DEFAULT '' (not nullable) so the UNIQUE
# constraint dedupes watchlist syncs — Postgres treats NULLs as distinct in
# unique constraints, which would allow duplicate rows.
CARD_TRACKER_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS tracked_cards (
        id              SERIAL PRIMARY KEY,
        name            TEXT NOT NULL,
        game            TEXT NOT NULL CHECK (game IN ('pokemon','one_piece')),
        set_name        TEXT NOT NULL DEFAULT '',
        card_number     TEXT NOT NULL DEFAULT '',
        variant         TEXT,
        release_date    DATE,
        justtcg_card_id TEXT,
        added_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (game, name, set_name, card_number)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS price_snapshots (
        id          SERIAL PRIMARY KEY,
        card_id     INTEGER NOT NULL REFERENCES tracked_cards(id) ON DELETE CASCADE,
        captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        price_low   NUMERIC,
        price_mid   NUMERIC,
        price_high  NUMERIC,
        source      TEXT NOT NULL DEFAULT 'justtcg'
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_price_snapshots_card_time ON price_snapshots (card_id, captured_at)",
    """
    CREATE TABLE IF NOT EXISTS card_scores (
        id              SERIAL PRIMARY KEY,
        card_id         INTEGER NOT NULL REFERENCES tracked_cards(id) ON DELETE CASCADE,
        computed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        momentum_7d     NUMERIC,
        momentum_30d    NUMERIC,
        liquidity_score NUMERIC,
        age_days        INTEGER,
        potential_score NUMERIC
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_card_scores_card_time ON card_scores (card_id, computed_at)",
]


async def ensure_card_tracker_schema(pool) -> None:
    """Create the tracker tables if they don't exist (idempotent)."""
    async with pool.acquire() as conn:
        for ddl in CARD_TRACKER_SCHEMA:
            await conn.execute(ddl)
    logger.info("Card tracker schema ensured")


async def sync_watchlist(pool) -> int:
    """Upsert watchlist.py entries into tracked_cards. Returns rows added.

    Never deletes — cards removed from the watchlist keep their history and
    must be removed from the DB manually if that's ever wanted.
    """
    added = 0
    async with pool.acquire() as conn:
        for entry in WATCHLIST:
            game = entry.get("game")
            name = (entry.get("name") or "").strip()
            if game not in VALID_GAMES or not name:
                logger.warning("Skipping invalid watchlist entry: %r", entry)
                continue
            result = await conn.execute(
                """
                INSERT INTO tracked_cards (name, game, set_name, card_number, variant)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (game, name, set_name, card_number) DO NOTHING
                """,
                name,
                game,
                (entry.get("set_name") or "").strip(),
                (entry.get("card_number") or "").strip(),
                (entry.get("variant") or None),
            )
            if result.endswith("1"):
                added += 1
    logger.info("Watchlist sync: %d new card(s), %d total in config", added, len(WATCHLIST))
    return added
