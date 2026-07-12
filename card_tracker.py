"""Card profit-potential tracker (trial) — schema + DB plumbing + ingest.

Owned by ava_dashboard (see data-system.md): tracked_cards, price_snapshots,
card_scores. Tables are created idempotently at app startup and by the ingest
script — matching ava_bot's CREATE TABLE IF NOT EXISTS convention, since
neither repo has a migration system.
"""

import logging

import httpx

import justtcg
from watchlist import WATCHLIST

logger = logging.getLogger("dashboard.card_tracker")

# Abort an ingest run after this many consecutive API failures — protects the
# free-tier call budget from burning on an outage or a wrong endpoint shape.
MAX_CONSECUTIVE_FAILURES = 8

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


async def run_ingest(pool) -> dict:
    """Fetch current JustTCG pricing for every tracked card and insert one
    price_snapshots row each. Resolves missing justtcg_card_id / release_date
    on the way. Per-card failures are logged and skipped, never fatal.

    Returns a summary dict: {"cards", "snapshots", "resolved", "failed": [...]}.
    """
    summary = {"cards": 0, "snapshots": 0, "resolved": 0, "failed": []}
    consecutive_failures = 0

    async with pool.acquire() as conn:
        cards = await conn.fetch(
            "SELECT id, name, game, set_name, card_number, release_date, justtcg_card_id "
            "FROM tracked_cards ORDER BY id"
        )
    summary["cards"] = len(cards)
    if not cards:
        logger.warning("Ingest: no tracked cards — run sync_watchlist first")
        return summary

    async with httpx.AsyncClient(timeout=20) as client:
        # ── Pass 1: resolve missing JustTCG ids (one search call per card) ──
        for c in cards:
            if c["justtcg_card_id"]:
                continue
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                summary["failed"].append("aborted: too many consecutive API failures")
                logger.error("Ingest aborted during id resolution — %d consecutive failures",
                             consecutive_failures)
                return summary
            try:
                match = await justtcg.search_card(
                    client, c["name"], c["game"], c["set_name"], c["card_number"])
                if match is None:
                    consecutive_failures += 1
                    summary["failed"].append(f"{c['name']}: no JustTCG match")
                    logger.warning("Ingest: no JustTCG match for %r (%s / %s)",
                                   c["name"], c["game"], c["set_name"])
                    continue
                consecutive_failures = 0
                jid = justtcg.card_identity(match)
                release = c["release_date"] or justtcg.extract_release_date(match)
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE tracked_cards SET justtcg_card_id = $1, "
                        "release_date = COALESCE(release_date, $2) WHERE id = $3",
                        jid or None, release, c["id"])
                summary["resolved"] += 1
                logger.info("Ingest: matched %r -> JustTCG id %s (%s)",
                            c["name"], jid or "?", (match.get("name") or "?"))
            except justtcg.JustTCGError:
                raise  # bad/missing key — pointless to continue
            except Exception as e:
                consecutive_failures += 1
                summary["failed"].append(f"{c['name']}: {e}")
                logger.exception("Ingest: id resolution failed for %r", c["name"])

        # ── Pass 2: fill missing Pokémon release dates (free API) ──
        for c in cards:
            if c["release_date"] or c["game"] != "pokemon":
                continue
            try:
                release = await justtcg.fetch_pokemon_release_date(client, c["name"], c["set_name"])
                if release:
                    async with pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE tracked_cards SET release_date = COALESCE(release_date, $1) "
                            "WHERE id = $2", release, c["id"])
            except Exception:
                logger.exception("Ingest: release-date lookup failed for %r", c["name"])

        # ── Pass 3: batch price fetch by resolved id ──
        async with pool.acquire() as conn:
            cards = await conn.fetch(
                "SELECT id, name, justtcg_card_id FROM tracked_cards "
                "WHERE justtcg_card_id IS NOT NULL ORDER BY id")
        by_jid = {c["justtcg_card_id"]: c for c in cards}
        try:
            fetched = await justtcg.fetch_cards_by_ids(client, list(by_jid.keys()))
        except justtcg.JustTCGError:
            raise
        except Exception as e:
            summary["failed"].append(f"price fetch: {e}")
            logger.exception("Ingest: price fetch failed")
            return summary

        async with pool.acquire() as conn:
            for jid, card_row in by_jid.items():
                obj = fetched.get(jid)
                if obj is None:
                    summary["failed"].append(f"{card_row['name']}: no price data returned")
                    continue
                low, mid, high = justtcg.extract_prices(obj)
                if low is None and mid is None and high is None:
                    summary["failed"].append(f"{card_row['name']}: no numeric prices in response")
                    logger.warning("Ingest: no prices found for %r — raw keys: %s",
                                   card_row["name"], sorted(obj.keys())[:15])
                    continue
                await conn.execute(
                    "INSERT INTO price_snapshots (card_id, price_low, price_mid, price_high, source) "
                    "VALUES ($1, $2, $3, $4, 'justtcg')",
                    card_row["id"], low, mid, high)
                summary["snapshots"] += 1

    logger.info("Ingest done: %d snapshot(s) across %d card(s), %d newly resolved, %d failure(s)",
                summary["snapshots"], summary["cards"], summary["resolved"], len(summary["failed"]))
    return summary
