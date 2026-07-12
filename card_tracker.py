"""Card profit-potential tracker (trial) — schema + DB plumbing + ingest.

Owned by ava_dashboard (see data-system.md): tracked_cards, price_snapshots,
card_scores. Tables are created idempotently at app startup and by the ingest
script — matching ava_bot's CREATE TABLE IF NOT EXISTS convention, since
neither repo has a migration system.
"""

import asyncio
import logging

import httpx

import justtcg
from watchlist import WATCHLIST

logger = logging.getLogger("dashboard.card_tracker")

# Abort an ingest run after this many consecutive API failures — protects the
# free-tier call budget from burning on an outage or a wrong endpoint shape.
MAX_CONSECUTIVE_FAILURES = 8

# Per-run JustTCG call caps, keyed by the ACTIVE key's detected plan and
# sized against that plan's daily/monthly limits (free: 100/day · 1K/month;
# starter: 1K/day but 10K/month ≈ 333/day sustainable). Resolution covers
# searches (incl. name-variant fallbacks); pricing covers batch fetches.
# A big fresh import resolves incrementally across runs instead of torching
# the budget at once.
PLAN_RUN_CAPS = {
    "free":       {"resolution": 55,  "pricing": 25},
    "starter":    {"resolution": 240, "pricing": 60},
    "pro":        {"resolution": 240, "pricing": 60},
    "enterprise": {"resolution": 500, "pricing": 100},
}

# Global ceiling on tracked cards. Guards the JustTCG free tier (1,000
# calls/month): if the batch endpoint works, 400 cards ≈ 4 pricing calls/day
# plus one-time id resolution; if batch falls back to per-card, 400/day would
# blow the budget in 2.5 days — the ingest's consecutive-failure abort plus
# this cap bound the damage. Raise deliberately, not casually.
MAX_TRACKED_CARDS = 400

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
    # What JustTCG actually matched (visibility for wrong-match debugging).
    "ALTER TABLE tracked_cards ADD COLUMN IF NOT EXISTS justtcg_name TEXT",
    "ALTER TABLE tracked_cards ADD COLUMN IF NOT EXISTS justtcg_set TEXT",
    "ALTER TABLE tracked_cards ADD COLUMN IF NOT EXISTS justtcg_number TEXT",
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
    summary = {"cards": 0, "snapshots": 0, "resolved": 0, "backfilled": 0, "failed": []}
    consecutive_failures = 0

    async with pool.acquire() as conn:
        cards = await conn.fetch(
            "SELECT id, name, game, set_name, card_number, variant, release_date, "
            "justtcg_card_id FROM tracked_cards ORDER BY id"
        )
    summary["cards"] = len(cards)
    if not cards:
        logger.warning("Ingest: no tracked cards — run sync_watchlist first")
        return summary

    async with httpx.AsyncClient(timeout=20) as client:
        # ── Pass 0: resolve JustTCG's real game slugs (1 call, cached
        #    process-wide). The response also tells us the active key's plan,
        #    so the run caps below match the plan we're actually on. ──
        await justtcg.resolve_game_slugs(client)
        caps = PLAN_RUN_CAPS.get(justtcg.current_plan(), PLAN_RUN_CAPS["free"])
        resolution_budget = justtcg.CallBudget(caps["resolution"])
        pricing_budget = justtcg.CallBudget(caps["pricing"])

        # ── Pass 1: resolve missing JustTCG ids (one+ search calls per card,
        #    paced for the 10/min limit, capped for the 100/day limit) ──
        searched = False
        for c in cards:
            if c["justtcg_card_id"]:
                continue
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                summary["failed"].append("aborted: too many consecutive API failures")
                logger.error("Ingest aborted during id resolution — %d consecutive failures",
                             consecutive_failures)
                return summary
            if searched:
                await asyncio.sleep(justtcg.current_interval())
            searched = True
            try:
                match = await justtcg.search_card(
                    client, c["name"], c["game"], c["set_name"], c["card_number"],
                    budget=resolution_budget, variant=c["variant"] or "")
                if match is None:
                    consecutive_failures += 1
                    summary["failed"].append(f"{c['name']}: no JustTCG match")
                    logger.warning("Ingest: no JustTCG match for %r (%s / %s) — set may be "
                                   "too new for their catalog or named differently; "
                                   "will retry next ingest",
                                   c["name"], c["game"], c["set_name"])
                    continue
                consecutive_failures = 0
                jid = justtcg.card_identity(match)
                mname, mset, mnum = justtcg.match_display(match)
                release = c["release_date"] or justtcg.extract_release_date(match)
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE tracked_cards SET justtcg_card_id = $1, "
                        "justtcg_name = $2, justtcg_set = $3, justtcg_number = $4, "
                        "release_date = COALESCE(release_date, $5) WHERE id = $6",
                        jid or None, mname or None, mset or None, mnum or None,
                        release, c["id"])
                summary["resolved"] += 1
                logger.info("Ingest: matched %r -> JustTCG id %s (%r, %s #%s)",
                            c["name"], jid or "?", mname or "?", mset or "?", mnum or "?")
            except justtcg.JustTCGError:
                raise  # bad/missing key — pointless to continue
            except justtcg.BudgetExhausted:
                summary["failed"].append(
                    "resolution paused: daily-safe JustTCG call budget reached — "
                    "remaining cards resolve on the next run")
                logger.warning("Ingest: resolution call budget (%d) reached — deferring "
                               "remaining unresolved cards to the next run",
                               resolution_budget.limit)
                break
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
                "SELECT id, name, variant, justtcg_card_id FROM tracked_cards "
                "WHERE justtcg_card_id IS NOT NULL ORDER BY id")
            have_rows = await conn.fetch(
                "SELECT DISTINCT card_id FROM price_snapshots WHERE card_id = ANY($1::int[])",
                [c["id"] for c in cards])
        have_snapshots = {r["card_id"] for r in have_rows}
        by_jid = {c["justtcg_card_id"]: c for c in cards}
        # Ask for 30d of priceHistory when any card is on its first fetch —
        # same number of API calls, just a bigger payload.
        need_history = any(c["id"] not in have_snapshots for c in cards)
        try:
            fetched = await justtcg.fetch_cards_by_ids(client, list(by_jid.keys()),
                                                       budget=pricing_budget,
                                                       include_history=need_history)
        except justtcg.JustTCGError:
            raise
        except Exception as e:
            summary["failed"].append(f"price fetch: {e}")
            logger.exception("Ingest: price fetch failed")
            return summary

        from datetime import datetime as _dt, timezone as _tz
        today_utc = _dt.now(_tz.utc).date()
        async with pool.acquire() as conn:
            for jid, card_row in by_jid.items():
                obj = fetched.get(jid)
                if obj is None:
                    summary["failed"].append(f"{card_row['name']}: no price data returned")
                    continue
                low, mid, high = justtcg.extract_prices(obj, card_row["variant"] or "")
                if low is None and mid is None and high is None:
                    summary["failed"].append(f"{card_row['name']}: no numeric prices in response")
                    logger.warning("Ingest: no prices found for %r — raw keys: %s",
                                   card_row["name"], sorted(obj.keys())[:15])
                    continue
                # First fetch for this card: backfill its 30d history so
                # momentum/liquidity are meaningful immediately. Only days
                # BEFORE today — today's live snapshot is inserted below.
                if card_row["id"] not in have_snapshots:
                    history = justtcg.extract_price_history(obj, card_row["variant"] or "")
                    for h in history:
                        if h["captured_at"].date() >= today_utc:
                            continue
                        await conn.execute(
                            "INSERT INTO price_snapshots (card_id, captured_at, price_low, "
                            "price_mid, price_high, source) VALUES ($1, $2, $3, $4, $5, 'justtcg-history')",
                            card_row["id"], h["captured_at"], h["price_low"],
                            h["price_mid"], h["price_high"])
                        summary["backfilled"] += 1
                await conn.execute(
                    "INSERT INTO price_snapshots (card_id, price_low, price_mid, price_high, source) "
                    "VALUES ($1, $2, $3, $4, 'justtcg')",
                    card_row["id"], low, mid, high)
                summary["snapshots"] += 1

    summary["justtcg_calls"] = resolution_budget.used + pricing_budget.used
    logger.info("Ingest done: %d snapshot(s) across %d card(s), %d backfilled history "
                "row(s), %d newly resolved, %d failure(s), %d JustTCG call(s) used",
                summary["snapshots"], summary["cards"], summary["backfilled"],
                summary["resolved"], len(summary["failed"]), summary["justtcg_calls"])
    return summary


async def run_scoring(pool) -> dict:
    """Compute and store a card_scores row for every tracked card, from the
    last 60 days of snapshots (enough history for a true 30d baseline).
    Pure math lives in card_scoring.py; this is just the DB glue."""
    import card_scoring

    summary = {"scored": 0, "skipped": 0}
    async with pool.acquire() as conn:
        cards = await conn.fetch("SELECT id, name, release_date FROM tracked_cards ORDER BY id")
        for c in cards:
            snaps = await conn.fetch(
                "SELECT captured_at, price_low, price_mid, price_high FROM price_snapshots "
                "WHERE card_id = $1 AND captured_at >= NOW() - INTERVAL '60 days' "
                "ORDER BY captured_at",
                c["id"])
            if not snaps:
                summary["skipped"] += 1
                continue
            s = card_scoring.score_card([dict(r) for r in snaps], c["release_date"])
            await conn.execute(
                "INSERT INTO card_scores (card_id, momentum_7d, momentum_30d, "
                "liquidity_score, age_days, potential_score) VALUES ($1, $2, $3, $4, $5, $6)",
                c["id"], s["momentum_7d_pct"], s["momentum_30d_pct"],
                s["liquidity_score"], s["age_days"], s["potential_score"])
            summary["scored"] += 1
    logger.info("Scoring done: %d scored, %d skipped (no snapshots)",
                summary["scored"], summary["skipped"])
    return summary
