"""Card profit-potential tracker (trial) — schema + DB plumbing + ingest.

Owned by ava_dashboard (see data-system.md): tracked_cards, price_snapshots,
card_scores, user_tracked_cards. Tables are created idempotently at app
startup and by the ingest script — matching ava_bot's CREATE TABLE IF NOT
EXISTS convention, since neither repo has a migration system.

tracked_cards/price_snapshots/card_scores are a SHARED catalog: a card and
its price history exist once regardless of how many members track it.
user_tracked_cards is the per-member portfolio join table (main.py's
/api/card-tracker/portfolio routes) — it's the only per-user data here. The
ingest/scoring functions below (run_ppt_ingest, run_ingest, run_scoring,
resolve_tcgplayer_ids) all operate on the shared catalog and don't need to
know who owns what; only the "what needs a price today" queries
(_MISSING_TODAY_SQL, _COVERAGE_SQL) filter to cards at least one member
actually tracks, so an orphaned catalog row stops costing credits.
"""

import os
import re
import asyncio
import logging
from datetime import datetime, timezone

import httpx

import justtcg
import price_sources
from watchlist import WATCHLIST

logger = logging.getLogger("dashboard.card_tracker")

# Per-run cap on PokemonPriceTracker calls (1 credit per card). PPT's budget is
# shared with the catalog backfill and the grading calculator, and has already
# been seen returning 429s, so the tracker takes a bounded slice rather than
# assuming the whole thing is free. Cards beyond the cap are picked up on the
# next run.
PPT_RUN_CAP = int(os.getenv("TRACKER_PPT_RUN_CAP", "150"))
# Cap on one-time resolution searches per run (PPT_SEARCH_LIMIT credits each).
# Bounds the cost of a large unresolved backlog — the rest resolve on the next
# sweep, and once resolved a card never needs searching again.
PPT_RESOLVE_CAP = int(os.getenv("TRACKER_PPT_RESOLVE_CAP", "20"))
# A 429 means "later", not "never" (same rule as the catalog backfill): back
# off, then stop the run cleanly so the next one resumes.
PPT_RATE_WAIT_S = float(os.getenv("TRACKER_PPT_RATE_WAIT_S", "30"))
PPT_RATE_RETRIES = int(os.getenv("TRACKER_PPT_RATE_RETRIES", "2"))
# Source label written to price_snapshots.source. Scoring groups by this, so
# it must stay distinct from the JustTCG labels.
PPT_SOURCE = "pokemonpricetracker"
# Manual backdate rows (run_ppt_backdate) — same estimator as PPT_SOURCE so it
# shares a scoring family (see card_scoring.SOURCE_FAMILIES), tagged separately
# so a backdated row is distinguishable from a live nightly one in the DB.
PPT_HISTORY_SOURCE = "pokemonpricetracker-history"
# Days a single backdate request may ask PPT for — matches the buttons on the
# card-tracker page. An arbitrary value is not accepted from the client.
BACKDATE_DAY_CHOICES = (30, 60, 180)
# Admin selects cards by hand for this action; capped so one click can't burn
# through the shared PPT daily credit budget (2 credits/card at includeHistory).
BACKDATE_MAX_CARDS = 10
# A brand-new card gets backdated this many days automatically the moment it
# resolves a tcgplayer_id (see resolve_tcgplayer_ids / run_ppt_ingest), so its
# graph isn't a single flat point until someone remembers to backdate it by
# hand. No admin confirmation gates this — unlike the manual action, it's an
# unconditional +2 PPT credits per newly-resolved card.
AUTO_BACKDATE_DAYS = 180
# Max auto-backdates per run_ppt_ingest call (shared across the free-catalog
# resolves in resolve_tcgplayer_ids and the paid-search resolves below it) —
# without this, importing a whole set (50+ cards) would auto-spend the day's
# PPT credit budget in one sweep. Cards beyond the cap still resolve and get
# today's live price normally; they just don't get history until the next
# sweep's cap resets, or a manual backdate.
AUTO_BACKDATE_RUN_CAP = 25

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

# Global ceiling on the SHARED catalog (distinct cards across every member's
# portfolio combined, not per-user). Sized against the PPT API plan's 20,000
# credits/day: steady-state cost is ~1 credit/card/day (the live-price sweep
# only re-prices what's missing today), so 3,000 cards ≈ 3,000 credits/day
# (~15% of budget), leaving room for the catalog backfill and grading
# calculator sharing the same PPT key, plus resolution/backdate spend. Raise
# deliberately, not casually — re-check against the actual PPT plan in use.
MAX_TRACKED_CARDS = 3000
# Per-member cap on portfolio size (see main.py's /api/card-tracker/portfolio
# routes). Independent of MAX_TRACKED_CARDS — that one bounds the shared
# catalog's total credit exposure, this one bounds how much of it any single
# member can claim.
MAX_USER_PORTFOLIO_CARDS = 100

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
    # Pokemon pricing moved to PokemonPriceTracker, pinned by TCGplayer id.
    # Resolved for free out of catalog_cards rather than by a paid search, so
    # a card only needs its set stocked once to be trackable forever.
    "ALTER TABLE tracked_cards ADD COLUMN IF NOT EXISTS tcgplayer_id TEXT",
    "ALTER TABLE tracked_cards ADD COLUMN IF NOT EXISTS catalog_matched_name TEXT",
    # Per-member portfolio membership. Deliberately just a join table — the
    # card itself and its price history are shared (see module docstring):
    # two members tracking the same card cost one PPT credit/day, not two,
    # and removing a card from one portfolio never touches the other's data.
    """
    CREATE TABLE IF NOT EXISTS user_tracked_cards (
        user_id    BIGINT NOT NULL,
        card_id    INTEGER NOT NULL REFERENCES tracked_cards(id) ON DELETE CASCADE,
        added_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (user_id, card_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_user_tracked_cards_user ON user_tracked_cards (user_id)",
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


# ── Pokemon pricing via PokemonPriceTracker ────────────────────────────────
# Replaces JustTCG for Pokemon entirely. Two things make it cheaper AND more
# reliable than what it replaces:
#   * Resolution is FREE. catalog_cards already holds tcgplayer_id for every
#     card in a stocked set, so matching happens against local rows instead of
#     one-or-more paid search calls per card — which was the ingest's most
#     expensive pass and its main source of wrong matches.
#   * Pricing is pinned by tcgPlayerId, so there is no name/set matching at
#     request time at all.
# JustTCG stays the source for One Piece, which PPT doesn't cover.


def _canon(value: str) -> str:
    """Lowercase, letters and digits only — for tolerant name comparison."""
    return re.sub(r"[^a-z0-9]", "", (value or "").lower())


def _number_key(value: str) -> str:
    """'215/203' and '015' both reduce to a comparable '215' / '15'."""
    head = str(value or "").split("/")[0].strip().lstrip("0").lower()
    return head


def match_catalog_row(card: dict, candidates: list):
    """Best catalog_cards row for a tracked card, or None if none is safe.

    Scored rather than first-match: a set can contain several printings of the
    same name, and picking the wrong one would silently track the wrong card's
    price for months. Requires a positive score, so an ambiguous match yields
    nothing and the card is reported as unresolved instead of guessed at.
    """
    want_name = _canon(card.get("name"))
    want_num = _number_key(card.get("card_number"))
    want_set = _canon(card.get("set_name"))
    want_variant = _canon(card.get("variant"))

    best, best_score = None, 0
    for row in candidates:
        score = 0
        row_name = _canon(row.get("card_name"))
        if not row_name or not want_name:
            continue
        if row_name == want_name:
            score += 4
        elif row_name.startswith(want_name) or want_name.startswith(row_name):
            score += 2
        else:
            continue                      # name must at least partially agree

        row_num = _number_key(row.get("card_number"))
        if want_num and row_num:
            # A number disagreement is disqualifying: same name, different
            # print is exactly the wrong-card case this guards against.
            if row_num != want_num:
                continue
            score += 4

        if want_set and _canon(row.get("set_name")):
            row_set = _canon(row.get("set_name"))
            if row_set == want_set or want_set in row_set or row_set in want_set:
                score += 2

        # Variant/rarity is a tiebreaker only — watchlist wording ("Alternate
        # Art Secret") rarely matches a catalog rarity string exactly.
        if want_variant and _canon(row.get("rarity")):
            row_rarity = _canon(row.get("rarity"))
            if want_variant in row_rarity or row_rarity in want_variant:
                score += 1

        if score > best_score:
            best, best_score = row, score

    return best


async def resolve_tcgplayer_ids(pool, client: httpx.AsyncClient, backdate_budget: int) -> dict:
    """Fill tracked_cards.tcgplayer_id from catalog_cards, then auto-backdate
    each newly-resolved card (see auto_backdate_new_card) — this is the
    first point a brand-new card (from watchlist sync or set import) has a
    usable tcgplayer_id, so it's the earliest a backdate can happen.

    The catalog match itself is free (no API calls); `client` is only for
    the auto-backdate PPT call, which does cost credits (AUTO_BACKDATE_DAYS,
    see the module docstring above it). `backdate_budget` caps how many of
    THIS call's resolves may spend on it — shared with the paid-search
    resolves in run_ppt_ingest via the returned 'backdate_budget_left', so a
    big import can't blow the whole cap here before the search path ever
    gets a turn. Cards beyond the cap still resolve (today's live price is
    unaffected) — they just don't get history until the cap resets on the
    next sweep, or a manual backdate.

    Only rows whose id is verified as a real TCGplayer product id are used —
    the same gate the catalog's buy links use, for the same reason: an
    unverified id is PPT's own, and pricing against it would return the wrong
    card's numbers.
    """
    summary = {"resolved": 0, "unmatched": [], "auto_backdated": 0,
              "auto_backdate_credits": 0, "auto_backdate_deferred": 0}
    remaining = backdate_budget
    async with pool.acquire() as conn:
        cards = await conn.fetch(
            "SELECT id, name, set_name, card_number, variant FROM tracked_cards t "
            "WHERE game = 'pokemon' AND (tcgplayer_id IS NULL OR tcgplayer_id = '') "
            # Same reasoning as _MISSING_TODAY_SQL: skip orphaned rows so an
            # abandoned/never-adopted card doesn't spend an auto-backdate
            # (2 PPT credits) on something nobody is actually tracking.
            "AND EXISTS (SELECT 1 FROM user_tracked_cards u WHERE u.card_id = t.id) "
            "ORDER BY id")
        for c in cards:
            candidates = await conn.fetch(
                "SELECT card_name, card_number, rarity, set_name, tcgplayer_id "
                "FROM catalog_cards "
                "WHERE game = 'pokemon' AND tcgplayer_verified = TRUE "
                "  AND tcgplayer_id <> '' AND card_name ILIKE $1",
                f"%{(c['name'] or '').strip()}%")
            match = match_catalog_row(dict(c), [dict(r) for r in candidates])
            if not match:
                summary["unmatched"].append(
                    f"{c['name']} ({c['set_name'] or 'no set'}): not in the catalog yet")
                continue
            await conn.execute(
                "UPDATE tracked_cards SET tcgplayer_id = $1, catalog_matched_name = $2 "
                "WHERE id = $3",
                match["tcgplayer_id"], match["card_name"], c["id"])
            summary["resolved"] += 1
            logger.info("Tracker: matched %r -> catalog %r (#%s) tcgPlayerId=%s",
                        c["name"], match["card_name"], match["card_number"],
                        match["tcgplayer_id"])
            if remaining > 0:
                remaining -= 1
                bd = await auto_backdate_new_card(client, pool, c["id"], c["name"], match["tcgplayer_id"])
                summary["auto_backdate_credits"] += bd["credits"]
                if bd["inserted"]:
                    summary["auto_backdated"] += 1
            else:
                summary["auto_backdate_deferred"] += 1
    summary["backdate_budget_left"] = remaining
    if summary["unmatched"]:
        logger.info("Tracker: %d Pokemon card(s) not resolvable from the catalog — "
                    "stock their set on /catalog and they resolve next run",
                    len(summary["unmatched"]))
    return summary


# Cards with no snapshot for the current UTC day, neediest first. UTC matches
# the day boundary the JustTCG history backfill already dedupes on, so the two
# can't disagree about what "today" means.
# The "AND EXISTS user_tracked_cards" clause on both queries below is what
# stops an orphaned catalog row (nobody's portfolio references it — e.g. a
# watchlist.py seed nobody actually added, or the last member dropped it)
# from costing a credit every day forever. A card only gets priced while at
# least one member is actually tracking it.
_MISSING_TODAY_SQL = """
    SELECT t.id, t.name, t.set_name, t.card_number, t.variant, t.tcgplayer_id,
           (SELECT MAX(p.captured_at) FROM price_snapshots p WHERE p.card_id = t.id)
               AS last_priced
    FROM tracked_cards t
    WHERE t.game = 'pokemon'
      AND EXISTS (SELECT 1 FROM user_tracked_cards u WHERE u.card_id = t.id)
      AND NOT EXISTS (
          SELECT 1 FROM price_snapshots p
          WHERE p.card_id = t.id
            AND (p.captured_at AT TIME ZONE 'UTC')::date = (NOW() AT TIME ZONE 'UTC')::date
      )
    ORDER BY last_priced ASC NULLS FIRST, t.id
    LIMIT $1
"""

_COVERAGE_SQL = """
    SELECT COUNT(*) AS total,
           COUNT(*) FILTER (WHERE EXISTS (
               SELECT 1 FROM price_snapshots p
               WHERE p.card_id = t.id
                 AND (p.captured_at AT TIME ZONE 'UTC')::date
                     = (NOW() AT TIME ZONE 'UTC')::date
           )) AS priced_today
    FROM tracked_cards t
    WHERE t.game = 'pokemon'
      AND EXISTS (SELECT 1 FROM user_tracked_cards u WHERE u.card_id = t.id)
"""


async def ppt_coverage(pool) -> dict:
    """{total, priced_today, missing} for Pokemon — the guarantee, as a number.

    Surfaced rather than assumed: if the credit budget can't cover the list,
    that has to be visible instead of quietly leaving holes in the history.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(_COVERAGE_SQL)
    total = int(row["total"] or 0)
    priced = int(row["priced_today"] or 0)
    return {"total": total, "priced_today": priced, "missing": total - priced}


async def run_ppt_ingest(pool) -> dict:
    """Ensure every tracked Pokemon card has a price for the current UTC day.

    GAP-FILLING, not a scheduled batch: it prices only cards with no snapshot
    today. That one property is what makes the per-day guarantee hold —
    repeated runs are idempotent and nearly free (a card already priced is
    skipped), so this can run every few hours and recover from a missed
    nightly, a rate limit part-way through, or a transient per-card error,
    without ever paying twice for the same card on the same day.

    Ordering is neediest-first (never priced, then longest since), so a run cut
    short by the cap or a 429 still makes progress on the cards furthest
    behind — the previous ORDER BY id would have re-priced the same first N
    forever and never reached the rest.

    Resolution is free where possible (catalog_cards) and falls back to a
    one-time PPT search, so no card is permanently stuck unpriced. Pricing is
    always PPT — never JustTCG — because mixing two definitions of "mid" into
    one series would corrupt momentum.
    """
    summary = {"cards": 0, "snapshots": 0, "resolved": 0, "resolved_by_search": 0,
               "auto_backdated": 0, "auto_backdate_deferred": 0, "skipped": 0, "credits": 0,
               "failed": [], "rate_limited": False, "total": 0, "priced_today": 0, "missing": 0}

    if not price_sources.pokemonpricetracker_available():
        logger.info("Tracker: PokemonPriceTracker not configured — skipping Pokemon pricing")
        summary.update(await ppt_coverage(pool))
        return summary

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Resolution + auto-backdate for newly-resolved cards (see
        # resolve_tcgplayer_ids) — needs the client, unlike before, since a
        # first-time resolve now also costs a PPT call. The budget it leaves
        # unspent carries over to the search-resolved cards below, so the
        # AUTO_BACKDATE_RUN_CAP is shared across both resolve paths, not
        # doubled.
        res = await resolve_tcgplayer_ids(pool, client, AUTO_BACKDATE_RUN_CAP)
        summary["resolved"] = res["resolved"]
        summary["auto_backdated"] += res["auto_backdated"]
        summary["credits"] += res["auto_backdate_credits"]
        auto_backdate_budget_left = res["backdate_budget_left"]

        async with pool.acquire() as conn:
            cards = await conn.fetch(_MISSING_TODAY_SQL, PPT_RUN_CAP)
        summary["cards"] = len(cards)
        if not cards:
            summary.update(await ppt_coverage(pool))
            logger.info("Tracker: every Pokemon card already has a price for today "
                        "(%d/%d) — %d credit(s) spent on auto-backdate (%d deferred to next sweep)",
                        summary["priced_today"], summary["total"], summary["credits"],
                        summary["auto_backdate_deferred"])
            return summary

        # Free catalog lookup (pokemontcg.io), not a PPT call — costs no
        # credits. Scoring needs release_date for age_days, so this has to keep
        # running now that Pokemon no longer passes through the JustTCG ingest.
        async with pool.acquire() as conn:
            undated = await conn.fetch(
                "SELECT id, name, set_name FROM tracked_cards "
                "WHERE game = 'pokemon' AND release_date IS NULL ORDER BY id")
        for c in undated:
            try:
                release = await justtcg.fetch_pokemon_release_date(
                    client, c["name"], c["set_name"])
                if release:
                    async with pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE tracked_cards SET release_date = COALESCE(release_date, $1) "
                            "WHERE id = $2", release, c["id"])
            except Exception:
                logger.exception("Tracker: release-date lookup failed for %r", c["name"])

        searches_left = PPT_RESOLVE_CAP
        for c in cards:
            tcg_id = (c["tcgplayer_id"] or "").strip()

            # Not in the catalog — resolve by search, once. Capped per run so a
            # large unresolved backlog can't drain the budget in one go; the
            # rest resolve on the next sweep.
            if not tcg_id:
                if searches_left <= 0:
                    summary["skipped"] += 1
                    summary["failed"].append(
                        f"{c['name']}: not in the catalog, search budget used for this run")
                    continue
                searches_left -= 1
                tcg_id, r_status = await price_sources.resolve_ppt_tcgplayer_id(
                    client, c["name"], c["set_name"] or "", c["card_number"] or "")
                summary["credits"] += price_sources.PPT_SEARCH_LIMIT
                if r_status == "rate_limited":
                    summary["rate_limited"] = True
                    summary["failed"].append(
                        "stopped: rate-limited while resolving — the next sweep resumes")
                    break
                if not tcg_id:
                    summary["skipped"] += 1
                    summary["failed"].append(f"{c['name']}: PPT has no match for it")
                    continue
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE tracked_cards SET tcgplayer_id = $1 WHERE id = $2",
                        tcg_id, c["id"])
                summary["resolved_by_search"] += 1
                # First tcgplayer_id this card has ever had — same auto-backdate
                # a free catalog resolve gets in resolve_tcgplayer_ids, sharing
                # the same run-level budget so the two paths can't double it.
                if auto_backdate_budget_left > 0:
                    auto_backdate_budget_left -= 1
                    bd = await auto_backdate_new_card(client, pool, c["id"], c["name"], tcg_id)
                    summary["credits"] += bd["credits"]
                    if bd["inserted"]:
                        summary["auto_backdated"] += 1
                else:
                    summary["auto_backdate_deferred"] += 1

            prices, status = {}, "error"
            for attempt in range(PPT_RATE_RETRIES + 1):
                prices, status, _daily_remaining = await price_sources.fetch_ppt_card_prices(client, tcg_id)
                if status != "rate_limited" or attempt >= PPT_RATE_RETRIES:
                    break
                wait = PPT_RATE_WAIT_S * (2 ** attempt)
                logger.warning("Tracker: rate limited on %r — waiting %.0fs (retry %d/%d)",
                               c["name"], wait, attempt + 1, PPT_RATE_RETRIES)
                await asyncio.sleep(wait)

            if status == "rate_limited":
                summary["rate_limited"] = True
                summary["failed"].append(
                    "stopped: PokemonPriceTracker is rate-limiting — remaining cards "
                    "are picked up on the next run")
                logger.warning("Tracker: stopping Pokemon ingest — still rate limited at %r",
                               c["name"])
                break
            summary["credits"] += 1
            if status != "ok":
                summary["skipped"] += 1
                summary["failed"].append(f"{c['name']}: no price returned ({status})")
                continue

            # price_mid carries the market price — the field card_scoring reads
            # for momentum. low/high are stored when present but are not always
            # supplied, so they stay nullable.
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO price_snapshots (card_id, price_low, price_mid, "
                    "price_high, source) VALUES ($1, $2, $3, $4, $5)",
                    c["id"], prices.get("low"), prices.get("market"),
                    prices.get("high"), PPT_SOURCE)
            summary["snapshots"] += 1

    summary.update(await ppt_coverage(pool))
    logger.info("Tracker (PPT): %d snapshot(s) from %d card(s) missing today, "
                "%d resolved from catalog, %d by search, %d auto-backdated (%d deferred), "
                "%d skipped, %d credit(s)%s | coverage today: %d/%d priced, %d still missing",
                summary["snapshots"], summary["cards"], summary["resolved"],
                summary["resolved_by_search"], summary["auto_backdated"],
                summary["auto_backdate_deferred"], summary["skipped"], summary["credits"],
                " — STOPPED on rate limit" if summary["rate_limited"] else "",
                summary["priced_today"], summary["total"], summary["missing"])
    if summary["missing"]:
        logger.warning("Tracker: %d Pokemon card(s) still have no price for today — "
                       "the next sweep retries them", summary["missing"])
    return summary


async def _backdate_one_card(client: httpx.AsyncClient, pool, card_id: int, card_name: str,
                             tcgplayer_id: str, days: int) -> dict:
    """Fetch + insert missing daily prices for ONE card. Shared by the manual
    /api/card-tracker/backdate action (run_ppt_backdate) and the automatic
    first-resolve backdate (auto_backdate_new_card) — same PPT call, same
    idempotent day-skip insert, just different callers and days.

    Returns {"inserted": int, "credits": int, "status": "ok"|"rate_limited"|"empty"|"error"}.
    Retries on a 429 the same way the rest of the tracker does (PPT_RATE_RETRIES,
    exponential backoff) before giving up and reporting rate_limited.
    """
    history, status = [], "error"
    for attempt in range(PPT_RATE_RETRIES + 1):
        history, status, _daily_remaining = await price_sources.fetch_ppt_price_history(
            client, tcgplayer_id, days)
        if status != "rate_limited" or attempt >= PPT_RATE_RETRIES:
            break
        wait = PPT_RATE_WAIT_S * (2 ** attempt)
        logger.warning("Backdate: rate limited on %r — waiting %.0fs (retry %d/%d)",
                       card_name, wait, attempt + 1, PPT_RATE_RETRIES)
        await asyncio.sleep(wait)

    if status == "rate_limited":
        return {"inserted": 0, "credits": 0, "status": "rate_limited"}
    credits = 2   # base lookup + includeHistory, billed at limit=1
    if status != "ok":
        return {"inserted": 0, "credits": credits, "status": status}

    async with pool.acquire() as conn:
        known_days = {r["day"] for r in await conn.fetch(
            "SELECT DISTINCT (captured_at AT TIME ZONE 'UTC')::date AS day "
            "FROM price_snapshots WHERE card_id = $1", card_id)}
        inserted = 0
        for point in history:
            if point["date"] in known_days:
                continue
            # Noon UTC, matching the stamp the JustTCG history backfill
            # already uses for backfilled (as opposed to live) rows.
            captured_at = datetime(point["date"].year, point["date"].month,
                                   point["date"].day, 12, tzinfo=timezone.utc)
            await conn.execute(
                "INSERT INTO price_snapshots (card_id, captured_at, price_mid, source) "
                "VALUES ($1, $2, $3, $4)",
                card_id, captured_at, point["market"], PPT_HISTORY_SOURCE)
            known_days.add(point["date"])
            inserted += 1
    return {"inserted": inserted, "credits": credits, "status": "ok"}


async def auto_backdate_new_card(client: httpx.AsyncClient, pool, card_id: int,
                                  card_name: str, tcgplayer_id: str) -> dict:
    """Best-effort AUTO_BACKDATE_DAYS-day backdate the moment a card gets its
    tcgplayer_id for the first time (see resolve_tcgplayer_ids and the
    search-fallback in run_ppt_ingest) — so a brand-new card's graph isn't a
    single flat point until someone remembers to backdate it by hand.

    Never raises: a failure here must not abort resolution for the rest of
    the cards in this run. Swallowing is safe because this is pure upside —
    worst case the card just waits for the next manual/nightly opportunity,
    same as it would have without this at all.
    """
    try:
        result = await _backdate_one_card(client, pool, card_id, card_name,
                                          tcgplayer_id, AUTO_BACKDATE_DAYS)
    except Exception:
        logger.exception("Auto-backdate: failed for %r (id %d)", card_name, card_id)
        return {"inserted": 0, "credits": 0, "status": "error"}
    if result["status"] == "rate_limited":
        logger.warning("Auto-backdate: rate limited on %r — skipping for this run", card_name)
    elif result["inserted"]:
        logger.info("Auto-backdate: %r -> %d day(s) of history backfilled on first resolve",
                    card_name, result["inserted"])
    return result


async def run_ppt_backdate(pool, card_ids: list, days: int) -> dict:
    """Backfill day-by-day raw prices for specific tracked Pokemon cards via
    PPT's includeHistory param (price_sources.fetch_ppt_price_history).

    Admin-triggered only, on cards picked by hand — separate from the
    automatic first-resolve backdate (auto_backdate_new_card), which covers
    every new card without a manual click. One Piece cards are reported as
    skipped (PPT has no coverage for them; JustTCG's own history already
    backfills automatically in run_ingest).

    Idempotent the same way the JustTCG history backfill is: a day PPT offers
    that's already in price_snapshots is left alone, so re-running a backdate
    (e.g. widening 30d to 180d) only ever adds what's missing.
    """
    summary = {"cards": 0, "inserted": 0, "credits": 0, "skipped": [], "failed": [],
               "rate_limited": False}
    if not price_sources.pokemonpricetracker_available():
        summary["failed"].append("PokemonPriceTracker is not configured")
        return summary
    if days not in BACKDATE_DAY_CHOICES:
        summary["failed"].append(f"days must be one of {BACKDATE_DAY_CHOICES}")
        return summary

    async with pool.acquire() as conn:
        cards = await conn.fetch(
            "SELECT id, name, game, tcgplayer_id FROM tracked_cards "
            "WHERE id = ANY($1::int[]) ORDER BY id", card_ids)
    summary["cards"] = len(cards)

    async with httpx.AsyncClient(timeout=30.0) as client:
        for c in cards:
            if c["game"] != "pokemon":
                summary["skipped"].append(f"{c['name']}: One Piece isn't covered by PokemonPriceTracker")
                continue
            tcg_id = (c["tcgplayer_id"] or "").strip()
            if not tcg_id:
                summary["skipped"].append(f"{c['name']}: no resolved TCGplayer id yet — run a refresh first")
                continue

            result = await _backdate_one_card(client, pool, c["id"], c["name"], tcg_id, days)
            summary["credits"] += result["credits"]
            if result["status"] == "rate_limited":
                summary["rate_limited"] = True
                summary["failed"].append(
                    "stopped: PokemonPriceTracker is rate-limiting — remaining selected "
                    "cards were not backdated; try again shortly")
                break
            if result["status"] != "ok":
                summary["skipped"].append(f"{c['name']}: no history returned ({result['status']})")
                continue
            summary["inserted"] += result["inserted"]
            logger.info("Backdate: %r -> %d new row(s) inserted", c["name"], result["inserted"])

    logger.info("Backdate done: %d card(s), %d row(s) inserted, %d credit(s)%s, "
                "%d skipped, %d failed",
                summary["cards"], summary["inserted"], summary["credits"],
                " — STOPPED on rate limit" if summary["rate_limited"] else "",
                len(summary["skipped"]), len(summary["failed"]))
    return summary


async def run_ingest(pool) -> dict:
    """JustTCG pricing for tracked ONE PIECE cards.

    Pokemon moved to run_ppt_ingest — PokemonPriceTracker resolves for free
    out of catalog_cards and prices by a pinned TCGplayer id, which is both
    cheaper and more accurate than JustTCG's search-and-match. One Piece has
    no PPT coverage, so it stays here — and now has the whole JustTCG budget
    to itself instead of competing with several hundred Pokemon cards.

    Per-card failures are logged and skipped, never fatal. Returns a summary
    dict: {"cards", "snapshots", "resolved", "backfilled", "failed": [...]}.
    """
    summary = {"cards": 0, "snapshots": 0, "resolved": 0, "backfilled": 0, "failed": []}
    consecutive_failures = 0

    async with pool.acquire() as conn:
        cards = await conn.fetch(
            "SELECT id, name, game, set_name, card_number, variant, release_date, "
            "justtcg_card_id FROM tracked_cards WHERE game = 'one_piece' ORDER BY id"
        )
    summary["cards"] = len(cards)
    if not cards:
        logger.info("Ingest: no tracked One Piece cards — nothing for JustTCG to do")
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

        # Pokemon release dates moved to run_ppt_ingest along with the rest of
        # the Pokemon path — the lookup is a free catalog call, not a JustTCG
        # one, so it belongs next to the cards it serves.

        # ── Pass 3: batch price fetch by resolved id ──
        async with pool.acquire() as conn:
            cards = await conn.fetch(
                "SELECT id, name, variant, justtcg_card_id FROM tracked_cards "
                "WHERE game = 'one_piece' AND justtcg_card_id IS NOT NULL ORDER BY id")
            # Which calendar days (UTC) each card already has a price for, so
            # the backfill below can add only what's missing.
            have_rows = await conn.fetch(
                "SELECT DISTINCT card_id, (captured_at AT TIME ZONE 'UTC')::date AS day "
                "FROM price_snapshots WHERE card_id = ANY($1::int[]) "
                "AND captured_at >= NOW() - INTERVAL '200 days'",
                [c["id"] for c in cards])
        have_days: dict = {}
        for r in have_rows:
            have_days.setdefault(r["card_id"], set()).add(r["day"])
        by_jid = {c["justtcg_card_id"]: c for c in cards}
        # Always ask for history. It costs no extra API calls (same requests,
        # bigger payload), and asking every run is what lets us fill days we
        # don't have: nights the ingest didn't fire, and cards first fetched
        # back when the request shape was silently getting 7 days instead of 90.
        need_history = True
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
                # Backfill any day the source knows about that we don't have.
                # Runs every ingest, not just on a card's first fetch, so a
                # missed night or a card stuck on an old 7-day fetch heals
                # itself. Skipping days we already have keeps it idempotent —
                # there's no unique index on (card_id, day) to lean on. Only
                # days BEFORE today; today's live snapshot is inserted below.
                known_days = have_days.setdefault(card_row["id"], set())
                history = justtcg.extract_price_history(obj, card_row["variant"] or "")
                inserted = 0
                for h in history:
                    day = h["captured_at"].date()
                    if day >= today_utc or day in known_days:
                        continue
                    await conn.execute(
                        "INSERT INTO price_snapshots (card_id, captured_at, price_low, "
                        "price_mid, price_high, source) VALUES ($1, $2, $3, $4, $5, 'justtcg-history')",
                        card_row["id"], h["captured_at"], h["price_low"],
                        h["price_mid"], h["price_high"])
                    known_days.add(day)
                    inserted += 1
                summary["backfilled"] += inserted
                if inserted or history:
                    logger.info("Ingest: %r -> source offered %d history day(s), %d were new "
                                "(source history near 7 days when we asked for %s means the "
                                "duration parameter is being ignored)",
                                card_row["name"], len(history), inserted,
                                justtcg.HISTORY_DURATION)
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
                "SELECT captured_at, price_low, price_mid, price_high, source "
                "FROM price_snapshots "
                "WHERE card_id = $1 AND captured_at >= NOW() - INTERVAL '60 days' "
                "ORDER BY captured_at",
                c["id"])
            if not snaps:
                summary["skipped"] += 1
                continue
            # Never score across two pricing sources — see select_scoring_series.
            series = card_scoring.select_scoring_series([dict(r) for r in snaps])
            if not series:
                summary["skipped"] += 1
                continue
            s = card_scoring.score_card(series, c["release_date"])
            await conn.execute(
                "INSERT INTO card_scores (card_id, momentum_7d, momentum_30d, "
                "liquidity_score, age_days, potential_score) VALUES ($1, $2, $3, $4, $5, $6)",
                c["id"], s["momentum_7d_pct"], s["momentum_30d_pct"],
                s["liquidity_score"], s["age_days"], s["potential_score"])
            summary["scored"] += 1
    logger.info("Scoring done: %d scored, %d skipped (no snapshots)",
                summary["scored"], summary["skipped"])
    return summary
