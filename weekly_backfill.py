"""Fill catalog_price_weekly with past weeks, so the weekly report's catalog
lists don't start empty.

catalog_cards only holds today's price, and weekly_report only starts
recording one price per card per week once it's deployed, so without this
the catalog side needs 2-5 weeks before its lists fill in. Three of the
price APIs we already pay for can hand back history for the SAME series the
catalog shows today (checked 2026-11-17):

  * One Piece  — OPTCG `/cards/{id}/price-history`: one point per weekly
                 refresh, back to April 2026. 1 request per card, under the
                 shared OPTCG limiter and daily cap.
  * Pokémon    — PPT `includeHistory` (price_sources.fetch_ppt_price_history,
                 the tracker's Backdate call): daily. 2 credits per card.
  * Riftbound  — TCGGO `/history-prices` (riftbound.fetch_history): daily
                 TCGplayer series. Up to 6 requests per card.

Not covered: Lorcana (Lorcast has no history, and TCGplayer's numbers only
matched Lorcast's on 474 of 816 printings) and Gundam (its history only
exists in TCGCSV's daily archives). Both fill in on their own.

Rules:
  * Only cards worth listing: priced at or above weekly_report.MIN_PRICE,
    whose current price_source is the series this API serves, and not
    mapped to a tracked card (the report uses the tracked series for those).
  * Each backfilled week is the LAST point in that Monday-week, stored under
    the catalog row's current price_source — the same thing the live capture
    stores — so weekly_report compares them as one series.
  * Never the current week (the live capture owns it), and never over a week
    already recorded (ON CONFLICT DO NOTHING).
  * Each card is fetched once (catalog_history_backfill remembers it); an
    error is retried after RETRY_ERRORS_AFTER_DAYS.
  * Per-game daily card budgets keep each API well inside its plan, and a
    429 / quota answer stops that game until the next pass.

Dashboard-owned; ava_bot never reads catalog_history_backfill.
"""

import asyncio
import logging
import math
import os
from datetime import date, datetime, timedelta, timezone

import httpx

import optcg
import price_sources
import riftbound
import weekly_report as wr

logger = logging.getLogger("weekly_backfill")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


ENABLED = os.getenv("WEEKLY_BACKFILL", "1").strip().lower() not in ("0", "false", "no", "off")
# Enough for the 90-day "usual price" and the 12-week comparison, and inside
# catalog_price_weekly's retention.
BACKFILL_WEEKS = min(_env_int("WEEKLY_BACKFILL_WEEKS", 16), wr.HISTORY_WEEKS)
# Cards fetched per game per UTC day.
#   one_piece: 1 request each, against OPTCG's 1,800/day cap (sync needs few)
#   pokemon:   2 PPT credits each, against 20,000/day shared with the tracker
#   riftbound: up to 6 requests each, against 2,500/day (daily sync ~95)
DAILY_CARDS = {
    "one_piece": _env_int("WEEKLY_BACKFILL_OP_PER_DAY", 1200),
    "pokemon": _env_int("WEEKLY_BACKFILL_PPT_PER_DAY", 2500),
    "riftbound": _env_int("WEEKLY_BACKFILL_RIFTBOUND_PER_DAY", 200),
}
# Stop Pokémon once PPT says fewer credits than this are left today, so the
# tracker's own refresh always has room.
PPT_MIN_REMAINING = _env_int("WEEKLY_BACKFILL_PPT_RESERVE", 6000)
RETRY_ERRORS_AFTER_DAYS = 7
PPT_PAUSE_S = 1.1        # PPT allows 60 calls/minute
RIFTBOUND_PAUSE_S = 0.5

GAME_ORDER = ("one_piece", "riftbound", "pokemon")
# The catalog price_source whose history each API serves. A card currently
# priced from anything else is skipped: its history would be another series.
SOURCES = {"one_piece": "tcgplayer", "pokemon": "pokemonpricetracker", "riftbound": "tcgplayer"}
# The catalog column each API identifies a card by (allowlisted: interpolated).
_IDENT_COL = {"one_piece": "source_id", "pokemon": "tcgplayer_id", "riftbound": "source_id"}
# The API has nothing more for this card now; don't ask again.
_DONE = {"ok", "empty", "not_found"}
# Stop this game for this pass; ask again next pass.
_STOP = {"rate_limited", "quota", "forbidden"}

BACKFILL_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS catalog_history_backfill (
        catalog_id INTEGER PRIMARY KEY,
        game       TEXT NOT NULL,
        status     TEXT NOT NULL,
        weeks      INTEGER NOT NULL DEFAULT 0,
        fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_catalog_history_backfill_game_day "
    "ON catalog_history_backfill (game, fetched_at)",
]

_lock = asyncio.Lock()


async def ensure_backfill_schema(pool) -> None:
    for ddl in BACKFILL_SCHEMA:
        try:
            async with pool.acquire() as conn:
                await conn.execute(ddl)
        except Exception:
            logger.exception("Weekly backfill: schema statement failed")


# ────────────────────────── pure ──────────────────────────

def weeks_from_points(points, current_week: date, weeks: int = None) -> dict:
    """{week: (price, priced_at)} from [(datetime|date, price)].

    Keeps the LAST point in each Monday-week (what the live capture keeps:
    the newest price seen that week), for the `weeks` full weeks before
    current_week. The current week and anything later are left out. A bare
    date is read as noon UTC.
    """
    weeks = BACKFILL_WEEKS if weeks is None else weeks
    oldest = current_week - timedelta(weeks=weeks)
    out = {}
    for ts, price in points or []:
        if ts is None or price is None:
            continue
        try:
            price = float(price)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(price) or price <= 0:
            continue
        if isinstance(ts, datetime):
            ts = wr._utc(ts)
        elif isinstance(ts, date):
            ts = datetime(ts.year, ts.month, ts.day, 12, tzinfo=timezone.utc)
        else:
            continue
        wk = wr.week_start(ts)
        if wk >= current_week or wk < oldest:
            continue
        if wk not in out or ts >= out[wk][1]:
            out[wk] = (round(price, 2), ts)
    return out


# ────────────────────────── fetchers ──────────────────────────
# Each: (client, pool, row) -> ([(datetime|date, price)], status, meta)

async def _fetch_one_piece(client, pool, row):
    body, status = await optcg.fetch_price_history(client, pool, row["source_id"])
    if status != "ok":
        return [], status, {}
    points = []
    for p in (body.get("points") if isinstance(body, dict) else None) or []:
        if not isinstance(p, dict):
            continue
        try:
            when = datetime.fromtimestamp(int(p.get("t")) / 1000, tz=timezone.utc)
        except (TypeError, ValueError, OverflowError, OSError):
            continue
        points.append((when, p.get("price")))
    return points, ("ok" if points else "empty"), {}


async def _fetch_pokemon(client, pool, row):
    history, status, remaining = await price_sources.fetch_ppt_price_history(
        client, row["tcgplayer_id"], BACKFILL_WEEKS * 7 + 7, language=row["language"])
    await asyncio.sleep(PPT_PAUSE_S)
    points = [(h["date"], h["market"]) for h in history] if status == "ok" else []
    return points, status, {"remaining": remaining}


async def _fetch_riftbound(client, pool, row):
    history, status = await riftbound.fetch_history(client, row["source_id"])
    await asyncio.sleep(RIFTBOUND_PAUSE_S)
    return [(h["date"], h["market"]) for h in history], status, {}


FETCHERS = {"one_piece": _fetch_one_piece, "pokemon": _fetch_pokemon,
            "riftbound": _fetch_riftbound}
AVAILABLE = {"one_piece": optcg.available,
             "pokemon": price_sources.pokemonpricetracker_available,
             "riftbound": riftbound.available}


# ────────────────────────── database ──────────────────────────

async def _spent_today(conn, game: str, now: datetime) -> int:
    day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    return int(await conn.fetchval(
        "SELECT COUNT(*) FROM catalog_history_backfill WHERE game = $1 AND fetched_at >= $2",
        game, day_start) or 0)


async def _candidates(conn, game: str, limit: int, now: datetime) -> list:
    ident = _IDENT_COL[game]
    if game == "pokemon":
        not_tracked = ("NOT EXISTS (SELECT 1 FROM tracked_cards t WHERE t.game = 'pokemon' "
                       "AND t.tcgplayer_id = c.tcgplayer_id AND t.language = c.language)")
    else:
        not_tracked = ("NOT EXISTS (SELECT 1 FROM tracked_cards t WHERE t.game = c.game "
                       "AND t.source_id = c.source_id)")
    return await conn.fetch(
        f"""
        SELECT c.id, c.game, c.language, c.source_id, c.tcgplayer_id, c.price_source
        FROM catalog_cards c
        LEFT JOIN catalog_history_backfill b ON b.catalog_id = c.id
        WHERE c.game = $1
          AND c.raw_price >= $2
          AND c.price_source = $3
          AND COALESCE(c.{ident}, '') <> ''
          AND (b.catalog_id IS NULL
               OR (b.status = 'error' AND b.fetched_at < $5::timestamptz - make_interval(days => $6)))
          AND {not_tracked}
        ORDER BY c.raw_price DESC, c.id
        LIMIT $4
        """,
        game, wr.MIN_PRICE, SOURCES[game], int(limit), now, RETRY_ERRORS_AFTER_DAYS)


async def _record(conn, row, status: str, weekly: dict, now: datetime) -> int:
    """Store the weeks (never over an existing week) and mark the card done.
    Returns how many weeks were new."""
    inserted = 0
    async with conn.transaction():
        if weekly:
            wks = sorted(weekly)
            tag = await conn.execute(
                """
                INSERT INTO catalog_price_weekly
                    (catalog_id, week, game, language, price, price_source, priced_at)
                SELECT $1, u.w, $2, $3, u.p, $4, u.t
                FROM unnest($5::date[], $6::numeric[], $7::timestamptz[]) AS u(w, p, t)
                ON CONFLICT (catalog_id, week) DO NOTHING
                """,
                row["id"], row["game"], row["language"], row["price_source"],
                wks, [weekly[w][0] for w in wks], [weekly[w][1] for w in wks])
            inserted = wr._count(tag)
        await conn.execute(
            "INSERT INTO catalog_history_backfill (catalog_id, game, status, weeks, fetched_at) "
            "VALUES ($1, $2, $3, $4, $5) ON CONFLICT (catalog_id) DO UPDATE "
            "SET status = EXCLUDED.status, weeks = EXCLUDED.weeks, fetched_at = EXCLUDED.fetched_at",
            row["id"], row["game"], status, inserted, now)
    return inserted


async def backfill_game(pool, game: str, client=None, now=None, fetch=None) -> dict:
    """Backfill up to today's remaining budget of cards for one game, most
    valuable first. Never raises."""
    summary = {"game": game, "cards": 0, "weeks": 0, "stopped": None}
    now = wr._utc(now) if now else wr._now_utc()
    fetch = fetch or FETCHERS[game]
    current = wr.week_start(now)
    try:
        async with pool.acquire() as conn:
            budget = DAILY_CARDS.get(game, 0) - await _spent_today(conn, game, now)
            if budget <= 0:
                summary["stopped"] = "daily budget"
                return summary
            rows = await _candidates(conn, game, budget, now)
        for row in rows:
            try:
                points, status, meta = await fetch(client, pool, row)
            except Exception:
                logger.exception("Weekly backfill: %s card %s fetch failed", game, row["id"])
                points, status, meta = [], "error", {}
            if status in _STOP:
                summary["stopped"] = status
                break
            weekly = weeks_from_points(points, current) if status == "ok" else {}
            if weekly:
                mark = "ok"
            elif status in _DONE:
                mark = "empty"
            else:
                mark = "error"
            async with pool.acquire() as conn:
                summary["weeks"] += await _record(conn, row, mark, weekly, now)
            summary["cards"] += 1
            remaining = (meta or {}).get("remaining")
            if remaining is not None and remaining < PPT_MIN_REMAINING:
                summary["stopped"] = "credit reserve"
                break
    except Exception:
        logger.exception("Weekly backfill: %s pass failed", game)
        summary["stopped"] = "error"
    if summary["cards"]:
        logger.info("Weekly backfill: %s — %d card(s), %d week(s) added%s", game,
                    summary["cards"], summary["weeks"],
                    f", stopped: {summary['stopped']}" if summary["stopped"] else "")
    return summary


async def run_backfill(pool, now=None, fetchers=None) -> dict:
    """One pass over every game with a history source. Safe to run every few
    hours: finished cards are remembered and each game keeps to its daily
    budget. `fetchers` (tests) replaces the real API calls and skips the
    key check. Never raises."""
    if not ENABLED:
        return {"games": [], "weeks": 0}
    if _lock.locked():
        return {"games": [], "weeks": 0, "busy": True}
    async with _lock:
        out = []
        async with httpx.AsyncClient(timeout=30.0) as client:
            for game in GAME_ORDER:
                if fetchers is not None:
                    if game not in fetchers:
                        continue
                    fetch = fetchers[game]
                else:
                    if not AVAILABLE[game]():
                        continue
                    fetch = None
                out.append(await backfill_game(pool, game, client, now, fetch))
        return {"games": out, "weeks": sum(g["weeks"] for g in out)}
