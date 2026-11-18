"""Weekly movers report: which cards moved this week, and why.

Reads only data the app already stores (no network):
  * tracked cards  -> price_snapshots (daily) + card_scores
  * the whole catalog -> catalog_price_weekly, a one-row-per-card-per-week
    history this module captures from catalog_cards (which only holds today's
    price)

Results are written to weekly_reports once a week and read back by the page.
Both tables are dashboard-owned; ava_bot never reads them.

The rules are pure functions (classify_tracked / classify_catalog) so they can
be unit-tested without a database. The lists describe recent price moves;
they are not buy or sell advice, and no text here says otherwise.

Same-source rule (see MEMORY.md, "scoring must never span two sources"):
tracked series go through card_scoring.select_scoring_series, and catalog
weekly points are only compared with points that share the newest point's
price_source, so a change of price source never reads as a price move.

Percent convention: classify_* and weekly_change work in FRACTIONS (0.18 =
+18%); the rows stored in weekly_reports / returned by read_report carry
PERCENT (18.0 = +18%), like card_scores.momentum_*.
"""

import bisect
import logging
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from statistics import median

import card_scoring

logger = logging.getLogger("weekly_report")


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, default)))
    except (TypeError, ValueError):
        return int(default)


# ────────────────────────── TUNING KNOBS (env-overridable) ──────────────────────────
MIN_PRICE = _env_float("WEEKLY_MIN_PRICE", 3.0)
LIST_SIZE = _env_int("WEEKLY_LIST_SIZE", 15)
BELOW_USUAL_PCT = _env_float("WEEKLY_BELOW_USUAL_PCT", 0.15)
SPIKE_PCT = _env_float("WEEKLY_SPIKE_PCT", 0.40)
COOLING_WEEKS = _env_int("WEEKLY_COOLING_WEEKS", 3)
NEW_RELEASE_DAYS = _env_int("WEEKLY_NEW_RELEASE_DAYS", 45)
WATCH_MIN_PCT = _env_float("WEEKLY_WATCH_MIN_PCT", 0.05)
HISTORY_WEEKS = _env_int("WEEKLY_HISTORY_WEEKS", 26)
# A catalog price older than this isn't captured, so a stale price never
# looks flat. One Piece prices refresh weekly (Mondays).
CATALOG_FRESH_DAYS = {
    "one_piece": _env_int("WEEKLY_CATALOG_FRESH_DAYS_ONE_PIECE", 9),
    "default": _env_int("WEEKLY_CATALOG_FRESH_DAYS", 3),
}

# Not in the contract's constant list; internal judgement calls, kept visible.
SPIKE_HISTORY_POINTS = 14          # tracked: "little history" = fewer daily points than this
CATALOG_SPIKE_HISTORY_POINTS = 4   # catalog weekly points: ~a month of weeks
BELOW_USUAL_MIN_POINTS = 14        # tracked: daily points needed for a "usual" price
CATALOG_BELOW_USUAL_MIN_POINTS = 4
TRACKED_MAX_STALE_DAYS = 3         # newest tracked snapshot older than this: skip the card
USUAL_WINDOW_DAYS = 90
LONG_DECLINE_FLOOR = -0.05         # below_usual needs the 90-day change to be at least this
SNAPSHOT_LOOKBACK_DAYS = 120

LISTS = [("risers", "Biggest risers"),
         ("fallers", "Biggest fallers"),
         ("watch", "Worth watching"),
         ("below_usual", "Below its usual price"),
         ("cooling", "Cooling off")]
LIST_KEYS = tuple(k for k, _ in LISTS)

WEEKLY_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS catalog_price_weekly (
        catalog_id   INTEGER NOT NULL,
        week         DATE NOT NULL,
        game         TEXT NOT NULL,
        language     TEXT,
        price        NUMERIC NOT NULL,
        price_source TEXT,
        priced_at    TIMESTAMPTZ,
        PRIMARY KEY (catalog_id, week)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_catalog_price_weekly_game_week "
    "ON catalog_price_weekly (game, week)",
    """
    CREATE TABLE IF NOT EXISTS weekly_reports (
        week            DATE NOT NULL,
        game            TEXT NOT NULL,
        list            TEXT NOT NULL,
        rank            INTEGER NOT NULL,
        card_kind       TEXT NOT NULL,
        tracked_card_id INTEGER,
        catalog_id      INTEGER,
        name            TEXT,
        set_name        TEXT,
        card_number     TEXT,
        version         TEXT,
        language        TEXT,
        image_url       TEXT,
        price_now       NUMERIC,
        price_then      NUMERIC,
        change_pct      NUMERIC,
        change_abs      NUMERIC,
        reason          TEXT,
        generated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (week, game, list, rank)
    )
    """,
]


async def ensure_weekly_schema(pool) -> None:
    """Create the weekly tables (idempotent). Each statement is isolated so one
    failure can't stop the rest."""
    for ddl in WEEKLY_SCHEMA:
        try:
            async with pool.acquire() as conn:
                await conn.execute(ddl)
        except Exception:
            logger.exception("Weekly report: schema statement failed")


# ────────────────────────── pure helpers ──────────────────────────

def _utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def week_start(dt_or_date) -> date:
    """The Monday (UTC) of the week containing a datetime or date."""
    if isinstance(dt_or_date, datetime):
        d = _utc(dt_or_date).date()
    else:
        d = dt_or_date
    return d - timedelta(days=d.weekday())


def _money(p: float) -> str:
    if abs(p - round(p)) < 0.005:
        return f"${round(p):,}"
    return f"${p:,.2f}"


def _pct_text(frac: float) -> str:
    v = abs(frac) * 100
    s = f"{v:.0f}" if v >= 10 else f"{v:.1f}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s + "%"


def _clean_series(series) -> list:
    """[(datetime, price)] sorted ascending, tz-aware UTC, unpriced dropped."""
    out = []
    for ts, price in series or []:
        if ts is None or price is None:
            continue
        price = float(price)
        if price <= 0:
            continue
        out.append((_utc(ts), price))
    out.sort(key=lambda t: t[0])
    return out


def _at_or_before(pts: list, times: list, t: datetime):
    """Newest (ts, price) at or before t, or None."""
    i = bisect.bisect_right(times, t)
    return pts[i - 1] if i else None


def weekly_change(series, now, max_age_days=None):
    """(price_now, price_then, pct, abs) from a daily series [(datetime, price)].

    price_now is the latest point at or before `now`; price_then is the newest
    point at or before now-7d (exactly 7 days old counts). None when either is
    missing, or when the latest point is itself the week-ago point (the series
    has stopped), or older than max_age_days when given. pct is a fraction.
    """
    now = _utc(now)
    pts = [p for p in _clean_series(series) if p[0] <= now]
    if len(pts) < 2:
        return None
    times = [p[0] for p in pts]
    latest = pts[-1]
    if max_age_days is not None and now - latest[0] > timedelta(days=max_age_days):
        return None
    then = _at_or_before(pts, times, now - timedelta(days=7))
    if then is None or then is latest:
        return None
    return (latest[1], then[1], (latest[1] - then[1]) / then[1], latest[1] - then[1])


def _empty() -> dict:
    return {"change": None, "flags": {}, "sort_keys": {}}


def _flag(out: dict, key: str, reason: str, sort_key: tuple) -> None:
    out["flags"][key] = reason
    out["sort_keys"][key] = sort_key


def _weekly_reasons(change) -> tuple:
    p_now, p_then, pct, _ = change
    move = f"{_money(p_then)} → {_money(p_now)}"
    return move


def _apply_move_rules(out: dict, change) -> None:
    """risers / fallers (shared by tracked and catalog)."""
    p_now, p_then, pct, dabs = change
    move = _weekly_reasons(change)
    if pct > 0:
        _flag(out, "risers", f"Up {_pct_text(pct)} this week ({move})", (pct, dabs))
    elif pct < 0:
        _flag(out, "fallers", f"Down {_pct_text(pct)} this week ({move})", (-pct, -dabs))


def classify_tracked(series, score_row, release_date, now) -> dict:
    """Apply the list rules to one tracked card.

    series: [(datetime, price)] for ONE pricing source (callers pass
    card_scoring.select_scoring_series output, see tracked_series).
    score_row: newest card_scores row (mapping) or None; its momentum_30d (a
    percent) is the 30-day change when present, else it is derived from series.
    Returns {"change": (now, then, pct, abs) | None, "flags": {list: reason},
    "sort_keys": {list: tuple, larger ranks first}}.
    """
    out = _empty()
    now = _utc(now)
    pts = [p for p in _clean_series(series) if p[0] <= now]
    if len(pts) < 2:
        return out
    times = [p[0] for p in pts]
    p_now = pts[-1][1]
    if p_now < MIN_PRICE:
        return out
    if now - pts[-1][0] > timedelta(days=TRACKED_MAX_STALE_DAYS):
        return out

    change = weekly_change(pts, now, TRACKED_MAX_STALE_DAYS)
    out["change"] = change
    out["price_now"] = p_now
    pct = change[2] if change else None

    # risers / fallers
    if change:
        _apply_move_rules(out, change)

    # watch: up this week and the month, the rise held, and not a spike
    if change and pct >= WATCH_MIN_PCT and pct < SPIKE_PCT:
        m30 = None
        if score_row is not None:
            try:
                v = score_row["momentum_30d"]
            except (KeyError, TypeError):
                v = None
            if v is not None:
                m30 = float(v) / 100.0
        if m30 is None:
            base = _at_or_before(pts, times, now - timedelta(days=30))
            if base is not None and base[1] > 0:
                m30 = (p_now - base[1]) / base[1]
        if m30 is not None and m30 > 0:
            held = 0
            for k in range(4):
                p = _at_or_before(pts, times, now - timedelta(days=k))
                if p is not None and p[1] >= change[1]:
                    held += 1
            if held >= 3:
                _flag(out, "watch",
                      f"Up {_pct_text(pct)} this week and {_pct_text(m30)} over the past month, "
                      f"and the rise has held ({_money(change[1])} → {_money(p_now)})",
                      (pct, change[3]))

    # below_usual: well under its 90-day median, but not in a long slide
    window = [p for p in pts if p[0] >= now - timedelta(days=USUAL_WINDOW_DAYS)]
    if len(window) >= BELOW_USUAL_MIN_POINTS:
        usual = median(p[1] for p in window)
        if usual > 0 and p_now <= usual * (1 - BELOW_USUAL_PCT):
            old = _at_or_before(pts, times, now - timedelta(days=USUAL_WINDOW_DAYS)) or pts[0]
            if old[1] > 0 and (p_now - old[1]) / old[1] >= LONG_DECLINE_FLOOR:
                gap = (usual - p_now) / usual
                _flag(out, "below_usual",
                      f"{_pct_text(gap)} below its usual price of {_money(usual)} "
                      f"over the past 3 months", (gap,))
                out["usual"] = usual

    # cooling
    prices = []
    for w in range(COOLING_WEEKS + 1):
        p = _at_or_before(pts, times, now - timedelta(days=7 * w))
        prices.append(p[1] if p else None)
    if (change and COOLING_WEEKS >= 1 and all(p is not None for p in prices)
            and all(prices[i] < prices[i + 1] for i in range(COOLING_WEEKS))):
        _flag(out, "cooling",
              f"Down {COOLING_WEEKS} weeks in a row ({_money(prices[-1])} → {_money(prices[0])})",
              (abs(pct), abs(change[3])))
    elif change and pct >= SPIKE_PCT and len(pts) < SPIKE_HISTORY_POINTS:
        _flag(out, "cooling",
              f"Up {_pct_text(pct)} in a week on little price history — "
              f"prices like this often settle", (abs(pct), abs(change[3])))
    elif change and pct < 0 and release_date is not None:
        rd = release_date.date() if isinstance(release_date, datetime) else release_date
        age = (now.date() - rd).days
        if 0 <= age < NEW_RELEASE_DAYS:
            _flag(out, "cooling",
                  f"New release, price easing (down {_pct_text(pct)} this week)",
                  (abs(pct), abs(change[3])))
    return out


def _catalog_points(weekly_points) -> list:
    """[(week_date, price)] sorted, restricted to the newest point's price_source."""
    rows = []
    for p in weekly_points or []:
        wk, price = p[0], p[1]
        src = p[2] if len(p) > 2 else None
        if isinstance(wk, datetime):
            wk = week_start(wk)
        if wk is None or price is None or float(price) <= 0:
            continue
        rows.append((wk, float(price), src or ""))
    rows.sort(key=lambda r: r[0])
    if not rows:
        return []
    src = rows[-1][2]
    return [(w, pr) for w, pr, s in rows if s == src]


def classify_catalog(weekly_points, now) -> dict:
    """Apply the list rules to one catalog card from its weekly points.

    weekly_points: [(week_date, price)] or [(week_date, price, price_source)].
    Week-over-week: the newest point must be this week's, and 'then' is last
    week's (a missing week leaves nothing to compare, never a longer span).
    Points with a different price_source than the newest are ignored.
    """
    out = _empty()
    now = _utc(now)
    cur = week_start(now)
    pts = _catalog_points(weekly_points)
    if len(pts) < 2:
        return out
    by_week = dict(pts)
    if pts[-1][0] != cur:
        return out
    p_now = pts[-1][1]
    if p_now < MIN_PRICE:
        return out

    then = by_week.get(cur - timedelta(days=7))
    change = None
    if then:
        change = (p_now, then, (p_now - then) / then, p_now - then)
    out["change"] = change
    out["price_now"] = p_now
    pct = change[2] if change else None

    if change:
        _apply_move_rules(out, change)

    # watch: up this week, up vs 4 weeks ago, not a spike
    if change and pct >= WATCH_MIN_PCT and pct < SPIKE_PCT:
        four = by_week.get(cur - timedelta(days=28))
        if four and p_now > four:
            _flag(out, "watch",
                  f"Up {_pct_text(pct)} this week and {_pct_text((p_now - four) / four)} "
                  f"over four weeks ({_money(four)} → {_money(p_now)})", (pct, change[3]))

    # below_usual
    cutoff = now.date() - timedelta(days=USUAL_WINDOW_DAYS)
    window = [p for w, p in pts if w >= cutoff]
    if len(window) >= CATALOG_BELOW_USUAL_MIN_POINTS:
        usual = median(window)
        if usual > 0 and p_now <= usual * (1 - BELOW_USUAL_PCT):
            older = [p for w, p in pts if w <= cur - timedelta(weeks=12)]
            old = older[-1] if older else pts[0][1]
            if (p_now - old) / old >= LONG_DECLINE_FLOOR:
                gap = (usual - p_now) / usual
                _flag(out, "below_usual",
                      f"{_pct_text(gap)} below its usual price of {_money(usual)} "
                      f"over the past 3 months", (gap,))
                out["usual"] = usual

    # cooling
    prices = [by_week.get(cur - timedelta(weeks=w)) for w in range(COOLING_WEEKS + 1)]
    if (change and COOLING_WEEKS >= 1 and all(p is not None for p in prices)
            and all(prices[i] < prices[i + 1] for i in range(COOLING_WEEKS))):
        _flag(out, "cooling",
              f"Down {COOLING_WEEKS} weeks in a row ({_money(prices[-1])} → {_money(prices[0])})",
              (abs(pct), abs(change[3])))
    elif change and pct >= SPIKE_PCT and len(pts) < CATALOG_SPIKE_HISTORY_POINTS:
        _flag(out, "cooling",
              f"Up {_pct_text(pct)} in a week on little price history — "
              f"prices like this often settle", (abs(pct), abs(change[3])))
    return out


def tracked_series(snapshots) -> list:
    """[(datetime, price)] from raw snapshot dicts, through the same-source
    selection scoring uses."""
    chosen = card_scoring.select_scoring_series([dict(s) for s in snapshots])
    out = []
    for s in chosen:
        p = card_scoring.snapshot_price(s)
        if p and p > 0:
            out.append((s["captured_at"], p))
    return out


# ────────────────────────── database ──────────────────────────

def _count(tag) -> int:
    try:
        return int(str(tag).split()[-1])
    except (ValueError, IndexError):
        return 0


async def capture_catalog_week(pool, now=None) -> int:
    """Record this week's price for every freshly priced catalog card (one row
    per card per week; re-running updates it to the newer price) and drop
    weeks past the retention window. Returns rows inserted or updated."""
    try:
        now = _utc(now) if now else _now_utc()
        wk = week_start(now)
        games = list(CATALOG_FRESH_DAYS.keys())
        days = [CATALOG_FRESH_DAYS[g] for g in games]
        async with pool.acquire() as conn:
            tag = await conn.execute(
                """
                INSERT INTO catalog_price_weekly
                    (catalog_id, week, game, language, price, price_source, priced_at)
                SELECT c.id, $1::date, c.game, c.language, c.raw_price, c.price_source, c.priced_at
                FROM catalog_cards c
                LEFT JOIN unnest($3::text[], $4::int[]) AS f(g, d) ON f.g = c.game
                WHERE c.raw_price >= 0.01
                  AND c.priced_at IS NOT NULL
                  AND c.priced_at >= $2::timestamptz
                        - make_interval(days => COALESCE(f.d, $5::int))
                  AND c.priced_at <= $2::timestamptz + INTERVAL '1 hour'
                ON CONFLICT (catalog_id, week) DO UPDATE
                    SET price = EXCLUDED.price,
                        price_source = EXCLUDED.price_source,
                        priced_at = EXCLUDED.priced_at,
                        game = EXCLUDED.game,
                        language = EXCLUDED.language
                    WHERE catalog_price_weekly.priced_at IS NULL
                       OR EXCLUDED.priced_at > catalog_price_weekly.priced_at
                """,
                wk, now, [g for g in games if g != "default"],
                [CATALOG_FRESH_DAYS[g] for g in games if g != "default"],
                CATALOG_FRESH_DAYS["default"])
            n = _count(tag)
            await conn.execute("DELETE FROM catalog_price_weekly WHERE week < $1::date",
                               wk - timedelta(weeks=HISTORY_WEEKS))
        if n:
            logger.info("Weekly report: captured %d catalog price(s) for week %s", n, wk)
        return n
    except Exception:
        logger.exception("Weekly report: catalog capture failed")
        return 0


def _f(v):
    return None if v is None else float(v)


async def _tracked_candidates(conn, now: datetime) -> list:
    """Classified tracked cards: [(game, kind-specific row dict, result)]."""
    cards = await conn.fetch(
        "SELECT id, name, game, language, set_name, card_number, variant, release_date "
        "FROM tracked_cards")
    if not cards:
        return [], set()
    snaps = await conn.fetch(
        "SELECT card_id, captured_at, price_low, price_mid, price_high, source "
        "FROM price_snapshots WHERE captured_at >= $1 ORDER BY card_id, captured_at",
        now - timedelta(days=SNAPSHOT_LOOKBACK_DAYS))
    by_card: dict = {}
    for s in snaps:
        by_card.setdefault(s["card_id"], []).append(dict(s))
    scores = {r["card_id"]: r for r in await conn.fetch(
        "SELECT DISTINCT ON (card_id) card_id, momentum_30d FROM card_scores "
        "ORDER BY card_id, computed_at DESC")}
    # tracked -> catalog row (identity for image/version, and so catalog skips it)
    links = {}
    for r in await conn.fetch(
            """
            SELECT DISTINCT ON (t.id) t.id AS tid, c.id AS cid, c.image_url, c.version
            FROM tracked_cards t
            JOIN catalog_cards c
              ON c.game = t.game
             AND ((t.game = 'pokemon' AND t.tcgplayer_id IS NOT NULL AND t.tcgplayer_id <> ''
                   AND c.tcgplayer_id = t.tcgplayer_id AND c.language = t.language)
               OR (t.game <> 'pokemon' AND t.source_id IS NOT NULL AND c.source_id = t.source_id))
            ORDER BY t.id, c.id
            """):
        links[r["tid"]] = r
    out = []
    for c in cards:
        snaps_c = by_card.get(c["id"])
        if not snaps_c:
            continue
        series = tracked_series(snaps_c)
        res = classify_tracked(series, scores.get(c["id"]), c["release_date"], now)
        if not res["flags"]:
            continue
        link = links.get(c["id"])
        out.append({
            "game": c["game"], "card_kind": "tracked", "tracked_card_id": c["id"],
            "catalog_id": link["cid"] if link else None, "name": c["name"],
            "set_name": c["set_name"], "card_number": c["card_number"],
            "version": (link["version"] if link else None) or c["variant"] or "",
            "language": c["language"], "image_url": link["image_url"] if link else None,
            "res": res})
    tracked_catalog_ids = {l["cid"] for l in links.values()}
    return out, tracked_catalog_ids


async def _catalog_candidates(conn, now: datetime, skip_ids: set) -> list:
    cur = week_start(now)
    out = []
    for g in [r["game"] for r in await conn.fetch(
            "SELECT DISTINCT game FROM catalog_price_weekly WHERE week = $1", cur)]:
        rows = await conn.fetch(
            """
            SELECT w.catalog_id, w.week, w.price, w.price_source
            FROM catalog_price_weekly w
            WHERE w.game = $1 AND w.week >= $2
              AND w.catalog_id IN (SELECT catalog_id FROM catalog_price_weekly
                                   WHERE game = $1 AND week = $3 AND price >= $4)
            ORDER BY w.catalog_id, w.week
            """,
            g, cur - timedelta(days=USUAL_WINDOW_DAYS + 14), cur, MIN_PRICE)
        grouped: dict = {}
        for r in rows:
            if r["catalog_id"] in skip_ids:
                continue
            grouped.setdefault(r["catalog_id"], []).append(
                (r["week"], float(r["price"]), r["price_source"]))
        classified = {}
        for cid, pts in grouped.items():
            res = classify_catalog(pts, now)
            if res["flags"]:
                classified[cid] = res
        if not classified:
            continue
        info = {r["id"]: r for r in await conn.fetch(
            "SELECT id, language, set_name, card_name, card_number, version, image_url "
            "FROM catalog_cards WHERE id = ANY($1::int[])", list(classified))}
        for cid, res in classified.items():
            c = info.get(cid)
            if c is None:
                continue
            out.append({
                "game": g, "card_kind": "catalog", "tracked_card_id": None,
                "catalog_id": cid, "name": c["card_name"], "set_name": c["set_name"],
                "card_number": c["card_number"], "version": c["version"] or "",
                "language": c["language"], "image_url": c["image_url"], "res": res})
    return out


def _build_rows(candidates: list, week: date) -> list:
    """Top LIST_SIZE per (game, list) -> flat insert tuples."""
    buckets: dict = {}
    for cand in candidates:
        for key in cand["res"]["flags"]:
            buckets.setdefault((cand["game"], key), []).append(cand)
    rows = []
    for (game, key), items in buckets.items():
        items.sort(key=lambda c: (tuple(-x for x in c["res"]["sort_keys"][key]),
                                  (c["name"] or "").lower(), c["card_kind"],
                                  c["tracked_card_id"] or 0, c["catalog_id"] or 0))
        for rank, c in enumerate(items[:LIST_SIZE], start=1):
            res = c["res"]
            ch = res["change"]
            if key == "below_usual":
                # price "then" is its usual price; the percent is the gap to it
                pn, pt = res.get("price_now"), res.get("usual")
                pct = (pn - pt) / pt if pn is not None and pt else None
                dabs = pn - pt if pn is not None and pt is not None else None
            else:
                pn, pt, pct, dabs = ch if ch else (res.get("price_now"), None, None, None)
            rows.append((week, game, key, rank, c["card_kind"], c["tracked_card_id"],
                         c["catalog_id"], c["name"], c["set_name"], c["card_number"],
                         c["version"], c["language"], c["image_url"],
                         pn, pt, None if pct is None else round(pct * 100, 2),
                         None if dabs is None else round(dabs, 2),
                         res["flags"][key]))
    return rows


async def generate_report(pool, week=None, now=None) -> dict:
    """Build every list for every game and replace that week's rows.

    Returns {"week": iso, "rows": n, "by_game": {game: n}}; on failure the
    same shape with rows 0 and an "error" note, and the existing rows are left
    alone."""
    now = _utc(now) if now else _now_utc()
    wk = week or week_start(now)
    # The report for week W describes the week that just ENDED (the 7 days
    # before W's Monday), measured as of Sunday 23:59:59 UTC. Measuring "as of
    # now" on a Monday would compare the catalog's barely-started current week
    # (a few hours of prices; One Piece often not yet repriced) against last
    # week, so most catalog cards would show no move at all. Week W-1's catalog
    # row is final once W begins, so rebuilding later in the week gives the
    # same answer.
    as_of = datetime(wk.year, wk.month, wk.day, tzinfo=timezone.utc) - timedelta(microseconds=1)
    try:
        async with pool.acquire() as conn:
            tracked, tracked_cat_ids = await _tracked_candidates(conn, as_of)
            catalog = await _catalog_candidates(conn, as_of, tracked_cat_ids)
        rows = _build_rows(tracked + catalog, wk)
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM weekly_reports WHERE week = $1", wk)
                if rows:
                    await conn.executemany(
                        "INSERT INTO weekly_reports (week, game, list, rank, card_kind, "
                        "tracked_card_id, catalog_id, name, set_name, card_number, version, "
                        "language, image_url, price_now, price_then, change_pct, change_abs, "
                        "reason) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,"
                        "$16,$17,$18)", rows)
        by_game: dict = {}
        for r in rows:
            by_game[r[1]] = by_game.get(r[1], 0) + 1
        logger.info("Weekly report %s: %d row(s) %s", wk, len(rows), by_game)
        return {"week": wk.isoformat(), "rows": len(rows), "by_game": by_game}
    except Exception as exc:
        logger.exception("Weekly report: generation failed")
        return {"week": wk.isoformat(), "rows": 0, "by_game": {}, "error": type(exc).__name__}


async def latest_week(pool):
    try:
        async with pool.acquire() as conn:
            return await conn.fetchval("SELECT MAX(week) FROM weekly_reports")
    except Exception:
        logger.exception("Weekly report: latest_week failed")
        return None


async def report_weeks(pool, limit: int = 12) -> list:
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT DISTINCT week FROM weekly_reports ORDER BY week DESC LIMIT $1",
                int(limit))
        return [r["week"] for r in rows]
    except Exception:
        logger.exception("Weekly report: report_weeks failed")
        return []


def _jsonable(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


async def read_report(pool, week, game=None) -> list:
    """Stored rows for a week (a date), ordered by game, list order, rank.
    Values are JSON-safe: dates as ISO strings, numbers as floats."""
    try:
        if isinstance(week, str):
            week = date.fromisoformat(week)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM weekly_reports WHERE week = $1 AND ($2::text IS NULL OR game = $2) "
                "ORDER BY game, COALESCE(array_position($3::text[], list), 99), rank",
                week, game, list(LIST_KEYS))
        return [{k: _jsonable(v) for k, v in dict(r).items()} for r in rows]
    except Exception:
        logger.exception("Weekly report: read_report failed")
        return []


async def report_due(pool, now=None) -> bool:
    """True from Monday 12:00 UTC on, while this week has no report yet."""
    try:
        now = _utc(now) if now else _now_utc()
        if now.weekday() == 0 and now.hour < 12:
            return False
        async with pool.acquire() as conn:
            have = await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM weekly_reports WHERE week = $1)",
                week_start(now))
        return not have
    except Exception:
        logger.exception("Weekly report: report_due failed")
        return False
