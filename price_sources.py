"""Graded-price sources for the grading calculator (admin trial).

Vendor-agnostic: every source maps its own grade vocabulary into GRADE_KEYS
and tags each quote with a `basis` — 'sold' (a real completed-sale figure) or
'ask' (what someone is currently asking). The page renders that badge, because
an unsold $900 listing is not a comp and the UI must never imply it is.

Sources, in priority order:
  1. manual              admin override, supplied by the caller (never fetched here)
  2. pokemonpricetracker sold-derived, per grade, POKEMON ONLY — see below
  3. ebay_browse         ask-side only — live listings, also gives supply depth;
                         the only source for One Piece

PriceCharting was removed as a source: it never returned graded prices for
this account (their API returns only the columns in the subscribed price
guide), and PPT covers the same ground with real graded sales data.
"""

from __future__ import annotations

import os
import re
import time
import asyncio
import base64
import logging
from collections import deque
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from urllib.parse import urlsplit

import httpx

logger = logging.getLogger("dashboard.price_sources")

# Canonical grade vocabulary. grading_roi.GRADE_ORDER must stay in sync.
# cgc_9/tag_10/tag_9 exist so each grading company's own top-two grades (see
# grading_tiers.GRADING_COMPANIES["report_grades"]) are representable — PPT's
# salesByGrade already returns cgc9/tag10/tag9, they just weren't read before.
# psa_1..psa_7 exist for the Market Prices "lowest grade shown" slider — PSA
# is whole-number only (no half grades), and PPT's salesByGrade carries these
# when a card has any sales at that grade (sparse below ~7 for most cards,
# which is fine: no data just means no price for that row, same as any grade).
GRADE_KEYS = ("raw", "psa_1", "psa_2", "psa_3", "psa_4", "psa_5", "psa_6", "psa_7",
              "psa_8", "psa_9", "psa_10", "bgs_9_5", "bgs_10",
              "cgc_9", "cgc_10", "sgc_10", "tag_9", "tag_10")

GRADE_LABELS = {
    "raw": "Raw",
    "psa_1": "PSA 1", "psa_2": "PSA 2", "psa_3": "PSA 3", "psa_4": "PSA 4",
    "psa_5": "PSA 5", "psa_6": "PSA 6", "psa_7": "PSA 7",
    "psa_8": "PSA 8", "psa_9": "PSA 9", "psa_10": "PSA 10",
    "bgs_9_5": "BGS 9.5", "bgs_10": "BGS 10", "cgc_9": "CGC 9", "cgc_10": "CGC 10",
    "sgc_10": "SGC 10", "tag_9": "TAG 9", "tag_10": "TAG 10",
}

# Numeric rank per grade, used only to filter the Market Prices table by the
# "lowest grade shown" slider (0 = always shown, i.e. raw). A row is shown
# when its level >= the slider's minimum. Grades sharing a number (psa_9,
# cgc_9, tag_9) share a level so the slider treats every company's "9" the
# same way; BGS's 9.5 sits between 9 and 10 as its own level.
GRADE_LEVEL = {
    "raw": 0,
    "psa_1": 1, "psa_2": 2, "psa_3": 3, "psa_4": 4, "psa_5": 5, "psa_6": 6, "psa_7": 7,
    "psa_8": 8, "psa_9": 9, "psa_10": 10,
    "bgs_9_5": 9.5, "bgs_10": 10,
    "cgc_9": 9, "cgc_10": 10,
    "sgc_10": 10,
    "tag_9": 9, "tag_10": 10,
}


@dataclass(frozen=True)
class CardRef:
    game: str                    # 'pokemon' | 'one_piece'
    name: str
    set_name: str = ""
    card_number: str = ""
    variant: str | None = None
    # PokemonPriceTracker's own id, when the card came from their catalog. Pins
    # the exact printing, so no name/set matching is needed at all.
    tcgplayer_id: str | None = None
    language: str = "english"    # PPT's `language` param: english | japanese

    def query(self) -> str:
        """Free-text search string shared by both vendors."""
        parts = [self.name, self.set_name, self.card_number]
        return " ".join(p.strip() for p in parts if p and p.strip())

    def key(self) -> str:
        """Stable identity for caching/snapshot rows. Language is part of it —
        the Japanese and English printings of a card are different products at
        different prices, so they must never share a cache entry."""
        norm = lambda s: re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")
        return "|".join((norm(self.game), norm(self.name),
                         norm(self.set_name), norm(self.card_number),
                         norm(self.language)))


@dataclass
class GradedQuote:
    grade: str
    price: float
    basis: str                   # 'sold' | 'ask' | 'manual'
    source: str
    as_of: str                   # ISO8601
    sample_size: int | None = None
    low: float | None = None     # lowest ask, when basis == 'ask'
    note: str | None = None
    recent_avg: float | None = None    # average of the most recent N sales
    recent_n: int | None = None        # how many sales that average covers
    recent_since: str | None = None    # date of the oldest sale in it

    def to_dict(self) -> dict:
        return asdict(self)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ───────────────────────── PokemonPriceTracker ─────────────────────────
# POKEMON ONLY — no One Piece coverage, so One Piece falls through to eBay
# only. Documented shapes (v2 API reference):
#   GET /api/v2/cards?search=&set=&limit=&includeEbay=true
#   -> {"data": [{tcgPlayerId, name, setName, cardNumber, rarity,
#                 prices: {market, low},
#                 ebay: {psa8: {avg}, psa9: {avg}, psa10: {avg}}}]}
# Values are DOLLARS here (unlike PriceCharting's pennies, back when that was a source).
#
# BILLED IN CREDITS, PER CARD RETURNED: 1 credit per card + 1 more per card
# for the eBay/PSA block. A careless `limit` multiplies the cost of every
# lookup, so the search is kept deliberately narrow and results are cached.
PPT_BASE = os.getenv("POKEMONPRICETRACKER_API_BASE",
                     "https://www.pokemonpricetracker.com/api/v2")
# Small on purpose: each returned card costs credits. Enough to pick the right
# printing from a name collision, not enough to bill for a whole set.
PPT_SEARCH_LIMIT = 5
# Graded prices move slowly; re-billing credits for the same card within the
# window is pure waste. {card_key: (fetched_at, [quotes])}
PPT_CACHE_TTL_SECONDS = 6 * 3600
_ppt_cache: dict[str, tuple[float, list]] = {}

# The `ebay` block is PokemonPriceTracker's aggregation of eBay graded sales,
# so it is treated as sold-derived rather than asking prices. NOT confirmed
# against their support — if it turns out to include live listings, change
# this one constant and every quote is relabelled.
PPT_BASIS = "sold"

# Window for the graded-sales lookup. Their API defaults to 30 days, which is
# routinely empty for a single printing — a card can easily go a month with no
# PSA 10 sale — and an empty block looks identical to "this vendor has no
# graded data". Their own quick-start example uses days=90.
PPT_EBAY_DAYS = 90

# Canonical-key -> our grade. Keys are matched after stripping non-alphanumerics
# and lowercasing, so "psa10", "PSA 10" and "psa_10" all land on the same entry.
# PPT tracks far more grades than the calculator models (ace/tag/psa1/half
# grades); anything not listed here is ignored.
PPT_GRADE_FIELDS = {
    "psa1":  "psa_1",
    "psa2":  "psa_2",
    "psa3":  "psa_3",
    "psa4":  "psa_4",
    "psa5":  "psa_5",
    "psa6":  "psa_6",
    "psa7":  "psa_7",
    "psa8":  "psa_8",
    "psa9":  "psa_9",
    "psa10": "psa_10",
    "bgs95": "bgs_9_5",
    "bgs10": "bgs_10",
    "cgc9":  "cgc_9",
    "cgc10": "cgc_10",
    "sgc10": "sgc_10",
    "tag9":  "tag_9",
    "tag10": "tag_10",
}

# Per-grade price, best first. smartMarketPrice is PPT's own filtered+weighted
# estimate and carries a confidence rating, so it beats the alternatives:
# averagePrice spans the WHOLE sales history (dateRangeStart is ~a year back),
# which badly lags a moving card — 856 PSA 10 sales averaged $1,342 while the
# card was actually trading at ~$1,199.
# How many recent sales the "last N sales" average covers.
PPT_RECENT_SALES = 5


def _ppt_recent_average(history: dict, want: int = PPT_RECENT_SALES):
    """Average of the most recent `want` sales, from PPT's daily history.

    priceHistory is keyed by date -> {average, count, totalValue}. Individual
    sale prices aren't exposed, so a day is consumed whole where possible and
    the day's own average is used for a partial day. That makes this an
    approximation: exact when each day's sales fit evenly, and within a day's
    spread otherwise.

    Returns (average, sales_counted, oldest_date_used) or (None, 0, None).
    """
    if not isinstance(history, dict) or not history:
        return None, 0, None
    total_value = 0.0
    counted = 0
    oldest = None
    for date in sorted(history, reverse=True):        # ISO dates sort correctly
        day = history.get(date)
        if not isinstance(day, dict):
            continue
        try:
            day_count = int(day.get("count") or 0)
            day_avg = float(day.get("average") or 0)
        except (TypeError, ValueError):
            continue
        if day_count <= 0 or day_avg <= 0:
            continue
        take = min(day_count, want - counted)
        total_value += day_avg * take
        counted += take
        oldest = date
        if counted >= want:
            break
    if not counted:
        return None, 0, None
    return round(total_value / counted, 2), counted, oldest


def _ppt_grade_price(entry: dict):
    smart = entry.get("smartMarketPrice")
    if isinstance(smart, dict):
        price = smart.get("price")
        if price:
            return price, smart.get("confidence"), smart.get("daysUsed")
    for key in ("marketPrice7Day", "marketPriceMedian7Day", "medianPrice", "averagePrice"):
        if entry.get(key):
            return entry[key], None, None
    return None, None, None


def _ppt_empty_status(resp) -> str:
    """Why an empty row list came back:
    'rate_limited' | 'forbidden' | 'error' | 'empty'.

    Four genuinely different outcomes that all arrive as an empty list, and
    callers act differently on each:
      rate_limited — 429. Temporary; back off and resume where we stopped.
      forbidden    — 403. The account's plan doesn't include this endpoint
                     (e.g. /population is Business-only) — permanent until
                     the plan changes, so a caller should stop retrying
                     rather than re-checking on every request.
      error        — no response at all, or another non-200 status.
      empty        — a clean 200 with no rows. PPT simply has nothing for this
                     query, and it cost no per-card credits, so retrying later
                     is nearly free.
    """
    if resp is None:
        return "error"
    code = getattr(resp, "status_code", None)
    if code == 429:
        return "rate_limited"
    if code == 403:
        return "forbidden"
    if code != 200:
        return "error"
    return "empty"


def _ppt_raw_price(row: dict):
    """Ungraded market price off a PPT card row, or None.

    `prices.market` is PPT's raw/ungraded figure, with `prices.low` as the
    fallback — the same pair `fetch_pokemonpricetracker` reads for its "raw"
    quote. Pulled out as a helper because EVERY /cards row carries this block,
    including the whole-set responses, so the catalog gets raw prices out of
    calls it was already paying for. (fetch_pokemonpricetracker still has this
    inline; left alone deliberately to keep this change additive.)
    """
    prices = (row or {}).get("prices") or {}
    for key in ("market", "low"):
        try:
            value = float(prices.get(key))
        except (TypeError, ValueError):
            continue
        if value > 0:
            return round(value, 2)
    return None


# Card image, preferred size first. 400x400 is the grid target: crisp on a
# retina tile without paying 800x800's weight on every card. The others are
# fallbacks — PPT documents all four, but a given row may not carry every one.
_PPT_IMAGE_FIELDS = ("imageCdnUrl400", "imageCdnUrl200", "imageCdnUrl",
                     "imageCdnUrl800", "imageUrl")

# Whatever host PPT points at has to be allow-listed in the CSP's img-src
# (main.py). Kept here next to the parser so the two can't drift apart
# silently: if PPT ever moves to a different CDN, this list is the thing that
# has to change alongside the policy.
PPT_IMAGE_HOSTS = ("tcgplayer-cdn.tcgplayer.com",)


def _ppt_image_url(row: dict):
    """Card image URL off a PPT row, or None.

    Only https URLs on a known host are accepted. PPT is a paid, trusted
    source, but this value is written to the DB and then rendered as an <img
    src> for every visitor — validating it here means a surprise value in the
    feed can't put an arbitrary origin (or a javascript:/data: URL) into the
    page. Anything unrecognised is dropped and the card simply shows no image.
    """
    for key in _PPT_IMAGE_FIELDS:
        value = (row or {}).get(key)
        if not value or not isinstance(value, str):
            continue
        value = value.strip()
        try:
            parts = urlsplit(value)
        except ValueError:
            continue
        # Parsed rather than string-sliced: urlsplit resolves userinfo and
        # port correctly, so "https://tcgplayer-cdn...@evil.com/x" reads as
        # host=evil.com and is refused. A hand-rolled split can get that
        # backwards. Credentials are rejected outright — a real PPT URL never
        # carries them, so their presence means something is off.
        if parts.scheme != "https" or parts.username or parts.password:
            continue
        if (parts.hostname or "").lower() in PPT_IMAGE_HOSTS:
            return value[:500]
    return None


def pokemonpricetracker_available() -> bool:
    return bool(os.getenv("POKEMONPRICETRACKER_API_KEY"))


# ── PPT as the CATALOG, not just a price source ──────────────────────────
# Driving the set and card pickers from PPT means the names in the dropdown are
# PPT's own, so a price lookup can pass tcgPlayerId and skip name/set matching
# entirely — that whole class of mismatch bug disappears.
#
# Sets and card lists barely change, and a card list bills per card returned,
# so both are cached for a day. {key: (fetched_at, payload)}
PPT_CATALOG_TTL_SECONDS = 24 * 3600
_ppt_sets_cache: dict[str, tuple[float, list]] = {}
_ppt_set_cards_cache: dict[str, tuple[float, list]] = {}

# PPT's `language` values. Allowlisted rather than passed through, so a bad
# value can't reach the vendor as an arbitrary query string.
PPT_LANGUAGES = ("english", "japanese")
PPT_DEFAULT_LANGUAGE = "english"

# /cards hard-caps a response at 200 rows (documented max for `limit`); a
# set with more printings than that needs multiple pages via `offset`.
PPT_CARDS_PAGE_SIZE = 200
# Safety bound on the pagination loop, mirroring set_import.py's page cap —
# no real Pokemon set is anywhere near this size; it just stops a runaway
# loop if PPT's `hasMore` ever misreports.
PPT_MAX_SET_CARDS = 2000


def ppt_language(value: str | None) -> str:
    """Normalise to a supported language, defaulting to English."""
    candidate = (value or "").strip().lower()
    return candidate if candidate in PPT_LANGUAGES else PPT_DEFAULT_LANGUAGE

_ppt_logged_set_shape = False
_ppt_logged_card_shape = False


def _first(payload: dict, *keys):
    """First present, non-empty value among `keys`. Response field names are
    unverified for these endpoints, so read a few plausible spellings."""
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return None


# PPT's hard per-minute cap (Free and API tiers are both 60/min; Business is
# 500, Enterprise 1000 — override via env if the account is on one of those).
# Enforced GLOBALLY, in-process, across every call this app makes: background
# sweeps (nightly price sweep, tracker ingest), live member/guest requests,
# and admin actions all draw on the SAME account limit, and slowapi's per-IP
# limits do nothing to stop the sum of concurrent traffic from blowing past
# it. This app is a single long-lived process (see the tracker's own
# scheduler comment), so an in-process gate is the whole limit, not a
# per-instance slice of it — no Redis or other shared store needed.
PPT_RATE_LIMIT_PER_MINUTE = int(os.getenv("PPT_RATE_LIMIT_PER_MINUTE", "60"))
PPT_RATE_WINDOW_S = 60.0

_ppt_rate_lock = asyncio.Lock()
# Monotonic timestamps of calls made in roughly the current window. Bounded
# by PPT_RATE_LIMIT_PER_MINUTE itself — it never holds more entries than the
# cap allows before the oldest ones age out.
_ppt_call_times: deque = deque()


async def _ppt_rate_limit() -> None:
    """Block until issuing another PPT request keeps the last
    PPT_RATE_WINDOW_S seconds at or under PPT_RATE_LIMIT_PER_MINUTE calls.

    Sliding window, not a fixed per-minute bucket — 60 calls at :59 and 60
    more at :01 would both be "within their own minute" under a fixed bucket
    but is 120 calls in 2 seconds, which is exactly the burst PPT's limit
    exists to prevent. A rolling window has no such seam.

    The wait happens OUTSIDE the lock so other waiting callers can still
    prune the deque and re-check while this one sleeps — otherwise every
    caller after the first would serialize behind the sleeping one instead
    of genuinely sharing the window.
    """
    while True:
        async with _ppt_rate_lock:
            now = time.monotonic()
            while _ppt_call_times and now - _ppt_call_times[0] >= PPT_RATE_WINDOW_S:
                _ppt_call_times.popleft()
            if len(_ppt_call_times) < PPT_RATE_LIMIT_PER_MINUTE:
                _ppt_call_times.append(now)
                return
            # Small fudge so the retry lands just after the oldest call
            # actually ages out, not exactly on the boundary.
            wait = PPT_RATE_WINDOW_S - (now - _ppt_call_times[0]) + 0.05
        await asyncio.sleep(max(wait, 0.05))


async def _ppt_get(client: httpx.AsyncClient, path: str, params: dict,
                   label: str) -> tuple[list, object]:
    """GET a PPT endpoint and return (rows, response).

    `response` is None ONLY when no HTTP response was obtained (no API key, or
    a transport failure). A non-200 still hands the response back, so callers
    can tell a 429 (temporary — back off and resume) from a 4xx/5xx (broken).
    Callers that only check `rows` are unaffected.
    """
    key = os.getenv("POKEMONPRICETRACKER_API_KEY")
    if not key:
        return [], None
    await _ppt_rate_limit()
    try:
        resp = await client.get(f"{PPT_BASE}{path}",
                                headers={"Authorization": f"Bearer {key}"},
                                params=params)
    except Exception:
        logger.exception("PokemonPriceTracker %s request failed", label)
        return [], None
    if resp.status_code == 429:
        # Log the budget headers: they're what distinguishes a short per-minute
        # throttle (wait and resume) from the daily cap being spent (nothing to
        # do until it resets).
        logger.warning("PokemonPriceTracker %s rate limited (429) | retry-after=%s "
                       "daily-remaining=%s consumed=%s | %s", label,
                       resp.headers.get("Retry-After", "?"),
                       resp.headers.get("X-RateLimit-Daily-Remaining", "?"),
                       resp.headers.get("X-API-Calls-Consumed", "?"),
                       resp.text[:200])
        return [], resp
    if resp.status_code != 200:
        logger.warning("PokemonPriceTracker %s HTTP %s: %s",
                       label, resp.status_code, resp.text[:300])
        return [], resp
    try:
        payload = resp.json()
    except Exception:
        logger.warning("PokemonPriceTracker %s returned non-JSON", label)
        return [], None
    data = (payload or {}).get("data")
    if isinstance(data, dict):
        data = [data]
    elif not isinstance(data, list):
        data = []
    return data, resp


def _ppt_daily_remaining(resp) -> int | None:
    """Parse `X-RateLimit-Daily-Remaining` off a PPT response, or None if
    absent/unparseable. Present on both 200 and 429 responses, so callers
    that spend a shared daily budget can track it after every call without
    a separate 'check my balance' request (PPT doesn't offer one)."""
    if resp is None:
        return None
    raw = resp.headers.get("X-RateLimit-Daily-Remaining")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


async def fetch_ppt_card_prices(client: httpx.AsyncClient, tcgplayer_id: str,
                                language: str = PPT_DEFAULT_LANGUAGE) -> tuple:
    """({low, market, high}, status, daily_remaining) — ungraded prices for
    ONE card.

    Deliberately the cheapest per-card refresh available: pinned by
    tcgPlayerId so no search is needed, and WITHOUT includeEbay, which bills a
    second credit per card for graded data the tracker doesn't store. One card
    in, one credit out.

    `daily_remaining` is PPT's own count of credits left today (None if the
    header was missing) — the only way to know it, since PPT has no separate
    balance-check endpoint. Lets a caller spending down a shared daily budget
    (the catalog price sweep) stop before it runs the account dry, without
    guessing from a locally-tracked count that daytime traffic could throw off.

    Separate from fetch_pokemonpricetracker, which builds full graded quotes
    for the calculator and costs considerably more per card.
    """
    tcg_id = str(tcgplayer_id or "").strip()
    if not tcg_id:
        return {}, "error", None
    rows, resp = await _ppt_get(client, "/cards",
                                {"tcgPlayerId": tcg_id, "limit": 1,
                                 "language": ppt_language(language)},
                                f"card-prices({tcg_id})")
    daily_remaining = _ppt_daily_remaining(resp)
    if not rows:
        return {}, _ppt_empty_status(resp), daily_remaining

    prices = (rows[0] or {}).get("prices") or {}

    def _num(key):
        try:
            value = float(prices.get(key))
        except (TypeError, ValueError):
            return None
        return round(value, 2) if value > 0 else None

    out = {"low": _num("low"), "market": _num("market"), "high": _num("high")}
    if out["market"] is None and out["low"] is None and out["high"] is None:
        # A 200 with a row but no usable numbers — the call happened and was
        # billed, so this is 'empty', not an error.
        return out, "empty", daily_remaining
    return out, "ok", daily_remaining


# Condition PPT's raw/ungraded prices key off of. Matches what this app already
# tracks as "market" via fetch_ppt_card_prices (TCGPlayer's Near Mint price).
PPT_HISTORY_CONDITION = "Near Mint"


async def fetch_ppt_price_history(client: httpx.AsyncClient, tcgplayer_id: str,
                                  days: int, language: str = PPT_DEFAULT_LANGUAGE) -> tuple:
    """([{date, market}], status, daily_remaining) — daily raw-price backfill
    for ONE card, oldest first.

    Manual/on-demand only (the card-tracker "Backdate" action) — NOT part of
    the nightly ingest, which only ever asks for today's live price
    (fetch_ppt_card_prices). `includeHistory=true` bills a second credit per
    card on top of the base lookup (2 credits total at limit=1), so this is
    deliberately a separate, explicitly-triggered call rather than something
    that runs automatically for every tracked card every night.

    Reads priceHistory.conditions["Near Mint"].history — falls back to
    whatever single condition PPT actually returned if Near Mint isn't present
    (some variants only carry one condition).
    """
    tcg_id = str(tcgplayer_id or "").strip()
    if not tcg_id:
        return [], "error", None
    rows, resp = await _ppt_get(client, "/cards",
                                {"tcgPlayerId": tcg_id, "limit": 1, "includeHistory": "true",
                                 "days": int(days), "language": ppt_language(language)},
                                f"price-history({tcg_id}, days={days})")
    daily_remaining = _ppt_daily_remaining(resp)
    if not rows:
        return [], _ppt_empty_status(resp), daily_remaining

    conditions = ((rows[0] or {}).get("priceHistory") or {}).get("conditions") or {}
    entry = conditions.get(PPT_HISTORY_CONDITION)
    if entry is None and conditions:
        entry = next(iter(conditions.values()))
    points = (entry or {}).get("history") or []

    out = []
    for point in points:
        if not isinstance(point, dict):
            continue
        try:
            market = float(point.get("market"))
        except (TypeError, ValueError):
            continue
        date_str = str(point.get("date") or "")[:10]
        if market <= 0 or not date_str:
            continue
        try:
            day = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        out.append({"date": day, "market": round(market, 2)})

    if not out:
        return [], "empty", daily_remaining
    return out, "ok", daily_remaining


async def fetch_ppt_population(client: httpx.AsyncClient, tcgplayer_id: str,
                               language: str = PPT_DEFAULT_LANGUAGE) -> tuple:
    """(population_dict, status) — GemRate grading-population data for ONE
    card, pinned by tcgPlayerId. 2 credits/card, billed regardless of how
    much population data comes back.

    Business-plan only — a 403 from PPT means the account's plan doesn't
    include this endpoint at all (documented on this endpoint specifically,
    not just a generic rejection), so it's surfaced as its own 'forbidden'
    status via _ppt_empty_status rather than folded into 'error'. Callers
    should treat 'forbidden' as a standing condition — stop asking for a
    while — rather than retrying the next card.

    Returns the raw `data` object from PPT (population-by-grader breakdown,
    combined totals, gem rates, match confidence) unmodified — the shape has
    open-ended per-grader keys (BGS carries g9_5/pristine/perfect that PSA
    doesn't), so this is stored and served as-is rather than forced into
    fixed columns.
    """
    tcg_id = str(tcgplayer_id or "").strip()
    if not tcg_id:
        return None, "error"
    rows, resp = await _ppt_get(client, "/population",
                                {"tcgPlayerId": tcg_id, "language": ppt_language(language)},
                                f"population({tcg_id})")
    if not rows:
        return None, _ppt_empty_status(resp)
    return rows[0], "ok"


async def resolve_ppt_tcgplayer_id(client: httpx.AsyncClient, name: str,
                                   set_name: str = "", card_number: str = "",
                                   language: str = PPT_DEFAULT_LANGUAGE) -> tuple:
    """(tcgplayer_id, status) — pin a card's TCGplayer id by search.

    Costs up to PPT_SEARCH_LIMIT credits, ONCE per card ever: the id is stored
    afterwards and every later price refresh is a pinned 1-credit lookup. This
    is the fallback for cards whose set isn't in catalog_cards, so a card is
    never left permanently unpriced waiting on a set to be stocked.

    Reuses _ppt_pick_card, the same matcher the grading calculator relies on,
    rather than a second bespoke one.
    """
    search = _ppt_search_name(name)
    if not search:
        return None, "error"
    language = ppt_language(language)
    card = CardRef(game="pokemon", name=name, set_name=(set_name or "").strip(),
                   card_number=(card_number or "").strip(), language=language)

    base = {"search": search, "limit": PPT_SEARCH_LIMIT, "language": language}
    attempts = []
    if card.set_name:
        attempts.append({**base, "set": card.set_name})
    # An empty result bills no per-card credits, so retrying without the set
    # filter is cheap — and PPT's set filter is case/format sensitive.
    attempts.append(base)

    for params in attempts:
        rows, resp = await _ppt_get(client, "/cards", params, f"resolve({search})")
        if not rows:
            status = _ppt_empty_status(resp)
            if status in ("rate_limited", "error"):
                return None, status
            continue
        row = _ppt_pick_card(rows, card)
        if not row:
            continue
        tcg_id = row.get("tcgPlayerId") or row.get("tcgplayerId")
        if tcg_id:
            logger.info("PPT resolve: %r -> %r %s (%s) tcgPlayerId=%s", name,
                        row.get("name"), row.get("cardNumber"), row.get("setName"),
                        tcg_id)
            return str(tcg_id), "ok"
    return None, "empty"


async def fetch_ppt_sets(client: httpx.AsyncClient,
                         language: str = PPT_DEFAULT_LANGUAGE) -> list:
    """[{id, name, year}] of every Pokemon set PPT knows, newest first.

    Cheap (one call) and cached for a day, per language. Names are PPT's own,
    which is the point: the set string in the dropdown is exactly what its
    price lookups expect.
    """
    global _ppt_logged_set_shape
    language = ppt_language(language)
    cache_key = f"pokemon:{language}"
    hit = _ppt_sets_cache.get(cache_key)
    if hit and (time.time() - hit[0]) < PPT_CATALOG_TTL_SECONDS:
        return list(hit[1])

    rows, resp = await _ppt_get(client, "/sets",
                                {"limit": 500, "language": language}, "sets")
    if not rows:
        return []
    if not _ppt_logged_set_shape:
        _ppt_logged_set_shape = True
        logger.info("PokemonPriceTracker /sets: %d row(s), first keys: %s",
                    len(rows), sorted(rows[0].keys()) if isinstance(rows[0], dict) else "?")
        if resp is not None:
            logger.info("PokemonPriceTracker /sets credits used=%s daily remaining=%s",
                        resp.headers.get("X-API-Calls-Consumed", "?"),
                        resp.headers.get("X-RateLimit-Daily-Remaining", "?"))

    sets = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = _first(row, "name", "setName", "set_name")
        if not name:
            continue
        released = str(_first(row, "releaseDate", "release_date", "releasedAt") or "")
        sets.append({
            # The name IS the id here: PPT's /cards endpoint filters by set name.
            "id": str(name),
            "name": str(name),
            "year": released[:4] if released[:4].isdigit() else "",
            "released": released[:10],
        })
    # Newest first where dates exist; undated sets sort last.
    sets.sort(key=lambda s: s["released"] or "0000", reverse=True)
    _ppt_sets_cache[cache_key] = (time.time(), list(sets))
    logger.info("PokemonPriceTracker: %d %s set(s) cached", len(sets), language)
    return sets


async def fetch_ppt_set_cards_detailed(client: httpx.AsyncClient, set_name: str,
                                       language: str = PPT_DEFAULT_LANGUAGE) -> tuple:
    """(cards, status) for a PPT set. status is 'ok' | 'empty' | 'error'.

    Same work as fetch_ppt_set_cards, but it keeps the reason an empty list
    came back. `_ppt_get` already knows the difference — it hands back a
    response object for a successful call that simply had no rows, and None
    when the request itself failed — and callers that batch over many sets
    need it: a set PPT has no data for yet (brand-new releases, routinely)
    looks nothing like the API being down, and treating them the same either
    aborts a healthy run or grinds through a broken one.
    """
    global _ppt_logged_card_shape
    language = ppt_language(language)
    cache_key = f"{language}:{_ppt_canon(set_name)}"
    hit = _ppt_set_cards_cache.get(cache_key)
    if hit and (time.time() - hit[0]) < PPT_CATALOG_TTL_SECONDS:
        logger.info("PokemonPriceTracker set-cards cache hit for %r [%s] (%d card(s), "
                    "no credits spent)", set_name, language, len(hit[1]))
        return list(hit[1]), "ok"

    # PPT's /cards hard-caps a single response at 200 cards regardless of the
    # `limit` requested (`fetchAllInSet=true` only raises the ceiling to that
    # same 200 — it does not mean "every card"). Sets with more than 200
    # printings — routine once alt-arts/promos are counted — need real
    # pagination via `offset`, walking pages while the response's
    # `metadata.hasMore` says there's another one.
    rows, resp = [], None
    offset = 0
    truncated_by_rate_limit = False
    while True:
        page_rows, page_resp = await _ppt_get(
            client, "/cards",
            {"set": set_name, "fetchAllInSet": "true", "limit": PPT_CARDS_PAGE_SIZE,
             "offset": offset, "language": language},
            f"set-cards({set_name}/{language}, offset={offset})")
        if page_resp is not None:
            resp = page_resp
        if not page_rows:
            # A 429 after at least one good page is a truncated fetch, not a
            # clean end-of-set — flagged below so the partial result isn't
            # cached as if it were the whole set.
            if offset > 0 and page_resp is not None and page_resp.status_code == 429:
                truncated_by_rate_limit = True
                logger.warning("PokemonPriceTracker set-cards %r [%s]: rate limited after "
                               "%d card(s) — returning what was fetched, not caching it as "
                               "the complete set", set_name, language, offset)
            break
        rows.extend(page_rows)
        has_more = False
        if page_resp is not None:
            try:
                has_more = bool((page_resp.json() or {}).get("metadata", {}).get("hasMore"))
            except Exception:
                has_more = False
        if not has_more:
            break
        offset += len(page_rows)
        if offset >= PPT_MAX_SET_CARDS:
            logger.warning("PokemonPriceTracker set-cards %r [%s]: stopped at the %d-card "
                           "safety bound — set may have more uncached cards", set_name,
                           language, offset)
            break
    if not rows:
        status = _ppt_empty_status(resp)
        logger.info("PokemonPriceTracker set-cards %r [%s]: no cards (%s)",
                    set_name, language, status)
        return [], status
    if not _ppt_logged_card_shape:
        _ppt_logged_card_shape = True
        logger.info("PokemonPriceTracker set-cards first row keys: %s",
                    sorted(rows[0].keys()) if isinstance(rows[0], dict) else "?")
        # One-shot diagnostic for the printing-variant question (normal vs
        # holofoil vs reverse holofoil): dump the actual values of the two
        # fields that look relevant, plus a genuinely multi-printing example
        # if this set has one, since the first card alone might only ever
        # have a single printing and tell us nothing.
        logger.info("PokemonPriceTracker set-cards first row printingsAvailable=%r variants=%r",
                    rows[0].get("printingsAvailable"), rows[0].get("variants"))
        multi = next((r for r in rows if isinstance(r, dict)
                      and isinstance(r.get("printingsAvailable"), list)
                      and len(r["printingsAvailable"]) > 1), None)
        if multi is not None and multi is not rows[0]:
            logger.info("PokemonPriceTracker set-cards multi-printing example %r: "
                        "printingsAvailable=%r variants=%r",
                        multi.get("name"), multi.get("printingsAvailable"), multi.get("variants"))
    if resp is not None:
        logger.info("PokemonPriceTracker set-cards %r: %d card(s) across %d page(s), "
                    "credits used=%s, daily remaining=%s", set_name, len(rows),
                    (offset // PPT_CARDS_PAGE_SIZE) + 1,
                    resp.headers.get("X-API-Calls-Consumed", "?"),
                    resp.headers.get("X-RateLimit-Daily-Remaining", "?"))

    cards = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = _first(row, "name", "cardName")
        if not name:
            continue
        # Kept separate from the `id` fallback below: only a value that came
        # from a real TCGplayer field can safely be turned into a
        # tcgplayer.com/product/<id> link. PPT's own `id` would point at the
        # wrong product entirely.
        tcg_id = _first(row, "tcgPlayerId", "tcgplayerId")
        cards.append({
            "name": str(name),
            "card_number": str(_first(row, "cardNumber", "number", "card_number") or ""),
            "variant": str(_first(row, "rarity", "variant") or ""),
            "tcgplayer_id": str(tcg_id or _first(row, "id") or ""),
            # True only when tcgplayer_id is genuinely a TCGplayer product id.
            # The existing `id` fallback is preserved for the price lookup that
            # already relies on it, but it must never become a buy link.
            "tcgplayer_id_verified": bool(tcg_id),
            "set_name": str(_first(row, "setName", "set_name") or set_name),
            # Already in this payload — the call is billed per card either way,
            # so carrying it costs nothing and lets the catalog cache raw
            # prices for a whole set for free. The grading calculator's picker
            # ignores this key.
            "raw_price": _ppt_raw_price(row),
            # Same deal: PPT documents imageCdnUrl* on every /cards row, so the
            # image arrives with a response we're already paying for. We STORE
            # the URL PPT hands us rather than constructing one from
            # tcgplayer_id — if the CDN's path scheme ever changes, PPT's field
            # changes with it and a re-stock heals every card, instead of every
            # image breaking at once.
            "image_url": _ppt_image_url(row),
        })

    def _sort_key(card):
        """Numeric card numbers in numeric order; promos/RC numbers after."""
        match = re.match(r"^(\d+)", card["card_number"])
        return (0, int(match.group(1))) if match else (1, 0)

    cards.sort(key=_sort_key)
    if cards and not truncated_by_rate_limit:
        _ppt_set_cards_cache[cache_key] = (time.time(), list(cards))
    # Rows came back but none survived normalisation (every row missing a
    # name): the call worked, there's just nothing usable — 'empty', not
    # 'error'.
    return cards, ("ok" if cards else "empty")


async def fetch_ppt_set_total(client: httpx.AsyncClient, set_name: str,
                              language: str = PPT_DEFAULT_LANGUAGE):
    """The true card count PPT reports for a set, or None if it couldn't be
    determined. Costs 1 credit — `limit=1` bills as a single-card lookup per
    PPT's docs, not the requested-limit rate — so a caller can cheaply check
    whether a cached set is already complete before paying for a full
    re-fetch (~1 credit/card) that would turn out to find nothing new.
    """
    language = ppt_language(language)
    _rows, resp = await _ppt_get(client, "/cards",
                                 {"set": set_name, "limit": 1, "language": language},
                                 f"set-total({set_name}/{language})")
    if resp is None or resp.status_code != 200:
        return None
    try:
        total = (resp.json() or {}).get("metadata", {}).get("total")
    except Exception:
        return None
    return int(total) if isinstance(total, (int, float)) else None


async def fetch_ppt_set_cards(client: httpx.AsyncClient, set_name: str,
                              language: str = PPT_DEFAULT_LANGUAGE) -> list:
    """Every card in a PPT set: [{name, card_number, variant, tcgplayer_id,
    set_name, raw_price}].

    BILLED PER CARD RETURNED, so this is cached for a day (per language) and
    the credit spend is logged. Carrying tcgPlayerId is what lets later price
    lookups skip the search entirely.

    Thin wrapper over fetch_ppt_set_cards_detailed for the callers that only
    care whether they got cards — the set/card pickers, which fall back to the
    baked catalog list either way.
    """
    cards, _status = await fetch_ppt_set_cards_detailed(client, set_name,
                                                        language=language)
    return cards


def _ppt_search_name(name: str) -> str:
    """Normalize a card name for PPT's text search.

    pokemontcg.io hyphenates suffixes ("Mewtwo-GX", "Charizard-EX"); every
    other catalog spaces them ("Mewtwo GX"). Sending the hyphenated form
    returns zero rows, so flatten the hyphens to spaces.
    """
    return re.sub(r"\s+", " ", (name or "").replace("-", " ")).strip()


def _ppt_canon(value: str) -> str:
    """Lowercase, with punctuation and whitespace removed.

    Uses \\W rather than [^a-z0-9] so Japanese (and any other non-Latin) card
    and set names survive canonicalisation instead of collapsing to an empty
    string — an empty name would otherwise match anything.
    """
    return re.sub(r"[\W_]+", "", (value or "").lower(), flags=re.UNICODE)


def _ppt_set_candidates(set_name: str) -> set:
    """Every name a PPT set could reasonably be known by.

    PPT uses a colon for two different things:
      "SV10: Destined Rivals"        release code : set name
      "Generations: Radiant Collection"  parent set : subset
    Both sides are therefore valid names to match against — the catalog calls
    the second one just "Generations", and its RC-numbered cards ship in that
    set. Matching stays exact against these candidates; no substring logic,
    which is what would wrongly equate "Base Set" with "Base Set 2".
    """
    raw = (set_name or "").strip()
    names = {raw}
    if ":" in raw:
        before, after = raw.split(":", 1)
        names.update({before.strip(), after.strip()})
    return {_ppt_canon(n) for n in names if n}


def _ppt_strip_name_suffix(name: str) -> str:
    """"Team Rocket's Mewtwo ex - 240/182" -> "Team Rocket's Mewtwo ex".

    PPT appends the card number to disambiguate printings; the catalogs keep
    it in a separate field, so drop it before comparing names.
    """
    return re.sub(r"\s*-\s*\d+[a-z]?(/\d+)?\s*$", "", (name or ""), flags=re.I).strip()


def _ppt_pick_card(rows: list, card: CardRef) -> dict | None:
    """Best row for the card we asked about.

    A known set is a hard filter; within it, tiers are tried in order:
    card number (numerator only, since "215/203" and "215" both occur), exact
    name, then name-as-prefix for PPT's variant suffixes. Returning the wrong
    printing would quietly price a different card, so an ambiguous result
    returns None rather than a guess.
    """
    if not rows:
        return None
    want_num = (card.card_number or "").split("/")[0].strip().lstrip("0").lower()
    want_name = _ppt_canon(_ppt_search_name(card.name))
    want_set = _ppt_canon(card.set_name)

    def by_number(row) -> bool:
        got = str(row.get("cardNumber") or "").split("/")[0].strip().lstrip("0").lower()
        return bool(want_num) and bool(got) and got == want_num

    def _row_name(row) -> str:
        return _ppt_canon(_ppt_search_name(
            _ppt_strip_name_suffix(str(row.get("name") or ""))))

    def by_name(row) -> bool:
        # want_name is empty when the name is entirely non-ASCII (Japanese
        # cards): canon() strips it to "". Without this guard "" == "" would
        # match the first row in the set, i.e. an arbitrary card.
        return bool(want_name) and _row_name(row) == want_name

    def by_name_prefix(row) -> bool:
        """"Gardevoir EX" matches "Gardevoir EX (Full Art)" but NOT
        "M Gardevoir EX (Full Art)" — a Mega is a different card. Anchoring at
        the start is what keeps the prefixed variants out."""
        return bool(want_name) and _row_name(row).startswith(want_name)

    def same_set(row) -> bool:
        """Exact match against any name the set goes by (see _ppt_set_candidates).

        Deliberately NOT substring matching: "Base Set" is contained in
        "Base Set 2", which is a different set with a different Charizard at a
        very different price. Unrecognised naming drift returns no match and
        falls through to the other vendors, which is the safe failure.
        """
        if not want_set:
            return True
        return want_set in _ppt_set_candidates(row.get("setName"))

    # A known set is a HARD filter, never a preference: card numbers repeat
    # across sets (78/73 in Shining Legends vs 78/68 in Hidden Fates), so a
    # bare number match from the wrong set is a different card at a completely
    # different price. Better to return nothing than a plausible wrong answer.
    if want_set:
        rows = [r for r in rows if same_set(r)]
        if not rows:
            return None

    for test in (by_number, by_name, by_name_prefix):
        for row in rows:
            if test(row):
                return row
    # Nothing matched on number or name. Falling back to the first row is only
    # safe when the set filter left exactly ONE candidate — with several, the
    # first is as likely to be a Mega or a different printing as the card asked
    # for, and a confident wrong price is worse than no price.
    return rows[0] if (want_set and len(rows) == 1) else None


async def fetch_pokemonpricetracker(client: httpx.AsyncClient,
                                    card: CardRef) -> list[GradedQuote]:
    """Graded (PSA 8/9/10) + market prices for one Pokemon card.

    Returns [] for non-Pokemon games, a missing key, or any failure — never
    raises, so one vendor being down can't take the calculator with it.
    """
    key = os.getenv("POKEMONPRICETRACKER_API_KEY")
    if not key or card.game != "pokemon":
        return []

    cache_key = card.key()
    hit = _ppt_cache.get(cache_key)
    if hit and (time.time() - hit[0]) < PPT_CACHE_TTL_SECONDS:
        logger.info("PokemonPriceTracker cache hit for %r (no credits spent)", cache_key)
        return list(hit[1])

    search = _ppt_search_name(card.name)
    language = ppt_language(card.language)

    row, resp = None, None
    # When the card came from PPT's own catalog (the set/card pickers) its id is
    # already known: skip the search entirely. That saves the search credits AND
    # removes every name/set matching step, which is where the mismatches were.
    tcg_id = (card.tcgplayer_id or "").strip() or None
    if tcg_id:
        logger.info("PokemonPriceTracker: using tcgPlayerId=%s directly (no search)",
                    tcg_id)
    else:
        # The search runs WITHOUT includeEbay: that flag bills an extra credit for
        # every card returned, and only the one card we actually pick needs graded
        # data. It's re-requested for that card alone below.
        base_params = {"search": search, "limit": PPT_SEARCH_LIMIT,
                       "language": language}
        attempts = []
        if card.set_name.strip():
            attempts.append({**base_params, "set": card.set_name.strip()})
        attempts.append(base_params)      # set filter dropped; matched client-side

        rows = []
        for i, params in enumerate(attempts):
            # Every PPT call goes through _ppt_get — it's the one choke point
            # the global 60/min limiter (_ppt_rate_limit) is enforced at. This
            # function used to call client.get() directly here, bypassing that
            # limiter entirely; with the Grading Calculator open to guests and
            # members alike, aggregate traffic across many callers could blow
            # past PPT's real per-minute cap even though each caller's own
            # per-IP/per-user rate limit looked fine, which is what got the key
            # rate-limited.
            rows, resp = await _ppt_get(client, "/cards", params, f"quotes-search({search})")
            if resp is None:
                return []
            row = _ppt_pick_card(rows, card)
            if row:
                break
            # An empty result costs no per-card credits, so retrying without the
            # set filter is cheap. Their own docs use a lowercase slug
            # ("celebrations"), so a title-cased set name may not match the filter.
            if i < len(attempts) - 1:
                logger.info("PokemonPriceTracker: %d row(s) for %r with set=%r — "
                            "retrying without the set filter",
                            len(rows), search, card.set_name)

        if not row:
            logger.info("PokemonPriceTracker: no match for %r (%s); %d row(s) seen: %s",
                        search, card.set_name, len(rows),
                        [(r.get("name"), r.get("cardNumber"), r.get("setName"))
                         for r in rows[:5]])
            return []

        # Credit accounting is in the response headers — log it so budget burn is
        # visible before the monthly quota runs out, not after.
        logger.info("PokemonPriceTracker %r -> matched %r %s (%s) | credits used=%s "
                    "daily remaining=%s",
                    card.name, row.get("name"), row.get("cardNumber"),
                    row.get("setName"),
                    resp.headers.get("X-API-Calls-Consumed", "?"),
                    resp.headers.get("X-RateLimit-Daily-Remaining", "?"))
        tcg_id = row.get("tcgPlayerId") or row.get("tcgplayerId")

    # Fetch the graded block for exactly this card. Billing is per card returned,
    # so one card costs a fraction of flagging a whole search.
    if tcg_id:
        detail_rows, detail_resp = await _ppt_get(
            client, "/cards",
            {"tcgPlayerId": str(tcg_id), "limit": 1, "includeEbay": "true",
             "days": PPT_EBAY_DAYS, "language": language},
            f"quotes-detail(tcgPlayerId={tcg_id})")
        if detail_rows:
            row = detail_rows[0]
            if detail_resp is not None:
                logger.info("PokemonPriceTracker graded fetch (tcgPlayerId=%s) | "
                            "credits used=%s daily remaining=%s", tcg_id,
                            detail_resp.headers.get("X-API-Calls-Consumed", "?"),
                            detail_resp.headers.get("X-RateLimit-Daily-Remaining", "?"))
    elif row is not None:
        logger.info("PokemonPriceTracker: row has no tcgPlayerId (keys: %s)",
                    sorted(row.keys()))

    # No row at all: an id was supplied but the detail lookup returned nothing.
    if row is None:
        logger.info("PokemonPriceTracker: tcgPlayerId=%s returned no card", tcg_id)
        return []

    # Summarise the graded block rather than dumping it — priceHistory alone
    # runs to thousands of lines per card.
    _ebay = row.get("ebay") or {}
    logger.info("PokemonPriceTracker graded block: %d grade(s) tracked, "
                "%s total sales, range %s..%s",
                len(_ebay.get("salesByGrade") or {}), _ebay.get("totalSales", "?"),
                str(_ebay.get("dateRangeStart"))[:10], str(_ebay.get("dateRangeEnd"))[:10])

    def _dollars(value):
        try:
            out = float(value)
        except (TypeError, ValueError):
            return None
        return round(out, 2) if out > 0 else None

    quotes = []
    prices = row.get("prices") or {}
    raw_price = _dollars(prices.get("market")) or _dollars(prices.get("low"))
    if raw_price:
        quotes.append(GradedQuote(
            grade="raw", price=raw_price, basis="sold",
            source="pokemonpricetracker", as_of=_now(),
            note=f"{row.get('setName') or ''} {row.get('cardNumber') or ''}".strip() or None,
        ))

    # Graded sales live under ebay.salesByGrade, NOT at the top of the ebay
    # block (which holds scrape timestamps, priceHistory and totals).
    ebay_block = row.get("ebay") or {}
    sales_by_grade = (ebay_block or {}).get("salesByGrade") or {}
    price_history = (ebay_block or {}).get("priceHistory") or {}
    for key, entry in sales_by_grade.items():
        grade = PPT_GRADE_FIELDS.get(_ppt_canon(key))
        if not grade or not isinstance(entry, dict):
            continue
        value, confidence, days_used = _ppt_grade_price(entry)
        price = _dollars(value)
        if not price:
            continue
        count = entry.get("count")
        detail = [f"{days_used}d" if days_used else f"{PPT_EBAY_DAYS}d"]
        if confidence:
            detail.append(f"{confidence} confidence")
        recent_avg, recent_n, recent_since = _ppt_recent_average(price_history.get(key))
        quotes.append(GradedQuote(
            grade=grade, price=price, basis=PPT_BASIS,
            source="pokemonpricetracker", as_of=_now(),
            sample_size=int(count) if isinstance(count, (int, float)) else None,
            note="eBay sold, " + ", ".join(detail),
            recent_avg=recent_avg, recent_n=recent_n, recent_since=recent_since,
        ))

    # Fall back to eBay's ungraded sold figure only if no market price was found.
    if not any(q.grade == "raw" for q in quotes):
        ungraded = sales_by_grade.get("ungraded")
        if isinstance(ungraded, dict):
            value, confidence, days_used = _ppt_grade_price(ungraded)
            price = _dollars(value)
            if price:
                quotes.append(GradedQuote(
                    grade="raw", price=price, basis=PPT_BASIS,
                    source="pokemonpricetracker", as_of=_now(),
                    sample_size=ungraded.get("count"),
                    note="eBay sold, ungraded",
                ))

    if not any(q.grade != "raw" for q in quotes):
        logger.info("PokemonPriceTracker: no graded prices for %r — "
                    "salesByGrade keys: %s", row.get("name"), list(sales_by_grade))
    if quotes:
        _ppt_cache[cache_key] = (time.time(), list(quotes))
    return quotes


# ────────────────────────────── eBay Browse ──────────────────────────────
# Browse returns ACTIVE listings only — sold comps live behind Marketplace
# Insights, which is a Limited Release API requiring eBay Partner Network
# approval. Everything from this source is basis='ask' and must be labelled
# as such in the UI.
EBAY_BASE = os.getenv("EBAY_API_BASE", "https://api.ebay.com")
EBAY_MARKETPLACE = os.getenv("EBAY_MARKETPLACE_ID", "EBAY_US")
EBAY_SEARCH_LIMIT = 50
# How many of the cheapest listings per grade get averaged into the quote.
EBAY_PRICE_SAMPLE = 5

# Title patterns per grade. Order matters for `raw` (checked last, by
# exclusion). \b on the trailing digit stops "PSA 9" matching "PSA 9.5".
_GRADE_PATTERNS = {
    "psa_10":  re.compile(r"\bpsa\s*10\b", re.I),
    "psa_9":   re.compile(r"\bpsa\s*9(?!\.5)\b", re.I),
    "psa_8":   re.compile(r"\bpsa\s*8\b", re.I),
    "bgs_10":  re.compile(r"\bbgs\s*10\b", re.I),
    "bgs_9_5": re.compile(r"\bbgs\s*9\.5\b", re.I),
    "cgc_9":   re.compile(r"\bcgc\s*9(?!\.5)\b", re.I),
    "cgc_10":  re.compile(r"\bcgc\s*10\b", re.I),
    "sgc_10":  re.compile(r"\bsgc\s*10\b", re.I),
    "tag_9":   re.compile(r"\btag\s*9(?!\.5)\b", re.I),
    "tag_10":  re.compile(r"\btag\s*10\b", re.I),
}
_ANY_GRADER = re.compile(r"\b(psa|bgs|cgc|sgc|tag|ace)\s*\d", re.I)

# Titles that poison a price average: lots, customs, and non-cards.
_JUNK_TITLE = re.compile(
    r"\b(lot|bundle|proxy|custom|reprint|fake|replica|digital|code\s*card|"
    r"empty|pack|booster|box|sleeve|toploader|binder|opened)\b", re.I)

_ebay_token = {"value": None, "expires_at": 0.0}


def ebay_available() -> bool:
    return bool(os.getenv("EBAY_CLIENT_ID") and os.getenv("EBAY_CLIENT_SECRET"))


async def _ebay_token_value(client: httpx.AsyncClient) -> str | None:
    """Application access token via OAuth2 client-credentials, cached in-process.

    Tokens last ~2h; we refresh 5 minutes early. Process-lifetime cache is
    fine (a deploy just re-fetches), matching how justtcg.py holds key state.
    """
    import time
    if _ebay_token["value"] and time.time() < _ebay_token["expires_at"]:
        return _ebay_token["value"]

    cid = os.getenv("EBAY_CLIENT_ID")
    secret = os.getenv("EBAY_CLIENT_SECRET")
    if not cid or not secret:
        return None
    basic = base64.b64encode(f"{cid}:{secret}".encode()).decode()
    try:
        resp = await client.post(
            f"{EBAY_BASE}/identity/v1/oauth2/token",
            headers={"Authorization": f"Basic {basic}",
                     "Content-Type": "application/x-www-form-urlencoded"},
            data={"grant_type": "client_credentials",
                  "scope": "https://api.ebay.com/oauth/api_scope"},
        )
    except Exception:
        logger.exception("eBay token request failed")
        return None
    if resp.status_code != 200:
        logger.warning("eBay token HTTP %s: %s", resp.status_code, resp.text[:300])
        return None
    payload = resp.json()
    token = payload.get("access_token")
    if not token:
        logger.warning("eBay token response had no access_token: %s", list(payload))
        return None
    _ebay_token["value"] = token
    _ebay_token["expires_at"] = time.time() + max(60, int(payload.get("expires_in", 7200)) - 300)
    logger.info("eBay app token acquired (expires in %ss)", payload.get("expires_in"))
    return token


def _classify_title(title: str) -> str | None:
    """Map a listing title to a canonical grade, or None if unusable."""
    if not title or _JUNK_TITLE.search(title):
        return None
    for grade, pattern in _GRADE_PATTERNS.items():
        if pattern.search(title):
            return grade
    if not _ANY_GRADER.search(title):
        return "raw"
    return None      # a grade we don't model (PSA 7, SGC, TAG…)


async def fetch_ebay(client: httpx.AsyncClient, card: CardRef) -> list[GradedQuote]:
    """Ask-side prices + live supply depth. Returns [] on any failure.

    One search per card (not per grade) — the response is bucketed by parsing
    grades out of listing titles, which keeps this to a single call against
    the 5,000/day default quota.
    """
    token = await _ebay_token_value(client)
    if not token:
        return []
    try:
        resp = await client.get(
            f"{EBAY_BASE}/buy/browse/v1/item_summary/search",
            headers={"Authorization": f"Bearer {token}",
                     "X-EBAY-C-MARKETPLACE-ID": EBAY_MARKETPLACE},
            params={"q": card.query(), "limit": EBAY_SEARCH_LIMIT,
                    "filter": "buyingOptions:{FIXED_PRICE}"},
        )
    except Exception:
        logger.exception("eBay search failed for %r", card.query())
        return []

    if resp.status_code == 401:
        _ebay_token["value"] = None      # force refresh on the next call
        logger.warning("eBay search got 401 — token cleared for retry")
        return []
    if resp.status_code != 200:
        logger.warning("eBay search HTTP %s for %r: %s",
                       resp.status_code, card.query(), resp.text[:300])
        return []

    items = (resp.json() or {}).get("itemSummaries") or []
    buckets: dict[str, list[float]] = {}
    for item in items:
        grade = _classify_title(item.get("title", ""))
        if not grade:
            continue
        price_obj = item.get("price") or {}
        if (price_obj.get("currency") or "USD") != "USD":
            continue
        try:
            value = float(price_obj.get("value"))
        except (TypeError, ValueError):
            continue
        if value > 0:
            buckets.setdefault(grade, []).append(value)

    quotes = []
    for grade, prices in buckets.items():
        prices.sort()
        # Average of the N CHEAPEST listings, not of all of them: the low end of
        # the ask stack is what a seller actually has to compete with, and it
        # ignores the aspirational $5k listing that has sat unsold for months.
        # Averaging several (rather than taking the single lowest) blunts the
        # one bad listing — wrong card, damaged copy, bait price.
        sample = prices[:EBAY_PRICE_SAMPLE]
        quotes.append(GradedQuote(
            grade=grade, price=round(sum(sample) / len(sample), 2), basis="ask",
            source="ebay_browse", as_of=_now(), sample_size=len(sample),
            low=round(prices[0], 2),
            note=(f"avg of {len(sample)} cheapest of {len(prices)} active listing(s)"
                  if len(prices) > len(sample)
                  else f"avg of {len(sample)} active listing(s)"),
        ))
    if not quotes:
        logger.info("eBay returned %d item(s) for %r, none classifiable",
                    len(items), card.query())
    return quotes


# ─────────────────────────────── Merging ───────────────────────────────
# Lower number wins when two sources quote the same grade. PokemonPriceTracker
# outranks eBay since it returns real graded sales, not asking prices.
SOURCE_PRIORITY = {
    "manual": 0,
    "pokemonpricetracker": 1,
    "ebay_browse": 2,
}


def merge_quotes(*groups: list[GradedQuote]) -> dict[str, GradedQuote]:
    """Best quote per grade, by source priority. Sold beats ask; a manual
    admin override beats both."""
    best: dict[str, GradedQuote] = {}
    for group in groups:
        for q in group or []:
            if q.grade not in GRADE_KEYS or q.price <= 0:
                continue
            current = best.get(q.grade)
            if current is None or (SOURCE_PRIORITY.get(q.source, 99)
                                   < SOURCE_PRIORITY.get(current.source, 99)):
                best[q.grade] = q
    return best


async def fetch_all(card: CardRef, manual: list[GradedQuote] | None = None,
                    timeout: float = 20.0) -> dict:
    """Query every configured vendor and merge. Never raises.

    Returns {"quotes": {grade: GradedQuote}, "sources": [...], "all": [...]}
    so the page can show both the winning number and what else was available.
    """
    groups: list[list[GradedQuote]] = [manual or []]
    used = ["manual"] if manual else []

    async with httpx.AsyncClient(timeout=timeout) as client:
        async def _run(name: str, fetcher) -> None:
            """Run one vendor. A bug or outage in any single source must not
            fail the whole lookup — the others still have useful prices."""
            try:
                quotes = await fetcher(client, card)
            except Exception:
                logger.exception("%s lookup failed for %r — continuing without it",
                                 name, card.query())
                return
            if quotes:
                used.append(name)
            groups.append(quotes or [])

        if pokemonpricetracker_available() and card.game == "pokemon":
            await _run("pokemonpricetracker", fetch_pokemonpricetracker)
        if ebay_available():
            await _run("ebay_browse", fetch_ebay)

    merged = merge_quotes(*groups)
    flat = [q for g in groups for q in (g or [])]
    return {"quotes": merged, "sources": used, "all": flat}


def configured_sources() -> dict:
    """What's actually wired up — surfaced on the page so a missing key is
    visible instead of looking like 'no data for this card'."""
    return {
        "pokemonpricetracker": pokemonpricetracker_available(),
        "ebay_browse": ebay_available(),
    }
