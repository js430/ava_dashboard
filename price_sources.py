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
import base64
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

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


async def _ppt_get(client: httpx.AsyncClient, path: str, params: dict,
                   label: str) -> tuple[list, object]:
    """GET a PPT endpoint and return (rows, response). ([], None) on failure."""
    key = os.getenv("POKEMONPRICETRACKER_API_KEY")
    if not key:
        return [], None
    try:
        resp = await client.get(f"{PPT_BASE}{path}",
                                headers={"Authorization": f"Bearer {key}"},
                                params=params)
    except Exception:
        logger.exception("PokemonPriceTracker %s request failed", label)
        return [], None
    if resp.status_code != 200:
        logger.warning("PokemonPriceTracker %s HTTP %s: %s",
                       label, resp.status_code, resp.text[:300])
        return [], None
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


async def fetch_ppt_set_cards(client: httpx.AsyncClient, set_name: str,
                              language: str = PPT_DEFAULT_LANGUAGE) -> list:
    """Every card in a PPT set: [{name, card_number, variant, tcgplayer_id}].

    BILLED PER CARD RETURNED, so this is cached for a day (per language) and
    the credit spend is logged. Carrying tcgPlayerId is what lets later price
    lookups skip the search entirely.
    """
    global _ppt_logged_card_shape
    language = ppt_language(language)
    cache_key = f"{language}:{_ppt_canon(set_name)}"
    hit = _ppt_set_cards_cache.get(cache_key)
    if hit and (time.time() - hit[0]) < PPT_CATALOG_TTL_SECONDS:
        logger.info("PokemonPriceTracker set-cards cache hit for %r [%s] (%d card(s), "
                    "no credits spent)", set_name, language, len(hit[1]))
        return list(hit[1])

    rows, resp = await _ppt_get(client, "/cards",
                                {"set": set_name, "fetchAllInSet": "true",
                                 "limit": 1000, "language": language},
                                f"set-cards({set_name}/{language})")
    if not rows:
        return []
    if not _ppt_logged_card_shape:
        _ppt_logged_card_shape = True
        logger.info("PokemonPriceTracker set-cards first row keys: %s",
                    sorted(rows[0].keys()) if isinstance(rows[0], dict) else "?")
    if resp is not None:
        logger.info("PokemonPriceTracker set-cards %r: %d card(s), credits used=%s, "
                    "daily remaining=%s", set_name, len(rows),
                    resp.headers.get("X-API-Calls-Consumed", "?"),
                    resp.headers.get("X-RateLimit-Daily-Remaining", "?"))

    cards = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = _first(row, "name", "cardName")
        if not name:
            continue
        cards.append({
            "name": str(name),
            "card_number": str(_first(row, "cardNumber", "number", "card_number") or ""),
            "variant": str(_first(row, "rarity", "variant") or ""),
            "tcgplayer_id": str(_first(row, "tcgPlayerId", "tcgplayerId", "id") or ""),
            "set_name": str(_first(row, "setName", "set_name") or set_name),
            # Already in this payload — the call is billed per card either way,
            # so carrying it costs nothing and lets the catalog cache raw
            # prices for a whole set for free. The grading calculator's picker
            # ignores this key.
            "raw_price": _ppt_raw_price(row),
        })

    def _sort_key(card):
        """Numeric card numbers in numeric order; promos/RC numbers after."""
        match = re.match(r"^(\d+)", card["card_number"])
        return (0, int(match.group(1))) if match else (1, 0)

    cards.sort(key=_sort_key)
    if cards:
        _ppt_set_cards_cache[cache_key] = (time.time(), list(cards))
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
    headers = {"Authorization": f"Bearer {key}"}

    async def _search(params: dict):
        """One request. Returns (rows, response) or (None, None) on failure."""
        try:
            resp = await client.get(f"{PPT_BASE}/cards", headers=headers, params=params)
        except Exception:
            logger.exception("PokemonPriceTracker request failed for %r", search)
            return None, None
        if resp.status_code == 401:
            logger.warning("PokemonPriceTracker rejected the API key (401)")
            return None, None
        if resp.status_code == 429:
            logger.warning("PokemonPriceTracker rate/credit limit hit (429)")
            return None, None
        if resp.status_code != 200:
            logger.warning("PokemonPriceTracker HTTP %s for %r: %s",
                           resp.status_code, search, resp.text[:300])
            return None, None
        try:
            payload = resp.json()
        except Exception:
            logger.warning("PokemonPriceTracker returned non-JSON for %r", search)
            return None, None
        # `data` is a list for a search but a single object for an id lookup —
        # normalise to a list so callers only handle one shape.
        data = (payload or {}).get("data")
        if isinstance(data, dict):
            data = [data]
        elif not isinstance(data, list):
            data = []
        return data, resp

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
            rows, resp = await _search(params)
            if rows is None:
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
        detail_rows, detail_resp = await _search(
            {"tcgPlayerId": str(tcg_id), "limit": 1, "includeEbay": "true",
             "days": PPT_EBAY_DAYS, "language": language})
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
