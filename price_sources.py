"""Graded-price sources for the grading calculator (admin trial).

Vendor-agnostic: every source maps its own grade vocabulary into GRADE_KEYS
and tags each quote with a `basis` — 'sold' (a real completed-sale figure) or
'ask' (what someone is currently asking). The page renders that badge, because
an unsold $900 listing is not a comp and the UI must never imply it is.

Sources, in priority order:
  1. manual         admin override, supplied by the caller (never fetched here)
  2. pricecharting  sold-derived, per grade, covers Pokemon AND One Piece
  3. ebay_browse    ask-side only — live listings, also gives supply depth

WRITTEN WITHOUT LIVE KEYS. Like justtcg.py before it, the exact response
shapes here are unverified against a real account, so extraction scans for
expected field names instead of assuming one shape, and the first response
from each vendor logs its keys so the mappings can be corrected after the
first real run. Treat every mapping table below as a hypothesis until a live
call confirms it.
"""

from __future__ import annotations

import os
import re
import base64
import logging
import statistics
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

import httpx

logger = logging.getLogger("dashboard.price_sources")

# Canonical grade vocabulary. grading_roi.GRADE_ORDER must stay in sync.
GRADE_KEYS = ("raw", "psa_8", "psa_9", "psa_10", "bgs_9_5", "bgs_10", "cgc_10")

GRADE_LABELS = {
    "raw": "Raw", "psa_8": "PSA 8", "psa_9": "PSA 9", "psa_10": "PSA 10",
    "bgs_9_5": "BGS 9.5", "bgs_10": "BGS 10", "cgc_10": "CGC 10",
}


@dataclass(frozen=True)
class CardRef:
    game: str                    # 'pokemon' | 'one_piece'
    name: str
    set_name: str = ""
    card_number: str = ""
    variant: str | None = None

    def query(self) -> str:
        """Free-text search string shared by both vendors."""
        parts = [self.name, self.set_name, self.card_number]
        return " ".join(p.strip() for p in parts if p and p.strip())

    def key(self) -> str:
        """Stable identity for caching/snapshot rows."""
        norm = lambda s: re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")
        return "|".join((norm(self.game), norm(self.name),
                         norm(self.set_name), norm(self.card_number)))


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

    def to_dict(self) -> dict:
        return asdict(self)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ───────────────────────────── PriceCharting ─────────────────────────────
# PriceCharting reuses its old video-game price-guide field names for cards,
# so the field -> grade mapping is NOT self-describing. This table is the
# mapping their card docs describe; their docs page returned HTTP 403 to
# automated fetches, so IT IS UNVERIFIED. _log_unknown_fields() dumps any
# price-looking key we didn't map on the first response of each process —
# check the logs after the first live call and correct this table.
# Values arrive in PENNIES.
PRICECHARTING_FIELD_MAP = {
    "loose-price":        "raw",
    "new-price":          "psa_8",
    "graded-price":       "psa_9",
    "box-only-price":     "bgs_9_5",
    "manual-only-price":  "psa_10",
    "bgs-10-price":       "bgs_10",
    "condition-17-price": "cgc_10",
}

PRICECHARTING_BASE = os.getenv("PRICECHARTING_API_BASE",
                               "https://www.pricecharting.com/api")
_pc_logged_shape = False


def _pc_query(card: CardRef) -> str:
    """PriceCharting-specific search string.

    Their products are named like "Mew ex #232" under a console named
    "Pokemon Paldean Fates". Two things differ from the generic query():
      - card_number arrives from pokemontcg.io as "232/091" (number/printedTotal);
        PriceCharting only knows the bare "232", and the "/091" is a token that
        matches nothing, which sinks the search.
      - the console is prefixed with the game ("Pokemon <set>"), so we prefix it
        too rather than sending the bare set name.
    """
    number = (card.card_number or "").split("/")[0].strip()
    game_word = {"pokemon": "Pokemon", "one_piece": "One Piece"}.get(card.game, "")
    parts = [card.name.strip()]
    if number:
        parts.append(number)
    if card.set_name.strip():
        parts.append(f"{game_word} {card.set_name}".strip())
    return " ".join(p for p in parts if p)


def pricecharting_available() -> bool:
    return bool(os.getenv("PRICECHARTING_API_TOKEN"))


def _log_unknown_fields(payload: dict) -> None:
    """Log price-looking keys we don't have a mapping for — once per process."""
    global _pc_logged_shape
    if _pc_logged_shape:
        return
    _pc_logged_shape = True
    keys = sorted(k for k in payload if k.endswith("-price"))
    unknown = [k for k in keys if k not in PRICECHARTING_FIELD_MAP]
    logger.info("PriceCharting response price fields: %s", keys)
    if unknown:
        logger.warning("PriceCharting fields with NO grade mapping: %s — "
                       "check PRICECHARTING_FIELD_MAP against their docs", unknown)


def _pennies(value) -> float | None:
    try:
        cents = float(value)
    except (TypeError, ValueError):
        return None
    if cents <= 0:
        return None
    return round(cents / 100.0, 2)


async def fetch_pricecharting(client: httpx.AsyncClient, card: CardRef) -> list[GradedQuote]:
    """Per-grade sold-derived prices. Returns [] on any failure — never raises."""
    token = os.getenv("PRICECHARTING_API_TOKEN")
    if not token:
        return []
    query = _pc_query(card)
    try:
        resp = await client.get(f"{PRICECHARTING_BASE}/product",
                                params={"t": token, "q": query})
    except Exception:
        logger.exception("PriceCharting request failed for %r", query)
        return []

    if resp.status_code != 200:
        logger.warning("PriceCharting HTTP %s for %r", resp.status_code, query)
        return []
    try:
        payload = resp.json()
    except Exception:
        logger.warning("PriceCharting returned non-JSON for %r", query)
        return []
    if not isinstance(payload, dict):
        return []
    if str(payload.get("status", "success")).lower() not in ("success", "ok", ""):
        logger.info("PriceCharting no match for %r: %s", query, payload.get("status"))
        return []

    _log_unknown_fields(payload)
    product = payload.get("product-name") or ""
    console = payload.get("console-name") or ""
    matched_name = " — ".join(p for p in (product, console) if p)
    # Logged every lookup (not once per process): a wrong-but-plausible match is
    # the failure mode that silently produces confident garbage, so the matched
    # product must always be checkable against what was asked for.
    logger.info("PriceCharting %r -> matched %r (id=%s)",
                query, matched_name or "?", payload.get("id"))

    quotes = []
    for field_name, grade in PRICECHARTING_FIELD_MAP.items():
        price = _pennies(payload.get(field_name))
        if price is None:
            continue
        quotes.append(GradedQuote(
            grade=grade, price=price, basis="sold", source="pricecharting",
            as_of=_now(), note=matched_name or None,
        ))
    if not quotes:
        logger.info("PriceCharting matched %r but returned no usable prices", query)
    return quotes


# ────────────────────────────── eBay Browse ──────────────────────────────
# Browse returns ACTIVE listings only — sold comps live behind Marketplace
# Insights, which is a Limited Release API requiring eBay Partner Network
# approval. Everything from this source is basis='ask' and must be labelled
# as such in the UI.
EBAY_BASE = os.getenv("EBAY_API_BASE", "https://api.ebay.com")
EBAY_MARKETPLACE = os.getenv("EBAY_MARKETPLACE_ID", "EBAY_US")
EBAY_SEARCH_LIMIT = 50

# Title patterns per grade. Order matters for `raw` (checked last, by
# exclusion). \b on the trailing digit stops "PSA 9" matching "PSA 9.5".
_GRADE_PATTERNS = {
    "psa_10":  re.compile(r"\bpsa\s*10\b", re.I),
    "psa_9":   re.compile(r"\bpsa\s*9(?!\.5)\b", re.I),
    "psa_8":   re.compile(r"\bpsa\s*8\b", re.I),
    "bgs_10":  re.compile(r"\bbgs\s*10\b", re.I),
    "bgs_9_5": re.compile(r"\bbgs\s*9\.5\b", re.I),
    "cgc_10":  re.compile(r"\bcgc\s*10\b", re.I),
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
        # Median, not lowest: the cheapest listing is disproportionately a
        # wrong card, a damaged copy, or bait. Lowest is carried separately
        # as `low` because it IS what you'd compete against on price.
        quotes.append(GradedQuote(
            grade=grade, price=round(statistics.median(prices), 2), basis="ask",
            source="ebay_browse", as_of=_now(), sample_size=len(prices),
            low=round(prices[0], 2),
            note=f"{len(prices)} active listing(s)",
        ))
    if not quotes:
        logger.info("eBay returned %d item(s) for %r, none classifiable",
                    len(items), card.query())
    return quotes


# ─────────────────────────────── Merging ───────────────────────────────
# Lower number wins when two sources quote the same grade.
SOURCE_PRIORITY = {"manual": 0, "pricecharting": 1, "ebay_browse": 2}


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
        if pricecharting_available():
            pc = await fetch_pricecharting(client, card)
            if pc:
                used.append("pricecharting")
            groups.append(pc)
        if ebay_available():
            eb = await fetch_ebay(client, card)
            if eb:
                used.append("ebay_browse")
            groups.append(eb)

    merged = merge_quotes(*groups)
    flat = [q for g in groups for q in (g or [])]
    return {"quotes": merged, "sources": used, "all": flat}


def configured_sources() -> dict:
    """What's actually wired up — surfaced on the page so a missing key is
    visible instead of looking like 'no data for this card'."""
    return {
        "pricecharting": pricecharting_available(),
        "ebay_browse": ebay_available(),
    }
