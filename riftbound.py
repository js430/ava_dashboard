"""Riftbound cards and sealed product — TCGGO's "Riftbound Prices API" on RapidAPI.

Fills the same tables Pokemon does (catalog_cards, sealed_products, and through
them tracked_cards/price_snapshots), with game = 'riftbound'. Nothing here
touches the database: main.py runs the sync, catalog.py and portfolios.py do
the writes — the same split as price_sources for Pokemon.

WHAT THE LIVE API ACTUALLY DOES. Checked against real responses on 2026-10-31;
several of these contradict its own documentation, and each one matters here:

  * `per_page` is ignored. Every list endpoint returns 20 rows a page, so the
    ~1,800 cards cost ~90 requests to walk. A daily full refresh therefore
    needs the paid plan's budget — the free plan's 100/day can't cover it.
  * A card can carry two prices, BOTH labelled "EUR":
      - prices.cardmarket.* — Cardmarket's own figures, genuinely in euros.
      - prices.tcg_player.market_price — TCGplayer's US market price DIVIDED
        BY A FIXED 1.17. Compared against TCGplayer's own market price, 17 of
        20 cards came out at exactly 1.17x; the other three were cards whose
        price had moved since. Multiplying by that constant recovers the real
        dollar figure. Today's exchange rate would read it 4-5% low.
    The TCGplayer figure is the main price (it's the US market, the same
    number Pokemon's catalog shows); Cardmarket's 7-day average, converted at
    the day's ECB rate, is the fallback for cards TCGplayer doesn't price.
    `price_source` records which one each card got.
  * Only one TCGplayer number per card: the non-foil printing's where both
    exist, the foil's where that's the only printing.
  * Price history is at /history-prices, not the documented
    /riftbound/history-prices, comes as a point every week or few, and has no
    7-day average — only the TCGplayer figure and Cardmarket's 30-day average.
  * The same name + number + rarity can exist twice as different VERSIONS
    ("Ezreal, Dashing" 082b as "V.3 - Showcase" and "V.4 - Showcase"), which
    is why catalog_cards carries `version` in its identity.
  * Many rows have `rarity` empty while `version` says what it is
    ("V.3 - Signed Showcase"), so the rarity falls back to that.
"""

import os
import re
import math
import time
import logging
from datetime import date
from decimal import Decimal
from urllib.parse import urlparse

import httpx

logger = logging.getLogger("dashboard.riftbound")

GAME = "riftbound"
# The API only serves English cards (every row's `lang` is "en"); stored under
# 'english' so Riftbound rows fit the catalog's language-aware queries as-is.
LANGUAGE = "english"

HOST = "riftbound-prices-api.p.rapidapi.com"
BASE = f"https://{HOST}"
# Safety ceiling on pages per listing. The biggest set is ~18 pages; this only
# stops a misbehaving `paging.total` from walking forever and spending quota.
MAX_PAGES = int(os.getenv("RIFTBOUND_MAX_PAGES", "40"))
# Undoes the API's own TCGplayer -> "EUR" conversion (see the module
# docstring). An env var rather than only a constant because it's the API's
# number, not ours: if TCGGO ever changes it, prices drift by the difference
# until this is updated.
TCGPLAYER_USD_PER_EUR = float(os.getenv("RIFTBOUND_TCGPLAYER_USD_PER_EUR", "1.17"))

# Values written to catalog_cards.price_source.
SOURCE_TCGPLAYER = "tcgplayer"
SOURCE_CARDMARKET = "cardmarket"

# Card images. Written to the DB and rendered as <img src> for every visitor,
# so only https on this host is accepted — same rule as price_sources'
# PPT_IMAGE_HOSTS. Must stay in step with the CSP's img-src in main.py.
IMAGE_HOSTS = ("images.tcggo.com",)

# The day's EUR -> USD rate, from the European Central Bank via Frankfurter
# (free, no key). Only Cardmarket prices need it.
FX_URL = "https://api.frankfurter.dev/v1/latest"
FX_TTL_S = 6 * 3600
# A rate this old is still used if a fresh one can't be had — exchange rates
# move far less in a few days than a missing price costs — but not forever.
FX_STALE_MAX_S = 5 * 86400
_fx_cache = {"rate": None, "fetched": 0.0, "as_of": None}

# Set grouping for the catalog's set picker, in the same shape card_eras uses
# for Pokemon's eras: (key, label), newest-first.
ERA_ORDER = [("riftbound_sets", "Sets"), ("riftbound_promos", "Promos & extras")]

_EPISODES_TTL_S = 12 * 3600
_episodes_cache = {"rows": [], "fetched": 0.0}

_VERSION_PREFIX = re.compile(r"^\s*V\.?\s*\d+\s*[-–:]\s*", re.I)


def available() -> bool:
    return bool(os.getenv("RIFTBOUND_API_KEY"))


async def _get(client: httpx.AsyncClient, path: str, params: dict, label: str) -> tuple:
    """(body, status) for one GET. status: ok | not_found | rate_limited | error.

    Never raises. A 429 is reported separately because it means "later", not
    "broken" — the sync stops cleanly and resumes on its next pass.
    """
    key = os.getenv("RIFTBOUND_API_KEY")
    if not key:
        return None, "error"
    try:
        resp = await client.get(BASE + path, params=params,
                                headers={"X-RapidAPI-Key": key, "X-RapidAPI-Host": HOST})
    except Exception:
        logger.exception("Riftbound %s request failed", label)
        return None, "error"
    remaining = resp.headers.get("x-ratelimit-requests-remaining")
    if resp.status_code == 429:
        logger.warning("Riftbound %s rate limited (429) | requests remaining today=%s",
                       label, remaining or "?")
        return None, "rate_limited"
    if resp.status_code == 404:
        return None, "not_found"
    if resp.status_code != 200:
        logger.warning("Riftbound %s HTTP %s: %s", label, resp.status_code, resp.text[:300])
        return None, "error"
    try:
        body = resp.json()
    except Exception:
        logger.warning("Riftbound %s returned non-JSON", label)
        return None, "error"
    if remaining is not None and remaining.isdigit() and int(remaining) < 25:
        logger.warning("Riftbound API: only %s request(s) left today", remaining)
    return body, "ok"


async def _get_all(client: httpx.AsyncClient, path: str, params: dict, label: str) -> tuple:
    """Every row of a paginated listing: (rows, status).

    On a failure part-way through, the rows fetched so far come back with the
    failing status — upserting them is still correct (nothing is ever deleted
    for being absent), and the caller can see the walk wasn't finished.
    """
    rows, page = [], 1
    while page <= MAX_PAGES:
        body, status = await _get(client, path, {**params, "page": page}, f"{label} p{page}")
        if status != "ok":
            return rows, status
        data = body.get("data") if isinstance(body, dict) else None
        if not isinstance(data, list):
            data = []
        rows.extend(d for d in data if isinstance(d, dict))
        try:
            pages = int((body.get("paging") or {}).get("total") or 0)
        except (TypeError, ValueError):
            pages = 0
        if not data or page >= pages:
            break
        page += 1
    return rows, ("ok" if rows else "empty")


def _iso_date(value):
    text = str(value or "").strip()[:10]
    return text if re.match(r"^\d{4}-\d{2}-\d{2}$", text) else None


# Words that mark a release as something other than a main set: promo groups
# ("Origins: Promos", "Project K Promos") and one-off products ("T1 2025 Worlds
# Champion Collection", "Secret Garden Bundle").
_NON_MAIN_WORDS = {"promo", "promos", "collection", "bundle"}


def is_main_set(name: str) -> bool:
    """'Radiance', 'Vendetta', 'Unleashed' — a main release. The guest window
    counts only these, the same way Pokemon's skips its promo era, and the set
    picker files the rest under "Promos & extras"."""
    words = re.findall(r"[a-z]+", (name or "").lower())
    return not any(w in _NON_MAIN_WORDS for w in words)


async def fetch_episodes(client: httpx.AsyncClient) -> tuple:
    """Riftbound sets ("episodes"), newest release first, undated last:
    ([{id, name, code, released, main}], status)."""
    rows, status = await _get_all(client, "/episodes", {}, "episodes")
    out = []
    for r in rows:
        slug = (r.get("game") or {}).get("slug")
        if slug not in (None, GAME):
            continue
        episode_id, name = r.get("id"), str(r.get("name") or "").strip()
        if not episode_id or not name:
            continue
        out.append({"id": str(episode_id), "name": name[:200], "code": r.get("code") or "",
                    "released": _iso_date(r.get("released_at")), "main": is_main_set(name)})
    out.sort(key=lambda s: (s["released"] or "", s["name"]), reverse=True)
    if out:
        _episodes_cache.update(rows=out, fetched=time.monotonic())
    return out, status


async def cached_episodes(client: httpx.AsyncClient) -> list:
    """The set list, re-fetched at most every _EPISODES_TTL_S (one request).
    Every sync refreshes it too, so in practice this rarely calls out."""
    fresh = time.monotonic() - _episodes_cache["fetched"] < _EPISODES_TTL_S
    if _episodes_cache["rows"] and fresh:
        return list(_episodes_cache["rows"])
    if not available():
        return list(_episodes_cache["rows"])
    rows, _status = await fetch_episodes(client)
    return rows or list(_episodes_cache["rows"])


async def fetch_episode_cards(client: httpx.AsyncClient, episode_id) -> tuple:
    """Every card in one set: (raw rows, status)."""
    return await _get_all(client, f"/episodes/{int(episode_id)}/cards", {},
                          f"set {episode_id} cards")


async def fetch_products(client: httpx.AsyncClient) -> tuple:
    """Every sealed product: (raw rows, status). ~3 requests."""
    return await _get_all(client, "/products", {}, "products")


async def fetch_history(client: httpx.AsyncClient, card_id, max_pages: int = 6) -> tuple:
    """A card's TCGplayer price history in USD: ([{date, market}], status).

    Only the TCGplayer figure is kept: it's the series the daily snapshots
    carry, and Cardmarket's 30-day average (the only other thing on offer
    here) is a different number that would read as a price move when the
    two met on a graph.
    """
    points, status, page = {}, "ok", 1
    while page <= max_pages:
        body, status = await _get(client, "/history-prices",
                                  {"id": int(card_id), "sort": "desc", "page": page},
                                  f"card {card_id} history p{page}")
        if status != "ok":
            break
        data = body.get("data") if isinstance(body, dict) else None
        if isinstance(data, dict):
            for day, row in data.items():
                when = _iso_date(day)
                price = _money((row or {}).get("tcg_player_market")) if isinstance(row, dict) else None
                if when and price is not None:
                    points[when] = round(price * TCGPLAYER_USD_PER_EUR, 2)
        try:
            pages = int((body.get("paging") or {}).get("total") or 0)
        except (TypeError, ValueError):
            pages = 0
        if not data or page >= pages:
            break
        page += 1
    out = [{"date": date.fromisoformat(d), "market": p} for d, p in sorted(points.items())]
    if status == "ok" and not out:
        status = "empty"
    return out, status


async def eur_usd_rate(client: httpx.AsyncClient):
    """Today's EUR -> USD rate, or None if there has never been one.

    A failed lookup falls back to the last good rate for up to FX_STALE_MAX_S.
    Returning None makes callers leave Cardmarket-only prices empty rather
    than store a euro figure as if it were dollars.
    """
    age = time.monotonic() - _fx_cache["fetched"]
    if _fx_cache["rate"] and age < FX_TTL_S:
        return _fx_cache["rate"]
    try:
        resp = await client.get(FX_URL, params={"base": "EUR", "symbols": "USD"})
        rate = float(resp.json()["rates"]["USD"]) if resp.status_code == 200 else None
    except Exception:
        logger.exception("Riftbound: exchange-rate lookup failed")
        rate = None
    if rate is not None and math.isfinite(rate) and 0.5 < rate < 3:
        _fx_cache.update(rate=rate, fetched=time.monotonic(), as_of=date.today().isoformat())
        return rate
    if _fx_cache["rate"] and age < FX_STALE_MAX_S:
        logger.warning("Riftbound: using the exchange rate from %s — today's lookup failed",
                       _fx_cache["as_of"])
        return _fx_cache["rate"]
    return None


def _money(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def price_usd(row: dict, eur_usd) -> tuple:
    """(price in USD, source) for a card or product row, or (None, None).

    TCGplayer first — undoing the API's fixed conversion, so no exchange rate
    is involved — then Cardmarket's 7-day average at the day's rate.
    """
    prices = (row or {}).get("prices") or {}
    tcg = _money((prices.get("tcg_player") or {}).get("market_price"))
    if tcg is not None:
        return round(tcg * TCGPLAYER_USD_PER_EUR, 2), SOURCE_TCGPLAYER
    avg7 = _money((prices.get("cardmarket") or {}).get("7d_average"))
    if avg7 is not None and eur_usd:
        return round(avg7 * eur_usd, 2), SOURCE_CARDMARKET
    return None, None


def card_rarity(row: dict) -> str:
    """The card's rarity, falling back to what its version label says
    ("V.3 - Signed Showcase" -> "Signed Showcase"). A lowercase value from
    the feed ("showcase") is capitalised so it doesn't sit beside "Showcase"
    as a second option in the rarity filter."""
    raw = str(row.get("rarity") or "").strip()
    if not raw:
        raw = _VERSION_PREFIX.sub("", str(row.get("version") or "")).strip()
    if raw and raw == raw.lower():
        raw = raw[0].upper() + raw[1:]
    return raw[:80]


def _image_url(row: dict):
    value = (row or {}).get("image")
    if not isinstance(value, str) or not value:
        return None
    parts = urlparse(value)
    if parts.scheme == "https" and (parts.hostname or "").lower() in IMAGE_HOSTS:
        return value[:500]
    return None


def _tcgplayer_id(row: dict) -> str:
    value = str((row or {}).get("tcgplayer_id") or "").strip()
    return value if value.isdigit() else ""


def parse_card(row: dict, eur_usd, set_name: str = ""):
    """One API card -> the dict catalog.upsert_set_cards takes, or None.

    The TCGplayer id is the API's own `tcgplayer_id` field, not a fallback to
    some other id, so it's marked verified: the catalog then links straight to
    the TCGplayer product (spot-checked against TCGplayer — they match).
    """
    if not isinstance(row, dict) or (row.get("lang") or "en") != "en":
        return None
    name = str(row.get("name") or "").strip()
    source_id = str(row.get("id") or "").strip()
    if not name or not source_id:
        return None
    price, source = price_usd(row, eur_usd)
    tcgplayer_id = _tcgplayer_id(row)
    return {
        "source_id": source_id,
        "name": name[:200],
        "card_number": str(row.get("card_number") or "").strip()[:40],
        "variant": card_rarity(row),
        "version": str(row.get("version") or "").strip()[:80],
        "tcgplayer_id": tcgplayer_id,
        "tcgplayer_id_verified": bool(tcgplayer_id),
        "set_name": str((row.get("episode") or {}).get("name") or set_name or "").strip()[:200],
        "raw_price": price,
        "price_source": source,
        "image_url": _image_url(row),
        "printing_prices": None,
        "card_type": None,
    }


def product_type(name: str, classify) -> str:
    """Riftbound's names, mapped onto the existing sealed types. Two cases
    the generic rules (portfolios.classify_sealed_type) get wrong for this
    game: "Origins Case (6x Booster Box)" is six boxes, not a booster box,
    and a bare "Origins Booster" is a single pack. Everything else — boxes,
    bundles, "Sleeved Booster" blisters — goes to the generic rules."""
    text = (name or "").strip().lower()
    if re.search(r"\bcase\b", text):
        return "other"
    if text.endswith(" booster") and "sleeved" not in text:
        return "booster_pack"
    return classify(name)


def parse_product(row: dict, eur_usd, classify):
    """One API product -> the dict portfolios.import_sealed_products takes, or
    None. `classify` is portfolios.classify_sealed_type, passed in so this
    module needn't import portfolios."""
    if not isinstance(row, dict):
        return None
    name = str(row.get("name") or "").strip()
    if not name:
        return None
    price, _source = price_usd(row, eur_usd)
    return {
        "name": name[:200],
        "set_name": str((row.get("episode") or {}).get("name") or "").strip()[:200],
        "product_type": product_type(name, classify),
        # The column is named for PokemonPriceTracker, but it's "the price
        # API's id for this product" — for these rows, TCGGO's.
        "ppt_product_id": str(row.get("id") or "")[:60],
        "image_url": _image_url(row) or "",
        "market_price": Decimal(str(price)) if price is not None else None,
    }


def annotate_sets(sets: list, name_key: str = "set_name") -> list:
    """Add `era`/`era_label` to each set dict, in place — card_eras.annotate's
    shape, so the catalog's grouped set picker works unchanged."""
    labels = dict(ERA_ORDER)
    for entry in sets:
        key = "riftbound_sets" if is_main_set(entry.get(name_key) or "") else "riftbound_promos"
        entry["era"] = key
        entry["era_label"] = labels[key]
    return sets
