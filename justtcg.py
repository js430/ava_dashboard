"""JustTCG pricing client + pokemontcg.io release-date helper (card tracker).

Written defensively: the exact JustTCG response shapes weren't verified
against a live key when this was built, so price/date extraction scans for
the expected field names rather than assuming one shape, and every request
logs enough to tune the endpoints after the first real run. Free tier is
1,000 calls/month — the ingest logs its call count so you can watch budget.
"""

import os
import logging
import statistics
from datetime import datetime, date

import httpx

logger = logging.getLogger("dashboard.justtcg")

JUSTTCG_API_BASE = os.getenv("JUSTTCG_API_BASE", "https://api.justtcg.com/v1")
POKEMONTCG_API = "https://api.pokemontcg.io/v2/cards"

# JustTCG game slugs for our two tracked games. If a search 404s or returns
# nothing for every card of one game, this slug is the first thing to check.
GAME_SLUGS = {
    "pokemon": "pokemon",
    "one_piece": "one-piece-card-game",
}

BATCH_SIZE = 100  # JustTCG batch lookup cap per request

# Field names scanned when extracting prices from a card/variant object.
_PRICE_FIELDS = ("price", "marketPrice", "market_price", "midPrice", "mid_price")
_LOW_FIELDS = ("lowPrice", "low_price", "minPrice", "min_price", "low")
_HIGH_FIELDS = ("highPrice", "high_price", "maxPrice", "max_price", "high")
_RELEASE_FIELDS = ("releaseDate", "release_date", "set_release_date", "setReleaseDate")


class JustTCGError(Exception):
    pass


def _headers() -> dict:
    key = os.getenv("JUSTTCG_API_KEY", "")
    if not key:
        raise JustTCGError("JUSTTCG_API_KEY is not set")
    return {"X-API-Key": key}


def _nums(obj: dict, fields: tuple) -> list:
    out = []
    for f in fields:
        v = obj.get(f)
        if isinstance(v, (int, float)) and v > 0:
            out.append(float(v))
    return out


def extract_prices(card: dict) -> tuple:
    """Return (low, mid, high) floats (any may be None) from a JustTCG card
    object, scanning the card itself and its variants list."""
    mids, lows, highs = [], [], []
    objs = [card] + [v for v in (card.get("variants") or []) if isinstance(v, dict)]
    for o in objs:
        mids += _nums(o, _PRICE_FIELDS)
        lows += _nums(o, _LOW_FIELDS)
        highs += _nums(o, _HIGH_FIELDS)
    # Fall back to the spread of observed mid prices when explicit low/high absent.
    pool = mids or (lows + highs)
    if not pool and not lows and not highs:
        return None, None, None
    low = min(lows) if lows else (min(pool) if pool else None)
    high = max(highs) if highs else (max(pool) if pool else None)
    mid = statistics.median(mids) if mids else (statistics.median(pool) if pool else None)
    return low, mid, high


def extract_release_date(card: dict):
    """Best-effort release date from a JustTCG card object (card or nested set)."""
    objs = [card]
    if isinstance(card.get("set"), dict):
        objs.append(card["set"])
    for o in objs:
        for f in _RELEASE_FIELDS:
            v = o.get(f)
            if isinstance(v, str) and v:
                for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
                    try:
                        return datetime.strptime(v[:10], fmt).date()
                    except ValueError:
                        continue
    return None


def card_identity(card: dict) -> str:
    """The JustTCG id for a card object, tolerating either field name."""
    return str(card.get("id") or card.get("cardId") or "")


async def search_card(client: httpx.AsyncClient, name: str, game: str,
                      set_name: str = "", card_number: str = "") -> dict | None:
    """Search JustTCG for a card; returns the best-matching card object or None."""
    slug = GAME_SLUGS.get(game)
    if not slug:
        return None
    params = {"q": name, "game": slug, "limit": 20}
    resp = await client.get(f"{JUSTTCG_API_BASE}/cards", params=params, headers=_headers())
    if resp.status_code == 401:
        raise JustTCGError("JustTCG rejected the API key (401)")
    if resp.status_code != 200:
        logger.warning("JustTCG search %r (%s) -> HTTP %s: %s",
                       name, slug, resp.status_code, resp.text[:200])
        return None
    payload = resp.json()
    cards = payload.get("data") if isinstance(payload, dict) else payload
    if not isinstance(cards, list) or not cards:
        return None

    def match_rank(c: dict) -> tuple:
        cname = str(c.get("name") or "").lower()
        cset = str(c.get("set") if isinstance(c.get("set"), str) else (c.get("set") or {}).get("name") or c.get("set_name") or "").lower()
        cnum = str(c.get("number") or c.get("card_number") or "").lower()
        name_exact = cname == name.lower()
        set_hit = bool(set_name) and set_name.lower() in cset
        num_hit = bool(card_number) and card_number.lower() in cnum
        return (num_hit, set_hit, name_exact)

    return sorted(cards, key=match_rank, reverse=True)[0]


async def fetch_cards_by_ids(client: httpx.AsyncClient, ids: list) -> dict:
    """Fetch current pricing for known JustTCG ids, batched where possible.

    Tries the batch POST endpoint first (100 ids/request); falls back to
    per-id GETs if the batch shape is rejected. Returns {id: card_obj}.
    """
    out: dict = {}
    for i in range(0, len(ids), BATCH_SIZE):
        chunk = ids[i:i + BATCH_SIZE]
        try:
            resp = await client.post(f"{JUSTTCG_API_BASE}/cards",
                                     json=[{"cardId": cid} for cid in chunk],
                                     headers=_headers())
            if resp.status_code == 200:
                payload = resp.json()
                cards = payload.get("data") if isinstance(payload, dict) else payload
                if isinstance(cards, list):
                    for c in cards:
                        cid = card_identity(c)
                        if cid:
                            out[cid] = c
                    continue
            logger.warning("JustTCG batch lookup -> HTTP %s (falling back to per-id): %s",
                           resp.status_code, resp.text[:200])
        except Exception:
            logger.exception("JustTCG batch lookup failed (falling back to per-id)")
        # Per-id fallback
        for cid in chunk:
            try:
                r = await client.get(f"{JUSTTCG_API_BASE}/cards",
                                     params={"cardId": cid}, headers=_headers())
                if r.status_code != 200:
                    logger.warning("JustTCG per-id %s -> HTTP %s", cid, r.status_code)
                    continue
                payload = r.json()
                cards = payload.get("data") if isinstance(payload, dict) else payload
                if isinstance(cards, list) and cards:
                    out[cid] = cards[0]
                elif isinstance(cards, dict):
                    out[cid] = cards
            except Exception:
                logger.exception("JustTCG per-id fetch failed for %s", cid)
    return out


async def fetch_pokemon_release_date(client: httpx.AsyncClient, name: str,
                                     set_name: str = "") -> date | None:
    """Release date for a Pokémon card via pokemontcg.io (free)."""
    key = os.getenv("POKEMON_TCG_API_KEY", "")
    headers = {"X-Api-Key": key} if key else {}
    q = f'name:"{name}"'
    if set_name:
        q += f' set.name:"{set_name}"'
    try:
        resp = await client.get(POKEMONTCG_API,
                                params={"q": q, "pageSize": 1, "orderBy": "-set.releaseDate"},
                                headers=headers)
        if resp.status_code != 200:
            return None
        results = resp.json().get("data", [])
        if not results:
            return None
        raw = (results[0].get("set") or {}).get("releaseDate")
        if raw:
            return datetime.strptime(raw[:10], "%Y/%m/%d").date()
    except Exception:
        logger.exception("pokemontcg.io release-date lookup failed for %r", name)
    return None
