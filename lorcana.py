"""Disney Lorcana cards and prices — Lorcast (lorcast.com).

Fills the same catalog_cards table Pokemon, Riftbound and One Piece do, with
game = 'lorcana'. Nothing here writes catalog rows: main.py runs the sync and
catalog.py does the writes — the same split as optcg.py. The only table this
module touches is `api_cache` (shared with optcg.py, keyed by `api`).

WHAT THE LIVE API DOES. Checked against real responses on 2026-11-15:

  * No key. Lorcast asks for 50-100 ms between requests and for data to be
    kept at least 24 hours; prices change once a day. So the sync runs once a
    day, one request for the set list plus one per set (about 28), with a
    pause between requests, and stops on a 429.
  * /sets gives every set with a real release date, promo and special sets
    included. A set's `code` ("1", "14", "P1", "D23") is what /sets/{code}/cards
    takes and is what we store as set_id.
  * A card has a `name` ("Ariel") and a `version` ("On Human Legs"); the full
    card name members know is "Ariel - On Human Legs". Enchanted, Epic and
    Iconic cards are their own rows with their own collector number.
  * Every card carries two prices, `prices.usd` (normal) and `prices.usd_foil`
    (foil), either of which can be missing: an Enchanted card only exists in
    foil. Each finish is stored as its own catalog row, told apart by
    `version` ("" for normal, "Foil"), so both can be sorted and filtered by
    price like any other card (see parse_card).
  * Pictures are AVIF files on cards.lorcast.io, which has no request limit
    and sends no header stopping other sites from showing them.
  * Lorcast's footer says the card content is used under Ravensburger's
    Community Code Policy. That policy has NOT been read for this project.
"""

import os
import json
import asyncio
import logging
from datetime import datetime, timezone
from urllib.parse import quote, urlparse

import httpx

logger = logging.getLogger("dashboard.lorcana")

GAME = "lorcana"
# Lorcast's cards are English ("en"); anything else is skipped.
LANGUAGE = "english"

BASE = "https://api.lorcast.com/v0"

# Card images. Written to the DB and rendered as <img src> for every visitor,
# so only https on this host is accepted. Must stay in step with the CSP's
# img-src in main.py.
IMAGE_HOSTS = ("cards.lorcast.io",)

# The price_source stored with every Lorcana price.
PRICE_SOURCE = "lorcast"
# The `version` of a card's foil row; the normal row's version is "".
FOIL = "Foil"
# Added to a card's Lorcast id to make the foil row's source_id, so the two
# finishes of one card never share an id (the page keys rows by source_id).
FOIL_ID_SUFFIX = "_f"


def _env_float(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, "") or default)
    except ValueError:
        return default
    return value if value == value and value not in (float("inf"), float("-inf")) else default


# Lorcast asks for data to be kept at least 24 hours, so this can be raised
# with the env var but never lowered below a day.
SYNC_EVERY_H = max(24.0, _env_float("LORCANA_SYNC_EVERY_H", 24.0))
# Pause between requests. Lorcast asks for 50-100 ms; this is well clear of it.
REQUEST_GAP_S = max(0.1, _env_float("LORCANA_REQUEST_GAP_S", 0.25))

API_NAME = "lorcast"

# Set grouping for the catalog's set picker, in the same shape card_eras uses
# for Pokemon's eras: (key, label).
ERA_ORDER = [
    ("lorcana_sets", "Main sets"),
    ("lorcana_promos", "Promos & special sets"),
]

# The same statement optcg.py runs. Repeated here so Lorcana doesn't depend on
# the One Piece module having created the table first.
LORCANA_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS api_cache (
        api        TEXT        NOT NULL,
        resource   TEXT        NOT NULL,
        fetched_at TIMESTAMPTZ NOT NULL,
        body       JSONB       NOT NULL,
        PRIMARY KEY (api, resource)
    )
    """,
]


async def ensure_lorcana_schema(pool) -> None:
    """Create the cache table if it isn't there. Never raises."""
    for statement in LORCANA_SCHEMA:
        try:
            async with pool.acquire() as conn:
                await conn.execute(statement)
        except Exception:
            logger.exception("Lorcana schema statement failed: %s",
                             " ".join(statement.split())[:80])


def available() -> bool:
    """Lorcast needs no key, so the sync is on unless LORCANA_SYNC_DISABLED is set."""
    return os.getenv("LORCANA_SYNC_DISABLED", "").strip().lower() not in ("1", "true", "yes", "on")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------
# Requests
# --------------------------------------------------------------------------

async def _get(client: httpx.AsyncClient, path: str, label: str) -> tuple:
    """(body, status) for one GET. status: ok | not_found | rate_limited | error.

    Never raises. Waits REQUEST_GAP_S first, so back-to-back calls stay well
    inside what Lorcast asks for. `rate_limited` (a 429) means "later, not
    broken": the sync stops cleanly and resumes on its next pass.
    """
    await asyncio.sleep(REQUEST_GAP_S)
    try:
        resp = await client.get(BASE + path, headers={"Accept": "application/json"})
    except Exception as exc:
        # Type only: the message could echo request details.
        logger.warning("Lorcast %s request failed (%s)", label, type(exc).__name__)
        return None, "error"
    if resp.status_code == 429:
        logger.warning("Lorcast rate limited (429) at %s — stopping this run", label)
        return None, "rate_limited"
    if resp.status_code == 404:
        return None, "not_found"
    if resp.status_code != 200:
        logger.warning("Lorcast %s HTTP %s: %s", label, resp.status_code, resp.text[:200])
        return None, "error"
    try:
        return resp.json(), "ok"
    except Exception:
        logger.warning("Lorcast %s returned non-JSON", label)
        return None, "error"


def _rows(body) -> list:
    """The list of dicts in a response: a bare list, or {"results": [...]}."""
    data = body.get("results") if isinstance(body, dict) else body
    return [d for d in data if isinstance(d, dict)] if isinstance(data, list) else []


async def fetch_sets(client: httpx.AsyncClient) -> tuple:
    """Every set, slimmed to what we keep: ([{id, name, released}], status). 1 request."""
    body, status = await _get(client, "/sets", "sets")
    if status != "ok":
        return [], status
    sets = [s for s in (parse_set(r) for r in _rows(body)) if s]
    return sets, ("ok" if sets else "empty")


async def fetch_set_cards(client: httpx.AsyncClient, set_id: str) -> tuple:
    """One set's cards as the API gives them: (rows, status). 1 request."""
    if not _valid_set_id(set_id):
        return [], "error"
    body, status = await _get(client, f"/sets/{quote(set_id, safe='')}/cards", f"set {set_id}")
    if status != "ok":
        return [], status
    rows = _rows(body)
    return rows, ("ok" if rows else "empty")


# --------------------------------------------------------------------------
# Cache (Postgres)
# --------------------------------------------------------------------------

async def cached(pool, resource: str) -> tuple:
    """(body, fetched_at) for a cached resource, or (None, None). Never raises."""
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT body, fetched_at FROM api_cache WHERE api = $1 AND resource = $2",
                API_NAME, resource)
    except Exception:
        logger.exception("Lorcana cache read failed for %s", resource)
        return None, None
    if not row:
        return None, None
    body = row["body"]
    if isinstance(body, (str, bytes)):
        try:
            body = json.loads(body)
        except ValueError:
            return None, None
    return body, row["fetched_at"]


async def store_cache(pool, resource: str, body) -> bool:
    """Save a resource with fetched_at = now. True when stored. Never raises."""
    try:
        payload = json.dumps(body)
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO api_cache (api, resource, fetched_at, body) "
                "VALUES ($1, $2, NOW(), $3::JSONB) "
                "ON CONFLICT (api, resource) DO UPDATE "
                "SET fetched_at = NOW(), body = EXCLUDED.body",
                API_NAME, resource, payload)
        return True
    except Exception:
        logger.exception("Lorcana cache write failed for %s", resource)
        return False


def older_than_a_sync(at, now=None) -> bool:
    """True when `at` is missing or SYNC_EVERY_H old or more."""
    if at is None:
        return True
    now = now or _utc_now()
    if at.tzinfo is None:
        at = at.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return (now - at).total_seconds() >= SYNC_EVERY_H * 3600


# --------------------------------------------------------------------------
# Sets
# --------------------------------------------------------------------------

def _valid_set_id(set_id) -> bool:
    """Set codes are short and plain ("1", "P1", "D23", "Coconut")."""
    text = str(set_id or "")
    return 0 < len(text) <= 20 and all(c.isalnum() or c in "-_" for c in text)


def _date(value) -> str:
    """'2026-10-16' from the API's date, or '' when it isn't one."""
    text = str(value or "").strip()[:10]
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return ""
    return text


def parse_set(row: dict):
    """{id, name, released} for one of the API's sets, or None without a usable code."""
    code = str((row or {}).get("code") or "").strip()
    if not _valid_set_id(code):
        return None
    name = " ".join(str(row.get("name") or "").split())[:200]
    return {"id": code, "name": name or code, "released": _date(row.get("released_at"))}


def is_main_set(set_id) -> bool:
    """The numbered sets ("1" ... "14") are the main releases; lettered codes
    (P1, D23, cp, Q3 ...) are promos, event cards and special products."""
    return str(set_id or "").strip().isdigit()


def set_era(set_id) -> str:
    return "lorcana_sets" if is_main_set(set_id) else "lorcana_promos"


def annotate_sets(sets: list, name_key: str = "set_name") -> list:
    """Add `era`/`era_label` to each set dict, in place — card_eras.annotate's
    shape. The era comes from the set's id (`set_id`, or `id` for the cached
    list); `name_key` is accepted for the same call shape as the other games."""
    labels = dict(ERA_ORDER)
    for entry in sets:
        key = set_era(entry.get("set_id") or entry.get("id"))
        entry["era"] = key
        entry["era_label"] = labels[key]
    return sets


def set_release_order(sets) -> list:
    """The cached sets, newest release first. Sets with no date go last; ties
    put a main set ahead of a promo set released the same day."""
    rows = [s for s in sets or [] if isinstance(s, dict) and s.get("id")]
    rows.sort(key=lambda s: str(s["id"]))
    rows.sort(key=lambda s: not is_main_set(s["id"]))
    rows.sort(key=lambda s: s.get("released") or "", reverse=True)
    return rows


# --------------------------------------------------------------------------
# Cards
# --------------------------------------------------------------------------

_RARITIES = {"super_rare": "Super Rare"}


def _rarity(value) -> str:
    """'Super_rare' -> 'Super Rare'; other rarities as the API writes them."""
    text = str(value or "").strip()
    return _RARITIES.get(text.lower(), text.replace("_", " "))[:40]


def _price(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and number != float("inf") and number > 0 else None


def _image_url(row: dict):
    """The card's normal-size picture, only if it is https on IMAGE_HOSTS."""
    digital = ((row or {}).get("image_uris") or {}).get("digital") or {}
    value = digital.get("normal") or digital.get("large") or digital.get("small")
    if not isinstance(value, str) or not value:
        return None
    parts = urlparse(value)
    if parts.scheme == "https" and (parts.hostname or "").lower() in IMAGE_HOSTS:
        return value[:500]
    return None


def _card_name(row: dict) -> str:
    """'Ariel - On Human Legs': the name plus its subtitle, when it has one."""
    name = " ".join(str(row.get("name") or "").split())
    subtitle = " ".join(str(row.get("version") or "").split())
    return (f"{name} - {subtitle}" if name and subtitle else name)[:200]


def parse_card(row: dict, set_name: str) -> list:
    """The catalog rows for one API card, in catalog.upsert_set_cards' shape.

    One row per finish that has a price: the normal printing (`version` "")
    and the foil (`version` "Foil"). A card with no price at all still gets
    its normal row, so it can be found; a foil-only card (Enchanted, most
    promos) gets just the foil row. [] for anything without a name or id, or
    not in English.
    """
    if not isinstance(row, dict):
        return []
    lang = str(row.get("lang") or "en").strip().lower()
    card_id = str(row.get("id") or "").strip()
    name = _card_name(row)
    if lang != "en" or not card_id or not name or len(card_id) > 40 - len(FOIL_ID_SUFFIX):
        return []
    prices = row.get("prices") if isinstance(row.get("prices"), dict) else {}
    normal, foil = _price(prices.get("usd")), _price(prices.get("usd_foil"))
    tcg = row.get("tcgplayer_id")
    kinds = row.get("type") if isinstance(row.get("type"), list) else [row.get("type")]
    base = {
        "name": name,
        "card_number": str(row.get("collector_number") or "").strip()[:40],
        "variant": _rarity(row.get("rarity")),
        "tcgplayer_id": str(tcg) if isinstance(tcg, int) and not isinstance(tcg, bool) else "",
        "tcgplayer_id_verified": isinstance(tcg, int) and not isinstance(tcg, bool),
        "set_name": str(set_name or "").strip()[:200],
        "image_url": _image_url(row),
        "printing_prices": None,
        "card_type": ", ".join(str(k).strip() for k in kinds if k)[:40] or None,
    }
    out = []
    if normal is not None or foil is None:
        out.append({**base, "version": "", "source_id": card_id, "raw_price": normal,
                    "price_source": PRICE_SOURCE if normal is not None else None})
    if foil is not None:
        out.append({**base, "version": FOIL, "source_id": card_id + FOIL_ID_SUFFIX,
                    "raw_price": foil, "price_source": PRICE_SOURCE})
    return out
