"""Gundam Card Game cards and prices — TCGCSV (tcgcsv.com), a daily copy of
TCGplayer's catalogue.

Fills the same catalog_cards table the other games do, with game = 'gundam'.
Nothing here writes catalog rows: main.py runs the sync and catalog.py does
the writes — the same split as lorcana.py. The only table this module touches
is `api_cache` (shared with optcg.py and lorcana.py, keyed by `api`).

WHY NOT gcgapi.com, the API first looked at: it has no prices and no
TCGplayer ids, and its pictures are links to Bandai's site, which sends
Cross-Origin-Resource-Policy: same-site (browsers refuse them on our site).

WHAT TCGCSV DOES. Checked against real responses on 2026-11-15:

  * No key. It is rebuilt once a day (about 20:00 UTC) and asks clients to:
    sync at most once a day, read /last-updated.txt first and only pull when
    it has changed, send a custom User-Agent, pause 100 ms between requests,
    and never call it from a visitor-facing page. The sync follows all of
    that: one tiny request to see whether anything changed, then one for the
    set list and two per set (cards, prices) — about 58 in all.
  * Gundam is TCGplayer category 86. A "group" is a set: `abbreviation` is the
    set code (GD01, ST01, EB01, GCG-PR ...), `publishedOn` its release date.
  * A group's products are its cards AND its sealed product. Cards are the
    ones with a `Number` in `extendedData`; the rest are skipped.
  * Every print is its own product with its own name: "Gundam (GD01-001)
    (LR++)", "Argama (SP) (R+)", "Freedom Gundam (Newtype Challenge 2026
    Mission 1)". A reprint keeps its original card number, so GD02-129 can
    sit in the GD06 group. Names were unique inside every group sampled.
  * Prices are one row per product and finish (`subTypeName`: Normal or
    Holofoil). Every card sampled had exactly one. `marketPrice` is the price
    we show; when it is missing the card has no price (low/mid/high asking
    prices are not used — they swing wildly on rare promos).
  * Pictures are on tcgplayer-cdn.tcgplayer.com, the host Pokemon's pictures
    already come from, so the CSP needs no change.
"""

import os
import re
import json
import asyncio
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx

logger = logging.getLogger("dashboard.gundam")

GAME = "gundam"
# TCGplayer's Gundam category is the English release.
LANGUAGE = "english"

BASE = "https://tcgcsv.com"
CATEGORY_ID = 86

# Card images. Written to the DB and rendered as <img src> for every visitor,
# so only https on this host is accepted. Already in the CSP's img-src in
# main.py (Pokemon's pictures use it); the two must stay in step.
IMAGE_HOSTS = ("tcgplayer-cdn.tcgplayer.com",)

# The price_source stored with every Gundam price: TCGplayer's market price.
PRICE_SOURCE = "tcgplayer"
# TCGplayer's name for the regular finish; any other finish becomes the row's
# `version` ("Holofoil").
NORMAL_FINISH = "Normal"


def _env_float(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, "") or default)
    except ValueError:
        return default
    return value if value == value and value not in (float("inf"), float("-inf")) else default


# TCGCSV is rebuilt once a day and asks for one sync a day, so this can be
# raised with the env var but never lowered below a day.
SYNC_EVERY_H = max(24.0, _env_float("GUNDAM_SYNC_EVERY_H", 24.0))
# Pause between requests. TCGCSV asks for 100 ms; this is well clear of it.
REQUEST_GAP_S = max(0.1, _env_float("GUNDAM_REQUEST_GAP_S", 0.25))
# TCGCSV may block generic or missing User-Agents and asks for one that names
# the application. Set GUNDAM_USER_AGENT to something that identifies the site.
USER_AGENT = (os.getenv("GUNDAM_USER_AGENT", "").strip() or "CardCatalogSync/1.0")[:100]

API_NAME = "tcgcsv_gundam"

# Set grouping for the catalog's set picker, in the same shape card_eras uses
# for Pokemon's eras: (key, label).
ERA_ORDER = [
    ("gundam_boosters", "Booster packs"),
    ("gundam_extra", "Extra boosters"),
    ("gundam_starters", "Starter decks"),
    ("gundam_promos", "Promos & other"),
]

# The same statement optcg.py and lorcana.py run. Repeated here so Gundam
# doesn't depend on another game's module having created the table first.
GUNDAM_SCHEMA = [
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


async def ensure_gundam_schema(pool) -> None:
    """Create the cache table if it isn't there. Never raises."""
    for statement in GUNDAM_SCHEMA:
        try:
            async with pool.acquire() as conn:
                await conn.execute(statement)
        except Exception:
            logger.exception("Gundam schema statement failed: %s",
                             " ".join(statement.split())[:80])


def available() -> bool:
    """TCGCSV needs no key, so the sync is on unless GUNDAM_SYNC_DISABLED is set."""
    return os.getenv("GUNDAM_SYNC_DISABLED", "").strip().lower() not in ("1", "true", "yes", "on")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------
# Requests
# --------------------------------------------------------------------------

async def _request(client: httpx.AsyncClient, path: str, label: str) -> tuple:
    """(response, status) for one GET. status: ok | not_found | rate_limited | error.

    Never raises. Waits REQUEST_GAP_S first, so back-to-back calls stay well
    inside what TCGCSV asks for. `rate_limited` (a 429) means "later, not
    broken": the sync stops cleanly and resumes on its next pass.
    """
    await asyncio.sleep(REQUEST_GAP_S)
    try:
        resp = await client.get(BASE + path, headers={"User-Agent": USER_AGENT})
    except Exception as exc:
        # Type only: the message could echo request details.
        logger.warning("TCGCSV %s request failed (%s)", label, type(exc).__name__)
        return None, "error"
    if resp.status_code == 429:
        logger.warning("TCGCSV rate limited (429) at %s — stopping this run", label)
        return None, "rate_limited"
    if resp.status_code == 404:
        return None, "not_found"
    if resp.status_code != 200:
        logger.warning("TCGCSV %s HTTP %s: %s", label, resp.status_code, resp.text[:200])
        return None, "error"
    return resp, "ok"


async def _get_results(client: httpx.AsyncClient, path: str, label: str) -> tuple:
    """(rows, status) for one of TCGCSV's JSON files: the dicts in `results`."""
    resp, status = await _request(client, path, label)
    if status != "ok":
        return [], status
    try:
        body = resp.json()
    except Exception:
        logger.warning("TCGCSV %s returned non-JSON", label)
        return [], "error"
    rows = body.get("results") if isinstance(body, dict) else None
    if not isinstance(rows, list):
        logger.warning("TCGCSV %s had no results list", label)
        return [], "error"
    return [r for r in rows if isinstance(r, dict)], "ok"


async def fetch_build_stamp(client: httpx.AsyncClient) -> tuple:
    """When TCGCSV was last rebuilt, as its own text: (stamp, status). 1 small request."""
    resp, status = await _request(client, "/last-updated.txt", "last-updated")
    if status != "ok":
        return "", status
    stamp = resp.text.strip()[:40]
    return stamp, ("ok" if stamp else "error")


async def fetch_sets(client: httpx.AsyncClient) -> tuple:
    """Every set, slimmed to what we keep: ([{id, group_id, name, released}], status)."""
    rows, status = await _get_results(client, f"/tcgplayer/{CATEGORY_ID}/groups", "groups")
    if status != "ok":
        return [], status
    sets = parse_sets(rows)
    return sets, ("ok" if sets else "empty")


async def fetch_set_cards(client: httpx.AsyncClient, group_id) -> tuple:
    """One set's products and price rows as TCGCSV gives them:
    (products, prices, status). 2 requests. A set with no products is `empty`."""
    try:
        gid = int(group_id)
    except (TypeError, ValueError):
        return [], [], "error"
    products, status = await _get_results(
        client, f"/tcgplayer/{CATEGORY_ID}/{gid}/products", f"group {gid} products")
    if status != "ok":
        return [], [], status
    if not products:
        return [], [], "empty"
    prices, status = await _get_results(
        client, f"/tcgplayer/{CATEGORY_ID}/{gid}/prices", f"group {gid} prices")
    if status != "ok":
        return [], [], status
    return products, prices, "ok"


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
        logger.exception("Gundam cache read failed for %s", resource)
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
        logger.exception("Gundam cache write failed for %s", resource)
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

# Set codes as TCGplayer writes them: GD01, ST14, EB01, GCG-PR, GD01_b, RP.
_SET_CODE = re.compile(r"[A-Za-z0-9_-]{1,20}")


def _date(value) -> str:
    """'2026-10-30' from '2026-10-30T00:00:00', or '' when it isn't a date."""
    text = str(value or "").strip()[:10]
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return ""
    return text


def parse_sets(rows) -> list:
    """[{id, group_id, name, released}] for TCGCSV's groups.

    `id` is the set code (the group's abbreviation) and is what we store as
    set_id; a group without a usable or unique code falls back to its numeric
    group id. `name` carries the code in brackets, the way One Piece's sets
    do: "Newtype Rising [GD01]".
    """
    out, seen = [], set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        try:
            group_id = int(row.get("groupId"))
        except (TypeError, ValueError):
            continue
        code = str(row.get("abbreviation") or "").strip()
        has_code = bool(_SET_CODE.fullmatch(code)) and code not in seen
        set_id = code if has_code else str(group_id)
        if set_id in seen:
            continue
        seen.add(set_id)
        title = " ".join(str(row.get("name") or "").split())[:180] or set_id
        out.append({"id": set_id, "group_id": group_id,
                    "name": f"{title} [{code}]" if has_code else title,
                    "released": _date(row.get("publishedOn"))})
    return out


def set_era(set_id) -> str:
    """The picker group for a set code: GDnn booster packs, EBnn extra
    boosters, STnn starter decks; anything else (promos, tokens, Edition Beta,
    deck build boxes) last. The whole code must match, so GD01_b (Edition
    Beta) is not a booster pack."""
    sid = str(set_id or "").upper()
    if re.fullmatch(r"GD\d+", sid):
        return "gundam_boosters"
    if re.fullmatch(r"EB\d+", sid):
        return "gundam_extra"
    if re.fullmatch(r"ST\d+", sid):
        return "gundam_starters"
    return "gundam_promos"


def is_main_set(set_id) -> bool:
    """The numbered booster packs (GD01, GD02 ...) are the main releases."""
    return set_era(set_id) == "gundam_boosters"


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
    put a booster pack ahead of anything else released the same day."""
    rows = [s for s in sets or [] if isinstance(s, dict) and s.get("id")]
    rows.sort(key=lambda s: str(s["id"]))
    rows.sort(key=lambda s: not is_main_set(s["id"]))
    rows.sort(key=lambda s: s.get("released") or "", reverse=True)
    return rows


# --------------------------------------------------------------------------
# Cards
# --------------------------------------------------------------------------

def _price(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and number != float("inf") and number > 0 else None


def _image_url(row: dict):
    """The product's picture, only if it is https on IMAGE_HOSTS."""
    value = (row or {}).get("imageUrl")
    if not isinstance(value, str) or not value:
        return None
    parts = urlparse(value)
    if parts.scheme == "https" and (parts.hostname or "").lower() in IMAGE_HOSTS:
        return value[:500]
    return None


def _details(row: dict) -> dict:
    """A product's extendedData as {name: value}: Number, Rarity, CardType ..."""
    out = {}
    for item in (row or {}).get("extendedData") or []:
        if isinstance(item, dict) and item.get("name"):
            out[str(item["name"])] = item.get("value")
    return out


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(text or "").lower())[:12] or "x"


def prices_by_product(price_rows) -> dict:
    """{productId: [price rows]} in the order TCGCSV lists them."""
    out = {}
    for row in price_rows or []:
        if isinstance(row, dict) and isinstance(row.get("productId"), int):
            out.setdefault(row["productId"], []).append(row)
    return out


def parse_card(row: dict, set_name: str, prices: dict) -> list:
    """The catalog rows for one TCGCSV product, in catalog.upsert_set_cards' shape.

    [] for sealed product (nothing in a group but its cards has a `Number`)
    and for anything without a name or product id. A card normally has one
    price row and so becomes one catalog row; if TCGplayer ever lists a card
    in two finishes, each finish gets its own row, told apart by `version`
    and by a suffix on `source_id` (the page keys rows by source_id).
    """
    if not isinstance(row, dict):
        return []
    product_id = row.get("productId")
    details = _details(row)
    number = " ".join(str(details.get("Number") or "").split())[:40]
    name = " ".join(str(row.get("name") or "").split())[:200]
    if not isinstance(product_id, int) or isinstance(product_id, bool) or not number or not name:
        return []
    base = {
        "name": name,
        "card_number": number,
        "variant": " ".join(str(details.get("Rarity") or "").split())[:40],
        "tcgplayer_id": str(product_id),
        "tcgplayer_id_verified": True,
        "set_name": str(set_name or "").strip()[:200],
        "image_url": _image_url(row),
        "printing_prices": None,
        "card_type": (" ".join(str(details.get("CardType")).split())[:40] or None)
                     if details.get("CardType") else None,
    }
    out, used = [], set()
    for price_row in prices.get(product_id) or [{}]:
        finish = " ".join(str(price_row.get("subTypeName") or NORMAL_FINISH).split())[:80]
        version = "" if finish == NORMAL_FINISH else finish
        if version in used:
            continue
        used.add(version)
        price = _price(price_row.get("marketPrice"))
        out.append({**base, "version": version,
                    "source_id": str(product_id) if not out else f"{product_id}_{_slug(version)}",
                    "raw_price": price,
                    "price_source": PRICE_SOURCE if price is not None else None})
    return out
