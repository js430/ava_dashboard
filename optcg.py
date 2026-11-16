"""One Piece Card Game cards and prices — the "OPTCG API" (opbindr.com).

Fills the same catalog_cards table Pokemon and Riftbound do, with game =
'one_piece'. Nothing here writes catalog rows: main.py runs the sync and
catalog.py does the writes — the same split as riftbound.py. The only tables
this module touches are its own two, `api_request_counts` and `api_cache`.

WHAT THE LIVE API DOES. Checked against real responses on 2026-11-15:

  * 60 requests a minute and 2,000 a day (the day resets at 00:00 UTC), and the
    key is shared with other work. So we keep our own lower ceilings (50 a
    minute, 1,800 a day), count every request in Postgres BEFORE sending it
    (a restart or a second worker can't forget the count), and stop for the day
    when the API's own "remaining today" header says it is nearly spent.
  * The full card list (/cards/index, ~2 MB) carries every card's price and
    which languages it exists in, but NOT which set it belongs to and no
    pictures. The API asks clients to keep it for a week or more, so it is
    cached in Postgres for at least 7 days. /sets/{id}/cards carries set
    membership, pictures and TCGplayer ids, one request per set; the list
    prices come from the index.
  * Cards come in prints: `OP01-120` is the base card, `OP01-120_p1`, `_p2`
    are alternate-art parallels, `_r1` a reprint, `_jp1` a Japanese-only print.
    The same name + number + rarity therefore exists several times in one set,
    which is why catalog_cards carries `version` in its identity (see
    version_labels).
  * No release dates anywhere. Newest-first ordering is an approximation (see
    set_release_order).
  * `price` is USD; `price_source` says where it came from (mostly tcgplayer,
    but also dotgg, positional, ebay, web_tcgplayer, manual ...).

The API key is read from OPTCG_API_KEY, sent only as the X-API-Key header, and
never logged or returned from anything here.
"""

import os
import re
import json
import time
import asyncio
import logging
from collections import deque
from datetime import datetime, timezone
from urllib.parse import quote, urlparse

import httpx

logger = logging.getLogger("dashboard.optcg")

GAME = "one_piece"
# The catalog is English-only: cards that exist only in Japanese are skipped.
LANGUAGE = "english"

BASE = "https://optcg-api.arjunbansal-ai.workers.dev"

# Card images. Written to the DB and rendered as <img src> for every visitor,
# so only https on these hosts is accepted. Must stay in step with the CSP's
# img-src in main.py.
IMAGE_HOSTS = ("en.onepiece-cardgame.com", "optcg-api.arjunbansal-ai.workers.dev")


def _env_float(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, "") or default)
    except ValueError:
        return default
    return value if value == value and value not in (float("inf"), float("-inf")) else default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "") or default)
    except ValueError:
        return default


# The API asks for the card list to be cached 7+ days, so this can be raised
# with the env var but never lowered below a week.
INDEX_TTL_DAYS = max(7.0, _env_float("OPTCG_INDEX_TTL_DAYS", 7.0))
# A set's cards are re-fetched at least this often even if its size is unchanged
# (pictures and TCGplayer ids get fixed upstream now and then).
SET_REFRESH_DAYS = _env_float("OPTCG_SET_REFRESH_DAYS", 28.0)

# Our own ceilings, below the API's 60 / 2,000.
MAX_PER_MIN = _env_int("OPTCG_MAX_PER_MIN", 50)
MAX_PER_DAY = _env_int("OPTCG_MAX_PER_DAY", 1800)
# Stop for the rest of the UTC day when the API says fewer than this are left.
MIN_REMAINING_DAY = 200
# Used when a 429 arrives without a usable Retry-After.
DEFAULT_RETRY_AFTER_S = 60.0

API_NAME = "optcg"

# Set grouping for the catalog's set picker, newest-first, in the same shape
# card_eras uses for Pokemon's eras: (key, label).
ERA_ORDER = [
    ("one_piece_boosters", "Booster packs"),
    ("one_piece_extra", "Extra & premium boosters"),
    ("one_piece_starters", "Starter decks"),
    ("one_piece_promos", "Promos & other"),
]

OPTCG_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS api_request_counts (
        api      TEXT NOT NULL,
        day      DATE NOT NULL,
        requests INT  NOT NULL DEFAULT 0,
        PRIMARY KEY (api, day)
    )
    """,
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


async def ensure_optcg_schema(pool) -> None:
    """Create this module's two tables. Each statement runs on its own so one
    failure can't skip the rest (the 2026-08-03 incident: a single try around
    the whole list left every later table uncreated)."""
    for statement in OPTCG_SCHEMA:
        try:
            async with pool.acquire() as conn:
                await conn.execute(statement)
        except Exception:
            logger.exception("OPTCG schema statement failed: %s", " ".join(statement.split())[:80])


def available() -> bool:
    return bool(os.getenv("OPTCG_API_KEY"))


# --------------------------------------------------------------------------
# Rate limiting
# --------------------------------------------------------------------------

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RateLimiter:
    """Decides whether a request may go out, and records what came back.

    `acquire(pool)` returns "ok" (go ahead), "quota" (a daily ceiling says
    stop) or "rate_limited" (the API told us to wait). It WAITS when only the
    per-minute window is full — that clears in under a minute — but refuses
    outright for anything that clears later, so a sync stops cleanly and
    resumes on its next pass instead of hanging.

    The daily count lives in Postgres and is bumped by one atomic statement
    BEFORE the request is sent, so concurrent callers and restarts can't
    undercount. A request that is then refused or fails still counts: the
    cost of overcounting is a slightly earlier stop, the cost of
    undercounting is a burned shared key.

    `clock` (monotonic seconds), `sleep` and `now` (UTC datetime) are
    injectable so the tests can run on a fake clock.
    """

    def __init__(self, per_min=None, per_day=None, clock=time.monotonic,
                 sleep=asyncio.sleep, now=_utc_now):
        self.per_min = MAX_PER_MIN if per_min is None else per_min
        self.per_day = MAX_PER_DAY if per_day is None else per_day
        self._clock, self._sleep, self._now = clock, sleep, now
        self._lock = asyncio.Lock()
        self._sent = deque()          # monotonic times of requests in the last minute
        self._blocked_until = 0.0     # monotonic; set by a 429
        self._closed_day = None       # UTC date the daily budget is spent for
        self._logged_remaining = False

    def _today(self):
        return self._now().date()

    def reset(self) -> None:
        """Forget in-process state (tests, and a fresh run after a long idle)."""
        self._sent.clear()
        self._blocked_until = 0.0
        self._closed_day = None
        self._logged_remaining = False

    async def acquire(self, pool) -> str:
        async with self._lock:
            now = self._clock()
            if now < self._blocked_until:
                return "rate_limited"
            today = self._today()
            if self._closed_day == today:
                return "quota"
            # Per-minute window: wait for the oldest request to age out.
            while True:
                now = self._clock()
                while self._sent and now - self._sent[0] >= 60.0:
                    self._sent.popleft()
                if len(self._sent) < self.per_min:
                    break
                await self._sleep(max(self._sent[0] + 60.0 - now, 0.01))
            # The day may have rolled over, or a 429 arrived, while we waited.
            if self._clock() < self._blocked_until:
                return "rate_limited"
            today = self._today()
            if self._closed_day == today:
                return "quota"
            try:
                async with pool.acquire() as conn:
                    used = await conn.fetchval(
                        "INSERT INTO api_request_counts (api, day, requests) VALUES ($1, $2, 1) "
                        "ON CONFLICT (api, day) DO UPDATE "
                        "SET requests = api_request_counts.requests + 1 RETURNING requests",
                        API_NAME, today)
            except Exception:
                # Can't count it, so don't send it: an unmetered request is the
                # one thing this class exists to prevent.
                logger.exception("OPTCG request counter failed — not sending")
                return "error"
            if used is None or used > self.per_day:
                self._closed_day = today
                logger.warning("OPTCG daily cap reached (%s of %s) — pausing until 00:00 UTC",
                               used, self.per_day)
                return "quota"
            self._sent.append(self._clock())
            return "ok"

    def note_response(self, status_code: int, headers) -> None:
        """Take in a response's status and rate-limit headers (never the key)."""
        remaining = headers.get("x-ratelimit-remaining-day")
        if remaining is not None and str(remaining).strip().isdigit():
            left = int(str(remaining).strip())
            if not self._logged_remaining:
                self._logged_remaining = True
                logger.info("OPTCG API: about %s request(s) left today (limit %s)",
                            left, headers.get("x-ratelimit-limit-day") or "?")
            if left < MIN_REMAINING_DAY:
                self._closed_day = self._today()
                logger.warning("OPTCG API: only %s request(s) left today — stopping until 00:00 UTC", left)
        if status_code == 429:
            try:
                wait = float(headers.get("retry-after"))
                if not wait > 0:
                    wait = DEFAULT_RETRY_AFTER_S
            except (TypeError, ValueError):
                wait = DEFAULT_RETRY_AFTER_S
            self._blocked_until = max(self._blocked_until, self._clock() + wait)
            logger.warning("OPTCG API rate limited (429) — pausing %.0f s", wait)


limiter = RateLimiter()


async def requests_today(pool) -> int:
    """How many requests we've spent today (UTC); 0 if it can't be read."""
    try:
        async with pool.acquire() as conn:
            value = await conn.fetchval(
                "SELECT requests FROM api_request_counts WHERE api = $1 AND day = $2",
                API_NAME, _utc_now().date())
        return int(value or 0)
    except Exception:
        logger.exception("OPTCG: couldn't read today's request count")
        return 0


# --------------------------------------------------------------------------
# Requests
# --------------------------------------------------------------------------

async def _get(client: httpx.AsyncClient, pool, path: str, label: str) -> tuple:
    """(body, status) for one GET. status: ok | not_found | rate_limited | quota | error.

    Never raises. `quota` means one of OUR ceilings (or the API's remaining
    count) says stop for today; `rate_limited` means the API answered 429 or
    is still inside a Retry-After. Both mean "later, not broken" — the sync
    stops cleanly and resumes on its next pass.
    """
    key = os.getenv("OPTCG_API_KEY")
    if not key:
        return None, "error"
    gate = await limiter.acquire(pool)
    if gate != "ok":
        return None, gate
    try:
        resp = await client.get(BASE + path, headers={"X-API-Key": key})
    except Exception as exc:
        # Type only: the message could echo request details.
        logger.warning("OPTCG %s request failed (%s)", label, type(exc).__name__)
        return None, "error"
    limiter.note_response(resp.status_code, resp.headers)
    if resp.status_code == 429:
        return None, "rate_limited"
    if resp.status_code == 404:
        return None, "not_found"
    if resp.status_code != 200:
        logger.warning("OPTCG %s HTTP %s: %s", label, resp.status_code, resp.text[:200])
        return None, "error"
    try:
        return resp.json(), "ok"
    except Exception:
        logger.warning("OPTCG %s returned non-JSON", label)
        return None, "error"


def _data_list(body) -> list:
    data = body.get("data") if isinstance(body, dict) else None
    return [d for d in data if isinstance(d, dict)] if isinstance(data, list) else []


async def fetch_sets(client: httpx.AsyncClient, pool) -> tuple:
    """Every set: ([{id, pack_id, label, card_count, type, ...}], status). 1 request."""
    body, status = await _get(client, pool, "/sets", "sets")
    if status != "ok":
        return [], status
    rows = [r for r in _data_list(body) if r.get("id") not in (None, "")]
    return rows, ("ok" if rows else "empty")


# The only index fields anything here reads; the rest of the ~2 MB is dropped
# before it is stored.
_INDEX_FIELDS = ("id", "name", "langs", "rarity", "variant_type", "parallel", "price", "price_source")


async def fetch_index(client: httpx.AsyncClient, pool) -> tuple:
    """The full card list, slimmed to _INDEX_FIELDS: (rows, status). 1 request, ~2 MB."""
    body, status = await _get(client, pool, "/cards/index", "cards/index")
    if status != "ok":
        return [], status
    rows = [{k: r.get(k) for k in _INDEX_FIELDS} for r in _data_list(body) if r.get("id")]
    return rows, ("ok" if rows else "empty")


async def fetch_set_cards(client: httpx.AsyncClient, pool, set_id) -> tuple:
    """Every print in one set (raw rows, status). 1 request."""
    sid = str(set_id or "").strip()
    if not sid:
        return [], "not_found"
    body, status = await _get(client, pool, f"/sets/{quote(sid, safe='')}/cards", f"set {sid} cards")
    if status != "ok":
        return [], status
    rows = _data_list(body)
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
        logger.exception("OPTCG cache read failed for %s", resource)
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
        logger.exception("OPTCG cache write failed for %s", resource)
        return False


def index_due(fetched_at, now=None) -> bool:
    """True when the card list was never fetched or is INDEX_TTL_DAYS old or more."""
    if fetched_at is None:
        return True
    now = now or _utc_now()
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return (now - fetched_at).total_seconds() >= INDEX_TTL_DAYS * 86400


# --------------------------------------------------------------------------
# Card index helpers
# --------------------------------------------------------------------------

def _base_id(card_id: str) -> str:
    return str(card_id).split("_", 1)[0]


def english_ids(index_rows) -> set:
    """Ids of cards that exist in English. Japanese-only prints (langs == ["ja"])
    are left out of the catalog."""
    out = set()
    for row in index_rows or []:
        langs = row.get("langs")
        if row.get("id") and isinstance(langs, list) and "en" in langs:
            out.add(str(row["id"]))
    return out


def _print_number(card_id: str) -> int:
    """`OP01-120_p10` -> 10, so p2 sorts before p10. No suffix -> 0."""
    match = re.search(r"_[A-Za-z]*(\d+)$", str(card_id))
    return int(match.group(1)) if match else 0


def version_labels(index_rows) -> dict:
    """{id: version label}: "" for a base card, otherwise what kind of print it is.

    A parallel is labelled with its variant_type ("Alternate Art", "Reprint",
    "Manga Art", "Serial"; "Parallel" if the API gives none), and the 2nd,
    3rd... print of the same kind of the same card get " 2", " 3"... so that
    name + number + rarity + version is unique inside a set — the catalog's
    identity. Numbers follow the id's numeric suffix, so _p1 < _p2 < _p10.

    Computed over the WHOLE index, never one set's cards, so a card's label
    doesn't change depending on which sets happen to have been fetched. Only
    prints that exist in English are counted: a Japanese-only print must not
    leave a gap ("Alternate Art 3" with no 2) in what members see.
    """
    groups = {}
    out = {}
    for row in index_rows or []:
        card_id = row.get("id")
        if not card_id:
            continue
        card_id = str(card_id)
        out[card_id] = ""
        langs = row.get("langs")
        if isinstance(langs, list) and "en" not in langs:
            continue
        if not row.get("parallel") and "_" not in card_id:
            continue
        kind = str(row.get("variant_type") or "").strip() or "Parallel"
        groups.setdefault((_base_id(card_id), kind), []).append(card_id)
    for (_base, kind), ids in groups.items():
        ids.sort(key=lambda i: (_print_number(i), i))
        for position, card_id in enumerate(ids, start=1):
            out[card_id] = kind if position == 1 else f"{kind} {position}"
    return out


def index_prices(index_rows, english) -> list:
    """[(source_id, price|None, price_source|None)] for every English card, so a
    card whose price disappeared gets its cached price cleared."""
    out = []
    for row in index_rows or []:
        card_id = row.get("id")
        if not card_id or str(card_id) not in english:
            continue
        price = _price(row.get("price"))
        out.append((str(card_id), price, (row.get("price_source") or None) if price is not None else None))
    return out


def _price(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and number not in (float("inf"),) and number > 0 else None


# --------------------------------------------------------------------------
# Sets
# --------------------------------------------------------------------------

# Words that stay as written although they have no lowercase letter.
_KEEP_UPPER = {"EX"}
_LABEL = re.compile(r"^[^-]+-(.+)-\s*\[([^\]]+)\]\s*$")
_FALLBACK_NAMES = {"569901": "Promotion Cards", "569801": "Other Product Cards"}


# Short words lowercased inside a title ("The Time of Battle"), never first.
_SMALL_WORDS = {"a", "an", "and", "at", "by", "for", "in", "of", "on", "the", "to"}


def _tidy_word(word: str, first: bool = True) -> str:
    """ROMANCE -> Romance. Anything already mixed-case (Monkey.D.Luffy, vol.2),
    in the keep list (EX), or mixing digits with letters (GEAR5, 3D2Y) stays
    as written. Colour pairs like RED/BLACK are tidied each side. Small words
    (OF, THE...) are lowercased unless they open the title."""
    if "/" in word:
        return "/".join(_tidy_word(part) for part in word.split("/"))
    if any(c.islower() for c in word) or word in _KEEP_UPPER:
        return word
    if any(c.isdigit() for c in word) and any(c.isalpha() for c in word):
        return word
    if not first and word.lower() in _SMALL_WORDS:
        return word.lower()
    return word[:1].upper() + word[1:].lower()


def set_display_name(label, set_id) -> str:
    """'BOOSTER PACK -ROMANCE DAWN- [OP-01]' -> 'Romance Dawn [OP-01]'.

    The words between the dashes become the name (shouting words tidied, see
    _tidy_word) and the set code stays in brackets. Labels without that shape
    are returned trimmed ('Promotion card' -> 'Promotion Cards' via a short
    list for the two non-pack sets).
    """
    text = " ".join(str(label or "").split())
    match = _LABEL.match(text)
    if match:
        title = " ".join(_tidy_word(w, i == 0) for i, w in enumerate(match.group(1).split()))
        if title:
            return f"{title} [{match.group(2).strip()}]"
    return _FALLBACK_NAMES.get(str(set_id), text) or str(set_id or "")


def set_era(set_id) -> str:
    """The picker group for a set id: OP-nn and OPnn-EBnn are booster packs;
    EB-nn and PRB-nn are extra/premium boosters; ST-nn starter decks; anything
    else (promos, 'other product') last."""
    sid = str(set_id or "").upper()
    if re.match(r"^OP(-\d+|\d+-EB\d+)$", sid):
        return "one_piece_boosters"
    if re.match(r"^(EB|PRB)-\d+$", sid):
        return "one_piece_extra"
    if re.match(r"^ST-\d+$", sid):
        return "one_piece_starters"
    return "one_piece_promos"


def annotate_sets(sets: list, name_key: str = "set_name") -> list:
    """Add `era`/`era_label` to each set dict, in place — card_eras.annotate's
    shape. The era comes from the set's id (`set_id`, or `id` for the API's own
    list); `name_key` is accepted for the same call shape as riftbound's."""
    labels = dict(ERA_ORDER)
    for entry in sets:
        key = set_era(entry.get("set_id") or entry.get("id"))
        entry["era"] = key
        entry["era_label"] = labels[key]
    return sets


def _pack_sort(pack_id) -> int:
    try:
        return int(str(pack_id).strip())
    except (TypeError, ValueError):
        return -1


def set_release_order(sets) -> list:
    """Set ids, newest first: boosters, then extra boosters, starter decks,
    promos; inside each group by the API's pack_id, highest first.

    AN APPROXIMATION. The API gives no release dates; pack_id rises with
    release inside a group, but the groups themselves are not interleaved by
    date (an extra booster is listed after every main booster regardless of
    when it came out). Fine for "newest booster sets" and for ordering the
    picker; don't present it as a date.
    """
    rank = {key: i for i, (key, _label) in enumerate(ERA_ORDER)}
    rows = [(str(s.get("id") or s.get("set_id") or ""), s.get("pack_id")) for s in sets or []]
    rows = [r for r in rows if r[0]]
    rows.sort(key=lambda r: (rank[set_era(r[0])], -_pack_sort(r[1]), r[0]))
    return [sid for sid, _pack in rows]


# --------------------------------------------------------------------------
# Cards
# --------------------------------------------------------------------------

def _image_url(row: dict):
    value = (row or {}).get("image_url")
    if not isinstance(value, str) or not value:
        return None
    parts = urlparse(value)
    if parts.scheme == "https" and (parts.hostname or "").lower() in IMAGE_HOSTS:
        return value[:500]
    return None


def parse_card(row: dict, set_name: str, versions: dict, english):
    """One /sets/{id}/cards row -> the dict catalog.upsert_set_cards takes, or None.

    None when the card isn't in `english` (Japanese-only, or not in the index)
    or has no id/name. The TCGplayer id is only used — and only marked
    verified — when the API lists exactly one: with several, picking one would
    risk linking a buy button to the wrong print.
    """
    if not isinstance(row, dict):
        return None
    source_id = str(row.get("id") or "").strip()
    name = str(row.get("name") or "").strip()
    if not source_id or not name or source_id not in english:
        return None
    ids = row.get("tcg_ids")
    tcg = str(ids[0]) if isinstance(ids, list) and len(ids) == 1 and str(ids[0]).isdigit() else ""
    price = _price(row.get("price"))
    return {
        "source_id": source_id,
        "name": name[:200],
        "card_number": str(row.get("base_id") or _base_id(source_id)).strip()[:40],
        "variant": str(row.get("rarity") or "").strip()[:80],
        "version": (versions or {}).get(source_id, ""),
        "tcgplayer_id": tcg,
        "tcgplayer_id_verified": bool(tcg),
        "set_name": str(set_name or "").strip()[:200],
        "raw_price": price,
        "price_source": (row.get("price_source") or None) if price is not None else None,
        "image_url": _image_url(row),
        "printing_prices": None,
        "card_type": (str(row.get("category")).strip()[:40] or None) if row.get("category") else None,
    }
