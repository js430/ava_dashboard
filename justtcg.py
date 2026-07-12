"""JustTCG pricing client + pokemontcg.io release-date helper (card tracker).

Written defensively: the exact JustTCG response shapes weren't verified
against a live key when this was built, so price/date extraction scans for
the expected field names rather than assuming one shape, and every request
logs enough to tune the endpoints after the first real run. Free tier is
1,000 calls/month — the ingest logs its call count so you can watch budget.
"""

import os
import re
import asyncio
import logging
import statistics
from datetime import datetime, date, timezone

import httpx

logger = logging.getLogger("dashboard.justtcg")

# ── JustTCG plan limits (pricing page, 2026-07). Each key's plan is detected
# from _metadata.apiPlan on live responses; until detected we assume FREE
# (the conservative choice). Keys can be on different plans — pacing/batch
# size always follow the ACTIVE key's plan.
#   free:    1K/month · 100/day · 10/min · 20 cards/request
#   starter: 10K/month · 1K/day · 50/min · 100 cards/request
PLAN_PROFILES = {
    "free":       {"interval": 6.5, "batch": 20},
    "starter":    {"interval": 1.4, "batch": 100},   # 50/min -> >=1.2s
    "pro":        {"interval": 1.4, "batch": 100},
    "enterprise": {"interval": 0.8, "batch": 200},
}
_429_WAITS = (10.0, 30.0)      # fallback backoff when no Retry-After header
_MAX_TRIES = 3


class BudgetExhausted(Exception):
    """Raised when a per-run JustTCG call budget is used up."""


class CallBudget:
    """Counts JustTCG API calls in a run so one run can't blow the 100/day cap."""

    def __init__(self, limit: int):
        self.limit = int(limit)
        self.used = 0

    def charge(self) -> None:
        if self.used >= self.limit:
            raise BudgetExhausted(f"JustTCG call budget ({self.limit}) exhausted for this run")
        self.used += 1

    @property
    def remaining(self) -> int:
        return max(0, self.limit - self.used)


def _charge(budget) -> None:
    if budget is not None:
        budget.charge()


async def _request_with_backoff(client: httpx.AsyncClient, method: str, url: str, **kwargs):
    """Issue a request with the active API key. Sleeps and retries (up to
    _MAX_TRIES) on HTTP 429; fails over to the backup key on quota/auth
    rejections (401/402/403) or a 429 that survives every retry. Sets the
    auth header itself — callers must not pass one."""
    switched = False
    attempt = 0
    while True:
        kwargs["headers"] = _headers()
        resp = await client.request(method, url, **kwargs)
        if resp.status_code in (401, 402, 403):
            if not switched and _switch_key(f"HTTP {resp.status_code}", exhaust_current=True):
                switched = True
                continue
            return resp
        if resp.status_code != 429:
            if resp.status_code == 200:
                _note_quota(resp)
            return resp
        if attempt >= _MAX_TRIES - 1:
            if not switched and _switch_key("429s persisted through backoff", exhaust_current=False):
                switched = True
                attempt = 0
                continue
            return resp
        retry_after = resp.headers.get("Retry-After", "")
        try:
            wait = max(1.0, float(retry_after))
        except ValueError:
            wait = _429_WAITS[min(attempt, len(_429_WAITS) - 1)]
        logger.warning("JustTCG rate-limited (429) — waiting %.0fs (attempt %d/%d)",
                       wait, attempt + 1, _MAX_TRIES)
        await asyncio.sleep(wait)
        attempt += 1

JUSTTCG_API_BASE = os.getenv("JUSTTCG_API_BASE", "https://api.justtcg.com/v1")
POKEMONTCG_API = "https://api.pokemontcg.io/v2/cards"

# Fallback game slugs, used until resolve_game_slugs() fetches the real ids
# from GET /games (docs show full-name slugs like "magic-the-gathering", so
# these guesses may be wrong — the dynamic lookup is authoritative).
_FALLBACK_GAME_SLUGS = {
    "pokemon": "pokemon",
    "one_piece": "one-piece-card-game",
}
_game_slug_cache: dict | None = None


def _slug_for(game: str) -> str | None:
    if _game_slug_cache and game in _game_slug_cache:
        return _game_slug_cache[game]
    return _FALLBACK_GAME_SLUGS.get(game)


async def resolve_game_slugs(client: httpx.AsyncClient, budget=None) -> dict:
    """Fetch GET /games once (1 API call, cached for the process) and map our
    game keys to JustTCG's actual slugs by name matching."""
    global _game_slug_cache
    if _game_slug_cache is not None:
        return _game_slug_cache
    _charge(budget)
    try:
        resp = await _request_with_backoff(client, "GET", f"{JUSTTCG_API_BASE}/games")
        if resp.status_code != 200:
            logger.warning("GET /games -> HTTP %s; using fallback game slugs", resp.status_code)
            return _FALLBACK_GAME_SLUGS
        payload = resp.json()
        games = payload.get("data") if isinstance(payload, dict) else payload
        slugs = dict(_FALLBACK_GAME_SLUGS)
        for g in games or []:
            gid = str(g.get("id") or "")
            gname = str(g.get("name") or "").lower()
            if not gid:
                continue
            if "pok" in gname:
                slugs["pokemon"] = gid
            elif "one piece" in gname:
                slugs["one_piece"] = gid
        _game_slug_cache = slugs
        logger.info("JustTCG game slugs resolved: %s", slugs)
        return slugs
    except Exception:
        logger.exception("GET /games failed; using fallback game slugs")
        return _FALLBACK_GAME_SLUGS

# Batch size is plan-dependent — use current_batch_size(). Kept for reference:
# free=20, starter/pro=100, enterprise=200 cards per request.

# Field names scanned when extracting prices from a card/variant object.
_PRICE_FIELDS = ("price", "marketPrice", "market_price", "midPrice", "mid_price")
_LOW_FIELDS = ("lowPrice", "low_price", "minPrice", "min_price", "low")
_HIGH_FIELDS = ("highPrice", "high_price", "maxPrice", "max_price", "high")
_RELEASE_FIELDS = ("releaseDate", "release_date", "set_release_date", "setReleaseDate")


class JustTCGError(Exception):
    pass


# ── API keys: JUSTTCG_API_KEY (primary) + optional JUSTTCG_API_KEY_2 (backup).
# The client fails over automatically when the active key is out of quota:
# _metadata.apiRequestsRemaining hits 0, a hard 401/402/403, or 429s that
# survive every backoff retry. Key state is process-lifetime (resets on
# deploy), which is fine — an exhausted key just gets re-discovered fast.
_key_state = {"active": 0, "exhausted": set(), "plans": {}}


def current_plan() -> str:
    """Detected plan of the ACTIVE key ('free' until a response tells us)."""
    return _key_state["plans"].get(_key_state["active"], "free")


def current_interval() -> float:
    """Seconds to sleep between bulk calls, per the active key's plan."""
    return PLAN_PROFILES[current_plan()]["interval"]


def current_batch_size() -> int:
    """Cards per batch request, per the active key's plan."""
    return PLAN_PROFILES[current_plan()]["batch"]


def _set_plan_for_active(raw_plan: str) -> None:
    plan = (raw_plan or "").strip().lower()
    for known in ("enterprise", "starter", "pro"):
        if known in plan:
            break
    else:
        known = "free"
    idx = _key_state["active"]
    if _key_state["plans"].get(idx) != known:
        _key_state["plans"][idx] = known
        logger.info("JustTCG %s key plan detected: %s (interval %.1fs, batch %d)",
                    _key_label(idx), known,
                    PLAN_PROFILES[known]["interval"], PLAN_PROFILES[known]["batch"])


def _api_keys() -> list:
    keys = []
    for env in ("JUSTTCG_API_KEY", "JUSTTCG_API_KEY_2"):
        k = os.getenv(env, "").strip()
        if k:
            keys.append(k)
    return keys


def _key_label(idx: int) -> str:
    return "backup" if idx else "primary"


def _headers() -> dict:
    keys = _api_keys()
    if not keys:
        raise JustTCGError("JUSTTCG_API_KEY is not set")
    avail = [i for i in range(len(keys)) if i not in _key_state["exhausted"]]
    if not avail:
        raise JustTCGError("All JustTCG API keys are out of quota")
    if _key_state["active"] not in avail:
        _key_state["active"] = avail[0]
    return {"X-API-Key": keys[_key_state["active"]]}


def _switch_key(reason: str, exhaust_current: bool) -> bool:
    """Move to another usable key if one exists. Returns True on switch."""
    keys = _api_keys()
    cur = _key_state["active"]
    if exhaust_current:
        _key_state["exhausted"].add(cur)
    for i in range(len(keys)):
        if i != cur and i not in _key_state["exhausted"]:
            _key_state["active"] = i
            logger.warning("JustTCG: switching %s key -> %s key (%s)",
                           _key_label(cur), _key_label(i), reason)
            return True
    return False


def _note_quota(resp) -> None:
    """Watch _metadata: detect the key's plan and fail over at zero quota."""
    try:
        meta = (resp.json() or {}).get("_metadata") or {}
        if meta.get("apiPlan"):
            _set_plan_for_active(str(meta["apiPlan"]))
        rem = meta.get("apiRequestsRemaining")
        if isinstance(rem, (int, float)):
            if rem <= 0:
                _switch_key("quota used up per _metadata", exhaust_current=True)
            elif rem <= 25:
                logger.warning("JustTCG %s key: only %d request(s) remaining",
                               _key_label(_key_state["active"]), int(rem))
    except Exception:
        pass


def _nums(obj: dict, fields: tuple) -> list:
    out = []
    for f in fields:
        v = obj.get(f)
        if isinstance(v, (int, float)) and v > 0:
            out.append(float(v))
    return out


_NM_CONDITIONS = ("NM", "NEAR MINT", "MINT", "M")


def extract_prices(card: dict) -> tuple:
    """Return (low, mid, high) floats (any may be None) from a JustTCG card
    object. Variants are (condition x printing) pairs each carrying a single
    `price` — so we restrict to Near Mint variants when any exist, otherwise
    mixing NM with Damaged prices would skew the level and make day-over-day
    momentum meaningless. Low/high then reflect the printing spread (e.g.
    Normal vs Holofoil); mid is the median across NM printings.
    """
    variants = [v for v in (card.get("variants") or []) if isinstance(v, dict)]
    nm = [v for v in variants
          if str(v.get("condition") or "").strip().upper() in _NM_CONDITIONS]
    objs = (nm or variants) or [card]
    mids, lows, highs = [], [], []
    for o in objs:
        mids += _nums(o, _PRICE_FIELDS)
        lows += _nums(o, _LOW_FIELDS)
        highs += _nums(o, _HIGH_FIELDS)
    # Fall back to the spread of observed prices when explicit low/high absent.
    pool = mids or (lows + highs)
    if not pool and not lows and not highs:
        return None, None, None
    low = min(lows) if lows else (min(pool) if pool else None)
    high = max(highs) if highs else (max(pool) if pool else None)
    mid = statistics.median(mids) if mids else (statistics.median(pool) if pool else None)
    return low, mid, high


def extract_price_history(card: dict) -> list:
    """Daily history points from NM-preferred variants' priceHistory arrays
    ([{"p": price, "t": epoch}] per the docs; seconds or ms auto-detected).
    Multiple NM printings on the same day collapse to low/median/high.
    Returns [{"captured_at", "price_low", "price_mid", "price_high"}, ...]
    sorted by day."""
    variants = [v for v in (card.get("variants") or []) if isinstance(v, dict)]
    nm = [v for v in variants
          if str(v.get("condition") or "").strip().upper() in _NM_CONDITIONS]
    by_day: dict = {}
    for v in (nm or variants):
        for pt in (v.get("priceHistory") or v.get("price_history") or []):
            if not isinstance(pt, dict):
                continue
            p, t = pt.get("p"), pt.get("t")
            if not isinstance(p, (int, float)) or p <= 0 or not isinstance(t, (int, float)):
                continue
            ts = float(t)
            if ts > 1e12:  # milliseconds
                ts /= 1000.0
            day = datetime.fromtimestamp(ts, tz=timezone.utc).date()
            by_day.setdefault(day, []).append(float(p))
    out = []
    for day in sorted(by_day):
        prices = by_day[day]
        out.append({
            "captured_at": datetime(day.year, day.month, day.day, 12, tzinfo=timezone.utc),
            "price_low": min(prices),
            "price_mid": statistics.median(prices),
            "price_high": max(prices),
        })
    return out


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


def _canon(s: str) -> str:
    """Punctuation/case-insensitive form for name comparison ("Team Rocket's
    Mewtwo ex" == "team rockets mewtwo ex")."""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", "", (s or "").lower())).strip()


def _search_variants(name: str) -> list:
    """Query strings to try, in order. Apostrophe-heavy trainer names
    ("Lillie's Clefairy ex") are classic search-breakers; the last variant
    drops the possessive prefix entirely (guarded by strict acceptance)."""
    variants = [name,
                name.replace("’", "'"),
                name.replace("'", "’"),
                name.replace("'", "").replace("’", "")]
    m = re.match(r"^.+?[’']s\s+(\S.*)$", name)
    if m:
        variants.append(m.group(1))
    out = []
    for v in variants:
        v = v.strip()
        if v and v not in out:
            out.append(v)
    return out


async def search_card(client: httpx.AsyncClient, name: str, game: str,
                      set_name: str = "", card_number: str = "",
                      budget: CallBudget | None = None) -> dict | None:
    """Search JustTCG for a card; returns the best-matching card object or None.

    Tries several query variants for punctuation-heavy names. A candidate is
    accepted ONLY if it matches on card number, set, or canonical name —
    never "first result and hope", which could silently track the wrong
    card's prices. No acceptable candidate -> None (retried next ingest).
    Raises BudgetExhausted when the run's call budget is used up.
    """
    slug = _slug_for(game)
    if not slug:
        return None

    def match_rank(c: dict) -> tuple:
        cname = str(c.get("name") or "")
        cset = str(c.get("set") if isinstance(c.get("set"), str) else (c.get("set") or {}).get("name") or c.get("set_name") or "").lower()
        cnum = str(c.get("number") or c.get("card_number") or "").lower()
        name_close = _canon(cname) == _canon(name)
        set_hit = bool(set_name) and set_name.lower() in cset
        num_hit = bool(card_number) and card_number.lower() in cnum
        return (num_hit, set_hit, name_close)

    for i, query in enumerate(_search_variants(name)):
        if i:
            await asyncio.sleep(current_interval())
        _charge(budget)
        resp = await _request_with_backoff(client, "GET", f"{JUSTTCG_API_BASE}/cards",
                                           params={"q": query, "game": slug, "limit": 20})
        if resp.status_code == 400:
            # 'q' is the one search param the docs didn't confirm — retry once
            # with 'name' in case that's the real parameter.
            _charge(budget)
            await asyncio.sleep(current_interval())
            resp = await _request_with_backoff(client, "GET", f"{JUSTTCG_API_BASE}/cards",
                                               params={"name": query, "game": slug, "limit": 20})
        if resp.status_code == 401:
            raise JustTCGError("JustTCG rejected the API key(s) (401)")
        if resp.status_code != 200:
            logger.warning("JustTCG search %r (%s) -> HTTP %s: %s",
                           query, slug, resp.status_code, resp.text[:200])
            continue
        payload = resp.json()
        cards = payload.get("data") if isinstance(payload, dict) else payload
        if not isinstance(cards, list) or not cards:
            continue
        best = sorted(cards, key=match_rank, reverse=True)[0]
        rank = match_rank(best)
        if any(rank):
            if i:
                logger.info("JustTCG matched %r via fallback query %r", name, query)
            return best
        logger.info("JustTCG search %r returned %d result(s) but none matched "
                    "number/set/name — rejecting to avoid tracking the wrong card",
                    query, len(cards))
    return None


async def fetch_cards_by_ids(client: httpx.AsyncClient, ids: list,
                             budget: CallBudget | None = None,
                             include_history: bool = False) -> dict:
    """Fetch current pricing for known JustTCG ids, batched 20/request (free
    tier cap). Falls back to per-id GETs if the batch shape is rejected.
    Stops (returning partial results) if the run's call budget runs out.
    include_history asks for 30 days of priceHistory on the same calls
    (bigger payload, zero extra API-call cost). Returns {id: card_obj}.
    """
    history_params = {"priceHistoryDuration": "30d"} if include_history else None
    out: dict = {}
    i = 0
    while i < len(ids):
        # Chunk size re-read each pass: the plan may be detected (or the key
        # switched to one on a different plan) mid-run.
        size = current_batch_size()
        chunk = ids[i:i + size]
        i += size
        if i:
            await asyncio.sleep(current_interval())
        try:
            _charge(budget)
        except BudgetExhausted:
            logger.warning("JustTCG pricing budget exhausted — fetched %d/%d cards this run",
                           len(out), len(ids))
            return out
        try:
            resp = await _request_with_backoff(client, "POST", f"{JUSTTCG_API_BASE}/cards",
                                               json=[{"cardId": cid} for cid in chunk],
                                               params=history_params)
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
            await asyncio.sleep(current_interval())
            try:
                _charge(budget)
            except BudgetExhausted:
                logger.warning("JustTCG pricing budget exhausted mid-fallback — "
                               "fetched %d/%d cards this run", len(out), len(ids))
                return out
            try:
                per_id_params = {"cardId": cid}
                if history_params:
                    per_id_params.update(history_params)
                r = await _request_with_backoff(client, "GET", f"{JUSTTCG_API_BASE}/cards",
                                                params=per_id_params)
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
