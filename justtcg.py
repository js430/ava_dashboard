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


def _select_variants(card: dict, variant_hint: str = "") -> list:
    """Pick the variant objects that represent 'the card's price':
    1. Near Mint condition only (mixing NM with Damaged skews levels), and
    2. when a variant hint is known (e.g. 'Holofoil', 'Reverse Holofoil',
       '1st Edition') and matches SOME printings, only those printings —
       so a base-vs-holo spread doesn't blur the number we track.
    """
    variants = [v for v in (card.get("variants") or []) if isinstance(v, dict)]
    nm = [v for v in variants
          if str(v.get("condition") or "").strip().upper() in _NM_CONDITIONS]
    pool = nm or variants
    hint = {t for t in _variant_tokens(variant_hint) if len(t) >= 3}
    if hint and len(pool) > 1:
        hinted = [v for v in pool
                  if any(t in str(v.get("printing") or "").lower() for t in hint)]
        if hinted:
            pool = hinted
    return pool


def extract_prices(card: dict, variant_hint: str = "") -> tuple:
    """Return (low, mid, high) floats (any may be None) from a JustTCG card
    object, using NM-preferred, hint-filtered variants (see _select_variants).
    Low/high reflect the remaining printing spread; mid is their median.
    """
    objs = _select_variants(card, variant_hint) or [card]
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


def extract_price_history(card: dict, variant_hint: str = "") -> list:
    """Daily history points from the same NM-preferred, hint-filtered variants
    extract_prices uses ([{"p": price, "t": epoch}] per the docs; seconds or
    ms auto-detected). Same-day points collapse to low/median/high.
    Returns [{"captured_at", "price_low", "price_mid", "price_high"}, ...]
    sorted by day."""
    by_day: dict = {}
    for v in _select_variants(card, variant_hint):
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


def _number_forms(num: str) -> set:
    """Comparable forms of a card number: '223/197' -> {'223/197', '223'},
    '045' -> {'045', '45'} — so our stored format matches however JustTCG
    stores theirs."""
    n = (num or "").strip().lower()
    if not n:
        return set()
    forms = {n}
    if "/" in n:
        head = n.split("/", 1)[0].strip()
        if head:
            forms.add(head)
            forms.add(head.lstrip("0") or head)
    stripped = n.lstrip("0")
    if stripped:
        forms.add(stripped)
    return forms


def _numbers_match(a: str, b: str) -> bool:
    return bool(_number_forms(a) & _number_forms(b))


_VARIANT_STOPWORDS = {"rare", "the", "of", "and", "card"}


def _variant_tokens(s: str) -> set:
    return {w for w in re.findall(r"[a-z]+", (s or "").lower())
            if len(w) >= 3 and w not in _VARIANT_STOPWORDS}


def _card_set_text(c: dict) -> str:
    """Human-readable set name for matching/display.

    Per JustTCG's documented Card schema, 'set_name' is the display name
    ("Ascended Heroes") while 'set' is the machine set-ID SLUG, not a name
    (a prior version of this code read 'set' first, which — since it's a
    plain string — meant every set-name comparison silently compared our
    set name against an ID slug instead of the real name, weakening the
    set_hit signal without ever raising an error).
    """
    name = c.get("set_name")
    if isinstance(name, str) and name:
        return name
    raw = c.get("set")
    if isinstance(raw, dict):
        return str(raw.get("name") or "")
    return ""


def match_display(card: dict) -> tuple:
    """(name, set, number) of a JustTCG card object, for storage/display."""
    return (str(card.get("name") or ""),
            _card_set_text(card),
            str(card.get("number") or card.get("card_number") or ""))


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


def _canon_tight(s: str) -> str:
    """Space- and punctuation-insensitive name form: 'Monkey.D.Luffy' ==
    'Monkey D Luffy' == 'monkeydluffy'."""
    return _canon(s).replace(" ", "")


# Max search pages scanned per card (across all query variants) when hunting
# for a number+name match. Page size follows the plan's limit cap.
_MAX_SEARCH_PAGES = 6


async def _search_page(client, slug: str, query: str, offset: int,
                       budget: CallBudget | None) -> tuple:
    """One search page. Returns (cards list or None-on-error, has_more)."""
    _charge(budget)
    page_limit = current_batch_size()  # plan's limit cap == its batch cap
    resp = await _request_with_backoff(
        client, "GET", f"{JUSTTCG_API_BASE}/cards",
        params={"q": query, "game": slug, "limit": page_limit, "offset": offset})
    if resp.status_code == 400:
        # 'q' is the one search param the docs didn't confirm — retry once
        # with 'name' in case that's the real parameter.
        _charge(budget)
        await asyncio.sleep(current_interval())
        resp = await _request_with_backoff(
            client, "GET", f"{JUSTTCG_API_BASE}/cards",
            params={"name": query, "game": slug, "limit": page_limit, "offset": offset})
    if resp.status_code == 401:
        raise JustTCGError("JustTCG rejected the API key(s) (401)")
    if resp.status_code != 200:
        logger.warning("JustTCG search %r (%s, offset %d) -> HTTP %s: %s",
                       query, slug, offset, resp.status_code, resp.text[:200])
        return None, False
    payload = resp.json()
    cards = payload.get("data") if isinstance(payload, dict) else payload
    if not isinstance(cards, list):
        return None, False
    meta = payload.get("meta") if isinstance(payload, dict) else {}
    has_more = bool((meta or {}).get("hasMore")) or len(cards) >= current_batch_size()
    return cards, has_more


async def search_card(client: httpx.AsyncClient, name: str, game: str,
                      set_name: str = "", card_number: str = "",
                      budget: CallBudget | None = None,
                      variant: str = "") -> dict | None:
    """Search JustTCG for a card; returns the matched card object or None.

    With a card number (the normal case — imports always have one), matching
    is STRICT: paginate through search results hunting for the unique
    number+name pair (format-tolerant numbers, space/punct-insensitive
    names). Nothing less is ever accepted — a popular name can have far more
    hits than one page, and "best effort" ranking is how wrong twins got
    tracked. Rarity is then double-checked and logged if it disagrees.

    Without a number, the older heuristic (set/name ranking with variant
    tie-break) applies. Raises BudgetExhausted when the run budget is gone.
    """
    slug = _slug_for(game)
    if not slug:
        return None
    if card_number:
        return await _search_by_number(client, slug, name, set_name, card_number, variant, budget)
    return await _search_by_rank(client, slug, name, set_name, variant, budget)


async def _search_by_number(client, slug: str, name: str, set_name: str, card_number: str,
                            variant: str, budget: CallBudget | None) -> dict | None:
    """Number-first matching, RANKED — never accept the first number hit.

    Card numbers repeat across the whole catalog (every set restarts at ~001),
    and a generic name like "Pikachu ex" recurs across many expansions — so a
    same-numbered, similarly-named card from an unrelated set/print can appear
    before the real target in search results. Every number-matching candidate
    across all pages/query variants is collected first, THEN ranked by:
    rarity match > set match > exact name match (per request: number+rarity
    lead, name is the tie-breaker) — never accepted on number alone.
    """
    hint = _variant_tokens(variant)
    set_target = (set_name or "").strip().lower()
    candidates: dict[str, dict] = {}   # keyed by identity/(name,set,number) to de-dupe
    seen_sample: list = []  # (name, set_name, number, rarity) diagnostic sample, any card seen
    total_scanned = 0       # EVERY card returned is number-checked; this counts them all
    hit_page_cap = False
    pages_used = 0
    for i, query in enumerate(_search_variants(name)):
        if i:
            await asyncio.sleep(current_interval())
        offset = 0
        while pages_used < _MAX_SEARCH_PAGES:
            if pages_used:
                await asyncio.sleep(current_interval())
            cards, has_more = await _search_page(client, slug, query, offset, budget)
            pages_used += 1
            if not cards:
                break
            for c in cards:
                total_scanned += 1
                cnum = str(c.get("number") or c.get("card_number") or "")
                if len(seen_sample) < 8:
                    seen_sample.append((c.get("name"), _card_set_text(c), cnum, c.get("rarity")))
                if _numbers_match(card_number, cnum):
                    key = card_identity(c) or (str(c.get("name")), _card_set_text(c), cnum)
                    candidates[key] = c
            if pages_used >= _MAX_SEARCH_PAGES and has_more:
                hit_page_cap = True  # more results existed but we stopped at the cap
            if not has_more or pages_used >= _MAX_SEARCH_PAGES:
                break
            offset += len(cards)
        if pages_used >= _MAX_SEARCH_PAGES:
            break

    if not candidates:
        # Diagnostic: show what numbers/sets search actually returned, so a
        # persistent zero-match can be told apart from "set not indexed yet"
        # vs. a real number-format mismatch, without more blind guessing.
        # total_scanned covers EVERY card checked (not just the 8 sampled for
        # display) — hit_page_cap flags whether more results existed beyond
        # what we scanned, vs. having genuinely exhausted the search.
        same_set = [s for s in seen_sample if set_target and set_target in (s[1] or "").lower()]
        cap_note = (" | WARNING: stopped at the page cap with more results still "
                    "available — raise _MAX_SEARCH_PAGES" if hit_page_cap else
                    " | search was exhausted (last page was partial, not cut off by the cap)")
        logger.info("JustTCG: no number match for %r #%s — scanned %d total card(s) "
                    "across %d page(s) (plan=%s, page_size=%d)%s. First 8 seen "
                    "(name, set, number, rarity): %s%s",
                    name, card_number, total_scanned, pages_used, current_plan(),
                    current_batch_size(), cap_note, seen_sample,
                    f" | same-set cards seen: {same_set}" if same_set else " | NO cards from our set were seen at all — set may not be indexed by JustTCG yet")
        return None

    def rank(c: dict) -> tuple:
        cname = str(c.get("name") or "")
        cset = _card_set_text(c).lower()
        crarity = str(c.get("rarity") or "")
        rarity_hit = bool(hint) and bool(hint & _variant_tokens(crarity))
        set_hit = bool(set_target) and set_target in cset
        name_close = _canon_tight(cname) == _canon_tight(name)
        return (rarity_hit, set_hit, name_close)

    pool = list(candidates.values())
    pool.sort(key=rank, reverse=True)
    best = pool[0]
    rarity_hit, set_hit, name_close = rank(best)

    # Number alone is not enough — require rarity, set, or exact name to
    # confirm it's actually the card we mean, not a same-numbered stranger.
    if not (rarity_hit or set_hit or name_close):
        logger.info("JustTCG: %d number-matching candidate(s) for %r #%s but none "
                    "confirmed by rarity/set/name — rejecting to avoid a wrong-set "
                    "collision; will retry next ingest", len(pool), name, card_number)
        return None

    if len(pool) > 1 and not (rarity_hit and set_hit):
        logger.info("JustTCG: %d number-matching candidates for %r #%s; picked %r/%s/#%s "
                    "(rarity_hit=%s set_hit=%s name_close=%s) — spot-check the price if unsure",
                    len(pool), name, card_number, best.get("name"), _card_set_text(best),
                    best.get("number") or best.get("card_number"), rarity_hit, set_hit, name_close)
    return best


async def _search_by_rank(client, slug: str, name: str, set_name: str,
                          variant: str, budget: CallBudget | None) -> dict | None:
    """Heuristic path for cards WITHOUT a number (hand-added watchlist seeds).
    Fill in card numbers to get the strict number+name path instead."""
    hint_tokens = _variant_tokens(variant)

    def match_rank(c: dict) -> tuple:
        cname = str(c.get("name") or "")
        cset = _card_set_text(c).lower()
        crarity = str(c.get("rarity") or "")
        name_close = _canon(cname) == _canon(name)
        set_hit = bool(set_name) and set_name.lower() in cset
        variant_hit = bool(hint_tokens) and bool(hint_tokens & _variant_tokens(crarity))
        return (set_hit, variant_hit, name_close)

    for i, query in enumerate(_search_variants(name)):
        if i:
            await asyncio.sleep(current_interval())
        cards, _ = await _search_page(client, slug, query, 0, budget)
        if not cards:
            continue
        best = sorted(cards, key=match_rank, reverse=True)[0]
        set_hit, _variant_hit, name_close = match_rank(best)
        # variant overlap is only a tie-breaker, never grounds for acceptance
        if set_hit or name_close:
            if i:
                logger.info("JustTCG matched %r via fallback query %r", name, query)
            return best
        logger.info("JustTCG search %r returned %d result(s) but none matched "
                    "set/name — rejecting to avoid tracking the wrong card",
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
