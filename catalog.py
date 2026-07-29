"""Browsable card catalog — set/rarity/price filtering over a local cache.

Owned by ava_dashboard (see data-system.md): `catalog_cards`. Created
idempotently at app startup, matching the `card_tracker` convention, since
neither repo has a migration system.

WHY A CACHE AT ALL: PokemonPriceTracker bills per card returned, so filtering
"every holo under $50 across all sets" cannot be answered by calling the
vendor — that question spans ~36,000 cards. It has to be answered from local
rows. This table is that local copy.

WHY IT COSTS NOTHING TO FILL: PPT's /cards response already carries the raw
market price and rarity for every card in a set, and
`price_sources.fetch_ppt_set_cards` already pays for that call to drive the
grading calculator's card picker — it just used to discard the price. A set
therefore enters this cache for free the first time anyone opens it, and is
served from Postgres from then on.

Nothing here fetches. `main.py` owns the network calls; this module owns the
schema, the upsert, and the read queries.
"""

import os
import re
import logging
from decimal import Decimal, InvalidOperation
from urllib.parse import quote_plus, parse_qsl, urlencode

logger = logging.getLogger("dashboard.catalog")

# TCGplayer product ids are plain integers. Belt-and-braces alongside the
# `verified` flag: both must hold before a link is offered.
_TCGPLAYER_ID_RE = re.compile(r"^\d{2,12}$")
TCGPLAYER_PRODUCT_URL = "https://www.tcgplayer.com/product/{}"
EBAY_SEARCH_URL = "https://www.ebay.com/sch/i.html?_nkw={}"


def tcgplayer_url(tcgplayer_id, verified: bool):
    """Product-page URL, or None when the id can't be trusted.

    `catalog_cards.tcgplayer_id` is populated from
    `_first(row, "tcgPlayerId", "tcgplayerId", "id")` — it falls back to PPT's
    OWN id when the TCGplayer one is absent, and that value points at a
    different product or nothing at all. `verified` records which source it
    came from; without it, no link.

    A missing link is honest. A wrong one sends someone to buy the wrong card,
    which is why this fails closed rather than guessing.
    """
    if not verified:
        return None
    value = str(tcgplayer_id or "").strip()
    if not _TCGPLAYER_ID_RE.match(value):
        return None
    return TCGPLAYER_PRODUCT_URL.format(value)


# eBay Partner Network tracking, appended to outbound eBay links. Set
# EBAY_AFFILIATE_PARAMS to the query string EPN gives you, for example:
#   mkcid=1&mkrid=<rotation id>&campid=<your campaign>&toolid=10001&mkevt=1
#
# Held as opaque configuration ON PURPOSE. EPN has changed its link format
# over time and the rotation id is site-specific, so hardcoding a parameter
# set here would be a guess that silently stops earning when it goes stale.
# Whatever the dashboard gives you is pasted in as-is; no code change, and no
# campaign id in git.
#
# `_nkw` is refused because it carries the search terms — an affiliate string
# that included one would replace the query and send every card to the same
# page.
_EBAY_RESERVED_PARAMS = {"_nkw"}
_ebay_params_cache: tuple = ("", [])


def _ebay_affiliate_pairs() -> list:
    """Parsed EPN parameters, or [] when unset or unusable.

    Fails closed to "no tracking" rather than raising: a malformed campaign
    string costs revenue, but it must never cost the member a working link.
    """
    global _ebay_params_cache
    raw = (os.getenv("EBAY_AFFILIATE_PARAMS", "") or "").strip().lstrip("?&")
    if _ebay_params_cache[0] == raw:
        return _ebay_params_cache[1]
    pairs = []
    if raw:
        try:
            pairs = [(k, v) for k, v in parse_qsl(raw, keep_blank_values=False)
                     if k and v and k not in _EBAY_RESERVED_PARAMS]
        except Exception:
            logger.warning("EBAY_AFFILIATE_PARAMS could not be parsed — "
                           "serving plain eBay links")
            pairs = []
        if not pairs:
            logger.warning("EBAY_AFFILIATE_PARAMS is set but yielded no usable "
                           "parameters — serving plain eBay links")
    _ebay_params_cache = (raw, pairs)
    return pairs


def ebay_affiliate_enabled() -> bool:
    """True when outbound eBay links carry EPN tracking.

    Drives the rel="sponsored" attribute and the disclosure line — both of
    which must appear only when the links are genuinely monetised.
    """
    return bool(_ebay_affiliate_pairs())


def ebay_search_url(card_name: str, set_name: str = "", card_number: str = ""):
    """An eBay search for this card, or None without a name.

    A search rather than a listing: listing ids go stale as items sell, and
    resolving live ones costs an API call per card, which a 50-row page can't
    afford. Sellers overwhelmingly title cards "<name> <number>", so that
    pairing is the most precise query available; the set name stands in when
    there's no number.

    Carries EPN tracking when configured, and is a plain search when not.
    """
    name = (card_name or "").strip()
    if not name:
        return None
    number = (card_number or "").strip()
    terms = f"{name} {number}".strip() if number else f"{name} {(set_name or '').strip()}".strip()
    url = EBAY_SEARCH_URL.format(quote_plus(terms))
    pairs = _ebay_affiliate_pairs()
    if pairs:
        url += "&" + urlencode(pairs)
    return url

# Cap on a single page of results. The UI asks for 50; this bounds a
# hand-crafted `limit=100000` from turning one request into a table scan dump.
MAX_PAGE_SIZE = 100

# Card numbers that don't start with digits (promos, "RC1", "TG05") sort after
# every numbered card rather than interleaving with them by string order.
_UNNUMBERED_SORT = 999_999

# ORDER BY is the one thing here that cannot be parameterized, so the sort key
# is resolved through this allowlist and never interpolated from user input
# (same rule as the raffle wheel's _pick_col).
SORT_COLUMNS = {
    "price_desc": "raw_price DESC NULLS LAST, card_name ASC",
    "price_asc":  "raw_price ASC NULLS LAST, card_name ASC",
    "name_asc":   "card_name ASC, number_sort ASC",
    "number_asc": "number_sort ASC, card_number ASC",
    "set_newest": "refreshed_at DESC, number_sort ASC",
}
DEFAULT_SORT = "price_desc"

# `game` stays permissive even though v1 only stocks Pokemon: One Piece's
# catalog source (optcgapi) has no prices, so it isn't wired up yet. Leaving
# the constraint open means adding it later is a code change, not a migration.
CATALOG_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS catalog_cards (
        id           SERIAL PRIMARY KEY,
        game         TEXT NOT NULL CHECK (game IN ('pokemon','one_piece')),
        language     TEXT NOT NULL DEFAULT 'english',
        set_id       TEXT NOT NULL,
        set_name     TEXT NOT NULL DEFAULT '',
        card_name    TEXT NOT NULL,
        card_number  TEXT NOT NULL DEFAULT '',
        rarity       TEXT NOT NULL DEFAULT '',
        tcgplayer_id TEXT NOT NULL DEFAULT '',
        number_sort  INTEGER NOT NULL DEFAULT 999999,
        raw_price    NUMERIC,
        price_source TEXT,
        priced_at    TIMESTAMPTZ,
        refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    # Identity of a printing. rarity is part of the key because One Piece
    # prints alt-art and manga variants under the SAME name and number —
    # without it those collapse into one row and a variant silently vanishes.
    # Every column is NOT NULL with a '' default so no row can escape the
    # index via a NULL (which compares unequal to itself in a unique index).
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_catalog_cards_identity
        ON catalog_cards (game, language, set_id, card_number, card_name, rarity)
    """,
    # Serves the cross-set price-range browse, which is the whole point of the
    # page and the only query that can touch every row.
    """
    CREATE INDEX IF NOT EXISTS idx_catalog_cards_price
        ON catalog_cards (game, language, raw_price)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_catalog_cards_set
        ON catalog_cards (game, language, set_id)
    """,
    # Added after the table shipped, so it needs its own ALTER — CREATE TABLE
    # IF NOT EXISTS won't touch an existing table (same pattern as
    # card_tracker's justtcg_name/justtcg_set columns). Defaults FALSE, so rows
    # cached before this simply get no product link until they're refreshed —
    # which is the safe direction.
    "ALTER TABLE catalog_cards ADD COLUMN IF NOT EXISTS "
    "tcgplayer_verified BOOLEAN NOT NULL DEFAULT FALSE",
]


async def ensure_catalog_schema(pool) -> None:
    """Create the catalog table and indexes if absent (idempotent)."""
    async with pool.acquire() as conn:
        for ddl in CATALOG_SCHEMA:
            await conn.execute(ddl)


def number_sort_key(card_number: str) -> int:
    """Leading integer of a card number, or _UNNUMBERED_SORT.

    Mirrors the ordering `price_sources.fetch_ppt_set_cards` applies in
    Python, so a set reads the same way from the cache as it does from the
    live picker.
    """
    match = re.match(r"^\s*(\d+)", str(card_number or ""))
    if not match:
        return _UNNUMBERED_SORT
    try:
        return min(int(match.group(1)), _UNNUMBERED_SORT)
    except (TypeError, ValueError):
        return _UNNUMBERED_SORT


def _as_numeric(value):
    """Coerce to Decimal for asyncpg, or None.

    asyncpg binds NUMERIC parameters as Decimal and rejects a float outright,
    so prices are converted here rather than at every call site. Goes through
    str() so the Decimal carries the value that was actually shown, not a
    binary-float expansion of it.
    """
    if value is None:
        return None
    try:
        out = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return out if out > 0 else None


_UPSERT = """
    INSERT INTO catalog_cards
        (game, language, set_id, set_name, card_name, card_number, rarity,
         tcgplayer_id, tcgplayer_verified, number_sort, raw_price, price_source,
         priced_at, refreshed_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::NUMERIC,$12,
            CASE WHEN $11::NUMERIC IS NULL THEN NULL ELSE NOW() END, NOW())
    ON CONFLICT (game, language, set_id, card_number, card_name, rarity)
    DO UPDATE SET
        set_name           = EXCLUDED.set_name,
        tcgplayer_id       = EXCLUDED.tcgplayer_id,
        tcgplayer_verified = EXCLUDED.tcgplayer_verified,
        number_sort        = EXCLUDED.number_sort,
        raw_price          = EXCLUDED.raw_price,
        price_source       = EXCLUDED.price_source,
        priced_at          = EXCLUDED.priced_at,
        refreshed_at       = NOW()
"""


async def upsert_set_cards(pool, game: str, language: str, set_id: str,
                           cards: list, price_source: str = "pokemonpricetracker") -> dict:
    """Write one set's cards into the cache. Returns {cards, priced}.

    `cards` is the shape `price_sources.fetch_ppt_set_cards` returns:
    {name, card_number, variant, tcgplayer_id, set_name, raw_price}.

    A refresh takes the new response as truth: if a card no longer carries a
    price, its cached price is cleared rather than left to look current.
    `priced_at` is set only when a price is actually stored, so it always
    describes the number sitting next to it.
    """
    rows = []
    for card in cards:
        name = str(card.get("name") or "").strip()
        if not name:
            continue
        price = _as_numeric(card.get("raw_price"))
        number = str(card.get("card_number") or "").strip()
        rows.append((
            game, language, set_id,
            str(card.get("set_name") or "").strip(),
            name, number,
            str(card.get("variant") or "").strip(),
            str(card.get("tcgplayer_id") or "").strip(),
            bool(card.get("tcgplayer_id_verified")),
            number_sort_key(number),
            price,
            price_source if price is not None else None,
        ))
    if not rows:
        return {"cards": 0, "priced": 0}

    async with pool.acquire() as conn:
        await conn.executemany(_UPSERT, rows)

    priced = sum(1 for r in rows if r[10] is not None)
    logger.info("Catalog: cached %d card(s) for %s/%s [%s] — %d with a raw price",
                len(rows), game, set_id, language, priced)
    return {"cards": len(rows), "priced": priced}


async def select_price_refresh_candidates(pool, game: str, language: str,
                                          priority_set_ids, limit: int) -> list:
    """Cards due for a price refresh, newest-priority-sets first, then oldest
    price first: [{id, tcgplayer_id, name, set_id}].

    Only cards with a VERIFIED tcgplayer_id qualify — same gate the catalog's
    buy links use (see `tcgplayer_url`). Without a real TCGplayer id there's
    no cheap pinned lookup, only an expensive search, which this per-card
    sweep isn't built to pay for; those cards catch up next time their set is
    stocked or refreshed instead.

    `priority_set_ids` (e.g. the newest N sets) sort first as a group; within
    each half, the longest-unpriced card sorts first. A single ORDER BY
    produces both tiers at once: cards outside the priority group never sort
    ahead of one inside it, no matter how stale.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, tcgplayer_id, card_name, set_id
            FROM catalog_cards
            WHERE game = $1 AND language = $2
              AND tcgplayer_id <> '' AND tcgplayer_verified = TRUE
            ORDER BY (set_id = ANY($3::TEXT[])) DESC,
                     priced_at ASC NULLS FIRST, id ASC
            LIMIT $4
            """,
            game, language, list(priority_set_ids or ()), limit)
    return [{"id": r["id"], "tcgplayer_id": r["tcgplayer_id"],
             "name": r["card_name"], "set_id": r["set_id"]} for r in rows]


async def get_cards_by_id(pool, game: str, language: str, ids) -> list:
    """Specific catalog_cards rows by primary key, for the "refresh these
    cards now" action: [{id, tcgplayer_id, tcgplayer_verified, name,
    priced_at}]. `game`/`language` scope it to the same table the id column
    is otherwise never enough to disambiguate on its own — belt-and-braces
    against an id from a different game/language slipping through.
    """
    ids = [int(i) for i in (ids or ())]
    if not ids:
        return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, tcgplayer_id, tcgplayer_verified, card_name, priced_at "
            "FROM catalog_cards WHERE game = $1 AND language = $2 AND id = ANY($3::INT[])",
            game, language, ids)
    return [{"id": r["id"], "tcgplayer_id": r["tcgplayer_id"],
             "tcgplayer_verified": r["tcgplayer_verified"], "name": r["card_name"],
             "priced_at": r["priced_at"]} for r in rows]


async def update_card_price(pool, card_id: int, raw_price, price_source: str) -> None:
    """Write a single card's refreshed price by primary key.

    Same "the new response is truth" rule as `upsert_set_cards`: a None
    price CLEARS the stored price and priced_at rather than leaving a stale
    number looking current.
    """
    price = _as_numeric(raw_price)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE catalog_cards
            SET raw_price = $1::NUMERIC,
                price_source = $2,
                priced_at = CASE WHEN $1::NUMERIC IS NULL THEN NULL ELSE NOW() END,
                refreshed_at = NOW()
            WHERE id = $3
            """,
            price, price_source if price is not None else None, card_id)


def _build_filters(game: str, language: str, set_ids=None, rarities=None,
                   min_price=None, max_price=None, search: str = "",
                   priced_only: bool = False) -> tuple:
    """(where_sql, params). Every value is a bind parameter, never inlined."""
    params = [game, language]
    where = ["game = $1", "language = $2"]

    def add(template: str, value):
        params.append(value)
        where.append(template.format(n=len(params)))

    if set_ids:
        add("set_id = ANY(${n}::TEXT[])", list(set_ids))
    if rarities:
        add("rarity = ANY(${n}::TEXT[])", list(rarities))
    # A price bound is meaningless for an unpriced card, and NULL comparisons
    # would drop them anyway — so a bound implicitly means "priced only".
    # Cast to DOUBLE PRECISION, not NUMERIC: the bounds arrive as floats from
    # the query string, and asyncpg would demand a Decimal for a NUMERIC
    # parameter. Postgres compares numeric to double precision fine.
    if min_price is not None:
        add("raw_price >= ${n}::DOUBLE PRECISION", min_price)
    if max_price is not None:
        add("raw_price <= ${n}::DOUBLE PRECISION", max_price)
    if priced_only:
        where.append("raw_price IS NOT NULL")
    if search:
        # ILIKE with a bound pattern: the % wrappers are part of the VALUE, so
        # the user's text is never concatenated into SQL. _ and % they type are
        # escaped so they can't turn into wildcards.
        escaped = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        params.append(f"%{escaped}%")
        n = len(params)
        where.append(f"(card_name ILIKE ${n} ESCAPE '\\' "
                     f"OR card_number ILIKE ${n} ESCAPE '\\')")
    return " AND ".join(where), params


async def _facet_counts(conn, game, language, set_ids, rarities,
                        min_price, max_price, search, priced_only) -> dict:
    """Option counts for the set and rarity filters, so each narrows the other.

    THE RULE: a facet's counts are computed with every filter applied EXCEPT
    its own. Rarity options ignore the rarity selection; set options ignore
    the set selection. Apply a facet to itself and picking one rarity would
    leave only that rarity listed — you could never select a second, and the
    filter would look broken.

    Options that match nothing are simply absent; the caller re-adds anything
    currently selected so a choice can always be undone.
    """
    rarity_where, rarity_params = _build_filters(
        game, language, set_ids, None, min_price, max_price, search, priced_only)
    set_where, set_params = _build_filters(
        game, language, None, rarities, min_price, max_price, search, priced_only)

    rarity_rows = await conn.fetch(
        f"SELECT rarity, COUNT(*) AS n FROM catalog_cards "
        f"WHERE {rarity_where} AND rarity <> '' "
        f"GROUP BY rarity ORDER BY n DESC, rarity ASC", *rarity_params)
    set_rows = await conn.fetch(
        f"SELECT set_id, MAX(set_name) AS set_name, COUNT(*) AS n "
        f"FROM catalog_cards WHERE {set_where} "
        f"GROUP BY set_id ORDER BY MAX(set_name) ASC", *set_params)

    return {
        "rarities": [{"rarity": r["rarity"], "count": int(r["n"])} for r in rarity_rows],
        "sets": [{"set_id": r["set_id"], "set_name": r["set_name"] or r["set_id"],
                  "cards": int(r["n"])} for r in set_rows],
    }


async def query_cards(pool, *, game: str, language: str = "english",
                      set_ids=None, rarities=None, min_price=None, max_price=None,
                      search: str = "", priced_only: bool = False,
                      sort: str = DEFAULT_SORT, limit: int = 50, offset: int = 0,
                      with_facets: bool = False) -> dict:
    """One page of catalog rows plus the unpaginated total.

    `total` is what drives the pager, so it's counted under the same filters
    rather than inferred from the page length.

    `with_facets` adds per-filter option counts so the set and rarity
    dropdowns can narrow each other — see _facet_counts for the one rule that
    makes faceted filtering work.
    """
    order_by = SORT_COLUMNS.get(sort, SORT_COLUMNS[DEFAULT_SORT])
    limit = max(1, min(int(limit or 50), MAX_PAGE_SIZE))
    offset = max(0, int(offset or 0))

    where_sql, params = _build_filters(game, language, set_ids, rarities,
                                       min_price, max_price, search, priced_only)

    page_params = params + [limit, offset]
    page_sql = (
        "SELECT id, set_id, set_name, card_name, card_number, rarity, tcgplayer_id, "
        "       tcgplayer_verified, raw_price, price_source, priced_at, refreshed_at "
        f"FROM catalog_cards WHERE {where_sql} "
        f"ORDER BY {order_by} "
        f"LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}"
    )
    count_sql = f"SELECT COUNT(*) FROM catalog_cards WHERE {where_sql}"

    async with pool.acquire() as conn:
        rows = await conn.fetch(page_sql, *page_params)
        total = await conn.fetchval(count_sql, *params)
        facets = await _facet_counts(
            conn, game, language, set_ids, rarities, min_price, max_price,
            search, priced_only) if with_facets else None

    return {
        **({"facets": facets} if facets is not None else {}),
        "cards": [{
            "id": r["id"],
            "set_id": r["set_id"],
            "set_name": r["set_name"],
            "name": r["card_name"],
            "card_number": r["card_number"],
            "rarity": r["rarity"],
            "tcgplayer_id": r["tcgplayer_id"],
            # Resolved server-side so the gate that decides a link is safe
            # lives in one tested place, not in template JavaScript.
            "tcgplayer_url": tcgplayer_url(r["tcgplayer_id"], r["tcgplayer_verified"]),
            "ebay_url": ebay_search_url(r["card_name"], r["set_name"], r["card_number"]),
            "raw_price": float(r["raw_price"]) if r["raw_price"] is not None else None,
            "price_source": r["price_source"],
            "priced_at": r["priced_at"].isoformat() if r["priced_at"] else None,
            "refreshed_at": r["refreshed_at"].isoformat() if r["refreshed_at"] else None,
        } for r in rows],
        "total": int(total or 0),
        "limit": limit,
        "offset": offset,
    }


async def facets(pool, game: str, language: str = "english") -> dict:
    """Filter options, derived from what's actually cached.

    Rarities and price bounds come from the rows themselves, so the UI can
    never offer a filter that matches nothing. `sets` doubles as the coverage
    report — it's exactly the list of sets stocked so far.
    """
    async with pool.acquire() as conn:
        rarity_rows = await conn.fetch(
            "SELECT rarity, COUNT(*) AS n FROM catalog_cards "
            "WHERE game = $1 AND language = $2 AND rarity <> '' "
            "GROUP BY rarity ORDER BY n DESC, rarity ASC",
            game, language)
        set_rows = await conn.fetch(
            "SELECT set_id, MAX(set_name) AS set_name, COUNT(*) AS cards, "
            "       COUNT(raw_price) AS priced, MAX(refreshed_at) AS refreshed_at "
            "FROM catalog_cards WHERE game = $1 AND language = $2 "
            "GROUP BY set_id ORDER BY MAX(set_name) ASC",
            game, language)
        bounds = await conn.fetchrow(
            "SELECT MIN(raw_price) AS lo, MAX(raw_price) AS hi, "
            "       COUNT(*) AS cards, COUNT(raw_price) AS priced "
            "FROM catalog_cards WHERE game = $1 AND language = $2",
            game, language)

    return {
        "rarities": [{"rarity": r["rarity"], "count": int(r["n"])} for r in rarity_rows],
        "sets": [{
            "set_id": r["set_id"],
            "set_name": r["set_name"] or r["set_id"],
            "cards": int(r["cards"]),
            "priced": int(r["priced"]),
            "refreshed_at": r["refreshed_at"].isoformat() if r["refreshed_at"] else None,
        } for r in set_rows],
        "price_min": float(bounds["lo"]) if bounds and bounds["lo"] is not None else None,
        "price_max": float(bounds["hi"]) if bounds and bounds["hi"] is not None else None,
        "total_cards": int(bounds["cards"]) if bounds else 0,
        "total_priced": int(bounds["priced"]) if bounds else 0,
    }


def select_backfill_window(sets: list, already_stocked, limit: int) -> list:
    """Which sets the startup seed should fetch: the newest `limit`, minus
    whatever is already cached.

    `sets` must be newest-first (`price_sources.fetch_ppt_sets` sorts that
    way).

    The slice happens BEFORE the already-stocked filter, and that order is the
    whole point. Filtering first would give "the next `limit` sets nobody has
    stocked yet", which walks further back through the catalog on every call —
    and since the app redeploys on every push, that would re-spend the credit
    budget forever. Slicing first pins a fixed window: once those sets are
    cached the result is empty and stays empty, while a newly released set
    enters the window on its own and gets picked up.
    """
    if limit <= 0:
        return []
    have = set(already_stocked or ())
    return [s for s in sets[:limit] if s.get("id") not in have]


def select_next_unstocked(sets: list, already_stocked, limit: int, skip=None) -> list:
    """The next `limit` sets, newest-first, that aren't cached yet.

    Unlike `select_backfill_window` this does NOT slice to a fixed window — it
    walks the whole catalog, so repeated calls march steadily backwards into
    older sets. **That is only safe for a human-triggered action.** The
    startup seed must never use it: every redeploy would stock another batch,
    forever. One click, one bounded batch, a person deciding to spend it.

    `skip` excludes sets already known to have no data. Without it a click
    would return the same empty sets every time and the walk would never
    advance past them.
    """
    if limit <= 0:
        return []
    have = set(already_stocked or ())
    skip = set(skip or ())
    out = []
    for entry in sets:
        set_id = entry.get("id")
        if set_id in have or set_id in skip:
            continue
        out.append(entry)
        if len(out) >= limit:
            break
    return out


def select_next_refresh(sets: list, already_stocked, already_refreshed, limit: int) -> list:
    """The next `limit` ALREADY-cached sets, newest-first, not yet refreshed
    this run.

    Exists for the 200-cards-per-set cap fix: sets cached before the PPT
    pagination fix landed have their tail silently missing, and
    `select_next_unstocked`/`select_backfill_window` will never revisit a set
    that's already in `catalog_cards`, no matter how incomplete. This walks
    the OPPOSITE membership test — sets that ARE stocked — so re-running
    `_catalog_stock_set` on them (upsert-safe, adds/updates by key) fills in
    whatever the old 200-card ceiling cut off.

    `already_refreshed` is the same kind of in-process, per-run skip set as
    `select_next_unstocked`'s `skip`, so repeated clicks walk forward through
    the whole cached list instead of re-refreshing the same batch. Only ever
    called from an admin action, same caveat as `select_next_unstocked`.
    """
    if limit <= 0:
        return []
    have = set(already_stocked or ())
    done = set(already_refreshed or ())
    out = []
    for entry in sets:
        set_id = entry.get("id")
        if set_id not in have or set_id in done:
            continue
        out.append(entry)
        if len(out) >= limit:
            break
    return out


async def cached_set_ids(pool, game: str, language: str = "english") -> set:
    """Set ids already in the cache — lets a caller skip a vendor round-trip."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT set_id FROM catalog_cards WHERE game = $1 AND language = $2",
            game, language)
    return {r["set_id"] for r in rows}


async def cached_set_counts(pool, game: str, language: str = "english") -> dict:
    """{set_id: cached card count} for sets already in the catalog.

    Lets a refresh compare against the vendor's real total (a 1-credit
    preflight, see price_sources.fetch_ppt_set_total) and skip a set that's
    already complete instead of paying for a full re-fetch to find that out.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT set_id, COUNT(*) AS n FROM catalog_cards "
            "WHERE game = $1 AND language = $2 GROUP BY set_id",
            game, language)
    return {r["set_id"]: int(r["n"]) for r in rows}
