"""Custom investment portfolios — cost basis, holdings, and sealed product.

Owned by ava_dashboard (see data-system.md): portfolios, portfolio_lots,
sealed_products. Created idempotently at app startup, matching the
CREATE TABLE IF NOT EXISTS convention used everywhere else in both repos
since neither has a migration system.

HOW THIS DIFFERS FROM THE CARD TRACKER
    user_tracked_cards is a WATCHLIST: PRIMARY KEY (user_id, card_id), one
    row per person per card, no quantity and no price. It answers "what am I
    watching". It is untouched by this module and keeps working exactly as
    before.

    A portfolio answers "what do I own, what did it cost me, what is it worth
    now". That needs LOTS: buying the same card twice at different prices on
    different dates is two rows, because a single averaged row loses the
    information you'd need to compute a real return. Everything here is
    per-lot for that reason.

PRICE FETCHING IS SHARED AND DEDUPED
    A lot points at a row in the SHARED tracked_cards catalog (or in
    sealed_products), never at a private copy. The nightly ingest prices a
    card once per day if ANYONE references it — see card_tracker's
    _MISSING_TODAY_SQL, extended by this module's ITEM_REFERENCED_SQL. So the
    same card sitting in the tracker and in three different members'
    portfolios costs exactly one API credit, not four. That property is the
    whole reason lots reference the shared catalog rather than storing their
    own card details.

MONEY
    Every amount is NUMERIC in the DB and Decimal in Python. Floats are not
    used for money anywhere in this module: 0.1 + 0.2 != 0.3 is a rounding
    error in a spreadsheet and a wrong ROI figure on someone's investments.
"""

import os
import re
import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

logger = logging.getLogger("dashboard.portfolios")

# How many portfolios one person may create. Deliberately small for now — the
# point is separating "vintage holds" from "flip inventory", not unlimited
# spreadsheets.
MAX_PORTFOLIOS_PER_USER = int(os.getenv("MAX_PORTFOLIOS_PER_USER", "5"))

# Distinct items (cards + sealed together) one person may hold across ALL
# their portfolios. Counted on distinct items, not lots: buying the same box
# five times is one item and one priced row, so it costs one credit a day no
# matter how many lots record the purchases.
MAX_ITEMS_PER_USER = int(os.getenv("MAX_PORTFOLIO_ITEMS_PER_USER", "500"))

MAX_PORTFOLIO_NAME_LEN = 60
MAX_NOTE_LEN = 500
MAX_SOURCE_LEN = 80
# One lot can't hold more than this. Guards a typo'd quantity from producing a
# portfolio worth more than the market.
MAX_LOT_QUANTITY = 10_000

# Condition/grade of a held item. Cards reuse the grading vocabulary already
# defined in price_sources.GRADE_LABELS so a lot's condition matches the
# grades the calculator and price tables speak. Sealed product has its own
# short list — "PSA 10" is meaningless for a booster box.
CARD_CONDITIONS = (
    "raw", "psa_1", "psa_2", "psa_3", "psa_4", "psa_5", "psa_6", "psa_7",
    "psa_8", "psa_9", "psa_10", "bgs_9_5", "bgs_10", "cgc_9", "cgc_10",
    "sgc_10", "tag_9", "tag_10",
)
SEALED_CONDITIONS = ("sealed", "opened", "damaged")
CONDITION_LABELS = {
    "sealed": "Factory sealed", "opened": "Opened", "damaged": "Damaged",
}

# Where an item came from. Free text is allowed, but offering a short list
# makes the data worth aggregating later ("you buy best at shows").
COMMON_SOURCES = ("Local shop", "eBay", "TCGplayer", "Big-box retail",
                  "Card show", "Trade", "Pulled", "Other")

SEALED_PRODUCT_TYPES = (
    "booster_box", "elite_trainer_box", "booster_bundle", "collection_box",
    "tin", "blister", "booster_pack", "premium_collection", "other",
)
SEALED_TYPE_LABELS = {
    "booster_box": "Booster Box", "elite_trainer_box": "Elite Trainer Box",
    "booster_bundle": "Booster Bundle", "collection_box": "Collection Box",
    "tin": "Tin", "blister": "Blister", "booster_pack": "Booster Pack",
    "premium_collection": "Premium Collection", "other": "Other",
}

TWO_DP = Decimal("0.01")
# Upper bound on any single money value. Guards quantize() against absurd but
# technically-finite input like "1e400", which raises rather than rounding.
MAX_MONEY = Decimal("1000000000000")   # 1 trillion


PORTFOLIO_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS portfolios (
        id          BIGSERIAL PRIMARY KEY,
        user_id     BIGINT NOT NULL,
        name        TEXT NOT NULL,
        note        TEXT NOT NULL DEFAULT '',
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_portfolios_user ON portfolios (user_id)",
    # Case-insensitive uniqueness per person: "Vintage" and "vintage" are the
    # same portfolio to a human, and two of them in a list is just confusing.
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_portfolios_user_name
        ON portfolios (user_id, lower(name))
    """,
    # Sealed product lives in its own table rather than squatting in
    # tracked_cards: that table's UNIQUE key is
    # (game, name, set_name, card_number, language) and a booster box has no
    # card number, so sealed rows would collide on an empty string and the
    # scoring formulas (momentum, liquidity) assume single-card behaviour.
    """
    CREATE TABLE IF NOT EXISTS sealed_products (
        id             SERIAL PRIMARY KEY,
        game           TEXT NOT NULL DEFAULT 'pokemon'
                       CHECK (game IN ('pokemon','one_piece')),
        language       TEXT NOT NULL DEFAULT 'english',
        set_name       TEXT NOT NULL DEFAULT '',
        name           TEXT NOT NULL,
        product_type   TEXT NOT NULL DEFAULT 'other',
        release_date   DATE,
        image_url      TEXT,
        ppt_product_id TEXT,
        added_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (game, language, set_name, name, product_type)
    )
    """,
    # Sealed prices come from the price API's own sealed records, so unlike
    # cards they don't live in price_snapshots (that table's FK points at
    # tracked_cards). One current value per product is enough for a portfolio.
    "ALTER TABLE sealed_products ADD COLUMN IF NOT EXISTS market_price NUMERIC(12,2)",
    "ALTER TABLE sealed_products ADD COLUMN IF NOT EXISTS price_updated_at TIMESTAMPTZ",
    "CREATE INDEX IF NOT EXISTS idx_sealed_set ON sealed_products (set_name)",
    # What we have already asked the price API about. Most sets — old ones,
    # promo sets, Japanese-only ones — have no sealed product at all, and the
    # daily call budget is SHARED with the catalog and the nightly price run.
    # Re-asking about them on every bulk run spends that budget for nothing.
    """
    CREATE TABLE IF NOT EXISTS sealed_set_checks (
        set_name    TEXT PRIMARY KEY,
        checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        found       INTEGER NOT NULL DEFAULT 0
    )
    """,
    # A lot is one purchase. card_id XOR sealed_id — enforced by the DB rather
    # than by application code, because a lot pointing at both (or neither)
    # would silently produce wrong portfolio totals rather than an error.
    """
    CREATE TABLE IF NOT EXISTS portfolio_lots (
        id              BIGSERIAL PRIMARY KEY,
        portfolio_id    BIGINT NOT NULL REFERENCES portfolios(id) ON DELETE CASCADE,
        card_id         INTEGER REFERENCES tracked_cards(id) ON DELETE CASCADE,
        sealed_id       INTEGER REFERENCES sealed_products(id) ON DELETE CASCADE,
        quantity        INTEGER NOT NULL DEFAULT 1
                        CHECK (quantity > 0 AND quantity <= 10000),
        condition       TEXT NOT NULL DEFAULT 'raw',
        unit_cost       NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (unit_cost >= 0),
        fees            NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (fees >= 0),
        purchased_on    DATE,
        acquired_from   TEXT NOT NULL DEFAULT '',
        sold_on         DATE,
        sale_unit_price NUMERIC(12,2) CHECK (sale_unit_price IS NULL OR sale_unit_price >= 0),
        sale_fees       NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (sale_fees >= 0),
        notes           TEXT NOT NULL DEFAULT '',
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT lot_targets_exactly_one_item
            CHECK ((card_id IS NOT NULL)::int + (sealed_id IS NOT NULL)::int = 1)
    )
    """,
    # Set when we filled the cost from market price because the member left
    # it blank. Kept so the UI can flag it as an estimate — an unmarked guess
    # sitting in someone's cost basis is worse than no number at all.
    "ALTER TABLE portfolio_lots ADD COLUMN IF NOT EXISTS "
    "cost_is_estimated BOOLEAN NOT NULL DEFAULT FALSE",
    "CREATE INDEX IF NOT EXISTS idx_lots_portfolio ON portfolio_lots (portfolio_id)",
    "CREATE INDEX IF NOT EXISTS idx_lots_card ON portfolio_lots (card_id)",
    "CREATE INDEX IF NOT EXISTS idx_lots_sealed ON portfolio_lots (sealed_id)",
]

# Used by the nightly ingest to decide what needs a price. A card qualifies if
# it is on ANY watchlist OR in ANY portfolio — one price fetch serves both, so
# the same card in the tracker and in five portfolios still costs one credit.
ITEM_REFERENCED_SQL = """
    (EXISTS (SELECT 1 FROM user_tracked_cards u WHERE u.card_id = t.id)
     OR EXISTS (SELECT 1 FROM portfolio_lots l WHERE l.card_id = t.id))
"""


async def ensure_portfolio_schema(pool) -> None:
    async with pool.acquire() as conn:
        for stmt in PORTFOLIO_SCHEMA:
            await conn.execute(stmt)
    logger.info("Portfolio schema ready")


# ── Money helpers ────────────────────────────────────────────────────────
# Everything below is pure and Decimal-based so it can be tested without a DB
# or a network, which is what you want for the arithmetic that tells someone
# whether they made money.

def to_money(value, default=None):
    """Coerce anything the API or DB hands us into a 2dp Decimal.

    Returns `default` for None, blanks and junk rather than raising: a lot
    with a missing price should read as "unknown", not blow up a page that
    renders forty other lots correctly.
    """
    if value is None or value == "":
        return default
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return default
    if d != d or d.is_infinite():          # NaN / Infinity
        return default
    # A finite-but-absurd value ("1e400") still blows up quantize(), so bound
    # the magnitude too. Anything past a trillion is a typo, not a holding.
    if abs(d) > MAX_MONEY:
        return default
    try:
        return d.quantize(TWO_DP, rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return default


def cost_basis(lot) -> Decimal:
    """What this lot cost in total: unit price x quantity, plus fees.

    Fees are included deliberately. Shipping and buyer premiums are real
    money, and a return that ignores them flatters every eBay purchase.
    """
    qty = int(lot.get("quantity") or 0)
    unit = to_money(lot.get("unit_cost"), Decimal("0"))
    fees = to_money(lot.get("fees"), Decimal("0"))
    return (unit * qty + fees).quantize(TWO_DP, rounding=ROUND_HALF_UP)


def is_closed(lot) -> bool:
    """A lot is closed once it has a sale price — that's what makes the gain
    realized. A sale DATE alone isn't enough: "I sold it" without a number
    can't be turned into a return."""
    return to_money(lot.get("sale_unit_price")) is not None


def proceeds(lot):
    """Net received for a closed lot, after selling fees. None if still held."""
    if not is_closed(lot):
        return None
    qty = int(lot.get("quantity") or 0)
    unit = to_money(lot.get("sale_unit_price"), Decimal("0"))
    fees = to_money(lot.get("sale_fees"), Decimal("0"))
    return (unit * qty - fees).quantize(TWO_DP, rounding=ROUND_HALF_UP)


def market_value(lot, unit_price):
    """Current worth of an OPEN lot. None when the item has no price yet.

    None is not zero. A sealed product with no price source must not drag a
    portfolio's total down to look like a loss — it's unknown, and the UI
    says so.
    """
    if is_closed(lot):
        return None
    price = to_money(unit_price)
    if price is None:
        return None
    qty = int(lot.get("quantity") or 0)
    return (price * qty).quantize(TWO_DP, rounding=ROUND_HALF_UP)


def lot_gain(lot, unit_price):
    """Gain for one lot: realized if sold, unrealized if held, None if the
    item has no price. Returns (amount, pct, kind)."""
    basis = cost_basis(lot)
    if is_closed(lot):
        value, kind = proceeds(lot), "realized"
    else:
        value, kind = market_value(lot, unit_price), "unrealized"
    if value is None:
        return None, None, kind
    gain = (value - basis).quantize(TWO_DP, rounding=ROUND_HALF_UP)
    # A free item (pulled from a pack, traded for) has no cost to divide by.
    # Percent is undefined there, not infinite — the dollar gain still stands.
    pct = None
    if basis > 0:
        pct = float((gain / basis * 100).quantize(TWO_DP, rounding=ROUND_HALF_UP))
    return gain, pct, kind


def summarize(lots, prices: dict) -> dict:
    """Roll up a portfolio.

    `prices` maps item key -> current unit price, where the key is
    ("card", id) or ("sealed", id) — see item_key(). Open lots whose item has
    no price are counted in `unpriced` and left out of market value, so the
    total is never quietly wrong; the UI reports the gap instead of implying
    a precision it doesn't have.
    """
    open_basis = Decimal("0")
    open_value = Decimal("0")
    closed_basis = Decimal("0")
    closed_proceeds = Decimal("0")
    unpriced = 0
    open_lots = 0
    closed_lots = 0
    items = set()

    for lot in lots:
        items.add(item_key(lot))
        basis = cost_basis(lot)
        if is_closed(lot):
            closed_lots += 1
            closed_basis += basis
            closed_proceeds += proceeds(lot) or Decimal("0")
            continue
        open_lots += 1
        open_basis += basis
        value = market_value(lot, prices.get(item_key(lot)))
        if value is None:
            unpriced += 1
        else:
            open_value += value

    realized = (closed_proceeds - closed_basis).quantize(TWO_DP, rounding=ROUND_HALF_UP)
    # Unrealized gain compares like with like: only the basis of lots we could
    # actually price. Including unpriced lots' cost would show a fake loss.
    priced_basis = open_basis if unpriced == 0 else _priced_basis(lots, prices)
    unrealized = (open_value - priced_basis).quantize(TWO_DP, rounding=ROUND_HALF_UP)

    return {
        "lots": open_lots + closed_lots,
        "open_lots": open_lots,
        "closed_lots": closed_lots,
        "items": len(items),
        "cost_basis": open_basis.quantize(TWO_DP),
        "market_value": open_value.quantize(TWO_DP),
        "unrealized": unrealized,
        "unrealized_pct": (float((unrealized / priced_basis * 100)
                                 .quantize(TWO_DP, rounding=ROUND_HALF_UP))
                           if priced_basis > 0 else None),
        "realized": realized,
        "realized_pct": (float((realized / closed_basis * 100)
                               .quantize(TWO_DP, rounding=ROUND_HALF_UP))
                         if closed_basis > 0 else None),
        "total_gain": (realized + unrealized).quantize(TWO_DP),
        "unpriced_lots": unpriced,
    }


def _priced_basis(lots, prices) -> Decimal:
    """Cost basis of open lots we have a price for — the denominator that
    makes unrealized percent honest when some items are unpriced."""
    total = Decimal("0")
    for lot in lots:
        if is_closed(lot):
            continue
        if to_money(prices.get(item_key(lot))) is not None:
            total += cost_basis(lot)
    return total


def item_key(lot):
    """("card", id) or ("sealed", id). One stable key for both kinds, so
    prices, dedupe and counting all speak the same language."""
    if lot.get("card_id"):
        return ("card", int(lot["card_id"]))
    if lot.get("sealed_id"):
        return ("sealed", int(lot["sealed_id"]))
    return (None, None)


# ── Validation ───────────────────────────────────────────────────────────

def clean_name(raw) -> str:
    return (str(raw or "").strip())[:MAX_PORTFOLIO_NAME_LEN]


def valid_condition(value, kind: str) -> bool:
    allowed = SEALED_CONDITIONS if kind == "sealed" else CARD_CONDITIONS
    return value in allowed


def default_condition(kind: str) -> str:
    return "sealed" if kind == "sealed" else "raw"


def validate_lot(payload: dict, kind: str) -> tuple:
    """Check a lot submission. Returns (cleaned, error) — error is a
    plain-English string suitable for showing a member directly.

    Rejects rather than coerces anything that would corrupt someone's
    numbers: a negative price or a quantity of zero is a mistake worth
    telling them about, not something to silently round up.
    """
    qty_raw = payload.get("quantity", 1)
    try:
        qty = int(qty_raw)
    except (TypeError, ValueError):
        return None, "Quantity must be a whole number."
    if qty < 1:
        return None, "Quantity must be at least 1."
    if qty > MAX_LOT_QUANTITY:
        return None, f"Quantity can't be more than {MAX_LOT_QUANTITY:,}."

    # BLANK and ZERO are different answers and must stay that way:
    #   blank -> "I don't remember what I paid"  -> fill from market price
    #   0     -> "it was free" (pack pull, gift)  -> a real cost basis of zero
    # Collapsing them would either erase free pulls or invent a cost for them.
    raw_cost = payload.get("unit_cost")
    if raw_cost is None or str(raw_cost).strip() == "":
        unit_cost = None
    else:
        unit_cost = to_money(raw_cost)
        if unit_cost is None:
            return None, "That price doesn't look like a number."
        if unit_cost < 0:
            return None, "Price paid can't be negative."
    fees = to_money(payload.get("fees", 0), Decimal("0"))
    if fees is None or fees < 0:
        return None, "Fees and shipping can't be negative."

    condition = payload.get("condition") or default_condition(kind)
    if not valid_condition(condition, kind):
        return None, "That condition isn't one of the options."

    sale_unit_price = to_money(payload.get("sale_unit_price"))
    if sale_unit_price is not None and sale_unit_price < 0:
        return None, "Sale price can't be negative."
    sale_fees = to_money(payload.get("sale_fees", 0), Decimal("0"))
    if sale_fees is None or sale_fees < 0:
        return None, "Selling fees can't be negative."

    purchased_on = payload.get("purchased_on") or None
    sold_on = payload.get("sold_on") or None
    if sold_on and purchased_on and str(sold_on) < str(purchased_on):
        return None, "The sale date is before the purchase date."
    if sold_on and sale_unit_price is None:
        return None, "Add the price it sold for, so the return can be worked out."

    return {
        "quantity": qty,
        "condition": condition,
        "unit_cost": unit_cost,
        "fees": fees,
        "purchased_on": purchased_on,
        "acquired_from": str(payload.get("acquired_from") or "")[:MAX_SOURCE_LEN],
        "sold_on": sold_on,
        "sale_unit_price": sale_unit_price,
        "sale_fees": sale_fees,
        "notes": str(payload.get("notes") or "")[:MAX_NOTE_LEN],
    }, None


# ── Portfolio CRUD ───────────────────────────────────────────────────────

async def list_portfolios(pool, user_id: int) -> list:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT p.id, p.name, p.note, p.created_at,
                   (SELECT COUNT(*) FROM portfolio_lots l
                     WHERE l.portfolio_id = p.id) AS lot_count
            FROM portfolios p
            WHERE p.user_id = $1
            ORDER BY p.created_at ASC, p.id ASC
            """, user_id)
    return [dict(r) for r in rows]


async def create_portfolio(pool, user_id: int, name: str, note: str = "") -> tuple:
    """Returns (portfolio, error). Enforces the per-person cap."""
    name = clean_name(name)
    if not name:
        return None, "Give the portfolio a name."
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM portfolios WHERE user_id = $1", user_id)
        if count >= MAX_PORTFOLIOS_PER_USER:
            return None, (f"You can have up to {MAX_PORTFOLIOS_PER_USER} portfolios. "
                          "Delete one to make room.")
        clash = await conn.fetchval(
            "SELECT 1 FROM portfolios WHERE user_id = $1 AND lower(name) = lower($2)",
            user_id, name)
        if clash:
            return None, "You already have a portfolio with that name."
        row = await conn.fetchrow(
            """
            INSERT INTO portfolios (user_id, name, note)
            VALUES ($1, $2, $3)
            RETURNING id, name, note, created_at
            """, user_id, name, str(note or "")[:MAX_NOTE_LEN])
    return dict(row), None


async def owns_portfolio(pool, user_id: int, portfolio_id: int) -> bool:
    """Ownership check. Every portfolio route calls this before doing
    anything — the id arrives from the browser, and without this any member
    could read or edit another member's holdings by guessing a number."""
    async with pool.acquire() as conn:
        found = await conn.fetchval(
            "SELECT 1 FROM portfolios WHERE id = $1 AND user_id = $2",
            portfolio_id, user_id)
    return bool(found)


async def rename_portfolio(pool, user_id: int, portfolio_id: int,
                           name: str, note=None) -> tuple:
    name = clean_name(name)
    if not name:
        return None, "Give the portfolio a name."
    async with pool.acquire() as conn:
        clash = await conn.fetchval(
            """SELECT 1 FROM portfolios
               WHERE user_id = $1 AND lower(name) = lower($2) AND id <> $3""",
            user_id, name, portfolio_id)
        if clash:
            return None, "You already have a portfolio with that name."
        row = await conn.fetchrow(
            """
            UPDATE portfolios
               SET name = $3,
                   note = COALESCE($4, note),
                   updated_at = NOW()
             WHERE id = $1 AND user_id = $2
            RETURNING id, name, note, created_at
            """, portfolio_id, user_id, name,
            None if note is None else str(note)[:MAX_NOTE_LEN])
    if not row:
        return None, "That portfolio doesn't exist."
    return dict(row), None


async def delete_portfolio(pool, user_id: int, portfolio_id: int) -> bool:
    """Deletes the portfolio and its lots (ON DELETE CASCADE). Does NOT touch
    tracked_cards or sealed_products — those are the shared catalog, and other
    members may hold the same items."""
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM portfolios WHERE id = $1 AND user_id = $2",
            portfolio_id, user_id)
    return result.endswith(" 1")


# ── Lots ─────────────────────────────────────────────────────────────────

LOT_COLUMNS = """
    l.id, l.portfolio_id, l.card_id, l.sealed_id, l.quantity, l.condition,
    l.unit_cost, l.fees, l.purchased_on, l.acquired_from,
    l.sold_on, l.sale_unit_price, l.sale_fees, l.notes, l.created_at,
    l.cost_is_estimated
"""


async def list_lots(pool, portfolio_id: int) -> list:
    """Lots with their item's display fields joined in from whichever catalog
    they point at."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT {LOT_COLUMNS},
                   COALESCE(t.name, s.name)            AS item_name,
                   COALESCE(t.set_name, s.set_name)    AS set_name,
                   COALESCE(t.language, s.language)    AS language,
                   t.card_number, t.variant,
                   s.product_type,
                   CASE WHEN l.card_id IS NOT NULL THEN 'card' ELSE 'sealed' END AS kind
            FROM portfolio_lots l
            LEFT JOIN tracked_cards   t ON t.id = l.card_id
            LEFT JOIN sealed_products s ON s.id = l.sealed_id
            WHERE l.portfolio_id = $1
            ORDER BY l.purchased_on DESC NULLS LAST, l.id DESC
            """, portfolio_id)
    return [dict(r) for r in rows]


async def distinct_item_count(pool, user_id: int) -> int:
    """Distinct items across ALL of one person's portfolios.

    Counts items, not lots, because that is what actually costs money: the
    nightly ingest prices an item once regardless of how many lots or
    portfolios reference it.
    """
    async with pool.acquire() as conn:
        return int(await conn.fetchval(
            """
            SELECT COUNT(*) FROM (
                SELECT DISTINCT l.card_id, l.sealed_id
                FROM portfolio_lots l
                JOIN portfolios p ON p.id = l.portfolio_id
                WHERE p.user_id = $1
            ) q
            """, user_id) or 0)


async def item_already_held(pool, user_id: int, card_id, sealed_id) -> bool:
    """Does this person already hold this item anywhere? If so, a new lot adds
    no price-fetch cost and shouldn't count against the item cap."""
    async with pool.acquire() as conn:
        return bool(await conn.fetchval(
            """
            SELECT 1 FROM portfolio_lots l
            JOIN portfolios p ON p.id = l.portfolio_id
            WHERE p.user_id = $1
              AND ($2::int IS NOT NULL AND l.card_id = $2::int
                   OR $3::int IS NOT NULL AND l.sealed_id = $3::int)
            LIMIT 1
            """, user_id, card_id, sealed_id))



async def latest_unit_price(pool, kind: str, item_id):
    """Most recent known market price for one item, or None.

    Used to fill in a blank "price paid". It reads the SAME shared
    price_snapshots history the tracker maintains — no API call, so leaving
    the price blank costs nothing.

    Sealed reads sealed_products.market_price. A product the price API never
    priced returns None, and the caller leaves the cost at zero WITHOUT
    flagging it as an estimate — there would be nothing behind the label.
    """
    if not item_id or kind not in ("card", "sealed"):
        return None
    try:
        async with pool.acquire() as conn:
            if kind == "sealed":
                value = await conn.fetchval(
                    "SELECT market_price FROM sealed_products WHERE id = $1",
                    int(item_id))
            else:
                value = await conn.fetchval(
                    "SELECT price_mid FROM price_snapshots WHERE card_id = $1 "
                    "AND price_mid IS NOT NULL ORDER BY captured_at DESC LIMIT 1",
                    int(item_id))
        return to_money(value)
    except Exception:
        # A lookup failure must not block someone recording a purchase.
        logger.exception("Portfolios: price lookup failed for %s %s", kind, item_id)
        return None


async def add_lot(pool, user_id: int, portfolio_id: int, kind: str,
                  item_id: int, payload: dict) -> tuple:
    """Add one purchase to a portfolio. Returns (lot, error)."""
    if kind not in ("card", "sealed"):
        return None, "Unknown item type."
    cleaned, error = validate_lot(payload, kind)
    if error:
        return None, error

    card_id = int(item_id) if kind == "card" else None
    sealed_id = int(item_id) if kind == "sealed" else None

    # Blank price paid: fall back to what the item is worth now, and mark it
    # as an estimate. Better than a silent zero, which would read as a free
    # pull and overstate every gain. If we have no price either (sealed today)
    # it stays zero and is NOT marked estimated — there's nothing to estimate
    # from, and claiming otherwise would be a lie in the UI.
    estimated = False
    if cleaned["unit_cost"] is None:
        market = await latest_unit_price(pool, kind, card_id or sealed_id)
        if market is not None:
            cleaned["unit_cost"] = market
            estimated = True
        else:
            cleaned["unit_cost"] = Decimal("0")

    # The cap counts distinct items, so adding a second lot of something
    # already held is always allowed — it costs no extra price fetches.
    if not await item_already_held(pool, user_id, card_id, sealed_id):
        if await distinct_item_count(pool, user_id) >= MAX_ITEMS_PER_USER:
            return None, (f"You're at the limit of {MAX_ITEMS_PER_USER:,} different "
                          "items across your portfolios. Remove something to add more.")

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO portfolio_lots
                (portfolio_id, card_id, sealed_id, quantity, condition, unit_cost,
                 fees, purchased_on, acquired_from, sold_on, sale_unit_price,
                 sale_fees, notes, cost_is_estimated)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8::date,$9,$10::date,$11,$12,$13,$14)
            RETURNING {LOT_COLUMNS}
            """,
            portfolio_id, card_id, sealed_id, cleaned["quantity"],
            cleaned["condition"], cleaned["unit_cost"], cleaned["fees"],
            cleaned["purchased_on"], cleaned["acquired_from"], cleaned["sold_on"],
            cleaned["sale_unit_price"], cleaned["sale_fees"], cleaned["notes"],
            estimated)
    return dict(row), None


async def update_lot(pool, user_id: int, lot_id: int, payload: dict) -> tuple:
    """Edit a lot. The ownership join in the WHERE clause is what stops one
    member editing another's holdings by lot id."""
    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            """
            SELECT l.id, l.card_id, l.sealed_id, l.unit_cost, l.cost_is_estimated
            FROM portfolio_lots l
            JOIN portfolios p ON p.id = l.portfolio_id
            WHERE l.id = $1 AND p.user_id = $2
            """, lot_id, user_id)
        if not existing:
            return None, "That item isn't in one of your portfolios."
        kind = "card" if existing["card_id"] else "sealed"
        cleaned, error = validate_lot(payload, kind)
        if error:
            return None, error
        if cleaned["unit_cost"] is None:
            # Left blank on an edit: keep whatever is already recorded rather
            # than re-estimating over a figure the member may have corrected.
            cleaned["unit_cost"] = to_money(existing["unit_cost"], Decimal("0"))
            still_estimated = bool(existing["cost_is_estimated"])
        else:
            # They typed a number, so it's theirs now, not our guess.
            still_estimated = False
        row = await conn.fetchrow(
            f"""
            UPDATE portfolio_lots
               SET quantity = $2, condition = $3, unit_cost = $4, fees = $5,
                   purchased_on = $6::date, acquired_from = $7, sold_on = $8::date,
                   sale_unit_price = $9, sale_fees = $10, notes = $11,
                   cost_is_estimated = $12, updated_at = NOW()
             WHERE id = $1
            RETURNING {LOT_COLUMNS}
            """,
            lot_id, cleaned["quantity"], cleaned["condition"], cleaned["unit_cost"],
            cleaned["fees"], cleaned["purchased_on"], cleaned["acquired_from"],
            cleaned["sold_on"], cleaned["sale_unit_price"], cleaned["sale_fees"],
            cleaned["notes"], still_estimated)
    return dict(row), None


async def delete_lot(pool, user_id: int, lot_id: int) -> bool:
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            DELETE FROM portfolio_lots l
            USING portfolios p
            WHERE l.portfolio_id = p.id AND l.id = $1 AND p.user_id = $2
            """, lot_id, user_id)
    return result.endswith(" 1")


# ── Prices for held items ────────────────────────────────────────────────

async def current_prices(pool, lots) -> dict:
    """Latest known unit price per item, keyed by item_key().

    Cards read the newest price_snapshots row — the same shared history the
    tracker already maintains, so a portfolio costs no extra API calls.
    price_mid is the market price (card_tracker writes PPT's "market" there
    and card_scoring reads it); low/high are the spread around it.

    Sealed reads sealed_products.market_price, filled by the sealed import
    (see import_sealed_products). A product the API never priced stays None
    and is reported as unpriced — never as zero.
    """
    card_ids = sorted({int(l["card_id"]) for l in lots if l.get("card_id")})
    sealed_ids = sorted({int(l["sealed_id"]) for l in lots if l.get("sealed_id")})
    prices = {}

    if sealed_ids:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, market_price FROM sealed_products WHERE id = ANY($1::int[])",
                sealed_ids)
        for r in rows:
            if r["market_price"] is not None:
                prices[("sealed", int(r["id"]))] = r["market_price"]

    if not card_ids:
        return prices
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (p.card_id) p.card_id, p.price_mid
            FROM price_snapshots p
            WHERE p.card_id = ANY($1::int[])
            ORDER BY p.card_id, p.captured_at DESC
            """, card_ids)
    for r in rows:
        if r["price_mid"] is not None:
            prices[("card", int(r["card_id"]))] = r["price_mid"]
    return prices


# ── Sealed product catalogue ─────────────────────────────────────────────
# WHERE SEALED PRODUCT COMES FROM
#     PokemonPriceTracker's documented v2 endpoints (see price_sources.py) are
#     /cards and /sets. Whether it serves sealed product is UNCONFIRMED — the
#     API key lives only on Railway, so it could not be probed from here. No
#     speculative client code is written against an endpoint nobody has seen.
#
#     Until that is settled, the sealed catalogue is seeded from the set list
#     we already have: every modern set ships the same handful of SKUs, so
#     "<Set Name> Booster Box" and friends are generated per set. Members can
#     record what they paid immediately. Prices stay blank — current_prices()
#     returns nothing for sealed, and the roll-up reports those lots as
#     unpriced rather than valuing them at zero.
#
#     If a sealed price endpoint does exist, the upgrade is small: fill
#     ppt_product_id here and add a sealed branch to current_prices().

# The first version of this module generated the same three SKUs for every
# set (Booster Box / ETB / Bundle). That was wrong: every set ships a
# different mix, so it invented products that were never printed and missed
# ones that were. Sealed product now comes from the price API's own per-set
# listing — see parse_sealed_rows and import_sealed_products below.


async def sealed_price_available(pool) -> bool:
    """Whether ANY sealed product has a price yet.

    Asked rather than assumed: it depends on whether the price API's sealed
    records carry prices, which is only knowable after a real import.
    """
    try:
        async with pool.acquire() as conn:
            return bool(await conn.fetchval(
                "SELECT 1 FROM sealed_products WHERE market_price IS NOT NULL LIMIT 1"))
    except Exception:
        return False


# ── Positions and partial sales ──────────────────────────────────────────
# A POSITION is every purchase of one item in one portfolio, seen as a single
# holding: total quantity, weighted average cost, current value. Lots remain
# the source of truth underneath — the position is a view over them, never a
# stored total that could drift out of step with the purchases it came from.
#
# Selling acts on the position ("sell 1 of these 3"), and consumes the OLDEST
# purchase first (FIFO). When a sale is smaller than the purchase it lands on,
# that lot is SPLIT: the sold units become their own closed lot carrying the
# original price, date and source, and the rest stays open. That's what keeps
# realized gain honest — the closed lot still knows what those exact units
# cost, instead of an average smeared across every purchase.

def group_positions(lots, prices: dict) -> list:
    """Group lots into positions, one per item, newest activity first.

    Closed lots stay attached to their position so a sold-out holding still
    shows its history rather than vanishing.
    """
    by_item = {}
    for lot in lots:
        by_item.setdefault(item_key(lot), []).append(lot)

    positions = []
    for key, group in by_item.items():
        open_lots = [l for l in group if not is_closed(l)]
        closed_lots = [l for l in group if is_closed(l)]
        unit_price = prices.get(key)

        open_qty = sum(int(l["quantity"] or 0) for l in open_lots)
        open_cost = sum((cost_basis(l) for l in open_lots), Decimal("0"))
        sold_qty = sum(int(l["quantity"] or 0) for l in closed_lots)
        sold_cost = sum((cost_basis(l) for l in closed_lots), Decimal("0"))
        sold_proceeds = sum((proceeds(l) or Decimal("0") for l in closed_lots),
                            Decimal("0"))

        value = None
        price = to_money(unit_price)
        if price is not None and open_qty:
            value = (price * open_qty).quantize(TWO_DP, rounding=ROUND_HALF_UP)

        unrealized = None
        unrealized_pct = None
        if value is not None:
            unrealized = (value - open_cost).quantize(TWO_DP, rounding=ROUND_HALF_UP)
            if open_cost > 0:
                unrealized_pct = float((unrealized / open_cost * 100)
                                       .quantize(TWO_DP, rounding=ROUND_HALF_UP))

        realized = (sold_proceeds - sold_cost).quantize(TWO_DP, rounding=ROUND_HALF_UP)
        realized_pct = None
        if sold_cost > 0:
            realized_pct = float((realized / sold_cost * 100)
                                 .quantize(TWO_DP, rounding=ROUND_HALF_UP))

        sample = group[0]
        positions.append({
            "kind": key[0],
            "item_id": key[1],
            "name": sample.get("item_name") or "",
            "set_name": sample.get("set_name") or "",
            "card_number": sample.get("card_number") or "",
            "variant": sample.get("variant") or "",
            "product_type": sample.get("product_type") or "",
            "language": sample.get("language") or "",
            "quantity": open_qty,
            "sold_quantity": sold_qty,
            "cost_basis": open_cost.quantize(TWO_DP),
            # The number your side-note wants: what a unit cost you on average,
            # across every purchase still held.
            "avg_unit_cost": ((open_cost / open_qty).quantize(TWO_DP, rounding=ROUND_HALF_UP)
                              if open_qty else None),
            "unit_price": price,
            "market_value": value,
            "unrealized": unrealized,
            "unrealized_pct": unrealized_pct,
            "realized": realized,
            "realized_pct": realized_pct,
            "unpriced": price is None and open_qty > 0,
            "lots": sorted(group, key=_purchase_sort_key),
        })

    positions.sort(key=lambda p: ((p["quantity"] == 0), p["name"].lower()))
    return positions


def _purchase_sort_key(lot):
    """Oldest purchase first. A lot with no date sorts LAST rather than first:
    an unknown date shouldn't jump the FIFO queue ahead of a purchase we can
    actually date."""
    when = lot.get("purchased_on")
    return (when is None, str(when or ""), int(lot.get("id") or 0))


def open_lots_fifo(lots) -> list:
    """Open lots for one item, oldest purchase first."""
    return sorted((l for l in lots if not is_closed(l)), key=_purchase_sort_key)


def plan_sale(lots, quantity, sale_fees=None) -> tuple:
    """Work out which purchases a sale consumes. Returns (plan, error).

    Pure, so the arithmetic that decides someone's realized gain can be tested
    without a database. Each plan entry says how many units come off which
    lot, how that lot's purchase fees split, and its share of the selling
    fees.

    Fee splitting is proportional AND exact: the final entry absorbs any
    rounding remainder, so the parts always add back to the original total.
    A cent leaking out of a cost basis on every partial sale is how a
    portfolio slowly stops reconciling.
    """
    try:
        want = int(quantity)
    except (TypeError, ValueError):
        return None, "How many did you sell?"
    if want < 1:
        return None, "Enter how many you sold."

    available = open_lots_fifo(lots)
    have = sum(int(l["quantity"] or 0) for l in available)
    if have == 0:
        return None, "You don't have any of these left to sell."
    if want > have:
        return None, ("You only have %d of these. Reduce the quantity, or add the "
                      "purchase first." % have)

    total_sale_fees = to_money(sale_fees, Decimal("0")) or Decimal("0")
    plan = []
    remaining = want
    for lot in available:
        if remaining <= 0:
            break
        lot_qty = int(lot["quantity"] or 0)
        take = min(lot_qty, remaining)
        lot_fees = to_money(lot.get("fees"), Decimal("0")) or Decimal("0")
        # Purchase fees follow the units they were paid on.
        sold_fees = (lot_fees * take / lot_qty).quantize(TWO_DP, rounding=ROUND_HALF_UP) \
            if lot_qty else Decimal("0")
        plan.append({
            "lot": lot,
            "lot_id": lot.get("id"),
            "take": take,
            "lot_quantity": lot_qty,
            "splits": take < lot_qty,
            "unit_cost": to_money(lot.get("unit_cost"), Decimal("0")),
            "purchased_on": lot.get("purchased_on"),
            "acquired_from": lot.get("acquired_from") or "",
            "condition": lot.get("condition"),
            "sold_fees": sold_fees,
            "kept_fees": (lot_fees - sold_fees).quantize(TWO_DP),
        })
        remaining -= take

    # Selling fees split across the consumed units, remainder on the last
    # entry so the shares sum to exactly what was entered.
    allocated = Decimal("0")
    for i, entry in enumerate(plan):
        if i == len(plan) - 1:
            entry["sale_fee_share"] = (total_sale_fees - allocated).quantize(TWO_DP)
        else:
            share = (total_sale_fees * entry["take"] / want).quantize(
                TWO_DP, rounding=ROUND_HALF_UP)
            entry["sale_fee_share"] = share
            allocated += share
    return plan, None


def plan_totals(plan, unit_price) -> dict:
    """What a planned sale comes to — shown for confirmation BEFORE it's
    committed, because splitting lots is not something to discover after."""
    price = to_money(unit_price, Decimal("0")) or Decimal("0")
    units = sum(e["take"] for e in plan)
    cost = sum((e["unit_cost"] * e["take"] + e["sold_fees"] for e in plan), Decimal("0"))
    fees = sum((e["sale_fee_share"] for e in plan), Decimal("0"))
    gross = (price * units).quantize(TWO_DP, rounding=ROUND_HALF_UP)
    net = (gross - fees).quantize(TWO_DP)
    gain = (net - cost).quantize(TWO_DP)
    return {
        "units": units,
        "lots_touched": len(plan),
        "splits": sum(1 for e in plan if e["splits"]),
        "cost_basis": cost.quantize(TWO_DP),
        "gross": gross,
        "fees": fees.quantize(TWO_DP),
        "net": net,
        "gain": gain,
        "gain_pct": (float((gain / cost * 100).quantize(TWO_DP, rounding=ROUND_HALF_UP))
                     if cost > 0 else None),
    }


async def sell_from_position(pool, user_id: int, portfolio_id: int, kind: str,
                             item_id: int, quantity, unit_price, sale_fees=None,
                             sold_on=None, notes: str = "") -> tuple:
    """Record a sale against a position. Returns (result, error).

    Runs in ONE transaction: a partial sale rewrites the lot it lands on and
    inserts the sold half, and a half-applied version of that would either
    duplicate units or lose them.
    """
    if kind not in ("card", "sealed"):
        return None, "Unknown item type."
    price = to_money(unit_price)
    if price is None or price < 0:
        return None, "Enter what it sold for."
    fees = to_money(sale_fees, Decimal("0"))
    if fees is None or fees < 0:
        return None, "Selling fees can't be negative."

    if not await owns_portfolio(pool, user_id, portfolio_id):
        return None, "That isn't one of your portfolios."

    card_id = int(item_id) if kind == "card" else None
    sealed_id = int(item_id) if kind == "sealed" else None

    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                """
                SELECT id, quantity, condition, unit_cost, fees, purchased_on,
                       acquired_from, sold_on, sale_unit_price, sale_fees, notes,
                       card_id, sealed_id
                FROM portfolio_lots
                WHERE portfolio_id = $1
                  AND ($2::int IS NOT NULL AND card_id = $2::int
                       OR $3::int IS NOT NULL AND sealed_id = $3::int)
                FOR UPDATE
                """, portfolio_id, card_id, sealed_id)
            lots = [dict(r) for r in rows]
            if not lots:
                return None, "That item isn't in this portfolio."

            plan, error = plan_sale(lots, quantity, fees)
            if error:
                return None, error

            for entry in plan:
                lot = entry["lot"]
                if entry["splits"]:
                    # Shrink the open lot, then record the sold units as their
                    # own closed lot carrying the ORIGINAL purchase details.
                    await conn.execute(
                        "UPDATE portfolio_lots SET quantity = $2, fees = $3, "
                        "updated_at = NOW() WHERE id = $1",
                        lot["id"], entry["lot_quantity"] - entry["take"],
                        entry["kept_fees"])
                    await conn.execute(
                        """
                        INSERT INTO portfolio_lots
                            (portfolio_id, card_id, sealed_id, quantity, condition,
                             unit_cost, fees, purchased_on, acquired_from,
                             sold_on, sale_unit_price, sale_fees, notes)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8::date,$9,$10::date,$11,$12,$13)
                        """,
                        portfolio_id, card_id, sealed_id, entry["take"],
                        entry["condition"], entry["unit_cost"], entry["sold_fees"],
                        entry["purchased_on"], entry["acquired_from"],
                        sold_on, price, entry["sale_fee_share"],
                        str(notes or "")[:MAX_NOTE_LEN])
                else:
                    # The whole purchase sold — close it where it stands, so
                    # its id and history survive.
                    await conn.execute(
                        """
                        UPDATE portfolio_lots
                           SET sold_on = $2::date, sale_unit_price = $3,
                               sale_fees = $4, updated_at = NOW(),
                               notes = CASE WHEN $5 = '' THEN notes ELSE $5 END
                         WHERE id = $1
                        """, lot["id"], sold_on, price, entry["sale_fee_share"],
                        str(notes or "")[:MAX_NOTE_LEN])

    totals = plan_totals(plan, price)
    logger.info("Portfolio %s: sold %d of %s %s (%d lot(s), %d split) for %s",
                portfolio_id, totals["units"], kind, item_id,
                totals["lots_touched"], totals["splits"], totals["net"])
    return totals, None


# ── Importing sealed product from the price API ──────────────────────────
# Every set ships a different mix — some get an ETB and a bundle, some get
# three different collection boxes, some get none. Generating the same SKUs
# for every set (which this module did first) invents products that were
# never printed and misses ones that were, so the catalogue is now driven by
# what the API actually lists.
#
# The record shape is UNVERIFIED (see price_sources.fetch_ppt_sealed), so the
# parser below reads several candidate key names rather than one, and returns
# None for anything it can't make sense of instead of storing a half-row. The
# caller previews what was parsed before writing.

# Key names to look for, most likely first. Extend rather than replace when
# the real shape is known.
_NAME_KEYS = ("name", "productName", "product_name", "title")
_SET_KEYS = ("setName", "set_name", "set", "expansion")
_ID_KEYS = ("id", "productId", "product_id", "tcgPlayerId", "tcgplayer_id")
_IMAGE_KEYS = ("imageCdnUrl", "imageUrl", "image_url", "image", "imageCdnUrlLarge")
_TYPE_KEYS = ("productType", "product_type", "type", "category")

# Name -> product_type. Order matters: "elite trainer box" must be tested
# before "box", or every ETB would be filed as a booster box.
_TYPE_PATTERNS = (
    ("elite_trainer_box", ("elite trainer box", "elite-trainer-box", " etb")),
    ("premium_collection", ("premium collection", "ultra premium")),
    ("booster_bundle", ("booster bundle", "bundle")),
    ("booster_box", ("booster box", "display box", "display case")),
    ("collection_box", ("collection", "box set", "gift set")),
    ("tin", ("tin",)),
    ("blister", ("blister", "sleeved booster", "three pack", "3 pack")),
    ("booster_pack", ("booster pack", "single pack")),
)


def classify_sealed_type(name: str, hint: str = "") -> str:
    """Best-effort product type from the product's name.

    Returns "other" rather than guessing when nothing matches — an item filed
    under the wrong type is harder to notice than one filed under "Other".
    """
    text = ((hint or "") + " " + (name or "")).lower()
    for product_type, needles in _TYPE_PATTERNS:
        if any(n in text for n in needles):
            return product_type
    return "other"


def _first(row: dict, keys) -> str:
    for k in keys:
        value = row.get(k)
        if value not in (None, ""):
            return value
    return ""


def _clean_money_text(value):
    """Strip currency decoration before parsing: "$1,299.00" -> "1299.00".

    APIs quote money as a display string more often than you'd like, and a
    Decimal() on "$129.99" raises, so a real price was being read as absent.
    """
    if isinstance(value, (int, float, Decimal)):
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    # Keep digits, sign and the decimal point; drop symbols, spaces, commas
    # and any trailing currency code.
    cleaned = re.sub(r"[^0-9.\-]", "", text.replace(",", ""))
    if not cleaned or cleaned in ("-", ".", "-."):
        return None
    return cleaned


# Keys whose value is plausibly a market price, best first. A key containing
# "market" wins over a bare "price", which wins over "low"/"mid" — we want the
# figure a portfolio should be valued at, not the cheapest listing.
_PRICE_KEY_RANKS = (
    (re.compile(r"market", re.I), 0),
    (re.compile(r"^price$|current|latest|avg|average", re.I), 1),
    (re.compile(r"mid|median", re.I), 2),
    (re.compile(r"price", re.I), 3),
)
# Never read a price out of these: they are historical or unrelated figures.
_PRICE_KEY_REJECT = re.compile(
    r"low|min|max|high|change|percent|pct|count|id$|qty|quantity|volume|"
    r"psa|bgs|cgc|graded|foil.*low|days?|weeks?|months?", re.I)


# Leaf names that carry no meaning of their own — they inherit it from the
# key above, if that one looked like a price.
_GENERIC_VALUE_KEY = re.compile(
    r"^(amount|value|usd|cents|raw|num|number|total)$", re.I)


def _rank_price_key(key: str):
    if _PRICE_KEY_REJECT.search(key or ""):
        return None
    for pattern, rank in _PRICE_KEY_RANKS:
        if pattern.search(key or ""):
            return rank
    return None


def find_price(record, _depth: int = 0):
    """Best market price anywhere in a record, or None.

    Walks the structure instead of guessing at a fixed set of key paths. The
    sealed endpoint's shape was never documented to us, and the first import
    showed every product parsing correctly EXCEPT its price — the figure was
    there, just not where the flat lookup looked.

    Shallower matches win over deeper ones, and a "market" key wins over a
    generic "price", so a top-level market price beats a nested historical
    one.
    """
    if _depth > 4 or record is None:
        return None

    best = None            # (rank, depth, value)
    stack = [(record, "", 0)]
    while stack:
        node, key, depth = stack.pop(0)
        if depth > 4:
            continue
        if isinstance(node, dict):
            for k, v in node.items():
                child = str(k)
                # A price-shaped parent carries its meaning down to a generic
                # leaf: {"marketPrice": {"amount": 129.99}} is a market price,
                # but "amount" alone tells you nothing.
                if _rank_price_key(child) is None and _rank_price_key(key) is not None                         and _GENERIC_VALUE_KEY.match(child):
                    child = key
                stack.append((v, child, depth + 1))
            continue
        if isinstance(node, list):
            # Only look into short lists — a long one is card rows, not a price.
            for item in node[:5]:
                stack.append((item, key, depth + 1))
            continue
        rank = _rank_price_key(key)
        if rank is None:
            continue
        money = to_money(_clean_money_text(node))
        if money is None or money <= 0:
            continue
        candidate = (rank, depth, money)
        if best is None or candidate[:2] < best[:2]:
            best = candidate
    return best[2] if best else None


def parse_sealed_row(row, fallback_set: str = "") -> dict:
    """One raw API record -> our columns, or None if it isn't usable.

    A row with no name is dropped: a nameless product is one nobody can find
    in a search, so storing it only pollutes the catalogue.
    """
    if not isinstance(row, dict):
        return None
    name = str(_first(row, _NAME_KEYS) or "").strip()
    if not name:
        return None
    set_name = str(_first(row, _SET_KEYS) or fallback_set or "").strip()
    image = str(_first(row, _IMAGE_KEYS) or "").strip()
    return {
        "name": name[:200],
        "set_name": set_name[:200],
        "product_type": classify_sealed_type(name, str(_first(row, _TYPE_KEYS) or "")),
        "ppt_product_id": str(_first(row, _ID_KEYS) or "")[:60],
        "image_url": image[:500] if image.startswith("http") else "",
        "market_price": find_price(row),
    }


def parse_sealed_rows(rows, fallback_set: str = "") -> tuple:
    """Parse a batch. Returns (parsed, skipped) so the caller can report how
    many records it could not read, rather than silently importing fewer."""
    parsed, skipped = [], 0
    seen = set()
    for row in rows or []:
        item = parse_sealed_row(row, fallback_set)
        if not item:
            skipped += 1
            continue
        # The natural key the table is unique on — dedupe within the batch so
        # one import can't collide with itself.
        key = (item["set_name"].lower(), item["name"].lower(), item["product_type"])
        if key in seen:
            skipped += 1
            continue
        seen.add(key)
        parsed.append(item)
    return parsed, skipped



# How long a "this set has no sealed product" answer is trusted. Long, because
# a set that shipped no sealed product never will — but not forever, since the
# API's coverage improves over time.
EMPTY_CHECK_TTL_DAYS = int(os.getenv("SEALED_EMPTY_CHECK_TTL_DAYS", "30"))


async def recently_checked_sets(pool) -> set:
    """Sets we recently asked about and found NOTHING for.

    Sets that DID have product are not skipped — re-running is how prices and
    newly-added products get refreshed.
    """
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT set_name FROM sealed_set_checks "
                "WHERE found = 0 AND checked_at > NOW() - ($1 || ' days')::interval",
                str(EMPTY_CHECK_TTL_DAYS))
        return {r["set_name"] for r in rows}
    except Exception:
        # If the log is unreadable, check everything — slower, never wrong.
        logger.exception("Sealed: could not read the check log")
        return set()


async def record_sealed_check(pool, set_name: str, found: int) -> None:
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO sealed_set_checks (set_name, checked_at, found)
                VALUES ($1, NOW(), $2)
                ON CONFLICT (set_name)
                DO UPDATE SET checked_at = NOW(), found = EXCLUDED.found
                """, set_name, int(found))
    except Exception:
        logger.exception("Sealed: could not record the check for %s", set_name)


async def import_sealed_products(pool, items, language: str = "english",
                                 game: str = "pokemon") -> dict:
    """Upsert parsed products. Idempotent on the natural key.

    Refreshes image and price on an existing row — those change — but never
    touches anything a member owns: lots reference sealed_products by id, and
    that id is preserved by the upsert.
    """
    added = updated = 0
    async with pool.acquire() as conn:
        for item in items or []:
            row = await conn.fetchrow(
                """
                INSERT INTO sealed_products
                    (game, language, set_name, name, product_type, image_url,
                     ppt_product_id, market_price, price_updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,
                        CASE WHEN $8::numeric IS NULL THEN NULL ELSE NOW() END)
                ON CONFLICT (game, language, set_name, name, product_type)
                DO UPDATE SET
                    image_url      = COALESCE(NULLIF(EXCLUDED.image_url, ''),
                                              sealed_products.image_url),
                    ppt_product_id = COALESCE(NULLIF(EXCLUDED.ppt_product_id, ''),
                                              sealed_products.ppt_product_id),
                    market_price   = COALESCE(EXCLUDED.market_price,
                                              sealed_products.market_price),
                    price_updated_at = CASE
                        WHEN EXCLUDED.market_price IS NOT NULL THEN NOW()
                        ELSE sealed_products.price_updated_at END
                RETURNING id, (xmax = 0) AS inserted
                """,
                game, language, item["set_name"], item["name"],
                item["product_type"], item.get("image_url") or "",
                item.get("ppt_product_id") or "", item.get("market_price"))
            if row and row["inserted"]:
                added += 1
            else:
                updated += 1
    logger.info("Sealed: imported %d new, %d updated", added, updated)
    return {"added": added, "updated": updated}
