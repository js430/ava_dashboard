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
    "CREATE INDEX IF NOT EXISTS idx_sealed_set ON sealed_products (set_name)",
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

    unit_cost = to_money(payload.get("unit_cost", 0), Decimal("0"))
    if unit_cost is None or unit_cost < 0:
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
    l.sold_on, l.sale_unit_price, l.sale_fees, l.notes, l.created_at
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
                 sale_fees, notes)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8::date,$9,$10::date,$11,$12,$13)
            RETURNING {LOT_COLUMNS}
            """,
            portfolio_id, card_id, sealed_id, cleaned["quantity"],
            cleaned["condition"], cleaned["unit_cost"], cleaned["fees"],
            cleaned["purchased_on"], cleaned["acquired_from"], cleaned["sold_on"],
            cleaned["sale_unit_price"], cleaned["sale_fees"], cleaned["notes"])
    return dict(row), None


async def update_lot(pool, user_id: int, lot_id: int, payload: dict) -> tuple:
    """Edit a lot. The ownership join in the WHERE clause is what stops one
    member editing another's holdings by lot id."""
    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            """
            SELECT l.id, l.card_id, l.sealed_id
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
        row = await conn.fetchrow(
            f"""
            UPDATE portfolio_lots
               SET quantity = $2, condition = $3, unit_cost = $4, fees = $5,
                   purchased_on = $6::date, acquired_from = $7, sold_on = $8::date,
                   sale_unit_price = $9, sale_fees = $10, notes = $11,
                   updated_at = NOW()
             WHERE id = $1
            RETURNING {LOT_COLUMNS}
            """,
            lot_id, cleaned["quantity"], cleaned["condition"], cleaned["unit_cost"],
            cleaned["fees"], cleaned["purchased_on"], cleaned["acquired_from"],
            cleaned["sold_on"], cleaned["sale_unit_price"], cleaned["sale_fees"],
            cleaned["notes"])
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

    SEALED PRODUCT HAS NO PRICE SOURCE YET. PokemonPriceTracker's documented
    v2 endpoints in price_sources.py are /cards and /sets only; whether it
    serves sealed is unconfirmed. Sealed items therefore return no price and
    are reported as unpriced rather than assumed to be worth nothing.
    """
    card_ids = sorted({int(l["card_id"]) for l in lots if l.get("card_id")})
    prices = {}
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

# The SKUs a modern English set almost always has. Deliberately conservative:
# a member can only pick from what's seeded, and a missing box is a smaller
# problem than a list full of products that were never printed.
DEFAULT_SEALED_TYPES = ("booster_box", "elite_trainer_box", "booster_bundle")


def sealed_display_name(set_name: str, product_type: str) -> str:
    label = SEALED_TYPE_LABELS.get(product_type, "Sealed")
    return f"{set_name} {label}".strip()


async def seed_sealed_for_set(pool, set_name: str, language: str = "english",
                              product_types=None, game: str = "pokemon") -> int:
    """Create the standard sealed SKUs for one set. Returns rows added.

    Idempotent — ON CONFLICT DO NOTHING against the natural key, so running it
    twice adds nothing and never duplicates a product members already hold.
    """
    set_name = (set_name or "").strip()[:200]
    if not set_name:
        return 0
    types = [t for t in (product_types or DEFAULT_SEALED_TYPES)
             if t in SEALED_PRODUCT_TYPES]
    if not types:
        return 0
    added = 0
    async with pool.acquire() as conn:
        for product_type in types:
            row = await conn.fetchrow(
                """
                INSERT INTO sealed_products (game, language, set_name, name, product_type)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (game, language, set_name, name, product_type) DO NOTHING
                RETURNING id
                """,
                game, language, set_name,
                sealed_display_name(set_name, product_type), product_type)
            if row:
                added += 1
    if added:
        logger.info("Sealed: seeded %d product(s) for %s", added, set_name)
    return added


async def sealed_price_available() -> bool:
    """Whether sealed product has a price source. False today — see the note
    above. Exposed as a function so the UI asks rather than hardcoding it, and
    so turning it on later is a one-line change here."""
    return False
