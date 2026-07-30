"""Sports card inventory / eBay pick tool — schema + core logic.

Owned by ava_dashboard. Unrelated to the restock-tracking tables this app
otherwise reads (see data-system.md) — this is a standalone feature for one
seller's physical card inventory, sharing this app's DB and auth only because
it's convenient, not because the data means anything to ava_bot.

CORE IDEA: SKU-as-address. eBay's Custom Label (SKU) field appears on the sold
order and packing slip. If the SKU is the storage bin code, the pick location
arrives with the sale for free. Everything here follows from that.

BIN, NOT SLOT. A bin holds ~25 cards with no internal order — pulling one card
doesn't require renumbering the rest. Capacity is enforced; position within a
bin is not tracked. This means a bin's SKU is shared by every card in it
(distinct from a per-card SKU) — that's intentional, not a bug: eBay's Custom
Label has no uniqueness requirement.

OCCUPANCY IS COMPUTED, NOT STORED. A bin's fill level is
COUNT(cards WHERE bin_id = X AND status IN PHYSICALLY_PRESENT), not a counter
column. That's what makes "free the slot on picked, not on sold" free: moving
a card to 'picked' changes what the COUNT sees, with no counter to keep in
sync and no way for it to drift from the cards table.

NOT YET BUILT (left as clean extension points, not stubs):
  - eBay OAuth + Sell Inventory API sync (createOffer / publishOffer)
  - Vision-model photo intake (front/back -> structured card JSON)
  - Checklist-database resolution (year+set+number -> canonical fields)
  - Fulfillment API polling for the pick queue
Intake here is manual entry; SKU assignment and the pick/status workflow are
real and are the part everything else attaches to.
"""

import logging
from datetime import datetime, timezone

logger = logging.getLogger("dashboard.inventory")

# Cards in these statuses physically occupy their bin. 'sold' is deliberately
# included: the card is sold but not yet in the seller's hand, so the space
# isn't free. Freed only at 'picked' — see the module docstring.
OCCUPYING_STATUSES = ("available", "listed", "sold")

# needs_location is a flag, not a status, so it can layer onto any of these
# without adding a cross-product of statuses.
STATUSES = ("available", "listed", "sold", "picked", "shipped",
           "at_grader", "on_consignment", "returned")

# Legal status transitions. Enforced in code (not a DB CHECK) because the
# violation needs a specific, actionable error message — "you can't ship a
# card that hasn't been picked" — not a generic constraint failure.
_TRANSITIONS = {
    "available":      {"listed", "sold", "at_grader", "on_consignment"},
    "listed":         {"available", "sold", "at_grader", "on_consignment"},
    "sold":           {"picked", "available"},   # available = cancel/refund
    "picked":         {"shipped", "available"},  # available = "can't find it" cancel
    "shipped":        set(),                     # terminal; a return re-enters via 'returned'
    "at_grader":      {"available"},
    "on_consignment": {"available"},
    "returned":       {"available"},              # restow as new, per the design notes
}

DEFAULT_BIN_CAPACITY = 25

# Zones by handling class, coarse on purpose — see the project context this
# was designed from: category picks the zone, sequential fill picks the bin.
DEFAULT_ZONES = [
    {"code": "PREM", "label": "Premium", "handling_class": "premium",
     "note": "Autos, numbered, SSP, graded — one-touches / toploaders."},
    {"code": "STD", "label": "Standard", "handling_class": "standard",
     "note": "Inserts, parallels, base."},
]

INVENTORY_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS inv_zones (
        id             SERIAL PRIMARY KEY,
        code           TEXT NOT NULL UNIQUE,
        label          TEXT NOT NULL,
        handling_class TEXT NOT NULL DEFAULT '',
        note           TEXT NOT NULL DEFAULT '',
        sort_order     INTEGER NOT NULL DEFAULT 0,
        created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS inv_bins (
        id          SERIAL PRIMARY KEY,
        zone_id     INTEGER NOT NULL REFERENCES inv_zones(id) ON DELETE CASCADE,
        code        TEXT NOT NULL UNIQUE,
        capacity    INTEGER NOT NULL DEFAULT 25,
        seq         INTEGER NOT NULL,
        is_active   BOOLEAN NOT NULL DEFAULT TRUE,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_inv_bins_zone_seq ON inv_bins (zone_id, seq)",
    """
    CREATE TABLE IF NOT EXISTS inv_cards (
        id              SERIAL PRIMARY KEY,
        bin_id          INTEGER REFERENCES inv_bins(id) ON DELETE RESTRICT,
        status          TEXT NOT NULL DEFAULT 'available',
        needs_location  BOOLEAN NOT NULL DEFAULT FALSE,
        cant_find       BOOLEAN NOT NULL DEFAULT FALSE,

        -- Listing identity, filled in manually for now; an eBay sync would
        -- write ebay_listing_id and ebay_sku back after publish.
        player          TEXT NOT NULL DEFAULT '',
        year            TEXT NOT NULL DEFAULT '',
        manufacturer    TEXT NOT NULL DEFAULT '',
        set_name        TEXT NOT NULL DEFAULT '',
        card_number     TEXT NOT NULL DEFAULT '',
        parallel        TEXT NOT NULL DEFAULT '',
        features        TEXT NOT NULL DEFAULT '',   -- free text: "auto, /25, rookie"
        sport           TEXT NOT NULL DEFAULT '',

        is_graded         BOOLEAN NOT NULL DEFAULT FALSE,
        grading_company   TEXT NOT NULL DEFAULT '',
        grade             TEXT NOT NULL DEFAULT '',
        cert_number       TEXT NOT NULL DEFAULT '',

        price           NUMERIC,
        ebay_listing_id TEXT NOT NULL DEFAULT '',
        ebay_sku        TEXT NOT NULL DEFAULT '',   -- the bin code, at listing time
        notes           TEXT NOT NULL DEFAULT '',

        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        picked_at       TIMESTAMPTZ,
        sold_at         TIMESTAMPTZ
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_inv_cards_bin ON inv_cards (bin_id)",
    "CREATE INDEX IF NOT EXISTS idx_inv_cards_status ON inv_cards (status)",
    "CREATE INDEX IF NOT EXISTS idx_inv_cards_needs_location ON inv_cards (needs_location) "
    "WHERE needs_location",
    # Every status change, so a wrong pick or a disputed "where did this go"
    # has an audit trail. Append-only, never updated or deleted.
    """
    CREATE TABLE IF NOT EXISTS inv_events (
        id          SERIAL PRIMARY KEY,
        card_id     INTEGER NOT NULL REFERENCES inv_cards(id) ON DELETE CASCADE,
        event       TEXT NOT NULL,
        from_status TEXT,
        to_status   TEXT,
        bin_id      INTEGER REFERENCES inv_bins(id),
        note        TEXT NOT NULL DEFAULT '',
        actor       TEXT NOT NULL DEFAULT '',
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_inv_events_card ON inv_events (card_id, created_at)",
]


async def ensure_inventory_schema(pool) -> None:
    """Create the inventory tables if absent, and seed the default zones.

    Idempotent — matches this repo's existing startup-ensure convention
    (card_tracker.py, catalog.py). Zones are seeded only when the table is
    empty, so a seller who renames or adds zones later isn't overwritten on
    every deploy.
    """
    async with pool.acquire() as conn:
        for ddl in INVENTORY_SCHEMA:
            await conn.execute(ddl)
        count = await conn.fetchval("SELECT COUNT(*) FROM inv_zones")
        if not count:
            for i, z in enumerate(DEFAULT_ZONES):
                await conn.execute(
                    "INSERT INTO inv_zones (code, label, handling_class, note, sort_order) "
                    "VALUES ($1, $2, $3, $4, $5)",
                    z["code"], z["label"], z["handling_class"], z["note"], i)
            logger.info("Inventory: seeded %d default zone(s)", len(DEFAULT_ZONES))
    logger.info("Inventory schema ensured")


class TransitionError(Exception):
    """A status change that isn't legal from the card's current status."""


def validate_transition(from_status: str, to_status: str) -> None:
    if to_status not in STATUSES:
        raise TransitionError(f"{to_status!r} isn't a known status")
    allowed = _TRANSITIONS.get(from_status, set())
    if to_status not in allowed:
        raise TransitionError(
            f"can't move a card from {from_status!r} to {to_status!r} "
            f"(allowed: {sorted(allowed) or 'none — terminal'})")


def next_bin_code(zone_code: str, seq: int) -> str:
    return f"{zone_code}-{seq}"


async def list_zones_with_bins(pool) -> list:
    """Zones, each with its bins and computed occupancy — the inventory
    overview. Occupancy is COUNT(...) at read time, per the module docstring;
    nothing here is a cached counter that could drift."""
    async with pool.acquire() as conn:
        zones = await conn.fetch(
            "SELECT id, code, label, handling_class, note FROM inv_zones ORDER BY sort_order, code")
        bins = await conn.fetch(
            """
            SELECT b.id, b.zone_id, b.code, b.capacity, b.seq, b.is_active,
                   COUNT(c.id) FILTER (WHERE c.status = ANY($1)) AS occupied
            FROM inv_bins b
            LEFT JOIN inv_cards c ON c.bin_id = b.id
            GROUP BY b.id
            ORDER BY b.zone_id, b.seq
            """,
            list(OCCUPYING_STATUSES))
    by_zone = {}
    for b in bins:
        by_zone.setdefault(b["zone_id"], []).append({
            "id": b["id"], "code": b["code"], "capacity": b["capacity"],
            "seq": b["seq"], "is_active": b["is_active"],
            "occupied": int(b["occupied"]),
            "full": int(b["occupied"]) >= b["capacity"],
        })
    return [{
        "id": z["id"], "code": z["code"], "label": z["label"],
        "handling_class": z["handling_class"], "note": z["note"],
        "bins": by_zone.get(z["id"], []),
    } for z in zones]


async def assign_bin(pool, zone_id: int, actor: str = "") -> dict:
    """Pick (or create) the current fill-pointer bin in a zone and return it.

    Fill pointer = the lowest-seq ACTIVE bin in the zone that isn't full.
    Sequential fill keeps the zone balanced without the caller ever choosing
    a specific bin — "no decisions at intake," per the design notes. A new
    bin is auto-created (next seq, same capacity) when every existing one in
    the zone is full, so intake is never blocked waiting on someone to add
    a bin by hand.
    """
    async with pool.acquire() as conn:
        zone = await conn.fetchrow("SELECT id, code FROM inv_zones WHERE id = $1", zone_id)
        if not zone:
            raise ValueError(f"no zone with id {zone_id}")

        candidate = await conn.fetchrow(
            """
            SELECT b.id, b.code, b.capacity, b.seq,
                   COUNT(c.id) FILTER (WHERE c.status = ANY($2)) AS occupied
            FROM inv_bins b
            LEFT JOIN inv_cards c ON c.bin_id = b.id
            WHERE b.zone_id = $1 AND b.is_active
            GROUP BY b.id
            HAVING COUNT(c.id) FILTER (WHERE c.status = ANY($2)) < b.capacity
            ORDER BY b.seq ASC
            LIMIT 1
            """,
            zone_id, list(OCCUPYING_STATUSES))
        if candidate:
            return {"id": candidate["id"], "code": candidate["code"],
                    "capacity": candidate["capacity"],
                    "occupied": int(candidate["occupied"])}

        max_seq = await conn.fetchval(
            "SELECT COALESCE(MAX(seq), 0) FROM inv_bins WHERE zone_id = $1", zone_id)
        new_seq = int(max_seq) + 1
        code = next_bin_code(zone["code"], new_seq)
        row = await conn.fetchrow(
            "INSERT INTO inv_bins (zone_id, code, capacity, seq) VALUES ($1, $2, $3, $4) "
            "RETURNING id, code, capacity",
            zone_id, code, DEFAULT_BIN_CAPACITY, new_seq)
        logger.info("Inventory: auto-created bin %r in zone %r (fill pointer advanced)",
                    code, zone["code"])
        return {"id": row["id"], "code": row["code"], "capacity": row["capacity"], "occupied": 0}


async def log_event(conn, card_id: int, event: str, from_status=None, to_status=None,
                    bin_id=None, note: str = "", actor: str = "") -> None:
    await conn.execute(
        "INSERT INTO inv_events (card_id, event, from_status, to_status, bin_id, note, actor) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7)",
        card_id, event, from_status, to_status, bin_id, note, actor)


async def stow_card(pool, card_id: int, zone_id: int, actor: str = "") -> dict:
    """Assign a bin to a newly-intaken card. Sets status='available' and
    ebay_sku to the bin code — the SKU-as-address the whole design rests on."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            b = await assign_bin(pool, zone_id, actor=actor)
            await conn.execute(
                "UPDATE inv_cards SET bin_id = $1, ebay_sku = $2, status = 'available', "
                "updated_at = NOW() WHERE id = $3",
                b["id"], b["code"], card_id)
            await log_event(conn, card_id, "stowed", to_status="available",
                            bin_id=b["id"], note=f"assigned {b['code']}", actor=actor)
    return b


async def set_status(pool, card_id: int, to_status: str, actor: str = "",
                     note: str = "") -> None:
    """Move a card to a new status, enforcing the transition table and the
    bin-freed-on-picked rule. Raises TransitionError on an illegal move —
    the caller decides whether that's a 400 or something else."""
    async with pool.acquire() as conn:
        card = await conn.fetchrow(
            "SELECT id, status, bin_id FROM inv_cards WHERE id = $1", card_id)
        if not card:
            raise ValueError(f"no card with id {card_id}")
        validate_transition(card["status"], to_status)

        async with conn.transaction():
            extra = ""
            if to_status == "picked":
                extra = ", picked_at = NOW()"
            elif to_status == "sold":
                extra = ", sold_at = NOW()"
            await conn.execute(
                f"UPDATE inv_cards SET status = $1, updated_at = NOW(){extra} WHERE id = $2",
                to_status, card_id)
            await log_event(conn, card_id, "status_change", from_status=card["status"],
                            to_status=to_status, bin_id=card["bin_id"], note=note, actor=actor)


async def flag_cant_find(pool, card_id: int, actor: str = "", note: str = "") -> None:
    """Fast cancel path when a picker can't locate a card — surfaces it for
    review instead of silently shipping the wrong thing or stalling the pick."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "UPDATE inv_cards SET cant_find = TRUE, updated_at = NOW() WHERE id = $1",
                card_id)
            await log_event(conn, card_id, "cant_find", note=note, actor=actor)


async def pick_queue(pool) -> list:
    """Cards sold but not yet picked, grouped by bin so a picker can batch a
    trip — "three sales in one bin is one trip," per the design notes."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT c.id, c.player, c.year, c.set_name, c.card_number, c.parallel,
                   c.ebay_listing_id, c.cant_find, c.sold_at,
                   b.code AS bin_code, z.code AS zone_code, z.label AS zone_label
            FROM inv_cards c
            JOIN inv_bins b ON b.id = c.bin_id
            JOIN inv_zones z ON z.id = b.zone_id
            WHERE c.status = 'sold'
            ORDER BY b.code, c.sold_at
            """)
    by_bin = {}
    for r in rows:
        by_bin.setdefault(r["bin_code"], {
            "bin_code": r["bin_code"], "zone_code": r["zone_code"],
            "zone_label": r["zone_label"], "cards": [],
        })["cards"].append({
            "id": r["id"], "player": r["player"], "year": r["year"],
            "set_name": r["set_name"], "card_number": r["card_number"],
            "parallel": r["parallel"], "ebay_listing_id": r["ebay_listing_id"],
            "cant_find": r["cant_find"],
            "sold_at": r["sold_at"].isoformat() if r["sold_at"] else None,
        })
    return sorted(by_bin.values(), key=lambda g: g["bin_code"])


async def needs_location_inbox(pool) -> list:
    """Cards flagged needs_location=TRUE — the landing spot for listings
    created outside this tool (see the module docstring: an eBay poller would
    set this flag on anything without a recognized SKU). Empty until that
    poller exists; the flag and the query are ready for it."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, player, year, set_name, card_number, ebay_listing_id, created_at "
            "FROM inv_cards WHERE needs_location = TRUE ORDER BY created_at")
    return [{
        "id": r["id"], "player": r["player"], "year": r["year"],
        "set_name": r["set_name"], "card_number": r["card_number"],
        "ebay_listing_id": r["ebay_listing_id"],
        "created_at": r["created_at"].isoformat() if r["created_at"] else None,
    } for r in rows]
