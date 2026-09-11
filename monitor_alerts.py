"""Monitor alert preferences — per-member keyword pings on Zephyr restocks.

WRITTEN HERE, READ BY THE BOT (WHICH DOES NOT EXIST YET). This app owns the
table and is its only writer. `ava_bot` will later read it to decide who to
ping when Zephyr posts a restock. Nothing in this repo depends on that code
existing, and nothing here should ever write `last_matched_at` — that column
belongs to the future matcher and is displayed read-only.

This is ADDITIVE to Zephyr's own role pings, which keep working untouched. A
member still gets whatever their monitor roles give them; this adds a second,
sharper ping for the exact products they name.

HOW IT DIFFERS FROM THE ORIGINAL HANDOFF SPEC, so the bot side isn't
surprised:

  * `tcg TEXT` is gone, replaced by `channel_ids BIGINT[]`. Scoping by the
    monitor channel a restock posts in is exact; a game taxonomy was a guess
    at how Zephyr names products, and the list it would have come from was
    defined for the For Sale forum tags, not for restocks.
  * `store_filter` is gone entirely. Zephyr's monitor channels ARE the
    stores — one channel per retailer — so a separate store filter asked
    the same question twice and could contradict the channel.
  * `dedupe_key` is new — a normalized signature of the whole alert, unique
    per user, so the same alert cannot be added twice even if two requests
    race. The bot can ignore this column entirely.

EMPTY ARRAY MEANS "ANY", NOT "NONE". `channel_ids = '{}'` means every monitor
channel. This is the one piece of meaning the bot has to share: a matcher that
treats an empty array as "matches nothing" would silently kill every
unfiltered alert, which is most of them.
"""

import logging
import os
import re
from decimal import Decimal, InvalidOperation

logger = logging.getLogger("dashboard.monitor_alerts")

MONITOR_ALERTS_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS product_subscriptions (
        id              SERIAL PRIMARY KEY,
        -- The Discord snowflake straight off the OAuth session
        -- (int(session["user"]["id"])), exactly as user_preferences and
        -- dashboard_sessions already store it. BIGINT because a snowflake
        -- overflows a 32-bit int.
        user_id         BIGINT NOT NULL,
        -- Matched case-insensitively as a substring of the restock's
        -- product name. Empty ONLY when match_all is true.
        keyword         TEXT NOT NULL,
        -- "Alert me for everything." When true the keyword is not consulted
        -- at all, and the alert fires on every restock in its channels that
        -- clears the price cap. An explicit column rather than a sentinel
        -- keyword: an empty keyword would match everything through
        -- strpos(x, '') > 0, which is true but far too easy to break by
        -- accident, and '*' would collide with a real product name.
        match_all       BOOLEAN NOT NULL DEFAULT FALSE,
        -- Empty array = any monitor channel. See the module docstring.
        channel_ids     BIGINT[] NOT NULL DEFAULT '{}',
        max_price_usd   NUMERIC(10, 2),
        delivery        TEXT NOT NULL DEFAULT 'channel'
                        CHECK (delivery IN ('channel', 'dm')),
        enabled         BOOLEAN NOT NULL DEFAULT TRUE,
        -- Normalized signature of the alert, so "add" is idempotent per
        -- user without a race. Written here, ignored by the bot.
        dedupe_key      TEXT NOT NULL,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        -- The future matcher's column. This app only ever reads it.
        last_matched_at TIMESTAMPTZ
    )
    """,
    "CREATE INDEX IF NOT EXISTS product_subscriptions_user_idx "
    "ON product_subscriptions (user_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS product_subscriptions_dedupe_idx "
    "ON product_subscriptions (user_id, dedupe_key)",
    # Added after the table shipped, so CREATE TABLE IF NOT EXISTS won't
    # reach it — same ALTER-not-CREATE pattern as catalog_cards.
    "ALTER TABLE product_subscriptions ADD COLUMN IF NOT EXISTS "
    "match_all BOOLEAN NOT NULL DEFAULT FALSE",
]

# A table created before the store filter was removed still has a `stores`
# TEXT[] NOT NULL DEFAULT '{}' column. Nothing writes or reads it, and the
# default means inserts that omit it succeed, so it is left alone rather than
# dropped automatically — dropping a column is not something a startup hook
# should do on its own. To clean it up by hand:
#     ALTER TABLE product_subscriptions DROP COLUMN IF EXISTS stores;

# Deliberately no cap on how many alerts one person may keep — that was an
# explicit call. These bound a single payload instead, which is input
# validation rather than a limit on the feature.
MAX_KEYWORD_LEN = 100
MIN_KEYWORD_LEN = 2
# Comfortably above the number of channels that exist, so selecting every
# one of them is never refused. (At 25 it was: there are 26.)
MAX_CHANNELS_PER_ALERT = 100
MAX_PRICE_USD = Decimal("100000")
# A blank "only under" box stores this rather than NULL, so every alert
# carries a real number and the bot never has to special-case a missing cap.
# High enough that nothing a monitor posts is excluded by it.
DEFAULT_MAX_PRICE_USD = Decimal("9999.00")

DELIVERY_MODES = ("channel", "dm")
DEFAULT_DELIVERY = "channel"

_SPACES = re.compile(r"\s+")


async def ensure_monitor_alerts_schema(pool) -> None:
    """Create the table if absent. Idempotent — matches this repo's existing
    startup-ensure convention (catalog.py, tips.py, inventory.py)."""
    async with pool.acquire() as conn:
        for ddl in MONITOR_ALERTS_SCHEMA:
            await conn.execute(ddl)


# The monitor channels Zephyr posts into. Defaulted here and overridable by
# MONITOR_CHANNELS, exactly like MOD_ROLE_IDS in main.py — so the page works
# the moment it deploys, and the list can still be changed from Railway
# without a code edit.
#
# The labels are deliberately NOT the raw Discord channel names. Those carry
# heavy sidebar decoration - arrows, emoji, and mathematical-alphanumeric
# letters that a screen reader spells out one character at a time. A filter
# chip has to be scannable, so each is written plainly here. The ids are what
# matter; the label is only what a member reads.
DEFAULT_MONITOR_CHANNELS = (
    (1497969273384075344, "Target — early info"),
    (1497969708522147972, "Walmart — early info"),
    (1497968729672384532, "Target — high stock"),
    (1496325629870604358, "Target"),
    (1497969667636203591, "Walmart"),
    (1495535049251229828, "Pokémon Center"),
    (1495516088861982790, "Amazon"),
    (1497969689413025803, "GT Collectibles"),
    (1497969487843168517, "Sam's Club"),
    (1497969434331975690, "Costco"),
    (1497970625564901406, "Crunchyroll"),
    (1497970954343551116, "Scheels"),
    (1497970154582048911, "Menards"),
    (1497971008953647337, "Dick's Sporting Goods"),
    (1497970984832073779, "Books-A-Million"),
    (1497968503125315604, "Best Buy"),
    (1497970196952907896, "GameStop"),
    (1497969551386742905, "Kohl's"),
    (1497968224212488293, "Macy's"),
    (1497970070956277843, "Pokene"),
    (1497969537394675742, "Lowe's"),
    (1497971029874708614, "Five Below"),
    (1497968178830246061, "Academy"),
    (1497970692577296434, "Barnes & Noble"),
    (1497968291304706058, "BoxLunch / Hot Topic"),
    (1497969950504128713, "Collector's Cache"),
    # Second batch. These channel names arrive already readable, so the
    # labels stay close to them rather than being reinvented — a member
    # picking "Target - MTG" should see the channel they know.
    (1496298876884353084, "Online monitor roles"),
    (1497970557247951070, "P-Bandai"),
    (1530322165524860958, "Online - Gundam"),
    (1497968528186409100, "Best Buy - MTG"),
    (1497968544493604966, "Best Buy - Sports"),
    (1497969210654064670, "Target - MTG"),
    (1497969229960577114, "Target - Sports"),
    (1495553146506969138, "Online - One Piece"),
    (1497969247417270473, "Target - One Piece"),
    (1497969727849496637, "Walmart - One Piece"),
    (1497970885645041685, "Riot Games Store"),
    (1496688024593895456, "Online - Riftbound"),
    (1497970016593903687, "Forge and Fire"),
    (1497970215957430393, "GameStop - One Piece"),
    (1497969899530752050, "GameNerdz - Pokémon"),
    (1497968317535621182, "BoxLunch / Hot Topic - One Piece"),
    (1497968342030614618, "BoxLunch / Hot Topic - Riftbound"),
)


def monitor_channels() -> list:
    """The monitor channels a member may pick from: [{id, name}].

    Read from MONITOR_CHANNELS as `id:Label` pairs, comma separated:

        MONITOR_CHANNELS="1406...:Pokemon Monitor,1407...:Sports Monitor"

    Falls back to DEFAULT_MONITOR_CHANNELS when the variable is unset, so a
    fresh deploy has a working list. Env rather than a table because every
    other Discord id in this app comes from env (REQUIRED_ROLE_ID,
    MOD_ROLE_IDS, INVENTORY_ROLE_IDS), and the list changes about as often.

    A malformed entry is skipped and logged rather than taking the page down
    — one typo should not cost every member their alerts page.
    """
    raw = (os.getenv("MONITOR_CHANNELS", "") or "").strip()
    if not raw:
        return [{"id": cid, "name": name} for cid, name in DEFAULT_MONITOR_CHANNELS]
    out, seen = [], set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        raw_id, _, label = part.partition(":")
        raw_id = raw_id.strip()
        if not raw_id.isdigit():
            logger.warning("MONITOR_CHANNELS: skipping %r — id is not numeric", part[:60])
            continue
        channel_id = int(raw_id)
        if channel_id in seen:
            continue
        seen.add(channel_id)
        out.append({"id": channel_id, "name": (label.strip() or raw_id)[:80]})
    return out


def normalize_keyword(raw) -> str:
    """Trimmed, single-spaced. Case is preserved for display; matching is
    case-insensitive on the bot's side, and the dedupe key lowercases."""
    return _SPACES.sub(" ", str(raw or "").strip())


def to_price(raw):
    """A price cap from the browser, or None. Returns (value, error).

    Accepts what someone actually types — "$45", "45.00", " 45 " — because
    the field is a plain text input, not a number spinner.
    """
    if raw is None:
        return None, None
    text = str(raw).strip().lstrip("$").replace(",", "").strip()
    if not text:
        return None, None
    try:
        value = Decimal(text)
    except (InvalidOperation, ValueError):
        return None, "That price doesn't look like a number."
    if not value.is_finite() or value <= 0:
        return None, "A price cap has to be more than zero."
    if value > MAX_PRICE_USD:
        return None, "That price cap is higher than anything worth capping."
    return value.quantize(Decimal("0.01")), None


def dedupe_key(keyword: str, channel_ids, max_price, delivery: str,
               match_all: bool = False) -> str:
    """A stable signature of one alert, so the same one can't be added twice.

    Sorted and lowercased, so ticking Target then Walmart matches ticking
    Walmart then Target — to a member those are plainly the same alert, and
    being told "you already have that one" is only sensible if it's true
    regardless of the order the boxes were checked.
    """
    parts = [
        "*all*" if match_all else keyword.strip().lower(),
        ",".join(str(c) for c in sorted(channel_ids or [])),
        "" if max_price is None else str(max_price),
        delivery,
    ]
    return "|".join(parts)


def validate(payload: dict, allowed_channel_ids) -> tuple:
    """(cleaned, error) for one submitted alert.

    `allowed_channel_ids` is the configured channel list. Anything outside it
    is rejected rather than trimmed away silently: a member whose channel
    quietly vanished would think they were covered when they weren't.
    """
    match_all = bool(payload.get("match_all"))
    keyword = normalize_keyword(payload.get("keyword"))
    if match_all:
        # The keyword box is ignored when someone asks for everything, and
        # blanked rather than stored — keeping it would leave a word on the
        # row that has no effect on what matches.
        keyword = ""
    elif len(keyword) < MIN_KEYWORD_LEN:
        return None, "Type at least a couple of characters to match on."
    elif len(keyword) > MAX_KEYWORD_LEN:
        return None, "That keyword is too long."

    raw_channels = payload.get("channel_ids") or []
    if not isinstance(raw_channels, list) or len(raw_channels) > MAX_CHANNELS_PER_ALERT:
        return None, "Pick a smaller set of channels."
    channel_ids = []
    for value in raw_channels:
        try:
            channel_id = int(value)
        except (TypeError, ValueError):
            return None, "One of those channels isn't valid."
        if channel_id not in allowed_channel_ids:
            return None, "One of those channels isn't a monitor channel."
        if channel_id not in channel_ids:
            channel_ids.append(channel_id)

    max_price, price_error = to_price(payload.get("max_price_usd"))
    if price_error:
        return None, price_error
    if max_price is None:
        max_price = DEFAULT_MAX_PRICE_USD

    # Not read from the payload. Direct messages were removed as a choice, so
    # every alert is a channel ping — honouring a hand-crafted `delivery: dm`
    # would put back the option the UI no longer offers. The column and its
    # CHECK stay, so turning DMs back on later is a UI change, not a
    # migration.
    delivery = DEFAULT_DELIVERY

    return {
        "keyword": keyword,
        "match_all": match_all,
        "channel_ids": sorted(channel_ids),
        "max_price_usd": max_price,
        "delivery": delivery,
        "dedupe_key": dedupe_key(keyword, channel_ids, max_price, delivery,
                                 match_all),
    }, None


_COLUMNS = ("id, keyword, match_all, channel_ids, max_price_usd, delivery, "
            "enabled, created_at, last_matched_at")


async def list_alerts(pool, user_id: int) -> list:
    """Every alert a member has, newest first."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT {_COLUMNS} FROM product_subscriptions "
            "WHERE user_id = $1 ORDER BY created_at DESC, id DESC",
            user_id)
    return [dict(r) for r in rows]


async def add_alert(pool, user_id: int, cleaned: dict) -> tuple:
    """(row, error). A duplicate is refused by the unique index, not by a
    read-then-write, so two quick clicks can't both land."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO product_subscriptions "
            "  (user_id, keyword, match_all, channel_ids, max_price_usd, "
            "   delivery, dedupe_key) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7) "
            "ON CONFLICT (user_id, dedupe_key) DO NOTHING "
            f"RETURNING {_COLUMNS}",
            user_id, cleaned["keyword"], cleaned["match_all"],
            cleaned["channel_ids"], cleaned["max_price_usd"],
            cleaned["delivery"], cleaned["dedupe_key"])
    if row is None:
        return None, "You already have that exact alert."
    return dict(row), None


async def delete_alert(pool, user_id: int, alert_id: int) -> bool:
    """True if a row was removed. Scoped by user_id, so an id belonging to
    somebody else simply isn't found."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "DELETE FROM product_subscriptions WHERE id = $1 AND user_id = $2 "
            "RETURNING id",
            alert_id, user_id)
    return row is not None


async def set_enabled(pool, user_id: int, alert_id: int, enabled: bool):
    """Pause or resume one alert without losing its filters. Returns the
    updated row, or None when it isn't this member's."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE product_subscriptions SET enabled = $3 "
            "WHERE id = $1 AND user_id = $2 "
            f"RETURNING {_COLUMNS}",
            alert_id, user_id, bool(enabled))
    return dict(row) if row else None
