"""Auto-delete rules — the mod-facing editor for For Sale forum removal rules.

WRITTEN HERE, ENFORCED BY THE BOT. `forum_auto_delete_rules` is owned by
`ava_bot`, which creates and migrates it on startup and reads it fresh on every
new For Sale post. This app only inserts, updates, ends and deletes rows. It
must NEVER issue CREATE/ALTER for the table — two apps altering one table
drift apart. If the editor needs a new column, it is added on the bot side.

The format below is the bot's (`utils/autodelete_rules.py` in ava_bot is the
source of truth).

THE CONDITIONS FORMAT. A rule is a list of groups. A post is removed if ANY
group matches; a group matches when the post contains EVERY term in `all` and
NONE of the terms in `none`:

    {"any": [{"all": ["30th", "ditto"], "none": ["birthday"]},
             {"all": ["greninja ex box"]}]}

WHY THE CHECKS BELOW EXIST. The bot skips a rule it cannot use and logs it
once, so the mod who saved a broken rule never finds out. Every check in
`validate()` is one the bot would silently swallow. The one that matters most:
a group with only `none` terms, or a blank term, matches (nearly) every post
in the forum.

LEGACY ROWS. Old rows carry a single `keyword` and an empty `conditions`. The
bot reads those as one group holding that one word. `rule_json()` shows them
the same way, so the editor lists them like any other rule, and saving one
writes `conditions` (the bot prefers it) without touching `keyword`.

TIMES. Mods think in Eastern, and the end time is what sellers are told is
when they can relist. The browser sends plain wall-clock strings
("2026-10-16T23:59"); they are read as Eastern HERE, not in the browser, so the
stored instant never depends on the mod's own timezone.
"""

import json
import logging
import os
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import asyncpg

logger = logging.getLogger("dashboard.auto_delete_rules")

EASTERN = ZoneInfo("America/New_York")

# Bounds on one payload — input validation, not a cap on how many rules exist.
MAX_NAME_LEN = 100
MAX_REASON_LEN = 500
MAX_TERM_LEN = 100
MAX_GROUPS = 25
MAX_TERMS_PER_LIST = 25

# The For Sale forum's tags. These are hardcoded in the bot and are not stored
# anywhere this app can read; there is no Discord bot token here to ask the
# forum directly. Overridable with AUTO_DELETE_TAGS (comma separated) exactly
# like MONITOR_CHANNELS, so a new tag is a Railway edit, not a deploy. Free
# text would not be safe: one misspelling and a rule quietly matches nothing.
DEFAULT_TAGS = ("Pokemon", "One Piece", "Riftbound", "DBZ", "Sports",
                "Lorcana", "Sealed", "Singles")

_SPACES = re.compile(r"\s+")
_CONTROL = re.compile(r"[\x00-\x1f\x7f]")

_COLUMNS = ("id, name, conditions, keyword, tags, start_date, end_date, "
            "reason, created_by, created_at")

_MISSING = (asyncpg.UndefinedTableError, asyncpg.UndefinedColumnError)


class RulesUnavailable(Exception):
    """The table (or a column the spec promises) isn't in the database yet —
    the bot creates it on startup, so this is expected until it has run."""


def for_sale_tags() -> list:
    """The tag names a rule may be limited to, in display order."""
    raw = (os.getenv("AUTO_DELETE_TAGS", "") or "").strip()
    names = [p.strip() for p in raw.split(",")] if raw else list(DEFAULT_TAGS)
    out, seen = [], set()
    for name in names:
        key = name.lower()
        if name and key not in seen:
            seen.add(key)
            out.append(name)
    return out


def clean_term(raw) -> str:
    """One term as the mod typed it: control characters dropped (a NUL is the
    bot's own field separator), whitespace collapsed, ends trimmed."""
    return _SPACES.sub(" ", _CONTROL.sub(" ", str(raw or ""))).strip()


def _clean_terms(raw, empty_message: str, allow_empty: bool):
    """(terms, error) for one `all` / `none` list."""
    if raw is None:
        raw = []
    if not isinstance(raw, list) or len(raw) > MAX_TERMS_PER_LIST:
        return None, (f"Too many words in one list — keep it to "
                      f"{MAX_TERMS_PER_LIST} or fewer.")
    terms, seen = [], set()
    for item in raw:
        term = clean_term(item)
        if not term:
            return None, "One of the words is blank. Remove it or type something."
        if len(term) > MAX_TERM_LEN:
            return None, f"“{term[:30]}…” is too long for a search word."
        if term.lower() not in seen:
            seen.add(term.lower())
            terms.append(term)
    if not terms and not allow_empty:
        return None, empty_message
    return terms, None


def parse_eastern(raw):
    """A datetime-local string read as Eastern wall-clock time -> aware UTC
    datetime, or None if it isn't a date. Anything that already carries an
    offset is converted rather than reinterpreted."""
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=EASTERN)
    return parsed.astimezone(timezone.utc)


def validate(payload, allowed_tags) -> tuple:
    """(cleaned, error) for one submitted rule.

    Refuses everything the spec lists as a block. The softer cases (a very
    short word, a redundant group, an end time already past) are warnings the
    page raises itself; the rule is still valid and still saves.
    """
    if not isinstance(payload, dict):
        return None, "That didn't look like a rule."

    name = clean_term(payload.get("name"))
    if len(name) > MAX_NAME_LEN:
        return None, f"Keep the name under {MAX_NAME_LEN} characters."

    reason = str(payload.get("reason") or "").strip()
    if len(reason) > MAX_REASON_LEN:
        return None, f"Keep the seller message under {MAX_REASON_LEN} characters."

    raw_groups = payload.get("groups")
    if not isinstance(raw_groups, list) or not raw_groups:
        return None, "Add at least one group of words to look for."
    if len(raw_groups) > MAX_GROUPS:
        return None, f"That's a lot of groups — keep it to {MAX_GROUPS} or fewer."
    any_groups = []
    for index, group in enumerate(raw_groups, start=1):
        if not isinstance(group, dict):
            return None, f"Group {index} didn't look right."
        must, error = _clean_terms(
            group.get("all"),
            f"Group {index} needs at least one word it must contain. A group "
            "with only “must not contain” words would match almost every post.",
            allow_empty=False)
        if error:
            return None, error
        none, error = _clean_terms(group.get("none"), "", allow_empty=True)
        if error:
            return None, error
        entry = {"all": must}
        if none:
            entry["none"] = none
        any_groups.append(entry)

    # Tags: matched to the real list case-insensitively and stored with the
    # list's own spelling. A blank or unknown tag is refused, never dropped —
    # dropping one could empty the list, and an empty list means "any tag".
    raw_tags = payload.get("tags") or []
    if not isinstance(raw_tags, list):
        return None, "The tags didn't look right."
    by_lower = {t.lower(): t for t in allowed_tags}
    tags = []
    for value in raw_tags:
        tag = clean_term(value)
        if not tag:
            return None, "One of the tags is blank."
        canonical = by_lower.get(tag.lower())
        if canonical is None:
            return None, f"“{tag[:30]}” isn't a tag on the For Sale forum."
        if canonical not in tags:
            tags.append(canonical)

    start = parse_eastern(payload.get("start"))
    if start is None:
        return None, "Pick a start date and time."
    end = parse_eastern(payload.get("end"))
    if end is None:
        return None, "Pick an end date and time."
    if end <= start:
        return None, "The end time has to come after the start time."

    return {
        "name": name or None,
        "conditions": json.dumps({"any": any_groups}),
        "tags": tags,
        "start_date": start,
        "end_date": end,
        "reason": reason or None,
    }, None


def _read_conditions(row: dict) -> tuple:
    """(groups, legacy, usable) for a stored row, tolerating every shape the
    bot tolerates. `usable` is False when the bot would skip the row."""
    cond = row.get("conditions")
    if isinstance(cond, (str, bytes)):
        try:
            cond = json.loads(cond)
        except ValueError:
            cond = None
    if isinstance(cond, dict) and isinstance(cond.get("any"), list) and cond["any"]:
        groups = []
        for raw in cond["any"]:
            if not isinstance(raw, dict):
                continue
            must = [t for t in (raw.get("all") or []) if isinstance(t, str)]
            none = [t for t in (raw.get("none") or []) if isinstance(t, str)]
            groups.append({"all": must, "none": none})
        usable = bool(groups) and all(
            g["all"] and all(clean_term(t) for t in g["all"] + g["none"])
            for g in groups)
        return groups, False, usable
    keyword = clean_term(row.get("keyword"))
    if keyword:
        return [{"all": [keyword], "none": []}], True, True
    return [], False, False


def _status(row: dict, now: datetime) -> str:
    if now < row["start_date"]:
        return "scheduled"
    if now < row["end_date"]:
        return "live"
    return "ended"


def rule_json(row: dict, names: dict, now: datetime) -> dict:
    groups, legacy, usable = _read_conditions(row)
    creator = row.get("created_by")
    created_at = row.get("created_at")
    return {
        "id": row["id"],
        "name": row.get("name") or "",
        "groups": groups,
        "legacy": legacy,
        "usable": usable,
        "tags": list(row.get("tags") or []),
        "start": row["start_date"].isoformat(),
        "end": row["end_date"].isoformat(),
        "reason": row.get("reason") or "",
        "status": _status(row, now),
        # A snowflake overflows a JS number, so it travels as text.
        "created_by": str(creator) if creator is not None else "",
        "created_by_name": names.get(creator, ""),
        "created_at": created_at.isoformat() if created_at else None,
    }


_STATUS_RANK = {"live": 0, "scheduled": 1, "ended": 2}


def _sort_key(rule: dict):
    # Live and scheduled: soonest-ending first. Ended: most recently ended first.
    end = datetime.fromisoformat(rule["end"]).timestamp()
    return (_STATUS_RANK[rule["status"]],
            -end if rule["status"] == "ended" else end)


async def _names(conn, user_ids) -> dict:
    """user_id -> username from the bot's `users` table (the same lookup the
    raffle wheel uses). A miss just leaves the id on screen."""
    ids = [i for i in set(user_ids) if i is not None]
    if not ids:
        return {}
    try:
        rows = await conn.fetch(
            "SELECT user_id, username FROM users WHERE user_id = ANY($1::bigint[])",
            ids)
    except asyncpg.PostgresError:
        return {}
    return {r["user_id"]: r["username"] for r in rows if r["username"]}


def _unavailable(exc: Exception):
    """Turn "table or column isn't there yet" into one error the routes can
    show; anything else is a real bug and propagates."""
    logger.warning("forum_auto_delete_rules unavailable: %s", exc)
    raise RulesUnavailable() from exc


async def _one(conn, row):
    if row is None:
        return None
    return rule_json(dict(row), await _names(conn, [row["created_by"]]),
                     datetime.now(timezone.utc))


async def list_rules(pool) -> list:
    """Every rule, live first."""
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT {_COLUMNS} FROM forum_auto_delete_rules ORDER BY id DESC")
            names = await _names(conn, [r["created_by"] for r in rows])
    except _MISSING as exc:
        _unavailable(exc)
    now = datetime.now(timezone.utc)
    rules = [rule_json(dict(r), names, now) for r in rows]
    rules.sort(key=_sort_key)
    return rules


async def create_rule(pool, user_id: int, cleaned: dict) -> dict:
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO forum_auto_delete_rules "
                "  (name, conditions, tags, start_date, end_date, reason, created_by) "
                "VALUES ($1, $2::jsonb, $3, $4, $5, $6, $7) "
                f"RETURNING {_COLUMNS}",
                cleaned["name"], cleaned["conditions"], cleaned["tags"],
                cleaned["start_date"], cleaned["end_date"], cleaned["reason"],
                user_id)
            return await _one(conn, row)
    except _MISSING as exc:
        _unavailable(exc)


async def update_rule(pool, rule_id: int, cleaned: dict):
    """The updated rule, or None if it's gone. `created_by` and `keyword` are
    deliberately left alone: the first is who made it, the second is the
    legacy field the bot only reads when `conditions` is empty."""
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE forum_auto_delete_rules "
                "SET name = $2, conditions = $3::jsonb, tags = $4, "
                "    start_date = $5, end_date = $6, reason = $7 "
                f"WHERE id = $1 RETURNING {_COLUMNS}",
                rule_id, cleaned["name"], cleaned["conditions"], cleaned["tags"],
                cleaned["start_date"], cleaned["end_date"], cleaned["reason"])
            return await _one(conn, row)
    except _MISSING as exc:
        _unavailable(exc)


async def end_rule_now(pool, rule_id: int) -> tuple:
    """(rule, error). Sets `end_date` to now, which keeps the rule's history.
    Only a rule that is live right now can be ended: for one that hasn't
    started, "now" would land before its start."""
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE forum_auto_delete_rules SET end_date = NOW() "
                "WHERE id = $1 AND start_date <= NOW() AND end_date > NOW() "
                f"RETURNING {_COLUMNS}",
                rule_id)
            if row is not None:
                return await _one(conn, row), None
            exists = await conn.fetchval(
                "SELECT 1 FROM forum_auto_delete_rules WHERE id = $1", rule_id)
    except _MISSING as exc:
        _unavailable(exc)
    if exists:
        return None, "That rule isn't live, so there's nothing to end."
    return None, "That rule is already gone."


async def delete_rule(pool, rule_id: int) -> bool:
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "DELETE FROM forum_auto_delete_rules WHERE id = $1 RETURNING id",
                rule_id)
    except _MISSING as exc:
        _unavailable(exc)
    return row is not None
