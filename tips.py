"""Tips & Tricks — community-submitted tips grouped by category and topic.

Owned by ava_dashboard. Standalone feature (e.g. "Japan Travel and TCG in
Japan"), no relation to the bot's shared restock tables — sharing this app's
DB and auth only because it's convenient, not because the data means
anything to ava_bot.

Categories and topics are admin-managed (create/rename/reorder/delete) from
the page itself. Entries (the actual tips) are member-submitted free text
within a topic, visible immediately — no moderation queue, since only known
Discord identities (not anonymous guests) can post at all. An entry is
editable/removable by its own author or by any admin; main.py enforces that,
this module just does the writes once a caller has already decided they're
allowed to.
"""

import logging

logger = logging.getLogger("dashboard.tips")

TIPS_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS tips_categories (
        id          SERIAL PRIMARY KEY,
        name        TEXT NOT NULL,
        sort_order  INTEGER NOT NULL DEFAULT 0,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tips_topics (
        id           SERIAL PRIMARY KEY,
        category_id  INTEGER NOT NULL REFERENCES tips_categories(id) ON DELETE CASCADE,
        name         TEXT NOT NULL,
        sort_order   INTEGER NOT NULL DEFAULT 0,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_tips_topics_category ON tips_topics (category_id)",
    """
    CREATE TABLE IF NOT EXISTS tips_entries (
        id          SERIAL PRIMARY KEY,
        topic_id    INTEGER NOT NULL REFERENCES tips_topics(id) ON DELETE CASCADE,
        user_id     BIGINT NOT NULL,
        username    TEXT NOT NULL,
        content     TEXT NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        edited_at   TIMESTAMPTZ
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_tips_entries_topic ON tips_entries (topic_id)",
]

# A tip is a short, freeform note, not an essay — bounded mainly so one
# submission can't blow up the page for everyone else viewing the topic.
MAX_CONTENT_LENGTH = 2000
MAX_NAME_LENGTH = 80


async def ensure_tips_schema(pool) -> None:
    """Create the tips tables if absent. Idempotent — matches this repo's
    existing startup-ensure convention (catalog.py, population.py, inventory.py)."""
    async with pool.acquire() as conn:
        for ddl in TIPS_SCHEMA:
            await conn.execute(ddl)


async def list_tree(pool) -> list:
    """Full category -> topic -> entries tree, in display order.

    Three flat queries plus in-Python grouping rather than one big JOIN — a
    JOIN across categories/topics/entries would repeat every category/topic
    row once per entry, which is wasted transfer for a board that's read far
    more often than it's written to.
    """
    async with pool.acquire() as conn:
        categories = await conn.fetch(
            "SELECT id, name, sort_order FROM tips_categories ORDER BY sort_order ASC, id ASC")
        topics = await conn.fetch(
            "SELECT id, category_id, name, sort_order FROM tips_topics "
            "ORDER BY sort_order ASC, id ASC")
        entries = await conn.fetch(
            "SELECT id, topic_id, user_id, username, content, created_at, edited_at "
            "FROM tips_entries ORDER BY created_at ASC")

    topics_by_category = {}
    for t in topics:
        topics_by_category.setdefault(t["category_id"], []).append(t)
    entries_by_topic = {}
    for e in entries:
        entries_by_topic.setdefault(e["topic_id"], []).append(e)

    out = []
    for c in categories:
        cat_topics = []
        for t in topics_by_category.get(c["id"], []):
            cat_topics.append({
                "id": t["id"],
                "name": t["name"],
                "entries": [{
                    "id": e["id"],
                    "user_id": str(e["user_id"]),
                    "username": e["username"],
                    "content": e["content"],
                    "created_at": e["created_at"].isoformat(),
                    "edited_at": e["edited_at"].isoformat() if e["edited_at"] else None,
                } for e in entries_by_topic.get(t["id"], [])],
            })
        out.append({"id": c["id"], "name": c["name"], "topics": cat_topics})
    return out


# ---- Categories (admin-managed) ----

async def create_category(pool, name: str) -> int:
    name = name.strip()[:MAX_NAME_LENGTH]
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO tips_categories (name, sort_order) "
            "VALUES ($1, (SELECT COALESCE(MAX(sort_order), 0) + 1 FROM tips_categories)) "
            "RETURNING id",
            name)
    return row["id"]


async def rename_category(pool, category_id: int, name: str) -> None:
    name = name.strip()[:MAX_NAME_LENGTH]
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE tips_categories SET name = $1 WHERE id = $2", name, category_id)


async def reorder_category(pool, category_id: int, direction: str) -> None:
    """Swap this category's sort_order with its neighbor above/below.
    A no-op at either end of the list rather than an error — there's nothing
    meaningful to do when "move up" is clicked on the first item."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                "SELECT id, sort_order FROM tips_categories "
                "ORDER BY sort_order ASC, id ASC")
            ids = [r["id"] for r in rows]
            if category_id not in ids:
                return
            idx = ids.index(category_id)
            swap_idx = idx - 1 if direction == "up" else idx + 1
            if swap_idx < 0 or swap_idx >= len(ids):
                return
            a, b = rows[idx], rows[swap_idx]
            await conn.execute(
                "UPDATE tips_categories SET sort_order = $1 WHERE id = $2",
                b["sort_order"], a["id"])
            await conn.execute(
                "UPDATE tips_categories SET sort_order = $1 WHERE id = $2",
                a["sort_order"], b["id"])


async def delete_category(pool, category_id: int) -> None:
    """Cascades to every topic and entry under it (ON DELETE CASCADE) — the
    confirmation that this is really what the admin wants lives in the UI,
    not here."""
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM tips_categories WHERE id = $1", category_id)


# ---- Topics (admin-managed, scoped to one category) ----

async def create_topic(pool, category_id: int, name: str) -> int:
    name = name.strip()[:MAX_NAME_LENGTH]
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO tips_topics (category_id, name, sort_order) "
            "VALUES ($1, $2, (SELECT COALESCE(MAX(sort_order), 0) + 1 "
            "                 FROM tips_topics WHERE category_id = $1)) "
            "RETURNING id",
            category_id, name)
    return row["id"]


async def rename_topic(pool, topic_id: int, name: str) -> None:
    name = name.strip()[:MAX_NAME_LENGTH]
    async with pool.acquire() as conn:
        await conn.execute("UPDATE tips_topics SET name = $1 WHERE id = $2", name, topic_id)


async def reorder_topic(pool, topic_id: int, direction: str) -> None:
    """Same swap as reorder_category, scoped to topics within the SAME
    category — reordering one category's topics must never touch another
    category's ordering."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            owner = await conn.fetchrow(
                "SELECT category_id FROM tips_topics WHERE id = $1", topic_id)
            if not owner:
                return
            rows = await conn.fetch(
                "SELECT id, sort_order FROM tips_topics WHERE category_id = $1 "
                "ORDER BY sort_order ASC, id ASC",
                owner["category_id"])
            ids = [r["id"] for r in rows]
            idx = ids.index(topic_id)
            swap_idx = idx - 1 if direction == "up" else idx + 1
            if swap_idx < 0 or swap_idx >= len(ids):
                return
            a, b = rows[idx], rows[swap_idx]
            await conn.execute(
                "UPDATE tips_topics SET sort_order = $1 WHERE id = $2",
                b["sort_order"], a["id"])
            await conn.execute(
                "UPDATE tips_topics SET sort_order = $1 WHERE id = $2",
                a["sort_order"], b["id"])


async def delete_topic(pool, topic_id: int) -> None:
    """Cascades to every entry under it (ON DELETE CASCADE)."""
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM tips_topics WHERE id = $1", topic_id)


# ---- Entries (member-submitted) ----

async def create_entry(pool, topic_id: int, user_id: int, username: str, content: str) -> dict:
    content = content.strip()[:MAX_CONTENT_LENGTH]
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO tips_entries (topic_id, user_id, username, content) "
            "VALUES ($1, $2, $3, $4) "
            "RETURNING id, created_at",
            topic_id, user_id, username, content)
    return {"id": row["id"], "created_at": row["created_at"].isoformat()}


async def get_entry_owner(pool, entry_id: int):
    """The submitting user_id of one entry, or None if it doesn't exist —
    the ownership check itself (author-or-admin) belongs to the caller in
    main.py, since only it knows who's asking."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT user_id FROM tips_entries WHERE id = $1", entry_id)
    return row["user_id"] if row else None


async def update_entry(pool, entry_id: int, content: str) -> None:
    content = content.strip()[:MAX_CONTENT_LENGTH]
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE tips_entries SET content = $1, edited_at = NOW() WHERE id = $2",
            content, entry_id)


async def delete_entry(pool, entry_id: int) -> None:
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM tips_entries WHERE id = $1", entry_id)
