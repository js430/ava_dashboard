"""Tips, Tricks, and Guide — community-submitted tips and locations grouped
by category, topic, and module.

Owned by ava_dashboard. Standalone feature, no relation to the bot's shared
restock tables — sharing this app's DB and auth only because it's
convenient, not because the data means anything to ava_bot.

FOUR-LEVEL HIERARCHY: category (e.g. "Japan Travel") -> topic (e.g. "Pokemon
in Japan", or "Kyoto Guide") -> module (e.g. Tokyo/Kyoto/Osaka under the
first, or Food/Sights/Shopping/Lodging under the second) -> the actual
content, which is entries (tips) and locations (a named place with an
address, shown as an embedded map). Categories, topics, and modules are all
admin-managed (create/rename/reorder/delete) from the page itself. Entries
and locations are member-submitted, visible immediately — no moderation
queue, since only known Discord identities (not anonymous guests) can post
at all. Either kind is editable/removable by its own author or by any admin;
main.py enforces that, this module just does the writes once a caller has
already decided they're allowed to.

MIGRATION NOTE: modules were added after topics already held entries and
locations directly. _migrate_orphaned_content moves any pre-existing
topic-level content into a "General" module created under that topic, so
nothing already submitted is lost or hidden by the schema change — it just
now lives one level deeper, exactly like everything created after.
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
    CREATE TABLE IF NOT EXISTS tips_modules (
        id          SERIAL PRIMARY KEY,
        topic_id    INTEGER NOT NULL REFERENCES tips_topics(id) ON DELETE CASCADE,
        name        TEXT NOT NULL,
        sort_order  INTEGER NOT NULL DEFAULT 0,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_tips_modules_topic ON tips_modules (topic_id)",
    """
    CREATE TABLE IF NOT EXISTS tips_entries (
        id          SERIAL PRIMARY KEY,
        topic_id    INTEGER REFERENCES tips_topics(id) ON DELETE CASCADE,
        module_id   INTEGER REFERENCES tips_modules(id) ON DELETE CASCADE,
        user_id     BIGINT NOT NULL,
        username    TEXT NOT NULL,
        content     TEXT NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        edited_at   TIMESTAMPTZ
    )
    """,
    # module_id is the live column going forward; topic_id is legacy (kept
    # nullable, not dropped) so a table created before modules existed still
    # matches this DDL on every startup rather than erroring on the missing
    # column. See _migrate_orphaned_content for how old topic_id rows move
    # over to a module_id.
    "ALTER TABLE tips_entries ADD COLUMN IF NOT EXISTS module_id INTEGER "
    "REFERENCES tips_modules(id) ON DELETE CASCADE",
    # A table deployed before modules existed has topic_id as NOT NULL —
    # new inserts only ever supply module_id now, so that constraint has to
    # be relaxed explicitly (CREATE TABLE IF NOT EXISTS never alters an
    # already-existing table's constraints). No-op if it's already nullable.
    "ALTER TABLE tips_entries ALTER COLUMN topic_id DROP NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_tips_entries_module ON tips_entries (module_id)",
    """
    CREATE TABLE IF NOT EXISTS tips_locations (
        id          SERIAL PRIMARY KEY,
        topic_id    INTEGER REFERENCES tips_topics(id) ON DELETE CASCADE,
        module_id   INTEGER REFERENCES tips_modules(id) ON DELETE CASCADE,
        name        TEXT NOT NULL,
        address     TEXT NOT NULL,
        user_id     BIGINT NOT NULL,
        username    TEXT NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "ALTER TABLE tips_locations ADD COLUMN IF NOT EXISTS module_id INTEGER "
    "REFERENCES tips_modules(id) ON DELETE CASCADE",
    "ALTER TABLE tips_locations ALTER COLUMN topic_id DROP NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_tips_locations_module ON tips_locations (module_id)",
    # One row per (item, user) — the UNIQUE constraint is what makes "like"
    # idempotent and race-safe at the DB level: a double-click can't produce
    # two likes from the same person no matter how the requests interleave.
    """
    CREATE TABLE IF NOT EXISTS tips_entry_likes (
        id          SERIAL PRIMARY KEY,
        entry_id    INTEGER NOT NULL REFERENCES tips_entries(id) ON DELETE CASCADE,
        user_id     BIGINT NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (entry_id, user_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_tips_entry_likes_entry ON tips_entry_likes (entry_id)",
    """
    CREATE TABLE IF NOT EXISTS tips_location_likes (
        id           SERIAL PRIMARY KEY,
        location_id  INTEGER NOT NULL REFERENCES tips_locations(id) ON DELETE CASCADE,
        user_id      BIGINT NOT NULL,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (location_id, user_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_tips_location_likes_location ON tips_location_likes (location_id)",
    # Stored as BYTEA in Postgres rather than on local disk — Railway
    # containers are ephemeral, so anything written to the filesystem is
    # lost on the next deploy/restart. This reuses the same DB connection
    # everything else here already has, at the cost of the images living in
    # the database rather than a dedicated object store; fine at the scale
    # of a community photo gallery, worth revisiting only if that changes.
    """
    CREATE TABLE IF NOT EXISTS tips_photos (
        id          SERIAL PRIMARY KEY,
        module_id   INTEGER NOT NULL REFERENCES tips_modules(id) ON DELETE CASCADE,
        image_data  BYTEA NOT NULL,
        media_type  TEXT NOT NULL,
        caption     TEXT NOT NULL DEFAULT '',
        user_id     BIGINT NOT NULL,
        username    TEXT NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_tips_photos_module ON tips_photos (module_id)",
    """
    CREATE TABLE IF NOT EXISTS tips_photo_likes (
        id          SERIAL PRIMARY KEY,
        photo_id    INTEGER NOT NULL REFERENCES tips_photos(id) ON DELETE CASCADE,
        user_id     BIGINT NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (photo_id, user_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_tips_photo_likes_photo ON tips_photo_likes (photo_id)",
]

# A tip is a short, freeform note, not an essay — bounded mainly so one
# submission can't blow up the page for everyone else viewing the module.
MAX_CONTENT_LENGTH = 2000
MAX_NAME_LENGTH = 80
MAX_ADDRESS_LENGTH = 300
MAX_CAPTION_LENGTH = 300

DEFAULT_MODULE_NAME = "General"


async def _migrate_orphaned_content(pool) -> None:
    """One-time-per-row cleanup: any entry/location still holding only the
    legacy topic_id (from before modules existed) gets a "General" module
    created under its topic and re-pointed to module_id. Safe to run on
    every startup — it only touches rows where module_id IS NULL, so once
    migrated a row is never touched again.
    """
    async with pool.acquire() as conn:
        orphaned_topic_ids = await conn.fetch(
            "SELECT DISTINCT topic_id FROM tips_entries WHERE module_id IS NULL "
            "AND topic_id IS NOT NULL "
            "UNION "
            "SELECT DISTINCT topic_id FROM tips_locations WHERE module_id IS NULL "
            "AND topic_id IS NOT NULL")
        for row in orphaned_topic_ids:
            topic_id = row["topic_id"]
            general = await conn.fetchrow(
                "SELECT id FROM tips_modules WHERE topic_id = $1 AND name = $2 "
                "ORDER BY id ASC LIMIT 1",
                topic_id, DEFAULT_MODULE_NAME)
            if general:
                module_id = general["id"]
            else:
                created = await conn.fetchrow(
                    "INSERT INTO tips_modules (topic_id, name, sort_order) "
                    "VALUES ($1, $2, 0) RETURNING id",
                    topic_id, DEFAULT_MODULE_NAME)
                module_id = created["id"]
            await conn.execute(
                "UPDATE tips_entries SET module_id = $1 "
                "WHERE topic_id = $2 AND module_id IS NULL",
                module_id, topic_id)
            await conn.execute(
                "UPDATE tips_locations SET module_id = $1 "
                "WHERE topic_id = $2 AND module_id IS NULL",
                module_id, topic_id)
            logger.info("Tips migration: moved orphaned content for topic %s into "
                       "module %r (id=%s)", topic_id, DEFAULT_MODULE_NAME, module_id)


async def ensure_tips_schema(pool) -> None:
    """Create the tips tables if absent, and migrate any pre-module content.
    Idempotent — matches this repo's existing startup-ensure convention
    (catalog.py, population.py, inventory.py)."""
    async with pool.acquire() as conn:
        for ddl in TIPS_SCHEMA:
            await conn.execute(ddl)
    await _migrate_orphaned_content(pool)


async def list_tree(pool, viewer_user_id=None) -> list:
    """Full category -> topic -> module -> entries/locations tree, in
    display order. Entries and locations within a module are sorted by like
    count descending (most-liked first) by default, tiebroken by oldest
    first — likes are the point of the ordering, submission time is just a
    stable tiebreak, not a competing sort key.

    `viewer_user_id`, when given, marks which entries/locations THIS viewer
    has already liked (`liked_by_me`) so the like button can render its
    pressed/unpressed state without a second round-trip. None (an
    unauthenticated caller, if this were ever exposed that way) means
    nothing is marked as liked-by-me.

    Seven flat queries plus in-Python grouping rather than one big JOIN — a
    JOIN across every level plus both like tables would repeat every parent
    row once per leaf/like row, which is wasted transfer for a board that's
    read far more often than it's written to. Like COUNTS are computed in
    Python from the raw (item_id, user_id) rows rather than a SQL COUNT/JOIN,
    for the same reason — a tip realistically has single-digit likes, so
    there's nothing to gain from pushing the aggregation into the database.
    """
    async with pool.acquire() as conn:
        categories = await conn.fetch(
            "SELECT id, name, sort_order FROM tips_categories ORDER BY sort_order ASC, id ASC")
        topics = await conn.fetch(
            "SELECT id, category_id, name, sort_order FROM tips_topics "
            "ORDER BY sort_order ASC, id ASC")
        modules = await conn.fetch(
            "SELECT id, topic_id, name, sort_order FROM tips_modules "
            "ORDER BY sort_order ASC, id ASC")
        entries = await conn.fetch(
            "SELECT id, module_id, user_id, username, content, created_at, edited_at "
            "FROM tips_entries WHERE module_id IS NOT NULL")
        locations = await conn.fetch(
            "SELECT id, module_id, name, address, user_id, username, created_at "
            "FROM tips_locations WHERE module_id IS NOT NULL")
        photos = await conn.fetch(
            "SELECT id, module_id, caption, user_id, username, created_at "
            "FROM tips_photos")
        entry_likes = await conn.fetch("SELECT entry_id, user_id FROM tips_entry_likes")
        location_likes = await conn.fetch("SELECT location_id, user_id FROM tips_location_likes")
        photo_likes = await conn.fetch("SELECT photo_id, user_id FROM tips_photo_likes")

    entry_likers = {}
    for r in entry_likes:
        entry_likers.setdefault(r["entry_id"], set()).add(r["user_id"])
    location_likers = {}
    for r in location_likes:
        location_likers.setdefault(r["location_id"], set()).add(r["user_id"])
    photo_likers = {}
    for r in photo_likes:
        photo_likers.setdefault(r["photo_id"], set()).add(r["user_id"])

    topics_by_category = {}
    for t in topics:
        topics_by_category.setdefault(t["category_id"], []).append(t)
    modules_by_topic = {}
    for m in modules:
        modules_by_topic.setdefault(m["topic_id"], []).append(m)
    entries_by_module = {}
    for e in entries:
        entries_by_module.setdefault(e["module_id"], []).append(e)
    locations_by_module = {}
    for l in locations:
        locations_by_module.setdefault(l["module_id"], []).append(l)
    photos_by_module = {}
    for p in photos:
        photos_by_module.setdefault(p["module_id"], []).append(p)

    def _entry_dict(e):
        likers = entry_likers.get(e["id"], set())
        return {
            "id": e["id"],
            "user_id": str(e["user_id"]),
            "username": e["username"],
            "content": e["content"],
            "created_at": e["created_at"].isoformat(),
            "edited_at": e["edited_at"].isoformat() if e["edited_at"] else None,
            "like_count": len(likers),
            "liked_by_me": viewer_user_id is not None and viewer_user_id in likers,
        }

    def _location_dict(l):
        likers = location_likers.get(l["id"], set())
        return {
            "id": l["id"],
            "user_id": str(l["user_id"]),
            "username": l["username"],
            "name": l["name"],
            "address": l["address"],
            "created_at": l["created_at"].isoformat(),
            "like_count": len(likers),
            "liked_by_me": viewer_user_id is not None and viewer_user_id in likers,
        }

    def _photo_dict(p):
        likers = photo_likers.get(p["id"], set())
        return {
            "id": p["id"],
            "user_id": str(p["user_id"]),
            "username": p["username"],
            "caption": p["caption"],
            "created_at": p["created_at"].isoformat(),
            "like_count": len(likers),
            "liked_by_me": viewer_user_id is not None and viewer_user_id in likers,
        }

    def _by_likes_then_age(item):
        return (-item["like_count"], item["created_at"])

    out = []
    for c in categories:
        cat_topics = []
        for t in topics_by_category.get(c["id"], []):
            topic_modules = []
            for m in modules_by_topic.get(t["id"], []):
                mod_entries = sorted(
                    (_entry_dict(e) for e in entries_by_module.get(m["id"], [])),
                    key=_by_likes_then_age)
                mod_locations = sorted(
                    (_location_dict(l) for l in locations_by_module.get(m["id"], [])),
                    key=_by_likes_then_age)
                mod_photos = sorted(
                    (_photo_dict(p) for p in photos_by_module.get(m["id"], [])),
                    key=_by_likes_then_age)
                topic_modules.append({
                    "id": m["id"], "name": m["name"],
                    "entries": mod_entries, "locations": mod_locations,
                    "photos": mod_photos,
                })
            cat_topics.append({"id": t["id"], "name": t["name"], "modules": topic_modules})
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
    """Cascades to every topic, module, entry, and location under it
    (ON DELETE CASCADE) — the confirmation that this is really what the
    admin wants lives in the UI, not here."""
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
    """Cascades to every module, entry, and location under it
    (ON DELETE CASCADE)."""
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM tips_topics WHERE id = $1", topic_id)


# ---- Modules (admin-managed, scoped to one topic) ----
# The "little modules" a user clicks into — Tokyo/Kyoto/Osaka under a
# "Pokemon in Japan" topic, or Food/Sights/Shopping/Lodging under a "Kyoto
# Guide" topic. Same create/rename/reorder/delete shape as topics, just one
# level down.

async def create_module(pool, topic_id: int, name: str) -> int:
    name = name.strip()[:MAX_NAME_LENGTH]
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO tips_modules (topic_id, name, sort_order) "
            "VALUES ($1, $2, (SELECT COALESCE(MAX(sort_order), 0) + 1 "
            "                 FROM tips_modules WHERE topic_id = $1)) "
            "RETURNING id",
            topic_id, name)
    return row["id"]


async def rename_module(pool, module_id: int, name: str) -> None:
    name = name.strip()[:MAX_NAME_LENGTH]
    async with pool.acquire() as conn:
        await conn.execute("UPDATE tips_modules SET name = $1 WHERE id = $2", name, module_id)


async def reorder_module(pool, module_id: int, direction: str) -> None:
    """Same swap as reorder_topic, scoped to modules within the SAME
    topic."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            owner = await conn.fetchrow(
                "SELECT topic_id FROM tips_modules WHERE id = $1", module_id)
            if not owner:
                return
            rows = await conn.fetch(
                "SELECT id, sort_order FROM tips_modules WHERE topic_id = $1 "
                "ORDER BY sort_order ASC, id ASC",
                owner["topic_id"])
            ids = [r["id"] for r in rows]
            idx = ids.index(module_id)
            swap_idx = idx - 1 if direction == "up" else idx + 1
            if swap_idx < 0 or swap_idx >= len(ids):
                return
            a, b = rows[idx], rows[swap_idx]
            await conn.execute(
                "UPDATE tips_modules SET sort_order = $1 WHERE id = $2",
                b["sort_order"], a["id"])
            await conn.execute(
                "UPDATE tips_modules SET sort_order = $1 WHERE id = $2",
                a["sort_order"], b["id"])


async def delete_module(pool, module_id: int) -> None:
    """Cascades to every entry and location under it (ON DELETE CASCADE)."""
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM tips_modules WHERE id = $1", module_id)


# ---- Entries (member-submitted, per module) ----

async def create_entry(pool, module_id: int, user_id: int, username: str, content: str) -> dict:
    content = content.strip()[:MAX_CONTENT_LENGTH]
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO tips_entries (module_id, user_id, username, content) "
            "VALUES ($1, $2, $3, $4) "
            "RETURNING id, created_at",
            module_id, user_id, username, content)
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


async def toggle_entry_like(pool, entry_id: int, user_id: int) -> dict:
    """Like the entry if this user hasn't already, unlike if they have.
    Returns {liked, like_count} reflecting the state AFTER this call.

    Read-then-act inside a transaction, with ON CONFLICT DO NOTHING as a
    backstop — the UNIQUE(entry_id, user_id) constraint means two concurrent
    toggles from the SAME user can't both register as a fresh "like" (one
    silently no-ops), so the count can never overcount one person's like.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            existing = await conn.fetchrow(
                "SELECT id FROM tips_entry_likes WHERE entry_id = $1 AND user_id = $2",
                entry_id, user_id)
            if existing:
                await conn.execute("DELETE FROM tips_entry_likes WHERE id = $1", existing["id"])
                liked = False
            else:
                await conn.execute(
                    "INSERT INTO tips_entry_likes (entry_id, user_id) VALUES ($1, $2) "
                    "ON CONFLICT (entry_id, user_id) DO NOTHING",
                    entry_id, user_id)
                liked = True
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM tips_entry_likes WHERE entry_id = $1", entry_id)
    return {"liked": liked, "like_count": count}


# ---- Locations (member-submitted, per module) ----
# A place worth knowing about for a module — "Mandarake Akihabara" with an
# address, shown as an embedded map on the page. Same member-submits,
# author-or-admin-edits model as entries; main.py enforces ownership, this
# module just does the writes.

async def create_location(pool, module_id: int, user_id: int, username: str,
                          name: str, address: str) -> dict:
    name = name.strip()[:MAX_NAME_LENGTH]
    address = address.strip()[:MAX_ADDRESS_LENGTH]
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO tips_locations (module_id, user_id, username, name, address) "
            "VALUES ($1, $2, $3, $4, $5) "
            "RETURNING id, created_at",
            module_id, user_id, username, name, address)
    return {"id": row["id"], "created_at": row["created_at"].isoformat()}


async def get_location_owner(pool, location_id: int):
    """The submitting user_id of one location, or None if it doesn't exist."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT user_id FROM tips_locations WHERE id = $1", location_id)
    return row["user_id"] if row else None


async def update_location(pool, location_id: int, name: str, address: str) -> None:
    name = name.strip()[:MAX_NAME_LENGTH]
    address = address.strip()[:MAX_ADDRESS_LENGTH]
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE tips_locations SET name = $1, address = $2 WHERE id = $3",
            name, address, location_id)


async def delete_location(pool, location_id: int) -> None:
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM tips_locations WHERE id = $1", location_id)


async def toggle_location_like(pool, location_id: int, user_id: int) -> dict:
    """Same toggle/race-safety shape as toggle_entry_like, for locations."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            existing = await conn.fetchrow(
                "SELECT id FROM tips_location_likes WHERE location_id = $1 AND user_id = $2",
                location_id, user_id)
            if existing:
                await conn.execute("DELETE FROM tips_location_likes WHERE id = $1", existing["id"])
                liked = False
            else:
                await conn.execute(
                    "INSERT INTO tips_location_likes (location_id, user_id) VALUES ($1, $2) "
                    "ON CONFLICT (location_id, user_id) DO NOTHING",
                    location_id, user_id)
                liked = True
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM tips_location_likes WHERE location_id = $1", location_id)
    return {"liked": liked, "like_count": count}


# ---- Photos (member-submitted, per module) ----
# Image bytes live in the DB (tips_photos.image_data) rather than on local
# disk or an external object store — see the tips_photos schema comment.
# list_tree() never selects image_data itself (it's metadata-only there);
# callers fetch the bytes separately via get_photo_image() for the dedicated
# image-serving endpoint, so the main tree payload stays light.

async def create_photo(pool, module_id: int, user_id: int, username: str,
                       image_data: bytes, media_type: str, caption: str) -> dict:
    caption = caption.strip()[:MAX_CAPTION_LENGTH]
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO tips_photos (module_id, user_id, username, image_data, media_type, caption) "
            "VALUES ($1, $2, $3, $4, $5, $6) "
            "RETURNING id, created_at",
            module_id, user_id, username, image_data, media_type, caption)
    return {"id": row["id"], "created_at": row["created_at"].isoformat()}


async def get_photo_owner(pool, photo_id: int):
    """The submitting user_id of one photo, or None if it doesn't exist."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT user_id FROM tips_photos WHERE id = $1", photo_id)
    return row["user_id"] if row else None


async def get_photo_image(pool, photo_id: int):
    """(image_data, media_type) for one photo, or None if it doesn't exist."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT image_data, media_type FROM tips_photos WHERE id = $1", photo_id)
    return (row["image_data"], row["media_type"]) if row else None


async def update_photo_caption(pool, photo_id: int, caption: str) -> None:
    caption = caption.strip()[:MAX_CAPTION_LENGTH]
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE tips_photos SET caption = $1 WHERE id = $2", caption, photo_id)


async def delete_photo(pool, photo_id: int) -> None:
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM tips_photos WHERE id = $1", photo_id)


async def toggle_photo_like(pool, photo_id: int, user_id: int) -> dict:
    """Same toggle/race-safety shape as toggle_entry_like, for photos."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            existing = await conn.fetchrow(
                "SELECT id FROM tips_photo_likes WHERE photo_id = $1 AND user_id = $2",
                photo_id, user_id)
            if existing:
                await conn.execute("DELETE FROM tips_photo_likes WHERE id = $1", existing["id"])
                liked = False
            else:
                await conn.execute(
                    "INSERT INTO tips_photo_likes (photo_id, user_id) VALUES ($1, $2) "
                    "ON CONFLICT (photo_id, user_id) DO NOTHING",
                    photo_id, user_id)
                liked = True
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM tips_photo_likes WHERE photo_id = $1", photo_id)
    return {"liked": liked, "like_count": count}
