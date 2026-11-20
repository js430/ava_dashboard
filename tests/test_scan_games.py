"""Standalone checks for the photo scanner reading Lorcana and Gundam cards.

    python3 tests/test_scan_games.py

Needs Postgres at localhost:54329 (user postgres, trust auth) with a database
named card_dash. It works inside its own schema (scan_games_test), builds
catalog_cards there from catalog.py's own DDL and drops the schema at the end.
The Anthropic client is a stand-in; no network calls are made.
"""
import asyncio
import json
import os
import sys
from types import SimpleNamespace

os.environ.setdefault("SESSION_SECRET", "t" * 40)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg
from fastapi import HTTPException

import catalog
import games
import main

DSN = "postgresql://postgres@localhost:54329/card_dash"
SCHEMA = "scan_games_test"
results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  [{detail}]"))


class FakeClaude:
    """Returns the given JSON as the model's reply."""
    def __init__(self, payload):
        self.payload = payload
        self.messages = SimpleNamespace(create=self._create)

    async def _create(self, **kwargs):
        self.prompt = kwargs["messages"][0]["content"][1]["text"]
        return SimpleNamespace(content=[SimpleNamespace(text=json.dumps(self.payload))])


class Req:
    def __init__(self, pool, body=None):
        self.app = SimpleNamespace(state=SimpleNamespace(db=pool))
        self._body = body or {}

    async def json(self):
        return self._body


# 1x1 PNG
PNG = ("data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk"
       "+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==")
USER = {"id": "1", "username": "u", "guest": False}

scan = main.api_grading_scan.__wrapped__
find = main.api_grading_find_card.__wrapped__


async def do_scan(pool, payload):
    main.anthropic_client = FakeClaude(payload)
    resp = await scan(Req(pool, {"image": PNG}), USER)
    return json.loads(resp.body)


async def do_find(pool, game, number="", name=""):
    try:
        resp = await find(Req(pool), game, number, name, USER)
    except HTTPException as e:
        return e.status_code
    return json.loads(resp.body)


async def main_test():
    admin = await asyncpg.connect(DSN)
    await admin.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    await admin.execute(f"CREATE SCHEMA {SCHEMA}")
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3,
                                     server_settings={"search_path": SCHEMA})
    try:
        await catalog.ensure_catalog_schema(pool)

        async def add(game, set_id, name, number, version="", sid=None):
            return await pool.fetchval(
                "INSERT INTO catalog_cards (game, set_id, set_name, card_name, card_number, version,"
                " source_id) VALUES ($1,$2,$2,$3,$4,$5,$6) RETURNING id",
                game, set_id, name, number, version, sid or f"{game}-{set_id}-{number}-{version}")

        # Lorcana: number restarts each set; foil is its own row; a foil-only card.
        l_norm = await add("lorcana", "set1", "Elsa - Spirit of Winter", "1")
        await add("lorcana", "set1", "Elsa - Spirit of Winter", "1", "Foil")
        l_other = await add("lorcana", "set2", "Ariel - On Human Legs", "1")
        l_ench = await add("lorcana", "set1", "Elsa - Snow Queen", "205", "Foil")
        l_seven = await add("lorcana", "set1", "Mickey Mouse - Brave Little Tailor", "7")
        # Gundam: names carry the code and rarity; number is the printed code.
        g_card = await add("gundam", "GD01", "Gundam (GD01-001) (LR++)", "GD01-001")
        g_aerial = await add("gundam", "ST01", "Gundam Aerial", "ST01-001")
        # Existing games, as before.
        rb = await add("riftbound", "OGN", "Ahri", "OGN-066")
        op = await add("one_piece", "OP01", "Monkey.D.Luffy", "OP01-003")

        # --- scan JSON -> game key ---
        for printed, key in (("lorcana", "lorcana"), ("gundam", "gundam"), ("one piece", "one_piece"),
                             ("riftbound", "riftbound"), ("pokemon", "pokemon"), ("magic", "magic")):
            out = await do_scan(pool, {"game": printed, "name": "X", "number": "1"})
            check(f"scan '{printed}' -> {key}", out["game"] == key, out)
        check("from_scan lorcana/gundam", games.from_scan("Lorcana") == "lorcana"
              and games.from_scan(" gundam ") == "gundam")
        check("existing from_scan unchanged", games.from_scan("one piece") == "one_piece"
              and games.from_scan("riftbound") == "riftbound" and games.from_scan("pokemon") == "pokemon"
              and games.from_scan("other") is None)
        out = await do_scan(pool, {"game": "gundam", "name": "Gundam Aerial", "number": "ST01-001",
                                   "set": "Starter Deck 01", "is_sp": False})
        check("scan keeps number/name/set", (out["name"], out["number"], out["set"]) ==
              ("Gundam Aerial", "ST01-001", "Starter Deck 01"), out)
        check("scan output keys unchanged", set(out) == {"game", "name", "number", "set", "is_sp",
              "is_manga", "is_alt_art", "promo_stamp"}, sorted(out))
        prompt = main.anthropic_client.prompt
        check("prompt lists both games", "lorcana, gundam, other" in prompt and "'lorcana' for Disney" in prompt
              and "'gundam' for the Gundam" in prompt)
        check("prompt keeps Riftbound/Pokemon rules", "'OGN-066/298': return it as 'OGN-066'" in prompt
              and "Do NOT strip the slash" in prompt)

        # --- find-card ---
        hit = await do_find(pool, "lorcana", "1/204", "Elsa - Spirit of Winter")
        check("lorcana: normal print preferred over foil", hit == {"set_id": "set1", "catalog_id": l_norm}, hit)
        hit = await do_find(pool, "lorcana", "001", "Elsa - Spirit of Winter")
        check("lorcana: leading zeros ignored", hit == {"set_id": "set1", "catalog_id": l_norm}, hit)
        hit = await do_find(pool, "lorcana", "007/204", "Mickey Mouse - Brave Little Tailor")
        check("lorcana: '007' finds '7'", hit == {"set_id": "set1", "catalog_id": l_seven}, hit)
        hit = await do_find(pool, "lorcana", "1", "Ariel - On Human Legs")
        check("lorcana: same number in another set resolved by name", hit == {"set_id": "set2",
              "catalog_id": l_other}, hit)
        hit = await do_find(pool, "lorcana", "205/204", "Elsa - Snow Queen")
        check("lorcana: foil-only card resolves to its foil row", hit == {"set_id": "set1",
              "catalog_id": l_ench}, hit)
        check("lorcana: number only, several sets -> 404", await do_find(pool, "lorcana", "1") == 404)
        check("lorcana: unknown number -> 404", await do_find(pool, "lorcana", "999", "Nobody") == 404)
        hit = await do_find(pool, "gundam", "GD01-001", "Gundam")
        check("gundam: printed code finds the row (name has code/rarity)", hit == {"set_id": "GD01",
              "catalog_id": g_card}, hit)
        hit = await do_find(pool, "gundam", "st01-001", "Gundam Aerial")
        check("gundam: case-insensitive code", hit == {"set_id": "ST01", "catalog_id": g_aerial}, hit)
        check("gundam: unknown code -> 404", await do_find(pool, "gundam", "GD99-999", "Nope") == 404)

        # --- existing games unchanged ---
        hit = await do_find(pool, "riftbound", "OGN-066/298", "Ahri")
        check("riftbound unchanged", hit == {"set_id": "OGN", "catalog_id": rb}, hit)
        hit = await do_find(pool, "one_piece", "OP01-003", "Monkey.D.Luffy")
        check("one piece unchanged", hit == {"set_id": "OP01", "catalog_id": op}, hit)
        check("pokemon still refused (not catalog-priced)", await do_find(pool, "pokemon", "1") is not None
              and isinstance(await do_find(pool, "pokemon", "1"), int))
        check("lorcana/gundam stay out of the grading game list",
              not {"lorcana", "gundam"} & set(games.keys_with("grading")))
    finally:
        await pool.close()
        await admin.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        await admin.close()


asyncio.run(main_test())
failed = [n for n, ok in results if not ok]
print(f"\n{len(results) - len(failed)}/{len(results)} passed")
sys.exit(1 if failed else 0)
