"""Standalone checks for the Discord role re-check and the refresh-access route.

    python3 tests/test_role_refresh.py

Needs Postgres at localhost:54329 (user postgres, trust auth) with a database
named roles_dash. It creates and drops only its own member_roles table there.
No real Discord or network calls are made.
"""
import asyncio
import base64
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

SECRET = "t" * 40
os.environ.update({
    "SESSION_SECRET": SECRET,
    "ADMIN_USER_IDS": "900",
    "REQUIRED_ROLE_ID": "111",
    "DENY_ROLE_IDS": "666",
    "MOD_ROLE_IDS": "222",
    "ALL_MODS_ROLE_IDS": "333",
    "INVENTORY_ROLE_IDS": "444",
    "REGION_ACCESS_ROLES": "555:TX",
    "ACTIVE_ROLE_ID": "777",
    "LOOKBACK_ROLE_WEEKS": "111:8,777:2",
    "HTTPS_ONLY": "true",
    "DISCORD_CLIENT_ID": "cid",
    "DISCORD_REDIRECT_URI": "https://example.test/callback",
})
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg
import httpx
import itsdangerous
import main

DSN = "postgresql://postgres@localhost:54329/roles_dash"
results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  [{detail}]"))


def cookie(session: dict) -> str:
    data = base64.b64encode(json.dumps(session).encode())
    return itsdangerous.TimestampSigner(SECRET).sign(data).decode()


def user(uid):
    return {"id": str(uid), "username": f"u{uid}", "avatar": None}


class CountingPool:
    """Wraps the real pool and counts member_roles queries."""
    def __init__(self, pool):
        self.pool, self.queries = pool, 0

    def acquire(self):
        outer = self
        inner = self.pool.acquire()

        class Ctx:
            async def __aenter__(self_):
                conn = await inner.__aenter__()
                return SimpleNamespace(fetchrow=outer._wrap(conn),
                                       execute=conn.execute, fetch=conn.fetch)

            async def __aexit__(self_, *a):
                return await inner.__aexit__(*a)
        return Ctx()

    def _wrap(self, conn):
        async def fetchrow(*a, **k):
            self.queries += 1
            return await conn.fetchrow(*a, **k)
        return fetchrow


def reset_state():
    main._role_cache.clear()
    main._role_inflight.clear()
    main._role_check.update(disabled_until=0.0, missing_logged=False)


@main.app.get("/__probe")
async def probe(request: main.Request):
    return dict(request.session)


async def set_row(pool, uid, roles, in_guild=True, updated_at=None):
    await pool.execute(
        "INSERT INTO member_roles (user_id, role_ids, in_guild, updated_at) "
        "VALUES ($1, $2::bigint[], $3, COALESCE($4, now())) "
        "ON CONFLICT (user_id) DO UPDATE SET role_ids = EXCLUDED.role_ids, "
        "in_guild = EXCLUDED.in_guild, updated_at = EXCLUDED.updated_at",
        uid, roles, in_guild, updated_at)


async def get(client, session, path="/__probe", **kw):
    client.cookies.clear()
    if session is not None:
        client.cookies.set("session", cookie(session), domain="testserver.local")
    return await client.get(path, **kw)


async def main_async():
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    await pool.execute("DROP TABLE IF EXISTS member_roles")
    await pool.execute(
        "CREATE TABLE member_roles (user_id BIGINT PRIMARY KEY, role_ids BIGINT[] NOT NULL, "
        "in_guild BOOLEAN NOT NULL, updated_at TIMESTAMPTZ NOT NULL)")
    counting = CountingPool(pool)
    main.app.state.db = counting
    transport = httpx.ASGITransport(app=main.app)
    client = httpx.AsyncClient(transport=transport, base_url="https://testserver.local",
                               follow_redirects=False)
    ago = lambda s: datetime.now(timezone.utc) - timedelta(seconds=s)

    # ---- middleware order: RoleRefresh must be INSIDE SessionMiddleware ----
    names = [m.cls.__name__ for m in main.app.user_middleware]  # outermost first
    check("middleware sits inside SessionMiddleware",
          names.index("SessionMiddleware") < names.index("RoleRefreshMiddleware"), names)

    old_session = lambda **kw: {"user": user(1), "member": False, "roles_at": 1.0, **kw}

    # ---- upgrade applies ----
    reset_state()
    await set_row(pool, 1, [111, 222, 444], updated_at=ago(5))
    r = await get(client, old_session())
    s = r.json()
    check("upgrade: becomes member", s.get("member") is True, s)
    check("upgrade: mod / inventory flags", s.get("mod") and s.get("inventory_access")
          and not s.get("all_mods"), s)
    check("upgrade: lookback from role", s.get("max_position") == 8, s)
    check("upgrade: roles_at stored", s.get("roles_at", 0) > 1.0, s)
    # the cookie was rewritten, so the next request already has it
    r = await client.get("/__probe")
    check("upgrade: persisted in cookie", r.json().get("member") is True, r.json())

    # ---- legacy session (no roles_at) re-checked once ----
    reset_state()
    calls = []
    real_apply = main._apply_member_roles
    main._apply_member_roles = lambda *a, **k: (calls.append(1), real_apply(*a, **k))[1]
    legacy = {"user": user(1), "member": False}
    r = await get(client, legacy)
    check("legacy session re-checked", r.json().get("member") is True and len(calls) == 1, r.json())
    # ---- unchanged updated_at does not recompute ----
    main._role_cache.clear()
    await client.get("/__probe")
    await client.get("/__probe")
    check("unchanged updated_at: no recompute", len(calls) == 1, len(calls))
    main._apply_member_roles = real_apply

    # ---- downgrade / lapse ----
    reset_state()
    await set_row(pool, 2, [], updated_at=ago(5))
    member = {"user": user(2), "member": True, "mod": True, "all_mods": True,
              "inventory_access": True, "max_position": 12, "roles_at": 1.0}
    s = (await get(client, member)).json()
    check("lapse: member removed", s.get("member") is False, s)
    check("lapse: mod/all_mods/inventory removed",
          not s.get("mod") and not s.get("all_mods") and not s.get("inventory_access"), s)
    check("lapse: max_position removed", "max_position" not in s, s)

    # ---- admin keeps admin-derived access with no roles ----
    reset_state()
    await set_row(pool, 900, [], in_guild=False, updated_at=ago(5))
    s = (await get(client, {"user": user(900), "member": True, "roles_at": 1.0})).json()
    check("admin stays member/mod when roles gone", s.get("member") and s.get("mod")
          and s.get("inventory_access"), s)

    # ---- in_guild FALSE ----
    reset_state()
    await set_row(pool, 3, [111, 222], in_guild=False, updated_at=ago(5))
    s = (await get(client, {"user": user(3), "member": True, "mod": True, "roles_at": 1.0})).json()
    check("in_guild FALSE: no roles applied", s.get("member") is False and not s.get("mod"), s)

    # ---- deny list ----
    reset_state()
    await set_row(pool, 4, [111, 666], updated_at=ago(5))
    r = await get(client, {"user": user(4), "member": True, "mod": True, "max_position": 8,
                           "roles_at": 1.0})
    check("deny-listed (even with member role): signed out", r.json() == {}, r.json())
    r = await get(client, {"user": user(4), "member": True, "roles_at": 1.0}, "/sample")
    check("deny-listed: protected page now sends to login",
          r.status_code in (302, 307) and r.headers["location"] == "/login", (r.status_code, r.headers))
    reset_state()
    await set_row(pool, 900, [666], updated_at=ago(4))
    s = (await get(client, {"user": user(900), "member": True, "roles_at": 1.0})).json()
    check("deny-listed admin keeps access (as at login)", s.get("member") is True, s)

    # ---- regional viewer ----
    reset_state()
    await set_row(pool, 5, [555], updated_at=ago(5))
    s = (await get(client, old_session(user=user(5)))).json()
    check("regional role: TX viewer, not member",
          s.get("member") is False and s.get("regions") == ["TX"] and s.get("max_position") == 2, s)
    reset_state()
    await set_row(pool, 5, [], updated_at=ago(1))
    s = (await get(client, s)).json()
    check("regional role removed: region + position cleared",
          "regions" not in s and "max_position" not in s, s)

    # ---- no row: unchanged ----
    reset_state()
    base = {"user": user(77), "member": True, "mod": True, "roles_at": 1.0}
    s = (await get(client, base)).json()
    check("no row: session unchanged", s == base, s)

    # ---- cache limits queries ----
    reset_state()
    await set_row(pool, 6, [111], updated_at=ago(5))
    counting.queries = 0
    sess = {"user": user(6), "member": False, "roles_at": 1.0}
    await get(client, sess)
    for _ in range(5):
        await client.get("/__probe")
    check("cache: 6 requests -> 1 query", counting.queries == 1, counting.queries)
    for k in list(main._role_cache):
        main._role_cache[k] = (0.0, main._role_cache[k][1])
    await client.get("/__probe")
    check("cache: refetch after expiry", counting.queries == 2, counting.queries)
    reset_state(); counting.queries = 0
    clients = [httpx.AsyncClient(transport=transport, base_url="https://testserver.local")
               for _ in range(10)]
    for c in clients:
        c.cookies.set("session", cookie(sess), domain="testserver.local")
    await asyncio.gather(*[c.get("/__probe") for c in clients])
    check("cache: 10 parallel requests -> 1 query", counting.queries == 1, counting.queries)
    for c in clients:
        await c.aclose()
    reset_state(); counting.queries = 0
    old_max = main.ROLE_CACHE_MAX
    main.ROLE_CACHE_MAX = 3
    for uid in range(100, 110):
        await main._fetch_member_roles(counting, uid)
    check("cache: bounded size", len(main._role_cache) <= 3, len(main._role_cache))
    main.ROLE_CACHE_MAX = old_max

    # ---- who is skipped ----
    reset_state(); counting.queries = 0
    await get(client, None)
    await get(client, {"guest": True})
    await get(client, {"guest": True, "user": user(6)})
    await get(client, {"account_id": 5})
    await client.get("/static/does-not-exist.css")
    check("anonymous / guest / account / static: no queries", counting.queries == 0, counting.queries)

    # ---- missing table tolerated ----
    reset_state(); counting.queries = 0
    await pool.execute("DROP TABLE member_roles")
    warnings = []

    class H(logging.Handler):
        def emit(self, rec):
            if "member_roles table not found" in rec.getMessage():
                warnings.append(rec)
    h = H(); main.logger.addHandler(h)
    sess = {"user": user(8), "member": True, "mod": True, "roles_at": 1.0}
    out = []
    for _ in range(4):
        r = await get(client, sess)
        out.append((r.status_code, r.json()))
    check("missing table: requests still work, session unchanged",
          all(o == (200, sess) for o in out), out)
    check("missing table: logged once, one query", len(warnings) == 1 and counting.queries == 1,
          (len(warnings), counting.queries))
    check("missing table: retry window ~10 min",
          540 <= main._role_check["disabled_until"] - time.monotonic() <= 600,
          main._role_check["disabled_until"] - time.monotonic())
    # bot creates the table later; after the retry window the check comes back
    await pool.execute(
        "CREATE TABLE member_roles (user_id BIGINT PRIMARY KEY, role_ids BIGINT[] NOT NULL, "
        "in_guild BOOLEAN NOT NULL, updated_at TIMESTAMPTZ NOT NULL)")
    await set_row(pool, 8, [], updated_at=ago(1))
    await get(client, sess)
    check("table back but window not over: still off", counting.queries == 1, counting.queries)
    main._role_check["disabled_until"] = 0.0
    s = (await get(client, sess)).json()
    check("after retry window: check resumes", s.get("member") is False, s)
    main.logger.removeHandler(h)

    # ---- /refresh-access ----
    def state_of(resp):
        loc = resp.headers["location"]
        q = parse_qs(urlsplit(loc).query)
        return loc, q["state"][0]

    r = await client.get("/refresh-access", params={"next": "/sample-map"})
    loc, st = state_of(r)
    check("refresh: redirects to Discord login", r.status_code in (302, 307)
          and loc.startswith("https://discord.com/"), loc)
    check("refresh: state valid and carries local next",
          main.verify_oauth_state(st) and main.oauth_state_next(st) == "/sample-map", st)
    for bad in ["https://evil.example/x", "//evil.example", "/\\evil.example", "http:evil",
                "javascript:alert(1)", "/logout", "/login", "/refresh-access?next=/x",
                "/ok\r\nSet-Cookie: a=b", "evil.example", "/" + "a" * 400]:
        r = await client.get("/refresh-access", params={"next": bad})
        loc, st = state_of(r)
        check(f"refresh: rejects next={bad[:30]!r}",
              main.verify_oauth_state(st) and main.oauth_state_next(st) is None
              and loc.startswith("https://discord.com/"), loc)
    r = await client.get("/refresh-access")
    check("refresh: works without next", main.oauth_state_next(state_of(r)[1]) is None)
    # tampering with the next in a signed state invalidates it
    _, st = state_of(await client.get("/refresh-access", params={"next": "/sample"}))
    parts = st.split(".")
    parts[2] = base64.urlsafe_b64encode(b"/admin").decode().rstrip("=")
    check("state with swapped next is rejected", not main.verify_oauth_state(".".join(parts)))
    check("plain login state still verifies", main.verify_oauth_state(main.make_oauth_state()))

    # ---- callback end to end (Discord faked) ----
    class FakeResp:
        status_code = 200
        def json(self): return {"access_token": "tok"}

    class FakeClient:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def post(self, *a, **k): return FakeResp()
    real_httpx, real_check = main.httpx, main.check_discord_role
    main.httpx = SimpleNamespace(AsyncClient=lambda *a, **k: FakeClient())
    try:
        reset_state()
        async def fake_check(uid, roles):
            async def f(_tok):
                return bool(main.REQUIRED_ROLE_IDS & set(roles)), user(uid), roles
            return f
        main.check_discord_role = await fake_check(10, ["111", "444"])
        t0 = time.time()
        st = main.make_oauth_state("/map")
        client.cookies.clear()
        r = await client.get("/callback", params={"code": "c", "state": st})
        check("callback: member returns to next page", r.status_code in (302, 307)
              and r.headers["location"] == "/map", (r.status_code, r.headers))
        after = await client.get("/__probe")
        s = after.json()
        check("callback: session set with roles_at", s.get("member") is True
              and s.get("inventory_access") and t0 - 1 <= s.get("roles_at", 0) <= time.time(), s)
        main.check_discord_role = await fake_check(11, [])
        client.cookies.clear()
        r = await client.get("/callback", params={"code": "c", "state": main.make_oauth_state()})
        check("callback: no next -> /sample for non-member", r.headers.get("location") == "/sample",
              r.headers)
        main.check_discord_role = await fake_check(12, ["666"])
        client.cookies.clear()
        r = await client.get("/callback", params={"code": "c", "state": main.make_oauth_state()})
        check("callback: deny-listed refused with 403", r.status_code == 403, r.status_code)
        s = (await client.get("/__probe")).json()
        check("callback: deny-listed gets no session", "user" not in s, s)
    finally:
        main.httpx, main.check_discord_role = real_httpx, real_check

    # ---- sample page shows the refresh link ----
    reset_state()
    await set_row(pool, 13, [], updated_at=ago(50))
    for page in ["/sample", "/sample-status", "/sample-map", "/sample-card-tracker"]:
        r = await get(client, {"user": user(13), "member": False, "roles_at": time.time()}, page)
        check(f"{page}: shows refresh link",
              r.status_code == 200 and f"/refresh-access?next={page}" in r.text
              and "Just bought premium? Refresh my access" in r.text, (r.status_code, r.text[:200]))

    await client.aclose()
    await pool.execute("DROP TABLE IF EXISTS member_roles")
    await pool.close()


asyncio.run(main_async())
failed = [n for n, ok in results if not ok]
print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
sys.exit(1 if failed else 0)
