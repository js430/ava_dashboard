"""Standalone checks for traffic monitoring (traffic.py and /admin/traffic*).

    python3 tests/test_traffic.py

Needs Postgres at localhost:54331 (user postgres, trust auth) with a database
named traffic_dash. Set TRAFFIC_TEST_PG_PORT to use another port. It creates
and drops only the traffic_requests / traffic_hourly / member_roles tables
there. No real Discord or network calls are made.
"""
import asyncio
import base64
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

SECRET = "t" * 40
os.environ.update({
    "SESSION_SECRET": SECRET,
    "ADMIN_USER_IDS": "900",
    "REQUIRED_ROLE_ID": "111",
    "HTTPS_ONLY": "true",
    "DISCORD_CLIENT_ID": "cid",
    "DISCORD_REDIRECT_URI": "https://example.test/callback",
})
os.environ.pop("TRAFFIC_IP_KEY", None)
os.environ.pop("TRAFFIC_LOG_DAILY_CAP", None)
os.environ.pop("TRAFFIC_RAW_DAYS", None)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg
import httpx
import itsdangerous
import main
import traffic

PORT = os.getenv("TRAFFIC_TEST_PG_PORT", "54331")
DSN = "postgresql://postgres@localhost:%s/traffic_dash" % PORT
results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else "  [%s]" % (detail,)))


def cookie(session: dict) -> str:
    data = base64.b64encode(json.dumps(session).encode())
    return itsdangerous.TimestampSigner(SECRET).sign(data).decode()


def member(uid=1):
    return {"user": {"id": str(uid), "username": "u%s" % uid, "avatar": None}, "member": True}


ADMIN = member(900)
NOW = datetime.now(timezone.utc)


@main.app.get("/__ok")
async def probe_ok():
    return {"ok": True}


@main.app.get("/__item/{thing}")
async def probe_item(thing: str):
    return {"thing": thing}


@main.app.get("/__boom")
async def probe_boom():
    raise RuntimeError("boom")


@main.app.post("/__post")
async def probe_post(request: main.Request):
    return {"size": len(await request.body())}


def rec(row):
    return dict(zip(traffic.COLUMNS, row))


def last_row():
    return rec(traffic._buffer[-1]) if traffic._buffer else None


class DeadPool:
    """Any use of the database fails: proves the request path never touches it."""
    def acquire(self):
        raise AssertionError("database used in the request path")

    async def execute(self, *a, **k):
        raise AssertionError("database used in the request path")


class FailingPool:
    def acquire(self):
        raise ConnectionError("db down")


async def get(client, session, path, headers=None, method="GET", csrf=False, **kw):
    client.cookies.clear()
    if session is not None:
        client.cookies.set("session", cookie(session), domain="testserver.local")
    if csrf:
        client.cookies.set("csrf_token", "tok", domain="testserver.local")
        headers = {**(headers or {}), "X-CSRF-Token": "tok"}
    return await client.request(method, path, headers=headers, **kw)


def row_tuple(ts, status=200, duration=10, ip="1", visitor="anon", user_id=None, account_id=None,
              automated=False, path="/p", route=None, ua="Mozilla/5.0", prefix=None, method="GET"):
    return (ts, method, path, route, status, duration, traffic.hash_ip(ip),
            prefix or traffic.ip_prefix(ip), ua, user_id, account_id, visitor, automated)


async def insert_raw(pool, rows):
    await pool.copy_records_to_table("traffic_requests", records=rows, columns=traffic.COLUMNS)


async def main_async():
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    for t in ("traffic_requests", "traffic_hourly", "member_roles"):
        await pool.execute("DROP TABLE IF EXISTS %s" % t)
    await pool.execute(
        "CREATE TABLE member_roles (user_id BIGINT PRIMARY KEY, role_ids BIGINT[] NOT NULL, "
        "in_guild BOOLEAN NOT NULL, updated_at TIMESTAMPTZ NOT NULL)")
    main.app.state.db = pool
    transport = httpx.ASGITransport(app=main.app)
    client = httpx.AsyncClient(transport=transport, base_url="https://testserver.local",
                               follow_redirects=False)
    client500 = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=main.app, raise_app_exceptions=False),
        base_url="https://testserver.local", follow_redirects=False)

    # ---- schema ----
    await traffic.ensure_traffic_schema(pool)
    await traffic.ensure_traffic_schema(pool)       # idempotent
    cols = {r["column_name"]: r["data_type"] for r in await pool.fetch(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'traffic_requests'")}
    check("schema: traffic_requests columns", set(cols) == set(traffic.COLUMNS), cols)
    idx = {r["indexdef"] for r in await pool.fetch(
        "SELECT indexdef FROM pg_indexes WHERE tablename = 'traffic_requests'")}
    check("schema: ts index and (ip_hash, ts) index",
          any("(ts)" in i for i in idx) and any("(ip_hash, ts)" in i for i in idx), idx)
    pk = await pool.fetchval(
        "SELECT count(*) FROM pg_constraint WHERE conrelid = 'traffic_requests'::regclass AND contype = 'p'")
    check("schema: raw table has no primary key", pk == 0)
    hcols = {r["column_name"] for r in await pool.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'traffic_hourly'")}
    check("schema: traffic_hourly columns", {"hour", "requests", "unique_ips", "signed_in_requests",
          "s2xx", "s3xx", "s4xx", "s403", "s404", "s429", "s5xx", "automated", "p50_ms", "p95_ms",
          "max_ms", "dropped"} <= hcols, hcols)

    class SchemaFail:
        def __init__(self):
            self.calls = 0

        async def execute(self, stmt):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("first statement fails")
    sf = SchemaFail()
    logging.getLogger("dashboard.traffic").setLevel(logging.CRITICAL)
    try:
        await traffic.ensure_traffic_schema(sf)
        raised = False
    except RuntimeError:
        raised = True
    check("schema: one failing statement does not skip the rest, and is reported",
          sf.calls == len(traffic.SCHEMA_STATEMENTS) and raised, sf.calls)
    logging.getLogger("dashboard.traffic").setLevel(logging.NOTSET)

    # ---- hashing / prefix ----
    h = traffic.hash_ip("1.2.3.4")
    check("hash: 16 hex chars", len(h) == 16 and all(c in "0123456789abcdef" for c in h), h)
    check("hash: same IP same hash", h == traffic.hash_ip("1.2.3.4"))
    check("hash: different IP different hash", h != traffic.hash_ip("1.2.3.5"))
    check("hash: does not contain the IP", "1.2.3.4" not in h)
    os.environ["TRAFFIC_IP_KEY"] = "another-key"
    h2 = traffic.hash_ip("1.2.3.4")
    os.environ.pop("TRAFFIC_IP_KEY")
    check("hash: TRAFFIC_IP_KEY changes the key", h2 != h and traffic.hash_ip("1.2.3.4") == h)
    check("prefix: IPv4 -> /24", traffic.ip_prefix("1.2.3.4") == "1.2.3.0/24")
    check("prefix: IPv6 -> /48", traffic.ip_prefix("2001:db8:abcd:12:1:2:3:4") == "2001:db8:abcd::/48",
          traffic.ip_prefix("2001:db8:abcd:12:1:2:3:4"))
    check("prefix: IPv4-mapped IPv6 -> /24", traffic.ip_prefix("::ffff:10.20.30.40") == "10.20.30.0/24")
    check("prefix: junk -> unknown",
          all(traffic.ip_prefix(x) == "unknown" for x in ("", "unknown", "not an ip", "1.2.3", None, "999.1.1.1")))

    # ---- automated ----
    ua_yes = ["", None, "python-requests/2.31", "Python-urllib/3.11", "aiohttp/3.9", "httpx/0.27", "curl/8.0",
              "Wget/1.21", "Go-http-client/1.1", "Java/11.0", "okhttp/4.9", "libwww-perl/6", "node-fetch/2",
              "axios/1.6", "Scrapy/2.11", "HeadlessChrome/120", "PhantomJS", "selenium", "Puppeteer",
              "Playwright", "Googlebot/2.1", "Discordbot/2.0", "SomeCrawler", "Spider-X", "Yahoo! Slurp", "   "]
    ua_no = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
             "Chrome/120.0.0.0 Safari/537.36",
             "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Safari/604.1"]
    check("automated: known clients", all(traffic.is_automated(u) for u in ua_yes),
          [u for u in ua_yes if not traffic.is_automated(u)])
    check("automated: normal browsers are not", not any(traffic.is_automated(u) for u in ua_no))

    # ---- middleware order ----
    names = [m.cls.__name__ for m in main.app.user_middleware]
    check("middleware: traffic log is outermost", names[0] == "TrafficLogMiddleware", names)
    check("middleware: pure ASGI, not BaseHTTPMiddleware",
          not issubclass(traffic.TrafficLogMiddleware, main.BaseHTTPMiddleware))

    # ---- middleware records, with the database unreachable ----
    traffic.reset_state()
    main.app.state.db = DeadPool()
    main._role_check.update(disabled_until=0.0, missing_logged=False)
    browser = {"User-Agent": "Mozilla/5.0 Chrome/120", "X-Real-IP": "1.2.3.4"}
    r = await get(client, None, "/__ok?secret=hunter2", headers=browser)
    row = last_row()
    check("middleware: anonymous request recorded", r.status_code == 200 and row is not None, r.status_code)
    check("middleware: status, method, path (no query string)",
          row["status"] == 200 and row["method"] == "GET" and row["path"] == "/__ok", row)
    check("middleware: query string never stored", "hunter2" not in repr(row))
    check("middleware: duration is a non-negative int", isinstance(row["duration_ms"], int) and row["duration_ms"] >= 0)
    check("middleware: anon visitor, no user", row["visitor"] == "anon" and row["user_id"] is None
          and row["account_id"] is None)
    check("middleware: IP hashed + prefixed, never stored in full",
          row["ip_hash"] == traffic.hash_ip("1.2.3.4") and row["ip_prefix"] == "1.2.3.0/24"
          and "1.2.3.4" not in repr(row), row)
    check("middleware: normal browser not automated", row["automated"] is False and row["user_agent"] == browser["User-Agent"])
    check("middleware: ts is timezone-aware", row["ts"].tzinfo is not None)

    await get(client, None, "/__item/5", headers=browser)
    row = last_row()
    check("middleware: matched route template recorded", row["route"] == "/__item/{thing}" and row["path"] == "/__item/5", row)
    await get(client, None, "/nope-no-such-page", headers=browser)
    row = last_row()
    check("middleware: unmatched path has NULL route and its 404", row["route"] is None and row["status"] == 404, row)

    await get(client, member(42), "/__ok", headers=browser)
    row = last_row()
    check("visitor: member with Discord id", row["visitor"] == "member" and row["user_id"] == 42, row)
    await get(client, {"guest": True}, "/__ok", headers=browser)
    row = last_row()
    check("visitor: guest session", row["visitor"] == "guest", row)
    await get(client, {"user": {"id": None, "username": "Guest", "guest": True}}, "/__ok", headers=browser)
    check("visitor: user dict flagged guest", last_row()["visitor"] == "guest")
    await get(client, {"account_id": 7}, "/__ok", headers=browser)
    row = last_row()
    check("visitor: paid account", row["visitor"] == "account" and row["account_id"] == 7 and row["user_id"] is None, row)
    await get(client, None, "/__ok", headers={"User-Agent": "python-requests/2.31", "X-Real-IP": "5.6.7.8"})
    check("middleware: automated client flagged", last_row()["automated"] is True)
    await get(client, None, "/__ok", headers={"X-Real-IP": "5.6.7.8", "User-Agent": ""})
    check("middleware: empty user agent flagged automated", last_row()["automated"] is True)
    await get(client, None, "/__ok", headers={"User-Agent": "x" * 500})
    check("middleware: user agent capped at 200 chars", len(last_row()["user_agent"]) == 200)
    await get(client, None, "/__item/" + "a" * 400)
    check("middleware: path capped at 200 chars", len(last_row()["path"]) == 200)
    check("middleware: rows are plain tuples of the expected width",
          all(isinstance(x, tuple) and len(x) == len(traffic.COLUMNS) for x in traffic._buffer))

    # skip paths
    before = len(traffic._buffer)
    for p in ("/static/csrf.js", "/static/brand/favicon-32.png", "/health", "/healthz", "/favicon.ico"):
        await get(client, None, p)
    check("skip: /static, /health, /favicon are not recorded", len(traffic._buffer) == before,
          len(traffic._buffer) - before)

    # errors
    r = await get(client500, None, "/__boom", headers=browser)
    row = last_row()
    check("error: a raising endpoint is recorded as 500", r.status_code == 500 and row["status"] == 500, (r.status_code, row))
    r = await get(client, member(42), "/__post", method="POST", headers=browser, content=b"{}")
    row = last_row()
    check("CSRF 403 (answered by an inner layer) is recorded", r.status_code == 403 and row["status"] == 403, (r.status_code, row))
    r = await get(client, member(42), "/__post", method="POST", csrf=True, headers=browser, content=b"x" * 10)
    check("a normal POST with a CSRF token works and is recorded", r.status_code == 200 and last_row()["method"] == "POST", r.status_code)
    r = await get(client500, member(42), "/__post", method="POST", csrf=True, headers=browser,
                  content=b"x" * (main.MAX_REQUEST_BODY_BYTES + 1))
    row = last_row()
    # The size limit's own error currently comes out as a 500 in this app (same
    # without the traffic log; see the report). The log must record what was sent.
    check("oversized request is recorded with the status the client got",
          row["status"] == r.status_code and r.status_code in (413, 500), (r.status_code, row["status"]))
    print("     info: oversized request answered %s" % r.status_code)

    # ---- caps ----
    traffic.reset_state()
    old_max = traffic.BUFFER_MAX
    traffic.BUFFER_MAX = 3
    for i in range(5):
        traffic.enqueue(row_tuple(NOW, ip=str(i)))
    check("buffer cap: keeps 3, counts 2 dropped", len(traffic._buffer) == 3 and sum(traffic._dropped.values()) == 2,
          (len(traffic._buffer), traffic._dropped))
    check("buffer cap: dropped counted under the right hour",
          list(traffic._dropped) == [NOW.replace(minute=0, second=0, microsecond=0)])
    traffic.BUFFER_MAX = old_max
    traffic.reset_state()
    os.environ["TRAFFIC_LOG_DAILY_CAP"] = "2"
    for i in range(5):
        traffic.enqueue(row_tuple(NOW, ip=str(i)))
    check("daily cap: 2 saved, 3 dropped", len(traffic._buffer) == 2 and sum(traffic._dropped.values()) == 3,
          (len(traffic._buffer), traffic._dropped))
    traffic.enqueue(row_tuple(NOW + timedelta(days=1), ip="9"))
    check("daily cap: resets on a new UTC day", len(traffic._buffer) == 3)
    os.environ.pop("TRAFFIC_LOG_DAILY_CAP")
    traffic.reset_state()

    # ---- flusher ----
    main.app.state.db = pool
    await pool.execute("TRUNCATE traffic_requests, traffic_hourly")
    await get(client, member(42), "/__item/1", headers=browser)
    await get(client, None, "/__ok", headers={"User-Agent": "curl/8", "X-Real-IP": "5.6.7.8"})
    nbuf = len(traffic._buffer)
    n = await traffic.flush_now(pool)
    saved = await pool.fetch("SELECT * FROM traffic_requests ORDER BY ts")
    check("flush: rows copied and buffer emptied", n == nbuf == 2 and len(saved) == 2 and not traffic._buffer, (n, nbuf, len(saved)))
    check("flush: values round-trip", saved[0]["user_id"] == 42 and saved[0]["route"] == "/__item/{thing}"
          and saved[0]["visitor"] == "member" and saved[1]["automated"] is True and saved[1]["status"] == 200)
    # a NUL byte or lone surrogate in a client-controlled string must not break the batch
    traffic.enqueue(traffic._build_row(
        {"type": "http", "method": "GET", "path": "/a\x00b\ud800", "headers": [(b"user-agent", b"ua\x00x")],
         "client": ("1.1.1.1", 1)}, 200, 1, None))
    n = await traffic.flush_now(pool)
    odd = await pool.fetchrow("SELECT path, user_agent FROM traffic_requests WHERE path LIKE '/a%'")
    check("flush: NUL bytes and bad characters are cleaned, not fatal", n == 1 and odd and "\x00" not in odd["path"]
          and "\x00" not in odd["user_agent"], odd)
    # failure: rows are kept, nothing raised
    traffic.reset_state()
    for i in range(3):
        traffic.enqueue(row_tuple(NOW, ip=str(i)))
    logging.getLogger("dashboard.traffic").setLevel(logging.CRITICAL)
    n = await traffic.flush_now(FailingPool())
    check("flush: database error keeps the rows and does not raise", n == 0 and len(traffic._buffer) == 3, (n, len(traffic._buffer)))
    traffic.BUFFER_MAX = 4
    traffic.enqueue(row_tuple(NOW, ip="x1"))
    traffic.enqueue(row_tuple(NOW, ip="x2"))   # buffer full at 4 -> dropped
    n = await traffic.flush_now(FailingPool())
    check("flush: stays within the buffer cap while the database is down",
          len(traffic._buffer) <= 4 and sum(traffic._dropped.values()) == 1, (len(traffic._buffer), traffic._dropped))
    traffic.BUFFER_MAX = old_max
    logging.getLogger("dashboard.traffic").setLevel(logging.NOTSET)
    # background loop
    traffic.reset_state()
    await pool.execute("TRUNCATE traffic_requests")
    traffic.enqueue(row_tuple(NOW))
    traffic.FLUSH_INTERVAL_S = 0.1
    task = asyncio.create_task(traffic.run_flusher(main.app))
    await asyncio.sleep(0.6)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    check("flusher task: writes on its own and stops when cancelled",
          await pool.fetchval("SELECT count(*) FROM traffic_requests") == 1 and task.cancelled())
    traffic.FLUSH_INTERVAL_S = 5

    # ---- hourly roll-up ----
    traffic.reset_state()
    await pool.execute("TRUNCATE traffic_requests, traffic_hourly")
    cur = NOW.replace(minute=0, second=0, microsecond=0)
    prev = cur - timedelta(hours=1)
    far = cur - timedelta(hours=5)
    t = lambda base, m: base + timedelta(minutes=m)
    rows = [
        row_tuple(t(cur, 1), 200, 10, ip="1", visitor="member", user_id=5),
        row_tuple(t(cur, 2), 200, 20, ip="1", visitor="member", user_id=5),
        row_tuple(t(cur, 3), 302, 30, ip="2", visitor="account", account_id=3),
        row_tuple(t(cur, 4), 403, 40, ip="3", visitor="anon", automated=True),
        row_tuple(t(cur, 5), 404, 50, ip="3", visitor="anon", automated=True),
        row_tuple(t(cur, 6), 429, 60, ip="3", visitor="anon", automated=True),
        row_tuple(t(cur, 7), 500, 70, ip="4", visitor="guest"),
        row_tuple(t(cur, 8), 200, 100, ip="4", visitor="guest"),
        row_tuple(t(prev, 5), 200, 5, ip="1", visitor="anon"),
        row_tuple(t(far, 5), 200, 5, ip="1", visitor="anon"),
    ]
    await insert_raw(pool, rows)
    await traffic.rollup_hourly(pool, now=cur + timedelta(minutes=30))
    hr = await pool.fetchrow("SELECT * FROM traffic_hourly WHERE hour = $1", cur)
    check("rollup: counts for the current hour",
          (hr["requests"], hr["unique_ips"], hr["signed_in_requests"], hr["s2xx"], hr["s3xx"], hr["s4xx"],
           hr["s403"], hr["s404"], hr["s429"], hr["s5xx"], hr["automated"]) == (8, 4, 3, 3, 1, 3, 1, 1, 1, 1, 3), dict(hr))
    check("rollup: p50 / p95 / max", (hr["p50_ms"], hr["p95_ms"], hr["max_ms"]) == (45, 89, 100), dict(hr))
    check("rollup: previous hour built too", await pool.fetchval("SELECT requests FROM traffic_hourly WHERE hour = $1", prev) == 1)
    check("rollup: only the last 3 hours (older stay out)",
          await pool.fetchval("SELECT count(*) FROM traffic_hourly WHERE hour = $1", far) == 0)
    await traffic.rollup_hourly(pool, now=cur + timedelta(minutes=31))
    check("rollup: running twice does not double count",
          await pool.fetchval("SELECT requests FROM traffic_hourly WHERE hour = $1", cur) == 8)
    traffic._count_dropped(cur + timedelta(minutes=3), 4)
    traffic._count_dropped(prev, 2)
    await traffic.rollup_hourly(pool, now=cur + timedelta(minutes=32))
    check("rollup: dropped counts written and cleared",
          await pool.fetchval("SELECT dropped FROM traffic_hourly WHERE hour = $1", cur) == 4
          and await pool.fetchval("SELECT dropped FROM traffic_hourly WHERE hour = $1", prev) == 2 and not traffic._dropped)
    traffic._count_dropped(cur, 3)
    await traffic.rollup_hourly(pool, now=cur + timedelta(minutes=33))
    check("rollup: dropped counts add up across runs, requests untouched",
          await pool.fetchval("SELECT dropped FROM traffic_hourly WHERE hour = $1", cur) == 7
          and await pool.fetchval("SELECT requests FROM traffic_hourly WHERE hour = $1", cur) == 8)
    traffic._count_dropped(cur - timedelta(hours=30), 5)
    await traffic.rollup_hourly(pool, now=cur)
    hr = await pool.fetchrow("SELECT requests, dropped FROM traffic_hourly WHERE hour = $1", cur - timedelta(hours=30))
    check("rollup: drops in an hour with no saved rows still recorded", hr and hr["dropped"] == 5 and hr["requests"] == 0, hr)

    # ---- retention ----
    await pool.execute("TRUNCATE traffic_requests, traffic_hourly")
    d = lambda days: NOW - timedelta(days=days)
    await insert_raw(pool, [row_tuple(d(20), ip="o1"), row_tuple(d(15), ip="o2"), row_tuple(d(13), ip="n1"), row_tuple(d(1), ip="n2")])
    await pool.executemany("INSERT INTO traffic_hourly (hour, requests) VALUES ($1, 1)",
                           [(d(500).replace(minute=0, second=0, microsecond=0),), (d(401).replace(minute=0, second=0, microsecond=0),),
                            (d(399).replace(minute=0, second=0, microsecond=0),), (d(10).replace(minute=0, second=0, microsecond=0),)])
    res = await traffic.purge_old(pool, now=NOW)
    check("retention: raw rows older than 14 days deleted, newer kept",
          res["raw"] == 2 and await pool.fetchval("SELECT count(*) FROM traffic_requests") == 2, res)
    check("retention: hourly older than 400 days deleted, newer kept",
          res["hourly"] == 2 and await pool.fetchval("SELECT count(*) FROM traffic_hourly") == 2, res)
    os.environ["TRAFFIC_RAW_DAYS"] = "30"
    await insert_raw(pool, [row_tuple(d(20), ip="o1")])
    res = await traffic.purge_old(pool, now=NOW)
    check("retention: TRAFFIC_RAW_DAYS respected", res["raw"] == 0)
    os.environ.pop("TRAFFIC_RAW_DAYS")
    await pool.execute("TRUNCATE traffic_requests")
    old_batch = traffic.PURGE_BATCH
    traffic.PURGE_BATCH = 2
    await insert_raw(pool, [row_tuple(d(30), ip=str(i)) for i in range(5)] + [row_tuple(d(1), ip="keep")])
    res = await traffic.purge_old(pool, now=NOW)
    traffic.PURGE_BATCH = old_batch
    check("retention: deletes in batches until done", res["raw"] == 5 and await pool.fetchval("SELECT count(*) FROM traffic_requests") == 1, res)

    # ---- routes: access ----
    await pool.execute("TRUNCATE traffic_requests, traffic_hourly")
    traffic.reset_state()
    main._role_cache.clear()
    main._role_check.update(disabled_until=0.0, missing_logged=False)
    for path in ("/admin/traffic", "/admin/traffic/api", "/admin/traffic/ip/0123456789abcdef"):
        r = await get(client, member(1), path)
        check("access: non-admin member gets 403 on %s" % path, r.status_code == 403, r.status_code)
        r = await get(client, None, path)
        check("access: anonymous blocked on %s" % path, r.status_code in (401, 302, 303, 307), r.status_code)
        r = await get(client, {**member(1), "member": False}, path)
        check("access: sample (no role) viewer blocked on %s" % path, r.status_code == 403, r.status_code)
    r = await get(client, ADMIN, "/admin/traffic")
    check("access: admin gets the page", r.status_code == 200 and "Traffic" in r.text and "chart.umd.min.js" in r.text, r.status_code)
    check("page: Traffic link is active and Admin Tools button active",
          '<a href="/admin/traffic" class="active">Traffic</a>' in r.text and 'nav-group-btn active' in r.text)
    check("page: privacy note and chart library URL",
          "full IP addresses are never stored" in r.text
          and "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js" in r.text)
    check("page: data is never put in with innerHTML", "innerHTML =" not in r.text and ".innerHTML" not in r.text.replace("never innerHTML", ""))
    r = await get(client, ADMIN, "/admin")
    check("page: /admin still works and links to Traffic", r.status_code == 200 and 'href="/admin/traffic"' in r.text)

    # security headers
    r = await get(client, ADMIN, "/admin/traffic")
    csp = r.headers.get("content-security-policy", "")
    check("headers: CSP on /admin/traffic still allows cdnjs and nothing new",
          "https://cdnjs.cloudflare.com" in csp and "script-src 'self' 'unsafe-inline' https://maps.googleapis.com "
          "https://maps.gstatic.com https://unpkg.com https://cdnjs.cloudflare.com;" in csp and "frame-ancestors 'none'" in csp, csp)
    check("headers: other security headers on /admin/traffic",
          r.headers.get("x-frame-options") == "DENY" and r.headers.get("x-content-type-options") == "nosniff"
          and "max-age=31536000" in r.headers.get("strict-transport-security", ""))
    r2 = await get(client, None, "/__ok")
    check("headers: CSP and others on an ordinary page", r2.headers.get("content-security-policy") == csp
          and r2.headers.get("x-frame-options") == "DENY")
    sm = [m for m in main.app.user_middleware if m.cls.__name__ == "SessionMiddleware"][0]
    check("headers: session cookie flags unchanged",
          sm.kwargs.get("https_only") is True and sm.kwargs.get("same_site") == "lax", sm.kwargs)

    # ---- routes: API ----
    r = await get(client, ADMIN, "/admin/traffic/api?window=evil", headers={"X-Real-IP": "9.9.9.9", "X-Forwarded-For": "8.8.8.8, 7.7.7.7"})
    j = r.json()
    check("api: unknown window falls back to 24h", r.status_code == 200 and j["window"] == "24h", j.get("window"))
    check("api: window allowlist", [traffic.normalize_window(x) for x in ("1h", "24h", "7d", "30d", "", None, "1h; DROP")]
          == ["1h", "24h", "7d", "24h", "24h", "24h", "24h"])
    ic = j["ip_check"]
    check("api: IP check shows the headers and what was chosen",
          ic["x_real_ip"] == "9.9.9.9" and ic["x_forwarded_for"] == "8.8.8.8, 7.7.7.7" and ic["cf_connecting_ip"] is None
          and ic["true_client_ip"] is None and ic["chosen"] == "9.9.9.9" and "socket_peer" in ic, ic)
    r = await get(client, ADMIN, "/admin/traffic/api", headers={"CF-Connecting-IP": "6.6.6.6", "True-Client-IP": "5.5.5.5"})
    ic = r.json()["ip_check"]
    check("api: IP check shows CF-Connecting-IP / True-Client-IP", ic["cf_connecting_ip"] == "6.6.6.6" and ic["true_client_ip"] == "5.5.5.5", ic)
    check("api: IP check is not stored", await pool.fetchval("SELECT count(*) FROM traffic_requests WHERE ip_prefix = '9.9.9.0/24'") == 0
          and not any("9.9.9.9" in repr(x) for x in traffic._buffer))
    r = await get(client, ADMIN, "/admin/traffic/ip/ZZZZ")
    check("ip detail: bad id rejected with 400", r.status_code == 400, r.status_code)
    for bad in ("0123456789ABCDEF", "0123456789abcde", "0123456789abcdef0", "0123456789abcdeg", "'; drop table x;--"):
        check("ip detail: validation rejects %r" % bad, await traffic.ip_detail(pool, bad) is None)
    r = await get(client, ADMIN, "/admin/traffic/ip/0123456789abcdef")
    check("ip detail: valid id with no rows gives an empty list", r.status_code == 200 and r.json()["requests"] == [], r.text[:100])

    # ---- report on seeded data ----
    await pool.execute("TRUNCATE traffic_requests, traffic_hourly")
    traffic.reset_state()
    base = NOW - timedelta(minutes=30)
    seed = []
    for i in range(60):    # a heavy anonymous scraper
        seed.append(row_tuple(base + timedelta(seconds=i), 200 if i % 3 else 404, 50 + i, ip="10.0.0.9",
                              visitor="anon", automated=True, path="/api/catalog/%d" % (i % 7),
                              route="/api/catalog/{game}", ua="python-requests/2.31"))
    for i in range(5):
        seed.append(row_tuple(base + timedelta(seconds=100 + i), 429, 5, ip="10.0.0.9", visitor="anon",
                              automated=True, ua="python-requests/2.31", path="/api/catalog/1", route="/api/catalog/{game}"))
    for i in range(20):    # a signed-in member from another network with 4 ids over time
        seed.append(row_tuple(base + timedelta(seconds=200 + i), 200, 20, ip="10.0.1.5", visitor="member",
                              user_id=1000 + (i % 4), ua="Mozilla/5.0", path="/", route="/"))
    for i in range(3):
        seed.append(row_tuple(base + timedelta(seconds=300 + i), 403, 3, ip="10.0.2.5", visitor="guest", ua="Mozilla/5.0", path="/admin"))
    seed.append(row_tuple(base + timedelta(seconds=400), 500, 900, ip="2001:db8::1", visitor="account", account_id=4,
                          ua="Mozilla/5.0", path="/boom"))
    seed.append(row_tuple(NOW - timedelta(days=3), 200, 10, ip="10.0.3.5", visitor="anon", ua="Mozilla/5.0", path="/old"))
    await insert_raw(pool, seed)
    for k in range(1, 7 * 24):   # a quiet week at ~50 requests an hour, then one huge hour
        hour = (NOW - timedelta(hours=k + 3)).replace(minute=0, second=0, microsecond=0)
        await pool.execute("INSERT INTO traffic_hourly (hour, requests, automated, s429, s5xx) VALUES ($1, 50, 10, 1, 0)", hour)
    spike_hour = (NOW - timedelta(hours=2)).replace(minute=0, second=0, microsecond=0)
    await pool.execute("INSERT INTO traffic_hourly (hour, requests, automated, s429, s5xx, dropped) VALUES ($1, 1000, 900, 40, 2, 7)"
                       " ON CONFLICT (hour) DO UPDATE SET requests = 1000, dropped = 7", spike_hour)
    small = (NOW - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    await pool.execute("INSERT INTO traffic_hourly (hour, requests) VALUES ($1, 150) ON CONFLICT (hour) DO UPDATE SET requests = 150", small)
    await pool.execute("INSERT INTO traffic_hourly (hour, requests) VALUES ($1, 900) ON CONFLICT (hour) DO NOTHING",
                       (NOW - timedelta(hours=60)).replace(minute=0, second=0, microsecond=0))

    rep = await traffic.traffic_report(pool, "24h", now=NOW)
    check("report: top-level keys", {"window", "generated_at", "bucket_seconds", "totals", "series", "top_ips", "top_signed_out",
          "top_agents", "top_paths", "blocked", "spikes", "long_term"} <= set(rep), set(rep))
    tt = rep["totals"]
    check("report: totals", (tt["requests"], tt["visitors"], tt["s403"], tt["s404"], tt["s429"], tt["s5xx"])
          == (89, 4, 3, 20, 5, 1), tt)
    check("report: shares and p95", tt["signed_in_share"] == round(100 * 21 / 89, 1) and tt["automated_share"] == round(100 * 65 / 89, 1)
          and tt["p95_ms"] is not None and tt["p95_ms"] >= 100, tt)
    check("report: 7d window includes the older request", (await traffic.traffic_report(pool, "7d", now=NOW))["totals"]["requests"] == 90)
    rep1 = await traffic.traffic_report(pool, "1h", now=NOW)
    check("report: 1h window uses 5-minute buckets", rep1["bucket_seconds"] == 300 and 12 <= len(rep1["series"]) <= 14, len(rep1["series"]))
    check("report: 24h window uses hourly buckets", rep["bucket_seconds"] == 3600 and 24 <= len(rep["series"]) <= 26, len(rep["series"]))
    s = rep["series"]
    check("report: series adds up and stacks",
          sum(p["requests"] for p in s) == 89 and all(p["ok"] + p["client_errors"] + p["throttled"] + p["server_errors"] == p["requests"] for p in s)
          and sum(p["throttled"] for p in s) == 5 and sum(p["server_errors"] for p in s) == 1, s)
    ips = rep["top_ips"]
    check("report: top visitors ordered, with the details",
          ips[0]["ip_prefix"] == "10.0.0.0/24" and ips[0]["ip_hash"] == traffic.hash_ip("10.0.0.9") and ips[0]["requests"] == 65
          and ips[0]["paths"] == 7 and ips[0]["s404"] == 20 and ips[0]["s429"] == 5 and ips[0]["automated_share"] == 100.0
          and ips[0]["top_agent"] == "python-requests/2.31" and ips[0]["user_ids"] == [] and ips[0]["first_seen"] and ips[0]["last_seen"], ips[0])
    member_row = [x for x in ips if x["ip_prefix"] == "10.0.1.0/24"][0]
    check("report: user ids listed (max 3), as strings", len(member_row["user_ids"]) == 3 and all(isinstance(u, str) for u in member_row["user_ids"]), member_row)
    check("report: IPv6 shown by /48", any(x["ip_prefix"] == "2001:db8::/48" for x in ips))
    out = rep["top_signed_out"]
    check("report: signed-out list has only anon/guest visitors",
          {x["ip_prefix"] for x in out} == {"10.0.0.0/24", "10.0.2.0/24"}, [x["ip_prefix"] for x in out])
    ag = rep["top_agents"]
    check("report: clients list", ag[0]["user_agent"] == "python-requests/2.31" and ag[0]["automated"] is True and ag[0]["requests"] == 65
          and ag[0]["ips"] == 1 and any(a["user_agent"] == "Mozilla/5.0" and a["automated"] is False for a in ag), ag)
    pg = rep["top_paths"]
    check("report: pages use the route template, with 4xx and average time",
          pg[0]["page"] == "/api/catalog/{game}" and pg[0]["requests"] == 65 and pg[0]["s4xx"] == 25 and pg[0]["avg_ms"] is not None, pg[0])
    bl = rep["blocked"]
    check("report: blocked list has only 403/429, newest first",
          len(bl) == 8 and {b["status"] for b in bl} == {403, 429} and bl[0]["ts"] >= bl[-1]["ts"]
          and {"ts", "ip_prefix", "ip_hash", "path", "user_agent", "visitor"} <= set(bl[0]), bl[:1])
    sp = rep["spikes"]
    check("report: spike found with ratio (and only the big one)",
          len(sp) == 1 and sp[0]["requests"] == 1000 and sp[0]["ratio"] is not None and 15 <= sp[0]["ratio"] <= 25
          and sp[0]["hour"].startswith(spike_hour.isoformat()[:13]), sp)
    check("report: 150/hour is not a spike (under 200) and a spike older than 48h is left out", all(x["requests"] != 150 and x["requests"] != 900 for x in sp))
    lt = rep["long_term"]
    check("report: long-term daily totals have the expected fields",
          bool(lt) and {"day", "requests", "automated", "s429", "s5xx", "dropped"} <= set(lt[0]), lt[:1])
    total_requests = await pool.fetchval("SELECT sum(requests) FROM traffic_hourly")
    check("report: long-term sums match the hourly table, dropped included",
          sum(x["requests"] for x in lt) == total_requests and sum(x["dropped"] for x in lt) == 7, (sum(x["requests"] for x in lt), total_requests))
    json.dumps(rep)
    check("report: JSON-able", True)

    r = await get(client, ADMIN, "/admin/traffic/api?window=1h")
    check("api: admin gets the report", r.status_code == 200 and r.json()["window"] == "1h" and r.json()["totals"]["requests"] == 89,
          r.text[:200])
    det = await get(client, ADMIN, "/admin/traffic/ip/%s" % traffic.hash_ip("10.0.1.5"))
    dj = det.json()
    check("ip detail: last requests for one visitor", det.status_code == 200 and len(dj["requests"]) == 20 and dj["ip_prefix"] == "10.0.1.0/24"
          and dj["requests"][0]["ts"] >= dj["requests"][-1]["ts"] and dj["requests"][0]["user_id"] in ("1000", "1001", "1002", "1003"), dj["requests"][:1])
    await insert_raw(pool, [row_tuple(NOW - timedelta(seconds=i), ip="10.0.4.4") for i in range(250)])
    check("ip detail: at most 200 rows", len((await traffic.ip_detail(pool, traffic.hash_ip("10.0.4.4")))["requests"]) == 200)

    # the admin's own page views were recorded too, without waiting on the database
    check("self: admin traffic views are logged like any other", any(rec(x)["path"].startswith("/admin/traffic") for x in traffic._buffer))

    await client.aclose()
    await client500.aclose()
    await pool.execute("DROP TABLE IF EXISTS traffic_requests, traffic_hourly, member_roles")
    await pool.close()


asyncio.run(main_async())
failed = [n for n, ok in results if not ok]
print("\n%d checks, %d passed, %d failed" % (len(results), len(results) - len(failed), len(failed)))
if failed:
    print("FAILED: " + "; ".join(failed))
    sys.exit(1)
