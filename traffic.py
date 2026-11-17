"""Traffic monitoring: one row per request, an hourly summary kept long-term,
and the queries behind the admin-only /admin/traffic page.

Design notes
- The request path only appends a small tuple to an in-memory deque. Nothing
  here awaits the database while a visitor's request is being served.
- A background task (run_flusher) copies the buffer into traffic_requests every
  few seconds. A second task (run_maintenance) keeps traffic_hourly up to date
  and deletes old rows.
- Full IP addresses are never stored: only a keyed hash (so a visitor can be
  followed across days) and a network prefix (/24 for IPv4, /48 for IPv6).
- Both tables are owned by the dashboard (the bot never touches them).
"""
import asyncio
import collections
import hashlib
import hmac
import ipaddress
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone

from starlette.requests import Request

logger = logging.getLogger("dashboard.traffic")

# ---- Tunables -------------------------------------------------------------
BUFFER_MAX = 5000               # rows waiting to be saved; beyond this we drop
FLUSH_INTERVAL_S = 5
MAINTENANCE_INTERVAL_S = 600    # 10 minutes
MAINTENANCE_FIRST_DELAY_S = 20  # so the first clean-up runs shortly after boot
PURGE_EVERY_S = 24 * 3600
HOURLY_KEEP_DAYS = 400
PURGE_BATCH = 20000
ERROR_LOG_EVERY_S = 60
MAX_PATH = 200
MAX_UA = 200
SKIP_PREFIXES = ("/static/", "/health", "/favicon")


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def daily_cap() -> int:
    return _int_env("TRAFFIC_LOG_DAILY_CAP", 200000)


def raw_days() -> int:
    return max(1, _int_env("TRAFFIC_RAW_DAYS", 14))


# ---- Schema ---------------------------------------------------------------
# One statement at a time: a failure in one must be logged and must not stop
# the rest from running (see MEMORY 2026-08-03).
SCHEMA_STATEMENTS = (
    """CREATE TABLE IF NOT EXISTS traffic_requests (
        ts          TIMESTAMPTZ NOT NULL,
        method      TEXT,
        path        TEXT,
        route       TEXT,
        status      SMALLINT,
        duration_ms INTEGER,
        ip_hash     TEXT,
        ip_prefix   TEXT,
        user_agent  TEXT,
        user_id     BIGINT,
        account_id  BIGINT,
        visitor     TEXT,
        automated   BOOLEAN NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS traffic_requests_ts_idx ON traffic_requests (ts)",
    "CREATE INDEX IF NOT EXISTS traffic_requests_ip_ts_idx ON traffic_requests (ip_hash, ts)",
    """CREATE TABLE IF NOT EXISTS traffic_hourly (
        hour               TIMESTAMPTZ PRIMARY KEY,
        requests           INT NOT NULL DEFAULT 0,
        unique_ips         INT NOT NULL DEFAULT 0,
        signed_in_requests INT NOT NULL DEFAULT 0,
        s2xx               INT NOT NULL DEFAULT 0,
        s3xx               INT NOT NULL DEFAULT 0,
        s4xx               INT NOT NULL DEFAULT 0,
        s403               INT NOT NULL DEFAULT 0,
        s404               INT NOT NULL DEFAULT 0,
        s429               INT NOT NULL DEFAULT 0,
        s5xx               INT NOT NULL DEFAULT 0,
        automated          INT NOT NULL DEFAULT 0,
        p50_ms             INT,
        p95_ms             INT,
        max_ms             INT,
        dropped            INT NOT NULL DEFAULT 0
    )""",
)


async def ensure_traffic_schema(pool) -> None:
    failed = []
    for stmt in SCHEMA_STATEMENTS:
        try:
            await pool.execute(stmt)
        except Exception:
            failed.append(stmt.strip().split("\n")[0][:80])
            logger.exception("Traffic schema statement failed: %s", stmt.strip()[:80])
    if failed:
        raise RuntimeError("Traffic schema: %d statement(s) failed: %s"
                           % (len(failed), "; ".join(failed)))


# ---- IP handling ----------------------------------------------------------
_key_cache = {}


def _ip_key() -> bytes:
    explicit = os.getenv("TRAFFIC_IP_KEY", "")
    secret = os.getenv("SESSION_SECRET", "")
    cached = _key_cache.get((explicit, secret))
    if cached is None:
        if explicit:
            cached = explicit.encode()
        else:
            cached = hmac.new(secret.encode(), b"traffic-ip-hash", hashlib.sha256).digest()
        _key_cache.clear()
        _key_cache[(explicit, secret)] = cached
    return cached


def hash_ip(ip: str) -> str:
    """Keyed, one-way, 16 hex chars. Same IP gives the same hash."""
    return hmac.new(_ip_key(), (ip or "").encode("utf-8", "replace"),
                    hashlib.sha256).hexdigest()[:16]


def ip_prefix(ip: str) -> str:
    """IPv4 -> its /24 network, IPv6 -> its /48 network, anything else -> 'unknown'."""
    try:
        text = (ip or "").strip().split("%", 1)[0]
        addr = ipaddress.ip_address(text)
        if addr.version == 6 and addr.ipv4_mapped is not None:
            addr = addr.ipv4_mapped
        bits = 24 if addr.version == 4 else 48
        return str(ipaddress.ip_network("%s/%d" % (addr, bits), strict=False))
    except Exception:
        return "unknown"


# ---- Automated client detection ------------------------------------------
_AUTOMATED_TOKENS = (
    "python-requests", "python-urllib", "aiohttp", "httpx", "curl", "wget",
    "go-http-client", "java/", "okhttp", "libwww", "node-fetch", "axios",
    "scrapy", "headless", "phantomjs", "selenium", "puppeteer", "playwright",
    "bot", "crawler", "spider", "slurp",
)
_AUTOMATED_RE = re.compile("|".join(re.escape(t) for t in _AUTOMATED_TOKENS), re.IGNORECASE)


def is_automated(ua) -> bool:
    if not ua or not str(ua).strip():
        return True
    return _AUTOMATED_RE.search(str(ua)) is not None


# ---- Buffer ---------------------------------------------------------------
COLUMNS = ["ts", "method", "path", "route", "status", "duration_ms", "ip_hash",
           "ip_prefix", "user_agent", "user_id", "account_id", "visitor", "automated"]

_buffer = collections.deque()
_dropped = {}                       # hour (UTC datetime) -> rows not saved
_daily = {"day": None, "count": 0}  # rows accepted so far this UTC day
_err_log = {"last": 0.0}
_flush_lock = None


def _hour_of(ts: datetime) -> datetime:
    return ts.replace(minute=0, second=0, microsecond=0)


def _count_dropped(ts: datetime, n: int = 1) -> None:
    h = _hour_of(ts)
    _dropped[h] = _dropped.get(h, 0) + n


def enqueue(row: tuple) -> bool:
    """Append one row, or drop-and-count it when over a cap. Never raises."""
    ts = row[0]
    day = ts.date()
    if _daily["day"] != day:
        _daily["day"] = day
        _daily["count"] = 0
    if _daily["count"] >= daily_cap() or len(_buffer) >= BUFFER_MAX:
        _count_dropped(ts)
        return False
    _daily["count"] += 1
    _buffer.append(row)
    return True


def reset_state() -> None:
    """Forget buffered rows and counters (used by tests)."""
    _buffer.clear()
    _dropped.clear()
    _daily.update(day=None, count=0)
    _err_log["last"] = 0.0


def _log_error_throttled(what: str) -> None:
    now = time.monotonic()
    if now - _err_log["last"] >= ERROR_LOG_EVERY_S:
        _err_log["last"] = now
        logger.warning("%s", what, exc_info=True)


def _clean(text, limit: int):
    if text is None:
        return None
    # Postgres text cannot hold NUL bytes or lone surrogates; either would
    # make a whole batch fail to save.
    s = str(text).encode("utf-8", "replace").decode("utf-8").replace("\x00", "")
    return s[:limit]


# ---- Middleware -----------------------------------------------------------
def _header(scope, name: bytes):
    for k, v in scope.get("headers") or ():
        if k == name:
            return v.decode("latin-1")
    return None


def _build_row(scope, status: int, duration_ms: int, ip_getter) -> tuple:
    try:
        ip = ip_getter(Request(scope)) if ip_getter else ""
    except Exception:
        ip = "unknown"
    ip = ip or "unknown"
    ua = _clean(_header(scope, b"user-agent"), MAX_UA)

    session = scope.get("session")
    if not isinstance(session, dict):
        session = {}
    user = session.get("user")
    if not isinstance(user, dict):
        user = {}
    user_id = None
    if user.get("id") is not None:
        try:
            user_id = int(user["id"])
        except (TypeError, ValueError):
            user_id = None
    account_id = None
    if session.get("account_id") is not None:
        try:
            account_id = int(session["account_id"])
        except (TypeError, ValueError):
            account_id = None
    if session.get("guest") or user.get("guest"):
        visitor = "guest"
    elif user_id is not None:
        visitor = "member"
    elif account_id is not None:
        visitor = "account"
    else:
        visitor = "anon"

    route = None
    r = scope.get("route")
    route_path = getattr(r, "path", None)
    if isinstance(route_path, str):
        route = _clean(route_path, MAX_PATH)

    return (
        datetime.now(timezone.utc),
        _clean(scope.get("method"), 16),
        _clean(scope.get("path") or "/", MAX_PATH),
        route,
        int(status),
        int(duration_ms),
        hash_ip(ip),
        ip_prefix(ip),
        ua,
        user_id,
        account_id,
        visitor,
        is_automated(ua),
    )


class TrafficLogMiddleware:
    """Pure ASGI (not BaseHTTPMiddleware): no extra task or body buffering per
    request. Records status, time taken and who it was, then appends one tuple
    to the in-memory buffer."""

    def __init__(self, app, ip_getter=None):
        self.app = app
        self.ip_getter = ip_getter

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or (scope.get("path") or "").startswith(SKIP_PREFIXES):
            await self.app(scope, receive, send)
            return
        started = time.perf_counter()
        seen = {"status": None}

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                seen["status"] = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            self._record(scope, 500, started)
            raise
        if seen["status"] is not None:
            self._record(scope, seen["status"], started)

    def _record(self, scope, status: int, started: float) -> None:
        try:
            duration_ms = int((time.perf_counter() - started) * 1000)
            enqueue(_build_row(scope, status, duration_ms, self.ip_getter))
        except Exception:
            _log_error_throttled("Traffic log: could not record a request")


# ---- Flusher --------------------------------------------------------------
async def flush_now(pool) -> int:
    """Save everything buffered. Returns rows written. Never raises."""
    global _flush_lock
    if _flush_lock is None:
        _flush_lock = asyncio.Lock()
    async with _flush_lock:
        written = 0
        while _buffer:
            batch = []
            while _buffer and len(batch) < BUFFER_MAX:
                batch.append(_buffer.popleft())
            try:
                async with pool.acquire() as conn:
                    await conn.copy_records_to_table(
                        "traffic_requests", records=batch, columns=COLUMNS, timeout=20)
                written += len(batch)
            except asyncio.CancelledError:
                _buffer.extendleft(reversed(batch))
                raise
            except Exception:
                _log_error_throttled("Traffic log: saving a batch failed; will retry")
                _buffer.extendleft(reversed(batch))
                while len(_buffer) > BUFFER_MAX:     # keep memory bounded
                    _count_dropped(_buffer.popleft()[0])
                break
        return written


async def run_flusher(app) -> None:
    while True:
        try:
            await asyncio.sleep(FLUSH_INTERVAL_S)
            pool = getattr(app.state, "db", None)
            if pool is not None:
                await flush_now(pool)
        except asyncio.CancelledError:
            raise
        except Exception:
            _log_error_throttled("Traffic log: flusher loop error")


# ---- Maintenance ----------------------------------------------------------
_HOURLY_UPSERT = """
INSERT INTO traffic_hourly
  (hour, requests, unique_ips, signed_in_requests, s2xx, s3xx, s4xx, s403, s404,
   s429, s5xx, automated, p50_ms, p95_ms, max_ms)
SELECT to_timestamp(floor(extract(epoch FROM ts) / 3600) * 3600) AS h,
       count(*),
       count(DISTINCT ip_hash),
       count(*) FILTER (WHERE visitor IN ('member', 'account')),
       count(*) FILTER (WHERE status BETWEEN 200 AND 299),
       count(*) FILTER (WHERE status BETWEEN 300 AND 399),
       count(*) FILTER (WHERE status BETWEEN 400 AND 499),
       count(*) FILTER (WHERE status = 403),
       count(*) FILTER (WHERE status = 404),
       count(*) FILTER (WHERE status = 429),
       count(*) FILTER (WHERE status >= 500),
       count(*) FILTER (WHERE automated),
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY duration_ms)::int,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)::int,
       max(duration_ms)
FROM traffic_requests
WHERE ts >= $1
GROUP BY 1
ON CONFLICT (hour) DO UPDATE SET
  requests = EXCLUDED.requests, unique_ips = EXCLUDED.unique_ips,
  signed_in_requests = EXCLUDED.signed_in_requests, s2xx = EXCLUDED.s2xx,
  s3xx = EXCLUDED.s3xx, s4xx = EXCLUDED.s4xx, s403 = EXCLUDED.s403,
  s404 = EXCLUDED.s404, s429 = EXCLUDED.s429, s5xx = EXCLUDED.s5xx,
  automated = EXCLUDED.automated, p50_ms = EXCLUDED.p50_ms,
  p95_ms = EXCLUDED.p95_ms, max_ms = EXCLUDED.max_ms
"""

_DROPPED_UPSERT = """
INSERT INTO traffic_hourly (hour, dropped) VALUES ($1, $2)
ON CONFLICT (hour) DO UPDATE SET dropped = traffic_hourly.dropped + EXCLUDED.dropped
"""


async def rollup_hourly(pool, now: datetime = None, hours_back: int = 2) -> None:
    """Rebuild the current hour and the previous `hours_back` from raw rows,
    then add the in-memory dropped counts."""
    now = now or datetime.now(timezone.utc)
    start = _hour_of(now) - timedelta(hours=hours_back)
    await pool.execute(_HOURLY_UPSERT, start)
    pending = list(_dropped.items())
    for hour, n in pending:
        await pool.execute(_DROPPED_UPSERT, hour, n)
        left = _dropped.get(hour, 0) - n
        if left > 0:
            _dropped[hour] = left
        else:
            _dropped.pop(hour, None)


async def _delete_in_batches(pool, table: str, column: str, cutoff: datetime) -> int:
    # table/column come from the two fixed pairs below, never from input.
    assert (table, column) in (("traffic_requests", "ts"), ("traffic_hourly", "hour"))
    sql = ("DELETE FROM %s WHERE ctid IN "
           "(SELECT ctid FROM %s WHERE %s < $1 LIMIT %d)" % (table, table, column, PURGE_BATCH))
    total = 0
    while True:
        tag = await pool.execute(sql, cutoff)
        n = int(tag.split()[-1])
        total += n
        if n < PURGE_BATCH:
            return total
        await asyncio.sleep(0)


async def purge_old(pool, now: datetime = None) -> dict:
    now = now or datetime.now(timezone.utc)
    raw = await _delete_in_batches(pool, "traffic_requests", "ts",
                                   now - timedelta(days=raw_days()))
    hourly = await _delete_in_batches(pool, "traffic_hourly", "hour",
                                      now - timedelta(days=HOURLY_KEEP_DAYS))
    return {"raw": raw, "hourly": hourly}


async def run_maintenance(app) -> None:
    last_purge = None
    first = True
    while True:
        try:
            await asyncio.sleep(MAINTENANCE_FIRST_DELAY_S if first else MAINTENANCE_INTERVAL_S)
            pool = getattr(app.state, "db", None)
            if pool is None:
                continue
            # After a restart, rebuild further back: the hour the app went down
            # in may not have been summed since its last few minutes were saved.
            await rollup_hourly(pool, hours_back=48 if first else 2)
            first = False
            if last_purge is None or time.monotonic() - last_purge >= PURGE_EVERY_S:
                result = await purge_old(pool)
                last_purge = time.monotonic()
                if result["raw"] or result["hourly"]:
                    logger.info("Traffic clean-up removed %d detailed and %d hourly rows",
                                result["raw"], result["hourly"])
        except asyncio.CancelledError:
            raise
        except Exception:
            _log_error_throttled("Traffic log: maintenance error")


# ---- Queries for the page -------------------------------------------------
WINDOWS = {
    "1h": (timedelta(hours=1), 300),
    "24h": (timedelta(hours=24), 3600),
    "7d": (timedelta(days=7), 3600),
}
HASH_RE = re.compile(r"^[0-9a-f]{16}$")


def normalize_window(window) -> str:
    return window if window in WINDOWS else "24h"


def _f(x, nd=1):
    return None if x is None else round(float(x), nd)


def _iso(ts):
    return ts.isoformat() if ts is not None else None


def _pct(part, whole):
    return round(100.0 * part / whole, 1) if whole else 0.0


_VISITOR_ROWS = """
SELECT ip_prefix, ip_hash,
       count(*) AS requests,
       count(DISTINCT path) AS paths,
       count(*) FILTER (WHERE status = 404) AS s404,
       count(*) FILTER (WHERE status = 429) AS s429,
       (array_agg(DISTINCT user_id) FILTER (WHERE user_id IS NOT NULL))[1:3] AS user_ids,
       avg(automated::int) AS automated_share,
       mode() WITHIN GROUP (ORDER BY user_agent) AS top_agent,
       min(ts) AS first_seen, max(ts) AS last_seen
FROM traffic_requests
WHERE ts >= $1 %s
GROUP BY ip_prefix, ip_hash
ORDER BY requests DESC, last_seen DESC
LIMIT 20
"""


def _visitor_rows(rows):
    return [{
        "ip_prefix": r["ip_prefix"],
        "ip_hash": r["ip_hash"],
        "requests": r["requests"],
        "paths": r["paths"],
        "s404": r["s404"],
        "s429": r["s429"],
        "user_ids": [str(u) for u in (r["user_ids"] or [])],
        "automated_share": _pct(float(r["automated_share"] or 0), 1),
        "top_agent": r["top_agent"] or "",
        "first_seen": _iso(r["first_seen"]),
        "last_seen": _iso(r["last_seen"]),
    } for r in rows]


async def traffic_report(pool, window: str = "24h", now: datetime = None) -> dict:
    window = normalize_window(window)
    span, bucket_s = WINDOWS[window]
    now = now or datetime.now(timezone.utc)
    since = now - span
    out = {"window": window, "generated_at": _iso(now), "bucket_seconds": bucket_s}

    async with pool.acquire() as conn:
        t = await conn.fetchrow(
            """
            SELECT count(*) AS requests,
                   count(DISTINCT ip_hash) AS visitors,
                   count(*) FILTER (WHERE visitor IN ('member', 'account')) AS signed_in,
                   count(*) FILTER (WHERE automated) AS automated,
                   count(*) FILTER (WHERE status = 403) AS s403,
                   count(*) FILTER (WHERE status = 404) AS s404,
                   count(*) FILTER (WHERE status = 429) AS s429,
                   count(*) FILTER (WHERE status >= 500) AS s5xx,
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95
            FROM traffic_requests WHERE ts >= $1
            """, since)
        out["totals"] = {
            "requests": t["requests"],
            "visitors": t["visitors"],
            "signed_in_share": _pct(t["signed_in"], t["requests"]),
            "automated_share": _pct(t["automated"], t["requests"]),
            "s403": t["s403"], "s404": t["s404"], "s429": t["s429"], "s5xx": t["s5xx"],
            "p95_ms": None if t["p95"] is None else int(round(t["p95"])),
        }

        rows = await conn.fetch(
            """
            SELECT floor(extract(epoch FROM ts) / $2::int)::bigint AS b,
                   count(*) AS total,
                   count(*) FILTER (WHERE status BETWEEN 400 AND 499 AND status <> 429) AS client_errors,
                   count(*) FILTER (WHERE status = 429) AS throttled,
                   count(*) FILTER (WHERE status >= 500) AS server_errors
            FROM traffic_requests WHERE ts >= $1
            GROUP BY 1 ORDER BY 1
            """, since, bucket_s)
        by_bucket = {r["b"]: r for r in rows}
        first = int(since.timestamp() // bucket_s)
        last = int(now.timestamp() // bucket_s)
        series = []
        for b in range(first, last + 1):
            r = by_bucket.get(b)
            total = r["total"] if r else 0
            ce = r["client_errors"] if r else 0
            th = r["throttled"] if r else 0
            se = r["server_errors"] if r else 0
            series.append({
                "t": datetime.fromtimestamp(b * bucket_s, timezone.utc).isoformat(),
                "requests": total, "ok": total - ce - th - se,
                "client_errors": ce, "throttled": th, "server_errors": se,
            })
        out["series"] = series

        out["top_ips"] = _visitor_rows(await conn.fetch(_VISITOR_ROWS % "", since))
        out["top_signed_out"] = _visitor_rows(await conn.fetch(
            _VISITOR_ROWS % "AND visitor IN ('anon', 'guest')", since))

        rows = await conn.fetch(
            """
            SELECT user_agent, count(*) AS requests, count(DISTINCT ip_hash) AS ips,
                   bool_or(automated) AS automated
            FROM traffic_requests WHERE ts >= $1
            GROUP BY user_agent ORDER BY requests DESC LIMIT 20
            """, since)
        out["top_agents"] = [{
            "user_agent": r["user_agent"] or "", "requests": r["requests"],
            "ips": r["ips"], "automated": bool(r["automated"]),
        } for r in rows]

        rows = await conn.fetch(
            """
            SELECT COALESCE(route, path) AS page, count(*) AS requests,
                   count(*) FILTER (WHERE status BETWEEN 400 AND 499) AS s4xx,
                   avg(duration_ms) AS avg_ms
            FROM traffic_requests WHERE ts >= $1
            GROUP BY 1 ORDER BY requests DESC LIMIT 20
            """, since)
        out["top_paths"] = [{
            "page": r["page"] or "", "requests": r["requests"],
            "s4xx": r["s4xx"], "avg_ms": _f(r["avg_ms"], 0),
        } for r in rows]

        rows = await conn.fetch(
            """
            SELECT ts, ip_prefix, ip_hash, path, user_agent, visitor, status
            FROM traffic_requests
            WHERE status IN (403, 429) AND ts >= $1
            ORDER BY ts DESC LIMIT 50
            """, since)
        out["blocked"] = [{
            "ts": _iso(r["ts"]), "ip_prefix": r["ip_prefix"], "ip_hash": r["ip_hash"],
            "path": r["path"], "user_agent": r["user_agent"] or "",
            "visitor": r["visitor"], "status": r["status"],
        } for r in rows]

        rows = await conn.fetch(
            """
            SELECT h.hour, h.requests, m.median
            FROM traffic_hourly h
            CROSS JOIN LATERAL (
                SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY p.requests) AS median
                FROM traffic_hourly p
                WHERE p.hour < h.hour AND p.hour >= h.hour - interval '7 days'
            ) m
            WHERE h.hour >= $1 AND h.requests > 200 AND h.requests > 3 * m.median
            ORDER BY h.hour DESC
            """, now - timedelta(hours=48))
        out["spikes"] = [{
            "hour": _iso(r["hour"]), "requests": r["requests"],
            "typical": _f(r["median"], 0),
            "ratio": round(r["requests"] / float(r["median"]), 1) if r["median"] else None,
        } for r in rows]

        rows = await conn.fetch(
            """
            SELECT (hour AT TIME ZONE 'UTC')::date AS day,
                   sum(requests)::bigint AS requests, sum(automated)::bigint AS automated,
                   sum(s429)::bigint AS s429, sum(s5xx)::bigint AS s5xx,
                   sum(dropped)::bigint AS dropped
            FROM traffic_hourly WHERE hour >= $1
            GROUP BY 1 ORDER BY 1
            """, now - timedelta(days=90))
        out["long_term"] = [{
            "day": r["day"].isoformat(), "requests": r["requests"],
            "automated": r["automated"], "s429": r["s429"], "s5xx": r["s5xx"],
            "dropped": r["dropped"],
        } for r in rows]
    return out


async def ip_detail(pool, ip_hash: str):
    """Last 200 requests from one scrambled ID. Returns None for a malformed
    ID (the route answers 400)."""
    if not isinstance(ip_hash, str) or not HASH_RE.match(ip_hash):
        return None
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT ts, method, path, route, status, duration_ms, user_agent,
                   user_id, visitor, automated, ip_prefix
            FROM traffic_requests WHERE ip_hash = $1
            ORDER BY ts DESC LIMIT 200
            """, ip_hash)
    return {
        "ip_hash": ip_hash,
        "ip_prefix": rows[0]["ip_prefix"] if rows else None,
        "requests": [{
            "ts": _iso(r["ts"]), "method": r["method"], "path": r["path"],
            "route": r["route"], "status": r["status"], "duration_ms": r["duration_ms"],
            "user_agent": r["user_agent"] or "",
            "user_id": None if r["user_id"] is None else str(r["user_id"]),
            "visitor": r["visitor"], "automated": r["automated"],
        } for r in rows],
    }


def ip_check_info(request, chosen_ip: str) -> dict:
    """What this admin's own request looked like to the app, so the maintainer
    can see whether a client-sent X-Real-IP survives the hosting proxy. Shown
    to the admin only and never stored."""
    h = request.headers
    return {
        "x_real_ip": h.get("X-Real-IP"),
        "x_forwarded_for": h.get("X-Forwarded-For"),
        "cf_connecting_ip": h.get("CF-Connecting-IP"),
        "true_client_ip": h.get("True-Client-IP"),
        "socket_peer": request.client.host if request.client else None,
        "chosen": chosen_ip,
    }
