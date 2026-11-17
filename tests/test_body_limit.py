"""Standalone checks for the global request-body size limit (413, not 500).

    python3 tests/test_body_limit.py

No database, Discord or network calls. Runs the real app with the limit set to
1,000 bytes and posts bodies through the full middleware stack.
"""
import asyncio
import json
import os
import sys

os.environ.update({
    "SESSION_SECRET": "t" * 40,
    "HTTPS_ONLY": "false",
    "MAX_REQUEST_BODY_BYTES": "1000",
})
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
import main

LIMIT = 1000
CSRF = "test-csrf-token"
results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  [{detail}]"))


@main.app.post("/__body_probe")
async def body_probe(request: main.Request):
    return {"n": len(await request.body())}


@main.app.post("/__json_probe")
async def json_probe(payload: dict):
    return {"keys": len(payload)}


@main.app.post("/__swallow")
async def swallow(request: main.Request):
    try:
        await request.body()
    except Exception:
        return {"ok": "route hid the error"}
    return {"ok": "read"}


@main.app.post("/__boom")
async def boom(request: main.Request):
    await request.body()
    raise RuntimeError("unrelated failure")


def client():
    transport = httpx.ASGITransport(app=main.app, raise_app_exceptions=False)
    c = httpx.AsyncClient(transport=transport, base_url="http://test")
    c.cookies.set("csrf_token", CSRF)
    c.headers["X-CSRF-Token"] = CSRF
    return c


def is_clean_413(r):
    return (r.status_code == 413
            and r.headers.get("content-type", "").startswith("application/json")
            and r.json() == {"detail": main.REQUEST_TOO_LARGE_MESSAGE})


async def raw_asgi_post(path, chunks, headers):
    """Drive the app directly so the body can disagree with Content-Length."""
    sent, received = [], {"n": 0}
    queue = [{"type": "http.request", "body": c, "more_body": i < len(chunks) - 1}
             for i, c in enumerate(chunks)]

    async def receive():
        if received["n"] < len(queue):
            received["n"] += 1
            return queue[received["n"] - 1]
        await asyncio.sleep(3600)          # never disconnects

    async def send(message):
        sent.append(message)

    scope = {
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
        "method": "POST", "scheme": "http", "path": path, "raw_path": path.encode(),
        "root_path": "", "query_string": b"", "server": ("test", 80),
        "client": ("127.0.0.1", 1234),
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
    }
    await asyncio.wait_for(main.app(scope, receive, send), timeout=10)
    start = next(m for m in sent if m["type"] == "http.response.start")
    body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return start["status"], body, received["n"]


async def main_async():
    async with client() as c:
        r = await c.post("/__body_probe", content=b"x" * 10)
        check("small body passes", r.status_code == 200 and r.json() == {"n": 10}, r.text)

        r = await c.post("/__body_probe", content=b"x" * LIMIT)
        check("body exactly at the limit passes", r.status_code == 200, r.status_code)

        r = await c.post("/__body_probe", content=b"x" * (LIMIT + 1))
        check("one byte over the limit is a clean 413", is_clean_413(r), (r.status_code, r.text))

        r = await c.post("/__body_probe", content=b"x" * 50_000)
        check("large body is a clean 413", is_clean_413(r), (r.status_code, r.text))
        check("413 keeps nosniff", r.headers.get("x-content-type-options") == "nosniff")

        big_json = json.dumps({"image": "A" * 5000}).encode()
        r = await c.post("/__json_probe", content=big_json,
                         headers={"Content-Type": "application/json"})
        check("oversized JSON body on a typed route is a clean 413", is_clean_413(r),
              (r.status_code, r.text))

        async def chunked():
            for _ in range(10):
                yield b"x" * 500
        r = await c.post("/__body_probe", content=chunked())
        check("chunked body with no Content-Length is a clean 413", is_clean_413(r),
              (r.status_code, r.text, r.request.headers.get("content-length")))

        r = await c.post("/__swallow", content=b"x" * 5000)
        check("a route that catches the error still yields a 413", is_clean_413(r),
              (r.status_code, r.text))

        r = await c.post("/__boom", content=b"x")
        check("an unrelated error is still a 500", r.status_code == 500, r.status_code)

    # Content-Length claims 10 bytes; ten 500-byte chunks actually arrive.
    status, body, reads = await raw_asgi_post(
        "/__body_probe", [b"x" * 500] * 10,
        {"Content-Length": "10", "Cookie": f"csrf_token={CSRF}", "X-CSRF-Token": CSRF})
    check("a false Content-Length doesn't get past the limit", status == 413,
          (status, body))
    check("413 body is the plain-English message",
          json.loads(body) == {"detail": main.REQUEST_TOO_LARGE_MESSAGE}, body)
    check("reading stops once over the limit (not all chunks read)", reads <= 3, reads)


asyncio.run(main_async())
failed = [n for n, ok in results if not ok]
print(f"\n{len(results) - len(failed)}/{len(results)} passed")
sys.exit(1 if failed else 0)
