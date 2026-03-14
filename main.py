import os
import logging
import httpx
import asyncpg
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("dashboard")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)

app = FastAPI()

app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET"),
    max_age=60 * 60 * 24  # 24 hour session
)

templates = Jinja2Templates(directory="templates")

# ---- Config ----
DATABASE_URL          = os.getenv("DATABASE_URL")
DISCORD_CLIENT_ID     = os.getenv("DISCORD_CLIENT_ID")
DISCORD_CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET")
DISCORD_REDIRECT_URI  = os.getenv("DISCORD_REDIRECT_URI")
DISCORD_GUILD_ID      = os.getenv("DISCORD_GUILD_ID")
REQUIRED_ROLE_ID      = os.getenv("REQUIRED_ROLE_ID")
ADMIN_USER_IDS        = {
    int(uid) for uid in os.getenv("ADMIN_USER_IDS", "").split(",") if uid.strip()
}

DISCORD_API = "https://discord.com/api/v10"
DISCORD_OAUTH_URL = (
    f"https://discord.com/oauth2/authorize"
    f"?client_id={DISCORD_CLIENT_ID}"
    f"&redirect_uri={DISCORD_REDIRECT_URI}"
    f"&response_type=code"
    f"&scope=identify+guilds.members.read"
)

# ---- DB pool ----
@app.on_event("startup")
async def startup():
    app.state.db = await asyncpg.create_pool(DATABASE_URL)

@app.on_event("shutdown")
async def shutdown():
    await app.state.db.close()
# --- Public IPv4 address getter ---#
def get_real_ip(request: Request) -> str:
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        # X-Forwarded-For can be a comma-separated list if there are multiple proxies
        # The first one is the original client IP
        return forwarded_for.split(",")[0].strip()
    return request.client.host

# ---- Auth helpers ----
def get_current_user(request: Request):
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user

async def check_discord_role(access_token: str) -> tuple[bool, dict]:
    """Returns (has_role, user_info)"""
    async with httpx.AsyncClient() as client:
        user_resp = await client.get(
            f"{DISCORD_API}/users/@me",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        if user_resp.status_code != 200:
            return False, {}
        user = user_resp.json()

        member_resp = await client.get(
            f"{DISCORD_API}/users/@me/guilds/{DISCORD_GUILD_ID}/member",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        if member_resp.status_code != 200:
            return False, user

        member = member_resp.json()
        roles = member.get("roles", [])
        has_role = REQUIRED_ROLE_ID in roles

        return has_role, user

# ---- Routes ----

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/login")
    if not request.session.get("terms_accepted"):
        return RedirectResponse("/terms")
    return templates.TemplateResponse("index.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"]
    })

@app.get("/terms", response_class=HTMLResponse)
async def terms_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/login")
    if request.session.get("terms_accepted"):
        return RedirectResponse("/")
    from datetime import date
    return templates.TemplateResponse("terms.html", {
        "request": request,
        "current_date": date.today().strftime("%B %d, %Y")
    })

@app.post("/accept-terms")
async def accept_terms(request: Request):
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401)
    request.session["terms_accepted"] = True
    ip_address=get_real_ip(request)
    try:
        async with app.state.db.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO terms_acceptance (user_id, username, accepted_at, ip_address)
                VALUES ($1, $2, NOW(), $3)
                ON CONFLICT (user_id) DO UPDATE
                SET accepted_at = NOW(), ip_address = EXCLUDED.ip_address
                """,
                int(user["id"]),
                user["username"],
                ip_address
            )
        logger.info(f"Terms accepted: {user['username']} ({user['id']})")
    except Exception as e:
        logger.error(f"Failed to log terms acceptance: {e}")
    return JSONResponse({"ok": True})

@app.get("/login")
async def login():
    return RedirectResponse(DISCORD_OAUTH_URL)

@app.get("/callback")
async def callback(request: Request, code: str = None, error: str = None):
    if error or not code:
        return HTMLResponse("<h3>Access denied.</h3>", status_code=403)

    async with httpx.AsyncClient() as client:
        token_resp = await client.post(
            f"{DISCORD_API}/oauth2/token",
            data={
                "client_id": DISCORD_CLIENT_ID,
                "client_secret": DISCORD_CLIENT_SECRET,
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": DISCORD_REDIRECT_URI,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )

    if token_resp.status_code != 200:
        return HTMLResponse("<h3>OAuth failed. Try again.</h3>", status_code=500)

    tokens = token_resp.json()
    access_token = tokens.get("access_token")

    has_role, user = await check_discord_role(access_token)

    if not user:
        return HTMLResponse("<h3>Could not verify your Discord account.</h3>", status_code=403)

    if not has_role:
        return HTMLResponse(
            "<h3>Access denied.</h3><p>You need the required role in the server to view this dashboard.</p>",
            status_code=403
        )

    request.session["user"] = {
        "id": user["id"],
        "username": user["username"],
        "avatar": user.get("avatar")
    }
    ip_address=get_real_ip(request)
    # Log the session
    try:
        async with app.state.db.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO dashboard_sessions (user_id, username, ip_address)
                VALUES ($1, $2, $3)
                """,
                int(user["id"]),
                user["username"],
                ip_address
            )
        logger.info(f"Dashboard login: {user['username']} ({user['id']}) from {request.client.host}")
    except Exception as e:
        logger.error(f"Failed to log dashboard session: {e}")

    return RedirectResponse("/")

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login")

# ---- Admin ----

@app.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request, user=Depends(get_current_user)):
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Not authorized")
    return templates.TemplateResponse("admin.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"]
    })

@app.get("/admin/api")
async def admin_api(
    request: Request,
    limit: int = 100,
    user=Depends(get_current_user)
):
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Not authorized")

    if limit not in (50, 100, 250, 500):
        limit = 100

    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT user_id, username, logged_in_at, ip_address
            FROM dashboard_sessions
            ORDER BY logged_in_at DESC
            LIMIT $1
            """,
            limit
        )

    return JSONResponse([
        {
            "user_id": str(r["user_id"]),
            "username": r["username"],
            "logged_in_at": r["logged_in_at"].isoformat(),
            "ip_address": r["ip_address"]
        }
        for r in rows
    ])

# ---- Data API ----

@app.get("/api/restocks")
async def get_restocks(
    days: int = 7,
    request: Request = None,
    user=Depends(get_current_user)
):
    if days not in (7, 28, 56):
        raise HTTPException(status_code=400, detail="days must be 7, 28, or 56")

    eastern = ZoneInfo("America/New_York")
    now = datetime.now(eastern)
    since = now - timedelta(days=days)

    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                location,
                store_name,
                channel_name,
                date AT TIME ZONE 'America/New_York' AS local_date
            FROM restock_reports
            WHERE date >= $1
            AND (channel_name IS NULL OR channel_name NOT IN (
                'online-restock-information',
                'other-online-restocks',
                'pokemon-center-drops'
            ))
            ORDER BY date ASC
            """,
            since
        )

    channel_to_region = {
    "nova-restock-information":             "NOVA",
    "md-restock-information":               "MD",
    "dc-restock-information":               "DC",
    "rva-central-va-restock-information":   "RVA",  # ← was "rva-restock-information"
}

    def time_slot(dt):
        h = dt.hour
        if h < 12:
            return "Morning"
        elif h < 17:
            return "Afternoon"
        else:
            return "Evening"

    result = []
    for row in rows:
        local_dt = row["local_date"]
        result.append({
            "location": row["location"],
            "store":    row["store_name"],
            "region":   channel_to_region.get(row["channel_name"], "NOVA"),
            "date":     local_dt.strftime("%Y-%m-%d"),
            "datetime": local_dt.strftime("%b %d %I:%M %p"),
            "slot":     time_slot(local_dt),
        })

    return JSONResponse(result)

@app.get("/api/locations")
async def get_locations(
    request: Request,
    region: str = "NOVA",
    user=Depends(get_current_user)
):
    # Map dashboard region keys → database state values
    region_to_state = {
        "NOVA": "VA",
        "MD":   "MD",
        "DC":   "DC",
        "RVA":  "CVA",
    }
    state = region_to_state.get(region, "VA")

    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT location, store_type, location_link
            FROM locations
            WHERE state = $1
            ORDER BY store_type ASC, location ASC
            """,
            state
        )
    return JSONResponse([
        {
            "location": r["location"],
            "store":    r["store_type"],
            "link":     r["location_link"]
        }
        for r in rows
    ])