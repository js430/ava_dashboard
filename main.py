import os
import logging
import httpx
import asyncpg
import re
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
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")


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
    ip_address = get_real_ip(request)
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
    ip_address = get_real_ip(request)
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
    if days not in (7, 14, 21, 28, 35, 42, 49, 56):
        raise HTTPException(status_code=400, detail="Invalid days value")

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
        "nova-restock-information":           "NOVA",
        "md-restock-information":             "MD",
        "dc-restock-information":             "DC",
        "rva-central-va-restock-information": "RVA",
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

# ---- Preferences API ----

@app.get("/api/preferences")
async def get_preferences(
    request: Request,
    region: str = "NOVA",
    user=Depends(get_current_user)
):
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT selected_locations FROM user_preferences
            WHERE user_id = $1 AND region = $2
            """,
            int(user["id"]), region
        )
    if row is None:
        return JSONResponse({"found": False, "selected": []})
    return JSONResponse({"found": True, "selected": list(row["selected_locations"])})


@app.post("/api/preferences")
async def save_preferences(
    request: Request,
    user=Depends(get_current_user)
):
    body = await request.json()
    region = body.get("region", "NOVA")
    selected = body.get("selected", [])

    if not isinstance(selected, list):
        raise HTTPException(status_code=400, detail="Invalid payload")

    async with request.app.state.db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO user_preferences (user_id, region, selected_locations, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (user_id, region) DO UPDATE
            SET selected_locations = EXCLUDED.selected_locations,
                updated_at = NOW()
            """,
            int(user["id"]), region, selected
        )
    return JSONResponse({"ok": True})
# ---- Map page route ----#

@app.get("/map", response_class=HTMLResponse)
async def map_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/login")
    if not request.session.get("terms_accepted"):
        return RedirectResponse("/terms")
    is_admin = int(user["id"]) in ADMIN_USER_IDS
    return templates.TemplateResponse("map.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "is_admin": is_admin,
        "google_maps_api_key": GOOGLE_MAPS_API_KEY,
    })
 
 
# ---- Lat/lng extractor ----
 
def _extract_latlng(maps_url: str):
    """
    Pull lat/lng from a full Google Maps URL.
    Handles:
      - /maps/place/.../@38.123,-77.456,...
      - /maps?q=38.123,-77.456
    Returns (lat, lng) floats or (None, None).
    """
    if not maps_url:
        return None, None
    m = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+)", maps_url)
    if m:
        return float(m.group(1)), float(m.group(2))
    m = re.search(r"[?&]q=(-?\d+\.\d+),(-?\d+\.\d+)", maps_url)
    if m:
        return float(m.group(1)), float(m.group(2))
    return None, None
 
 
# ---- /api/map ----
 
@app.get("/api/map")
async def get_map_data(
    request: Request,
    region: str = "NOVA",
    window: str = "day",        # "day" = today only, "week" = last 7 days
    user=Depends(get_current_user)
):
    region_to_state = {
        "NOVA": "VA",
        "MD":   "MD",
        "DC":   "DC",
        "RVA":  "CVA",
    }
    state = region_to_state.get(region, "VA")
 
    eastern = ZoneInfo("America/New_York")
    now = datetime.now(eastern)
 
    if window == "week":
        since = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        # Today from midnight Eastern
        since = now.replace(hour=0, minute=0, second=0, microsecond=0)
 
    channel_to_region = {
        "nova-restock-information":           "NOVA",
        "md-restock-information":             "MD",
        "dc-restock-information":             "DC",
        "rva-central-va-restock-information": "RVA",
    }
 
    def time_slot(dt):
        h = dt.hour
        if h < 12:   return "Morning"
        elif h < 17: return "Afternoon"
        else:        return "Evening"
 
    async with request.app.state.db.acquire() as conn:
        locations = await conn.fetch(
            """
            SELECT location, store_type, location_link
            FROM locations
            WHERE state = $1
              AND location_link IS NOT NULL
              AND location_link <> ''
            ORDER BY store_type ASC, location ASC
            """,
            state
        )
 
        restocks = await conn.fetch(
            """
            SELECT
                location,
                channel_name,
                date AT TIME ZONE 'America/New_York' AS local_date
            FROM restock_reports
            WHERE date >= $1
              AND (channel_name IS NULL OR channel_name NOT IN (
                  'online-restock-information',
                  'other-online-restocks',
                  'pokemon-center-drops'
              ))
              AND store_name='Target'
            ORDER BY date ASC
            """,
            since
        )
 
    # Group restocks by location, filtered to the requested region
    restock_map: dict[str, list] = {}
    for r in restocks:
        row_region = channel_to_region.get(r["channel_name"], "NOVA")
        if row_region != region:
            continue
        loc_name = r["location"]
        local_dt = r["local_date"]
        if loc_name not in restock_map:
            restock_map[loc_name] = []
        restock_map[loc_name].append({
            "datetime": local_dt.strftime("%b %d %I:%M %p"),
            "slot": time_slot(local_dt),
        })
 
    result = []
    for loc in locations:
        lat, lng = _extract_latlng(loc["location_link"])
        if lat is None:
            continue
        result.append({
            "location": loc["location"],
            "store":    loc["store_type"],
            "link":     loc["location_link"],
            "lat":      lat,
            "lng":      lng,
            "restocks": restock_map.get(loc["location"], []),
        })
 
    return JSONResponse(result)


ACTIVE_INFORMANT_ROLE_ID = os.getenv("ACTIVE_INFORMANT_ROLE_ID", "")
 
# ---- Analytics page route ----
 
@app.get("/analytics", response_class=HTMLResponse)
async def analytics_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/login")
    if not request.session.get("terms_accepted"):
        return RedirectResponse("/terms")
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Not authorized")
    return templates.TemplateResponse("analytics.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
    })
 
 
# ---- /api/analytics ----
 
@app.get("/api/analytics")
async def get_analytics(
    request: Request,
    days: int = 30,
    user=Depends(get_current_user)
):
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Not authorized")
 
    if days not in (7, 30, 60, 90):
        days = 30
 
    eastern = ZoneInfo("America/New_York")
    now = datetime.now(eastern)
    since = now - timedelta(days=days)
 
    async with request.app.state.db.acquire() as conn:
 
        # --- Restock events in range ---
        restock_rows = await conn.fetch(
            """
            SELECT
                user_id,
                store_name,
                location,
                date AT TIME ZONE 'America/New_York' AS local_date,
                CASE WHEN store_name IN ('Costco', 'Sams Club') THEN 0.5 ELSE 1.0 END AS pts
            FROM restock_reports
            WHERE date >= $1
              AND channel_name NOT IN (
                  'online-restock-information',
                  'other-online-restocks',
                  'pokemon-center-drops'
              )
            ORDER BY date ASC
            """,
            since
        )
 
        # --- Empty ping events in range ---
        empty_rows = await conn.fetch(
            """
            SELECT
                user_id,
                location,
                timestamp AT TIME ZONE 'America/New_York' AS local_date,
                EXTRACT(DOW FROM timestamp AT TIME ZONE 'America/New_York') AS dow
            FROM command_logs
            WHERE command_used = 'empty'
              AND timestamp >= $1
            ORDER BY timestamp ASC
            """,
            since
        )
 
        # --- Plus one events in range ---
        plusone_rows = await conn.fetch(
            """
            SELECT
                receiver_id AS user_id,
                timestamp AT TIME ZONE 'America/New_York' AS local_date,
                value AS pts
            FROM plusones
            WHERE timestamp >= $1
            ORDER BY timestamp ASC
            """,
            since
        )
 
        # --- Username lookup ---
        user_name_rows = await conn.fetch(
            """
            SELECT DISTINCT user_id, username
            FROM dashboard_sessions
            ORDER BY user_id
            """
        )
 
    username_map = {r["user_id"]: r["username"] for r in user_name_rows}
 
    # Build date range list (strings)
    date_list = []
    d = since.date()
    while d <= now.date():
        date_list.append(d.isoformat())
        d += timedelta(days=1)
 
    # Collect all user_ids that appear in any event
    all_user_ids = set()
    for r in restock_rows:
        all_user_ids.add(r["user_id"])
    for r in empty_rows:
        all_user_ids.add(r["user_id"])
    for r in plusone_rows:
        all_user_ids.add(r["user_id"])
 
    # Build per-user per-day data structures
    # user_daily[uid][date_str] = {restock_pts, empty_pts, plusone_pts, total}
    from collections import defaultdict
 
    user_daily = {uid: {d: {"restock_pts": 0.0, "empty_pts": 0.0, "plusone_pts": 0.0, "total": 0.0}
                        for d in date_list}
                  for uid in all_user_ids}
 
    # Raw activity log per user for the detail table
    user_activity = defaultdict(list)
 
    # --- Process restocks ---
    for r in restock_rows:
        uid = r["user_id"]
        date_str = r["local_date"].date().isoformat()
        pts = float(r["pts"])
        if date_str in user_daily[uid]:
            user_daily[uid][date_str]["restock_pts"] += pts
            user_daily[uid][date_str]["total"] += pts
        user_activity[uid].append({
            "date": date_str,
            "type": "Restock",
            "store": r["store_name"],
            "location": r["location"],
            "points": pts,
            "flagged": False,
            "flag_reasons": [],
        })
 
    # --- Process empty pings ---
    for r in empty_rows:
        uid = r["user_id"]
        date_str = r["local_date"].date().isoformat()
        dow = int(r["dow"])  # 0=Sun, 6=Sat
        is_weekend = dow in (0, 6)
        pts = 0.05 if is_weekend else 0.1
        if date_str in user_daily.get(uid, {}):
            user_daily[uid][date_str]["empty_pts"] += pts
            user_daily[uid][date_str]["total"] += pts
        loc_parts = r["location"].split("|") if r["location"] else ["", ""]
        user_activity[uid].append({
            "date": date_str,
            "type": "Empty" + (" (wknd)" if is_weekend else ""),
            "store": loc_parts[1] if len(loc_parts) > 1 else "",
            "location": loc_parts[0] if loc_parts else "",
            "points": pts,
            "flagged": False,
            "flag_reasons": [],
        })
 
    # --- Process plusones ---
    for r in plusone_rows:
        uid = r["user_id"]
        date_str = r["local_date"].date().isoformat()
        pts = float(r["pts"])
        if date_str in user_daily.get(uid, {}):
            user_daily[uid][date_str]["plusone_pts"] += pts
            user_daily[uid][date_str]["total"] += pts
        user_activity[uid].append({
            "date": date_str,
            "type": "+1",
            "store": "",
            "location": "",
            "points": pts,
            "flagged": False,
            "flag_reasons": [],
        })
 
    # --- Anomaly detection per user ---
    def detect_flags(uid, daily, activity):
        daily_totals = [v["total"] for v in daily.values() if v["total"] > 0]
        avg = sum(daily_totals) / len(daily_totals) if daily_totals else 0
 
        # Spike: days where total is 3x the average
        spike_days = sum(1 for v in daily.values() if v["total"] >= max(avg * 3, 1.0))
 
        # Empty/restock ratio
        total_restocks = sum(v["restock_pts"] for v in daily.values())
        total_empty = sum(v["empty_pts"] for v in daily.values())
        restock_count = sum(
            1 for a in activity if a["type"] == "Restock"
        )
        empty_count = sum(
            1 for a in activity if a["type"].startswith("Empty")
        )
        empty_ratio = (empty_count / restock_count) if restock_count > 0 else (empty_count if empty_count > 0 else 0)
 
        # Repeat location: max times same location reported empty
        loc_counts = defaultdict(int)
        for a in activity:
            if a["type"].startswith("Empty") and a["location"]:
                loc_counts[a["location"]] += 1
        repeat_max = max(loc_counts.values()) if loc_counts else 0
 
        # Weekend % of empty pings
        weekend_empty = sum(1 for a in activity if a["type"] == "Empty (wknd)")
        weekend_pct = (weekend_empty / empty_count * 100) if empty_count > 0 else 0
 
        flags = {
            "spike": spike_days > 0,
            "ratio": empty_ratio > 5,
            "repeat": repeat_max >= 5,
            "weekend": weekend_pct >= 60,
            "spike_days": spike_days,
            "empty_ratio": empty_ratio,
            "repeat_max": repeat_max,
            "weekend_pct": weekend_pct,
        }
 
        # Mark individual activity rows
        spike_threshold = max(avg * 3, 1.0)
        daily_running = defaultdict(float)
        for a in activity:
            daily_running[a["date"]] += a["points"]
 
        for a in activity:
            reasons = []
            if daily_running[a["date"]] >= spike_threshold and a["type"] != "+1":
                reasons.append("spike day")
            if a["type"].startswith("Empty") and empty_ratio > 5:
                reasons.append("high ratio")
            if a["type"].startswith("Empty") and loc_counts.get(a["location"], 0) >= 5:
                reasons.append("repeat loc")
            if reasons:
                a["flagged"] = True
                a["flag_reasons"] = reasons
 
        return flags
 
    # --- Assemble final response ---
    users_out = []
    for uid in all_user_ids:
        daily = user_daily[uid]
        activity = sorted(user_activity[uid], key=lambda x: x["date"], reverse=True)
        flags = detect_flags(uid, daily, activity)
        total_pts = sum(v["total"] for v in daily.values())
 
        users_out.append({
            "user_id": str(uid),
            "username": username_map.get(uid, f"User {uid}"),
            "total_pts": total_pts,
            "daily": daily,
            "flags": flags,
            "activity": activity,
        })
 
    # Sort by total points descending
    users_out.sort(key=lambda x: x["total_pts"], reverse=True)
 
    return JSONResponse({
        "dates": date_list,
        "users": users_out,
    })
 