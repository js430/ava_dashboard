import os
import io
import math
import hmac
import asyncio
import hashlib
import secrets
import logging
import httpx
import asyncpg
import re
import json
import base64
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo
from collections import defaultdict
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from anthropic import AsyncAnthropic
from PIL import Image
from dotenv import load_dotenv

from card_tracker import (ensure_card_tracker_schema, sync_watchlist, run_ingest,
                          run_scoring, MAX_TRACKED_CARDS)
import card_scoring
import set_import
import price_sources
import grading_roi
import grading_tiers
import grading_sets

load_dotenv()

logger = logging.getLogger("dashboard")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)


class _RedactSecretsFilter(logging.Filter):
    """Strip API keys out of logged URLs.

    httpx logs every request line at INFO including the full query string, so
    a vendor that authenticates via a query param (?t=<token>, ?key=<key>)
    would otherwise write its own credential into the logs on every call. No
    currently-wired vendor does this (PPT and eBay both use an Authorization
    header), but it's cheap insurance against the next one that does.
    """
    _PATTERN = re.compile(r"([?&](?:t|token|key|api_key|apikey)=)[^&\s\"']+",
                          re.IGNORECASE)

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:
            return True
        if "=" in msg:
            redacted = self._PATTERN.sub(r"\1<redacted>", msg)
            if redacted != msg:
                record.msg = redacted
                record.args = ()
        return True


for _noisy in ("httpx", "httpcore"):
    logging.getLogger(_noisy).addFilter(_RedactSecretsFilter())

def get_real_ip(request: Request) -> str:
    """Real client IP behind Railway's proxy.

    Railway's edge sets X-Real-IP to the true client address (it matches the
    left-most X-Forwarded-For entry). Both are populated by the edge from the
    actual connection, so a client cannot forge them past it. Fall back to the
    left-most X-Forwarded-For entry, then the socket peer.
    """
    real_ip = request.headers.get("X-Real-IP")
    if real_ip and real_ip.strip():
        return real_ip.strip()
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        parts = [p.strip() for p in forwarded_for.split(",") if p.strip()]
        if parts:
            return parts[0]
    return request.client.host if request.client else "unknown"


# ---- Card tracker: daily scheduled ingest (11pm America/New_York) ----
# In-process asyncio loop rather than a separate Railway cron service or a
# new scheduler dependency — this app is a single long-lived web dyno, so a
# sleep-until-next-run loop started at startup is the lowest-friction option
# and needs nothing new in requirements.txt.
CARD_TRACKER_SCHEDULE_HOUR = 23    # 11pm, America/New_York
CARD_TRACKER_SCHEDULE_MINUTE = 0

def _seconds_until_next_tracker_run() -> float:
    eastern = ZoneInfo("America/New_York")
    now = datetime.now(eastern)
    target = now.replace(hour=CARD_TRACKER_SCHEDULE_HOUR, minute=CARD_TRACKER_SCHEDULE_MINUTE,
                         second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()

async def _card_tracker_daily_scheduler(app: FastAPI) -> None:
    while True:
        wait_s = _seconds_until_next_tracker_run()
        logger.info("Card tracker: next scheduled ingest in %.0f min (11pm America/New_York)",
                    wait_s / 60)
        await asyncio.sleep(wait_s)
        st = _tracker_refresh_state(app)
        if st["running"]:
            logger.warning("Card tracker: skipping scheduled ingest — a refresh "
                           "(manual or scheduled) is already running")
            continue
        st.update(running=True, started_at=datetime.utcnow().isoformat() + "Z",
                  finished_at=None, result=None, error=None)
        logger.info("Card tracker: starting scheduled daily ingest")
        await _run_tracker_refresh(app)


@asynccontextmanager
async def lifespan(app: FastAPI):
    secret = os.getenv("SESSION_SECRET", "")
    if not secret or len(secret) < 32:
        raise RuntimeError("SESSION_SECRET env var must be set and at least 32 characters long")
    app.state.db = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    try:
        await ensure_card_tracker_schema(app.state.db)
    except Exception:
        logger.exception("Card tracker schema ensure failed — /card-tracker may be unavailable")
    scheduler_task = asyncio.create_task(_card_tracker_daily_scheduler(app))
    try:
        yield
    finally:
        scheduler_task.cancel()
        await app.state.db.close()


app = FastAPI(lifespan=lifespan)

# ---- Rate limiter ----
# Key on the trusted client IP (not the spoofable left-most XFF entry).
limiter = Limiter(key_func=get_real_ip)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ---- Security headers ----
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
        response.headers.setdefault("Permissions-Policy", "geolocation=(), camera=(), microphone=()")
        response.headers.setdefault("Content-Security-Policy",
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://maps.googleapis.com https://maps.gstatic.com "
            "https://unpkg.com https://cdnjs.cloudflare.com; "
            "style-src 'self' 'unsafe-inline' https://unpkg.com; "
            "img-src 'self' data: blob: https://cdn.discordapp.com https://images.pokemontcg.io "
            "https://api.scryfall.com https://db.ygoprodeck.com https://www.optcgapi.com "
            "https://maps.googleapis.com https://maps.gstatic.com https://assets.pokemon.com; "
            "connect-src 'self' https://maps.googleapis.com; "
            "font-src 'self' data:; "
            "frame-ancestors 'none'; frame-src 'none'; object-src 'none'; base-uri 'self';"
        )
        return response

app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET"),
    max_age=60 * 60 * 24 * 7,   # 7 day session
    https_only=os.getenv("HTTPS_ONLY", "true").lower() != "false",
    same_site="lax",            # "lax" required for OAuth redirect flow
)

templates = Jinja2Templates(directory="templates")

# ---- Config ----
DATABASE_URL             = os.getenv("DATABASE_URL")
DISCORD_CLIENT_ID        = os.getenv("DISCORD_CLIENT_ID")
DISCORD_CLIENT_SECRET    = os.getenv("DISCORD_CLIENT_SECRET")
DISCORD_REDIRECT_URI     = os.getenv("DISCORD_REDIRECT_URI")
DISCORD_GUILD_ID         = os.getenv("DISCORD_GUILD_ID")
REQUIRED_ROLE_IDS        = {r.strip() for r in os.getenv("REQUIRED_ROLE_ID", "").split(",") if r.strip()}
DENY_ROLE_IDS            = {r.strip() for r in os.getenv("DENY_ROLE_IDS", "").split(",") if r.strip()}
# Server-mod role(s): may preview the /sample page even as full members.
MOD_ROLE_IDS             = {r.strip() for r in os.getenv("MOD_ROLE_IDS", "1406753334051737631").split(",") if r.strip()}
# all_mods: role-management group. A SEPARATE, broader set used only to open the
# invite-network page beyond admins. It is deliberately NOT MOD_ROLE_IDS — adding
# a role here does not grant any of the mod-only pages (those stay on MOD_ROLE_IDS).
ALL_MODS_ROLE_IDS        = {r.strip() for r in os.getenv("ALL_MODS_ROLE_IDS", "1481770294367748228,1406753334051737631").split(",") if r.strip()}
GOOGLE_MAPS_API_KEY      = os.getenv("GOOGLE_MAPS_API_KEY", "")
ANTHROPIC_API_KEY        = os.getenv("ANTHROPIC_API_KEY", "")
# Reused across requests instead of constructing a client per scan.
anthropic_client = AsyncAnthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None
ACTIVE_INFORMANT_ROLE_ID = os.getenv("ACTIVE_INFORMANT_ROLE_ID", "")
ADMIN_USER_IDS           = {
    int(uid) for uid in os.getenv("ADMIN_USER_IDS", "").split(",") if uid.strip()
}

# LOOKBACK_ROLE_WEEKS: comma-separated role_id:max_position pairs.
# Positions 1-8 = weeks, 9-12 = 3m/4m/5m/6m (91/120/150/180 days).
# Users get the highest tier that matches any of their roles. Default = 1.
LOOKBACK_ROLE_WEEKS: dict[str, int] = {}
for _pair in os.getenv("LOOKBACK_ROLE_WEEKS", "").split(","):
    _pair = _pair.strip()
    if ":" in _pair:
        _rid, _pos = _pair.split(":", 1)
        try:
            LOOKBACK_ROLE_WEEKS[_rid.strip()] = int(_pos.strip())
        except ValueError:
            pass

STATE_LABELS = {
    "VA":   "NOVA",
    "CVA":  "CVA",
    "DC":   "DC",
    "WMD":  "Western MD",
    "CMD":  "Central MD",
    "SEMD": "South-Eastern MD",
    "Charm":"Charm MD",
    "TW":   "Tidewater",
    "WVA":  "Western VA",
}
VALID_REGIONS = frozenset(STATE_LABELS.keys())

# ---- Card Scanner Constants ----
MAX_IMAGE_BYTES = 3 * 1024 * 1024 + 500_000  # ~3.5 MB raw
MAX_DIMENSION   = 2048

EXTRACT_PROMPT = (
    "You are reading a trading card game card image. Extract the following and return "
    "ONLY a valid JSON object with no extra text, markdown, or code fences:\n\n"
    "{\n"
    '  "game": "one of: pokemon, magic, yugioh, one piece, other",\n'
    '  "name": "the card name exactly as printed — see formatting rules below",\n'
    '  "number": "the card number as printed (e.g. 045, OP01-001), without the set total — null if not visible",\n'
    '  "set": "the set or expansion name if visible, otherwise null",\n'
    '  "is_sp": false,\n'
    '  "is_manga": false,\n'
    '  "is_alt_art": false,\n'
    '  "promo_stamp": null\n'
    "}\n\n"
    "For the game field: use 'magic' for Magic: The Gathering, 'yugioh' for Yu-Gi-Oh!, "
    "'one piece' for One Piece TCG, 'pokemon' for Pokemon TCG, or 'other' for anything else.\n\n"
    "promo_stamp: if the card artwork has a retailer or event stamp printed over it "
    "(e.g. 'GameStop', 'Target', 'Walmart', 'Best Buy', 'PAX', 'Pokemon Center'), set this "
    "to the stamp name as a string. Otherwise null. "
    "IMPORTANT: ignore any such stamp when reading the card name, number, set, and game — "
    "the stamp is not part of the card identity.\n\n"
    "=== ONE PIECE VARIANT DETECTION (set the relevant fields to true) ===\n\n"
    "is_sp — Special Parallel. True if ANY of these are visible on the card:\n"
    "  • A small 'SP' box or label printed to the left of the card number at the bottom (e.g. 'SP EB03-053').\n"
    "  • A circular stamp near the top-right corner containing the kanji '特' and/or the word 'SPECIAL'.\n"
    "The card will have normal full-color artwork. Either indicator alone is sufficient.\n\n"
    "is_manga — Manga Art. True if the card artwork is composed of manga comic panels, "
    "including speech bubbles, panel borders, screentone shading, or manga page layouts. "
    "The artwork looks like pages from the One Piece manga rather than a standard card illustration.\n\n"
    "is_alt_art — Alternate Art. True if there is a ★ (star) symbol printed above or next to "
    "the rarity designation in the bottom-right corner of the card.\n\n"
    "These three variants are independent — a card can be manga only, alt art only, SP only, or any combination.\n\n"
    "=== POKEMON NAME FORMATTING ===\n"
    "- GX cards: always use a hyphen → 'Mewtwo-GX', 'Charizard-GX'\n"
    "- EX cards: always use a hyphen → 'Charizard-EX', 'Darkrai-EX'\n"
    "- Mega/M EX cards: format as 'M [Name]-EX' → 'M Charizard-EX', 'M Rayquaza-EX'\n"
    "- V / VMAX / VStar / VUnion cards: always use a hyphen → 'Charizard-V', 'Charizard-VMAX'\n"
    "- ex (lowercase, modern era): no hyphen, lowercase → 'Charizard ex'\n\n"
    "The card number is usually at the bottom in a format like '5/62' or '192/165' or 'OP01-001'.\n"
    "For Pokemon cards, return the FULL number as printed including the set total (e.g. '5/62', '192/165'). "
    "Do NOT strip the slash or the total — both parts are needed to identify the exact set.\n\n"
    "=== POKEMON PROMO CARDS ===\n"
    "Some Pokemon cards are promos. You can tell because:\n"
    "  • A ★ (black star) symbol appears in the bottom-left OR bottom-right corner next to the card number.\n"
    "  • The card number uses a prefix-number format: 'SWSH260', 'XY77', 'XY-P123', 'SM-P456', 'SV-P789', 'BW-P45'.\n"
    "  • There is NEVER a slash in a promo number. 'XY77' is correct. 'XY/77' is WRONG. '77' alone is WRONG.\n"
    "IMPORTANT: Return the full promo number exactly as printed — prefix AND digits together (e.g. 'XY77', 'SWSH260'). "
    "Do NOT split it into a fraction. Do NOT drop the prefix. "
    "The ★ symbol and regulation mark letter (e.g. 'F', 'D', 'E') near the number are NOT part of the number — "
    "ignore them when reading the number field.\n\n"
    "For One Piece cards, if 'SP' appears immediately before the card number (e.g. 'SP EB03-053'), include the SP prefix "
    "in the number field exactly as printed (e.g. number: 'SP EB03-053') AND set is_sp to true."
)


def _compress_card_image(data: bytes) -> tuple[bytes, str]:
    img = Image.open(io.BytesIO(data)).convert("RGB")
    w, h = img.size
    if w > MAX_DIMENSION or h > MAX_DIMENSION:
        img.thumbnail((MAX_DIMENSION, MAX_DIMENSION), Image.LANCZOS)
    quality = 85
    while quality >= 30:
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=quality, optimize=True)
        compressed = buf.getvalue()
        if len(compressed) <= MAX_IMAGE_BYTES:
            return compressed, "image/jpeg"
        quality -= 10
    return compressed, "image/jpeg"


async def _claude_identify(client: AsyncAnthropic, image_bytes: bytes, media_type: str) -> dict:
    import asyncio
    if len(image_bytes) > MAX_IMAGE_BYTES:
        image_bytes, media_type = _compress_card_image(image_bytes)
    b64_image = base64.standard_b64encode(image_bytes).decode("utf-8")

    last_err = None
    for attempt in range(3):
        try:
            response = await client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=256,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": b64_image}},
                        {"type": "text", "text": EXTRACT_PROMPT},
                    ],
                }],
            )
            break
        except Exception as e:
            last_err = e
            if "overloaded" in str(e).lower() or "529" in str(e):
                logger.warning(f"Claude API overloaded, retry {attempt + 1}/3")
                await asyncio.sleep(2 ** attempt)
            else:
                raise
    else:
        raise last_err

    text = response.content[0].text.strip()
    if text.startswith("```"):
        parts = text.split("```")
        text = parts[1].lstrip("json").strip() if len(parts) > 1 else text
    return json.loads(text)


DISCORD_API = "https://discord.com/api/v10"
# Base OAuth URL — state is appended dynamically in /login to prevent CSRF
DISCORD_OAUTH_BASE_URL = (
    f"https://discord.com/oauth2/authorize"
    f"?client_id={DISCORD_CLIENT_ID}"
    f"&redirect_uri={DISCORD_REDIRECT_URI}"
    f"&response_type=code"
    f"&scope=identify+guilds.members.read"
)

# Scan endpoint — allowed image MIME types for Claude Vision
ALLOWED_MEDIA_TYPES = frozenset({"image/jpeg", "image/png", "image/gif", "image/webp"})

# ---- OAuth state helpers (stateless HMAC — no session cookie required) ----
# This avoids browser SameSite/ITP issues where the session cookie is not sent
# after a cross-site OAuth redirect round-trip.
def _oauth_secret() -> bytes:
    return os.getenv("SESSION_SECRET", "").encode()

def make_oauth_state() -> str:
    """Return a signed state token: '<nonce>.<hmac-sha256-hex>'."""
    nonce = secrets.token_hex(24)
    sig = hmac.new(_oauth_secret(), nonce.encode(), hashlib.sha256).hexdigest()
    return f"{nonce}.{sig}"

def verify_oauth_state(state: str | None) -> bool:
    """Return True iff *state* carries a valid HMAC signature."""
    if not state or "." not in state:
        return False
    try:
        nonce, sig = state.rsplit(".", 1)
        expected = hmac.new(_oauth_secret(), nonce.encode(), hashlib.sha256).hexdigest()
        return secrets.compare_digest(sig, expected)
    except Exception:
        return False

# ---- Helpers ----
def is_demo(request: Request) -> bool:
    """True if the session belongs to a role-less 'sample' viewer. Missing flag
    (pre-existing member sessions) defaults to full access."""
    return request.session.get("member", True) is False

def get_current_user(request: Request):
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    # Sample/demo viewers never reach live-data endpoints.
    if is_demo(request):
        raise HTTPException(status_code=403, detail="Live data requires the member role.")
    return user

async def terms_current(request: Request, user: dict) -> bool:
    """Return True if user accepted terms within the last 30 days."""
    if request.session.get("terms_accepted"):
        return True
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT accepted_at FROM terms_acceptance WHERE user_id = $1",
            int(user["id"])
        )
    if row and row["accepted_at"] > datetime.now(ZoneInfo("UTC")) - timedelta(days=30):
        request.session["terms_accepted"] = True
        return True
    return False

def _get_max_position(roles: list[str]) -> int:
    """Return the max slider position (1-12) for a user based on their Discord roles.
    Values above 12 are treated as full access (12). Use any value >= 12 in
    LOOKBACK_ROLE_WEEKS to mean 'unlimited / full history'."""
    best = 1  # default minimum for anyone who passes the access check
    for role_id in roles:
        if role_id in LOOKBACK_ROLE_WEEKS:
            best = max(best, LOOKBACK_ROLE_WEEKS[role_id])
    return min(best, 12)  # clamp to valid slider range



async def check_discord_role(access_token: str) -> tuple[bool, dict, list[str]]:
    """Returns (has_role, user_info, member_roles)"""
    async with httpx.AsyncClient() as client:
        user_resp = await client.get(
            f"{DISCORD_API}/users/@me",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        if user_resp.status_code != 200:
            return False, {}, []
        user = user_resp.json()

        member_resp = await client.get(
            f"{DISCORD_API}/users/@me/guilds/{DISCORD_GUILD_ID}/member",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        if member_resp.status_code != 200:
            return False, user, []

        member = member_resp.json()
        roles = member.get("roles", [])
        # Deny list takes priority — blocked even if they have an allowed role
        if DENY_ROLE_IDS & set(roles):
            return False, user, roles
        has_role = bool(REQUIRED_ROLE_IDS & set(roles))

        return has_role, user, roles

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

# ---- Routes ----

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/login")
    if is_demo(request):
        return RedirectResponse("/sample")
    if not await terms_current(request, user):
        return RedirectResponse("/terms")
    is_admin = int(user["id"]) in ADMIN_USER_IDS
    max_position = 12 if is_admin else request.session.get("max_position", 1)
    return templates.TemplateResponse("index.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "is_admin": is_admin,
        "is_mod": request.session.get("mod", False),
        "max_position": max_position,
    })

@app.get("/sample", response_class=HTMLResponse)
async def sample_page(request: Request):
    """Demo dashboard with fake data for authenticated users who lack the
    member role. Serves no live data — all numbers are generated client-side."""
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/login")
    # Non-premium users see the sample; admins and server mods may also preview
    # it. Regular premium members have no reason to, so send them to the live board.
    is_staff = int(user["id"]) in ADMIN_USER_IDS or request.session.get("mod", False)
    if not is_demo(request) and not is_staff:
        return RedirectResponse("/")
    return templates.TemplateResponse("sample.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "upgrade_url": os.getenv("DEMO_UPGRADE_URL", "https://discord.com/channels/1406738815854317658/1502114854335549470"),
    })

@app.get("/sample-status", response_class=HTMLResponse)
async def sample_status_page(request: Request):
    """Demo store-status page with fake data. Same audience as /sample."""
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/login")
    is_staff = int(user["id"]) in ADMIN_USER_IDS or request.session.get("mod", False)
    if not is_demo(request) and not is_staff:
        return RedirectResponse("/status")
    return templates.TemplateResponse("sample_status.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "upgrade_url": os.getenv("DEMO_UPGRADE_URL", "https://discord.com/channels/1406738815854317658/1502114854335549470"),
    })

@app.get("/sample-map", response_class=HTMLResponse)
async def sample_map_page(request: Request):
    """Demo map page with fake data and a self-contained mock map (no Google
    Maps API). Same audience as /sample."""
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/login")
    is_staff = int(user["id"]) in ADMIN_USER_IDS or request.session.get("mod", False)
    if not is_demo(request) and not is_staff:
        return RedirectResponse("/map")
    return templates.TemplateResponse("sample_map.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "upgrade_url": os.getenv("DEMO_UPGRADE_URL", "https://discord.com/channels/1406738815854317658/1502114854335549470"),
    })

@app.get("/terms", response_class=HTMLResponse)
async def terms_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/login")
    if await terms_current(request, user):
        return RedirectResponse("/")
    from datetime import date
    return templates.TemplateResponse("terms.html", {
        "request": request,
        "current_date": date.today().strftime("%B %d, %Y")
    })

@app.post("/accept-terms")
@limiter.limit("10/minute")
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
async def login(request: Request):
    state = make_oauth_state()
    return RedirectResponse(DISCORD_OAUTH_BASE_URL + f"&state={state}")

@app.get("/callback")
@limiter.limit("30/minute")
async def callback(request: Request, code: str = None, error: str = None, state: str = None):
    if error or not code:
        return HTMLResponse("<h3>Access denied.</h3>", status_code=403)

    # Validate OAuth state parameter to prevent CSRF.
    # Uses a stateless HMAC signature so no session cookie is required —
    # avoids browser SameSite/ITP issues with the cross-site OAuth round-trip.
    if not verify_oauth_state(state):
        return HTMLResponse("<h3>Invalid session state. Please try logging in again.</h3>", status_code=400)

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

    has_role, user, member_roles = await check_discord_role(access_token)

    if not user:
        return HTMLResponse("<h3>Could not verify your Discord account.</h3>", status_code=403)

    is_admin_user = int(user["id"]) in ADMIN_USER_IDS
    is_member = bool(has_role) or is_admin_user

    # Users explicitly on the deny list are hard-blocked — no sample access.
    if not is_member and (DENY_ROLE_IDS & set(member_roles)):
        return HTMLResponse(
            "<h3>Access denied.</h3><p>Your access to this dashboard has been restricted.</p>",
            status_code=403
        )

    request.session["user"] = {
        "id": user["id"],
        "username": user["username"],
        "avatar": user.get("avatar")
    }
    request.session["member"] = is_member
    request.session["mod"] = bool(MOD_ROLE_IDS & set(member_roles)) or is_admin_user
    request.session["all_mods"] = bool(ALL_MODS_ROLE_IDS & set(member_roles)) or is_admin_user
    if is_member:
        request.session["max_position"] = _get_max_position(member_roles)

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
        logger.info(f"Dashboard login: {user['username']} ({user['id']}) from {ip_address} "
                    f"[{'member' if is_member else 'sample'}]")
    except Exception as e:
        logger.error(f"Failed to log dashboard session: {e}")

    return RedirectResponse("/" if is_member else "/sample")

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
        "user_id": user["id"],
        "is_admin": True,
        "is_mod": request.session.get("mod", False),
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

# ---- Raffle Wheel (admin-only) ----
# The raffles / raffle_entries tables are owned by the bot, so the table names
# and columns are discovered at runtime from information_schema rather than
# hard-coded. Only names matched against these fixed allowlists are ever
# interpolated into SQL, so this is not an injection vector.
_RAFFLE_TABLES    = ("raffles", "raffle")
_ENTRY_TABLES     = ("raffle_entries", "raffle_entry")
_RAFFLE_ID_COLS   = ("id", "raffle_id", "message_id", "uuid")
_RAFFLE_NAME_COLS = ("name", "title", "raffle_name", "prize", "description", "label")
_ENTRY_FK_COLS    = ("raffle_id", "raffle", "message_id", "raffle_uuid", "raffle_fk")
_ENTRY_USER_COLS  = ("user_id", "userid", "discord_id", "member_id")
_ENTRY_NAME_COLS  = ("username", "display_name", "displayname", "user_name")
_ENTRY_COUNT_COLS = ("entries", "num_entries", "entry_count", "tickets", "ticket_count", "count", "quantity")
_ENTRY_PK_COLS    = ("id", "entry_id", "raffle_entry_id")
# Tables to resolve a user_id -> display name when the entries table has none.
_NAME_SOURCES = (
    "SELECT user_id, username FROM users WHERE user_id = ANY($1::bigint[])",
    "SELECT DISTINCT ON (user_id) user_id, username FROM dashboard_sessions "
    "WHERE user_id = ANY($1::bigint[]) ORDER BY user_id, logged_in_at DESC",
    "SELECT DISTINCT ON (user_id) user_id, username FROM member_joins WHERE user_id = ANY($1::bigint[])",
)

MAX_WHEEL_TICKETS = 20000  # guard against pathological payloads

async def _table_columns(conn, table: str) -> set[str]:
    rows = await conn.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name = $1",
        table,
    )
    return {r["column_name"] for r in rows}

async def _find_table(conn, candidates: tuple[str, ...]) -> str | None:
    rows = await conn.fetch(
        "SELECT table_name FROM information_schema.tables WHERE table_name = ANY($1)",
        list(candidates),
    )
    found = {r["table_name"] for r in rows}
    for c in candidates:
        if c in found:
            return c
    return None

def _pick_col(cols: set[str], candidates: tuple[str, ...]) -> str | None:
    lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand in lower:
            return lower[cand]
    return None

async def _resolve_usernames(conn, uids) -> dict[str, str]:
    """Map user_id (str) -> display name, drawing from users / dashboard_sessions
    / member_joins. Each source is best-effort so a missing table is ignored."""
    ints: list[int] = []
    for u in uids:
        try:
            ints.append(int(u))
        except (TypeError, ValueError):
            pass
    if not ints:
        return {}
    names: dict[str, str] = {}
    for query in _NAME_SOURCES:
        missing = [i for i in ints if str(i) not in names]
        if not missing:
            break
        try:
            rows = await conn.fetch(query, missing)
        except Exception:
            continue
        for r in rows:
            if r["username"]:
                names[str(r["user_id"])] = r["username"]
    return names

async def _raffle_schema(conn) -> dict:
    rtable = await _find_table(conn, _RAFFLE_TABLES)
    etable = await _find_table(conn, _ENTRY_TABLES)
    if not rtable or not etable:
        raise HTTPException(status_code=500, detail="raffles / raffle_entries tables not found")
    rcols = await _table_columns(conn, rtable)
    ecols = await _table_columns(conn, etable)
    return {
        "rtable": rtable,
        "etable": etable,
        "rid":    _pick_col(rcols, _RAFFLE_ID_COLS) or "message_id",
        "rname":  _pick_col(rcols, _RAFFLE_NAME_COLS),
        "efk":    _pick_col(ecols, _ENTRY_FK_COLS) or "message_id",
        "euid":   _pick_col(ecols, _ENTRY_USER_COLS) or "user_id",
        "ename":  _pick_col(ecols, _ENTRY_NAME_COLS),
        "ecount": _pick_col(ecols, _ENTRY_COUNT_COLS),
        "epk":    _pick_col(ecols, _ENTRY_PK_COLS),
    }

async def _load_raffle_entries(conn, raffle_id: str, with_names: bool = True) -> tuple[dict, list[dict], list[dict]]:
    """Return (raffle_meta, users, tickets) for a raffle, normalized so the
    front-end is schema-agnostic. Each ticket is one elimination unit.
    Pass with_names=False to skip the display-name lookup (e.g. on spins)."""
    s = await _raffle_schema(conn)
    rtable, etable = s["rtable"], s["etable"]
    rid, rname = s["rid"], s["rname"]
    efk, euid, ename, ecount, epk = s["efk"], s["euid"], s["ename"], s["ecount"], s["epk"]

    name_sel = f'r."{rname}"' if rname else "NULL"
    raffle_row = await conn.fetchrow(
        f'SELECT r."{rid}" AS id, {name_sel} AS name FROM "{rtable}" r WHERE r."{rid}"::text = $1',
        raffle_id,
    )
    if raffle_row is None:
        raise HTTPException(status_code=404, detail="Raffle not found")
    raffle_meta = {
        "id": str(raffle_row["id"]),
        "name": raffle_row["name"] or f"Raffle #{raffle_row['id']}",
    }

    name_col = f'e."{ename}"' if ename else "NULL"
    order_by = f' ORDER BY e."{euid}"' + (f', e."{epk}"' if epk else "")
    rows = await conn.fetch(
        f'SELECT e."{euid}" AS uid, {name_col} AS uname'
        + (f', e."{ecount}" AS cnt' if ecount else "")
        + (f', e."{epk}" AS pk' if epk else "")
        + f' FROM "{etable}" e WHERE e."{efk}"::text = $1'
        + order_by,
        raffle_id,
    )

    # Resolve display names from auxiliary tables when the entry table has none.
    name_map: dict[str, str] = {}
    if with_names and not ename:
        name_map = await _resolve_usernames(conn, {str(r["uid"]) for r in rows})

    users: dict[str, dict] = {}
    tickets: list[dict] = []
    for row in rows:
        uid = str(row["uid"])
        uname = (row["uname"] if ename else None) or name_map.get(uid) or f"User {uid}"
        if ecount:
            # One row per user with a count column.
            try:
                n = int(row["cnt"] or 0)
            except (TypeError, ValueError):
                n = 0
            n = max(0, n)
            u = users.setdefault(uid, {"user_id": uid, "username": uname, "entries": 0})
            base = u["entries"]
            for i in range(n):
                tickets.append({"id": f"{uid}#{base + i}", "user_id": uid, "username": uname})
            u["entries"] += n
        else:
            # One row per ticket.
            tid = str(row["pk"]) if epk and row["pk"] is not None else f"{uid}#{len(tickets)}"
            tickets.append({"id": tid, "user_id": uid, "username": uname})
            u = users.setdefault(uid, {"user_id": uid, "username": uname, "entries": 0})
            u["entries"] += 1

    users_list = sorted(users.values(), key=lambda u: (-u["entries"], u["username"].lower()))
    return raffle_meta, users_list, tickets


@app.get("/raffle-wheel", response_class=HTMLResponse)
async def raffle_wheel_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/login")
    if not await terms_current(request, user):
        return RedirectResponse("/terms")
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Not authorized")
    return templates.TemplateResponse("raffle_wheel.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "is_admin": True,
        "is_mod": request.session.get("mod", False),
    })

@app.get("/api/raffle-wheel/raffles")
async def api_raffle_list(request: Request, user=Depends(get_current_user)):
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Not authorized")
    async with request.app.state.db.acquire() as conn:
        s = await _raffle_schema(conn)
        rtable, etable = s["rtable"], s["etable"]
        rid, rname = s["rid"], s["rname"]
        efk, euid, ecount = s["efk"], s["euid"], s["ecount"]
        name_sel = f'r."{rname}"' if rname else "NULL"
        raffles = await conn.fetch(
            f'SELECT r."{rid}" AS id, {name_sel} AS name FROM "{rtable}" r ORDER BY r."{rid}" DESC'
        )
        entry_expr = f'COALESCE(SUM(e."{ecount}"),0)' if ecount else "COUNT(*)"
        agg = await conn.fetch(
            f'SELECT e."{efk}"::text AS rid, {entry_expr} AS entries,'
            f' COUNT(DISTINCT e."{euid}") AS users FROM "{etable}" e GROUP BY e."{efk}"'
        )
    agg_map = {a["rid"]: a for a in agg}
    out = []
    for r in raffles:
        key = str(r["id"])
        a = agg_map.get(key)
        out.append({
            "id": key,
            "name": r["name"] or f"Raffle #{r['id']}",
            "entries": int(a["entries"]) if a else 0,
            "users": int(a["users"]) if a else 0,
        })
    return JSONResponse(out, headers={"Cache-Control": "no-store"})

@app.get("/api/raffle-wheel/entries")
async def api_raffle_entries(request: Request, raffle_id: str, user=Depends(get_current_user)):
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Not authorized")
    async with request.app.state.db.acquire() as conn:
        raffle_meta, users_list, tickets = await _load_raffle_entries(conn, raffle_id)
    if len(tickets) > MAX_WHEEL_TICKETS:
        raise HTTPException(status_code=413, detail="Too many entries to run the wheel")
    return JSONResponse({
        "raffle": raffle_meta,
        "users": users_list,
        "tickets": tickets,
        "total_entries": len(tickets),
        "total_users": len(users_list),
    }, headers={"Cache-Control": "no-store"})

@app.post("/api/raffle-wheel/spin")
@limiter.limit("120/minute")
async def api_raffle_spin(request: Request, user=Depends(get_current_user)):
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Not authorized")
    body = await request.json()
    raffle_id = body.get("raffle_id")
    remaining = body.get("remaining")
    if not raffle_id or not isinstance(remaining, list) or not remaining:
        raise HTTPException(status_code=400, detail="raffle_id and a non-empty remaining list are required")
    if len(remaining) > MAX_WHEEL_TICKETS:
        raise HTTPException(status_code=413, detail="Too many entries")

    # Rebuild the authoritative ticket set and validate the submitted remaining
    # set is a genuine subset — the client cannot invent or duplicate tickets.
    async with request.app.state.db.acquire() as conn:
        _, _, tickets = await _load_raffle_entries(conn, str(raffle_id), with_names=False)
    owner = {t["id"]: t for t in tickets}
    seen = set()
    for tid in remaining:
        if not isinstance(tid, str) or tid not in owner or tid in seen:
            raise HTTPException(status_code=400, detail="Invalid remaining ticket set")
        seen.add(tid)

    if len(remaining) == 1:
        winner = owner[remaining[0]]
        return JSONResponse({"winner": True, "ticket": remaining[0],
                             "user_id": winner["user_id"], "username": winner["username"]},
                            headers={"Cache-Control": "no-store"})

    # Cryptographically secure, unbiased uniform selection over remaining tickets.
    idx = secrets.randbelow(len(remaining))
    eliminated = remaining[idx]
    info = owner[eliminated]
    return JSONResponse({
        "winner": False,
        "eliminated": eliminated,
        "index": idx,
        "user_id": info["user_id"],
        "username": info["username"],
        "remaining_after": len(remaining) - 1,
    }, headers={"Cache-Control": "no-store"})

# ---- Card Tracker (staff: admins + server/regional mods via MOD_ROLE_IDS) ----

def require_staff(request: Request) -> dict:
    """Session gate for the card tracker: admins or mods (MOD_ROLE_IDS flag).
    Deliberately does NOT require the premium member role — a regional mod
    without premium still gets tracker access, but demo users do not."""
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if int(user["id"]) not in ADMIN_USER_IDS and not request.session.get("mod", False):
        raise HTTPException(status_code=403, detail="Not authorized")
    return user

def require_admin(request: Request) -> dict:
    """Admin-only session gate (ADMIN_USER_IDS). Used by the card tracker,
    which is admin-only — mods no longer have tracker access."""
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Not authorized")
    return user

@app.get("/card-tracker", response_class=HTMLResponse)
async def card_tracker_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/login")
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Not authorized")
    return templates.TemplateResponse("card_tracker.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "is_admin": True,
        "is_mod": request.session.get("mod", False),
    })

@app.get("/api/card-tracker/list")
async def api_card_tracker_list(request: Request, user=Depends(require_admin)):
    def _f(x):
        return float(x) if x is not None else None
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT tc.id, tc.name, tc.game, tc.set_name, tc.card_number, tc.variant,
                   tc.release_date, tc.justtcg_name, tc.justtcg_set, tc.justtcg_number,
                   ps.price_low, ps.price_mid, ps.price_high, ps.captured_at,
                   cs.momentum_7d, cs.momentum_30d, cs.liquidity_score,
                   cs.age_days, cs.potential_score, cs.computed_at
            FROM tracked_cards tc
            LEFT JOIN LATERAL (
                SELECT * FROM price_snapshots WHERE card_id = tc.id
                ORDER BY captured_at DESC LIMIT 1
            ) ps ON true
            LEFT JOIN LATERAL (
                SELECT * FROM card_scores WHERE card_id = tc.id
                ORDER BY computed_at DESC LIMIT 1
            ) cs ON true
            ORDER BY cs.potential_score DESC NULLS LAST, tc.name ASC
            """
        )
    def _canon_ident(s):
        return re.sub(r"[^a-z0-9]", "", (s or "").lower())
    return JSONResponse([
        {
            "id": r["id"],
            "name": r["name"],
            "game": r["game"],
            "set_name": r["set_name"],
            "card_number": r["card_number"],
            "variant": r["variant"],
            "justtcg_name": r["justtcg_name"],
            "justtcg_set": r["justtcg_set"],
            "justtcg_number": r["justtcg_number"],
            # Flag likely wrong matches: JustTCG's name differs from ours.
            "match_suspect": bool(r["justtcg_name"]) and
                             _canon_ident(r["justtcg_name"]) != _canon_ident(r["name"]),
            "release_date": r["release_date"].isoformat() if r["release_date"] else None,
            "price_low": _f(r["price_low"]),
            "price_mid": _f(r["price_mid"]),
            "price_high": _f(r["price_high"]),
            "captured_at": r["captured_at"].isoformat() if r["captured_at"] else None,
            "momentum_7d": _f(r["momentum_7d"]),
            "momentum_30d": _f(r["momentum_30d"]),
            "liquidity_score": _f(r["liquidity_score"]),
            "age_days": r["age_days"],
            "potential_score": _f(r["potential_score"]),
            "computed_at": r["computed_at"].isoformat() if r["computed_at"] else None,
        }
        for r in rows
    ], headers={"Cache-Control": "no-store"})

@app.get("/api/card-tracker/history")
async def api_card_tracker_history(request: Request, card_id: int, user=Depends(require_admin)):
    def _f(x):
        return float(x) if x is not None else None
    async with request.app.state.db.acquire() as conn:
        card = await conn.fetchrow(
            "SELECT id, name, game, set_name, card_number, variant, release_date, "
            "justtcg_name, justtcg_set, justtcg_number "
            "FROM tracked_cards WHERE id = $1", card_id)
        if card is None:
            raise HTTPException(status_code=404, detail="Card not found")
        snaps = await conn.fetch(
            "SELECT captured_at, price_low, price_mid, price_high FROM price_snapshots "
            "WHERE card_id = $1 ORDER BY captured_at ASC", card_id)
    # Recompute the full component breakdown live so the UI can show WHY the
    # card scores what it does (card_scores stores only the headline numbers).
    explain = card_scoring.score_card([dict(s) for s in snaps], card["release_date"]) if snaps else None
    return JSONResponse({
        "card": {
            "id": card["id"], "name": card["name"], "game": card["game"],
            "set_name": card["set_name"], "card_number": card["card_number"],
            "variant": card["variant"],
            "justtcg_name": card["justtcg_name"], "justtcg_set": card["justtcg_set"],
            "justtcg_number": card["justtcg_number"],
            "release_date": card["release_date"].isoformat() if card["release_date"] else None,
        },
        "snapshots": [
            {"captured_at": s["captured_at"].isoformat(),
             "price_low": _f(s["price_low"]), "price_mid": _f(s["price_mid"]),
             "price_high": _f(s["price_high"])}
            for s in snaps
        ],
        "explain": explain,
    }, headers={"Cache-Control": "no-store"})

# Refresh runs as a background task: properly paced for JustTCG's 10/min
# limit, a full run can take several minutes — far too long for one request.
def _tracker_refresh_state(app) -> dict:
    st = getattr(app.state, "tracker_refresh", None)
    if st is None:
        st = {"running": False, "started_at": None, "finished_at": None,
              "result": None, "error": None}
        app.state.tracker_refresh = st
    return st

async def _run_tracker_refresh(app) -> None:
    st = app.state.tracker_refresh
    pool = app.state.db
    try:
        await ensure_card_tracker_schema(pool)
        added = await sync_watchlist(pool)
        ingest = await run_ingest(pool)
        scoring = await run_scoring(pool)
        st["result"] = {
            "watchlist_added": added,
            "snapshots": ingest["snapshots"],
            "resolved": ingest["resolved"],
            "backfilled": ingest.get("backfilled", 0),
            "failures": ingest["failed"][:20],
            "justtcg_calls": ingest.get("justtcg_calls", 0),
            "scored": scoring["scored"],
        }
    except Exception as e:
        logger.exception("Card tracker refresh failed")
        st["error"] = str(e)
    finally:
        st["running"] = False
        st["finished_at"] = datetime.utcnow().isoformat() + "Z"

@app.post("/api/card-tracker/refresh")
@limiter.limit("3/hour")
async def api_card_tracker_refresh(request: Request, user=Depends(require_admin)):
    # Spending the JustTCG call budget stays admin-only; mods can view.
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Refresh is admin-only")
    st = _tracker_refresh_state(request.app)
    if st["running"]:
        raise HTTPException(status_code=409, detail="A refresh is already running")
    st.update(running=True, started_at=datetime.utcnow().isoformat() + "Z",
              finished_at=None, result=None, error=None)
    asyncio.create_task(_run_tracker_refresh(request.app))
    return JSONResponse({"ok": True, "started": True}, headers={"Cache-Control": "no-store"})

@app.get("/api/card-tracker/refresh/status")
async def api_card_tracker_refresh_status(request: Request, user=Depends(require_admin)):
    state = dict(_tracker_refresh_state(request.app))
    if not state["running"]:
        eastern = ZoneInfo("America/New_York")
        now = datetime.now(eastern)
        target = now.replace(hour=CARD_TRACKER_SCHEDULE_HOUR, minute=CARD_TRACKER_SCHEDULE_MINUTE,
                             second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        state["next_scheduled"] = target.isoformat()
    return JSONResponse(state, headers={"Cache-Control": "no-store"})

@app.post("/api/card-tracker/rematch")
@limiter.limit("30/hour")
async def api_card_tracker_rematch(request: Request, user=Depends(require_admin)):
    """Clear a card's JustTCG match AND its price/score history (which belong
    to the wrongly-matched card). The next ingest re-resolves it with the
    current matching logic. Admin-only because it deletes data."""
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Re-matching is admin-only")
    body = await request.json()
    card_id = body.get("card_id")
    if not isinstance(card_id, int):
        raise HTTPException(status_code=400, detail="card_id (int) required")
    async with request.app.state.db.acquire() as conn:
        card = await conn.fetchrow("SELECT id, name FROM tracked_cards WHERE id = $1", card_id)
        if card is None:
            raise HTTPException(status_code=404, detail="Card not found")
        async with conn.transaction():
            await conn.execute("DELETE FROM card_scores WHERE card_id = $1", card_id)
            deleted = await conn.execute("DELETE FROM price_snapshots WHERE card_id = $1", card_id)
            await conn.execute(
                "UPDATE tracked_cards SET justtcg_card_id = NULL, justtcg_name = NULL, "
                "justtcg_set = NULL, justtcg_number = NULL WHERE id = $1", card_id)
    logger.info("Card tracker: match cleared for %r (id %d) by admin %s; %s",
                card["name"], card_id, user["id"], deleted)
    return JSONResponse({"ok": True}, headers={"Cache-Control": "no-store"})

@app.post("/api/card-tracker/reset-history")
@limiter.limit("30/hour")
async def api_card_tracker_reset_history(request: Request, user=Depends(require_admin)):
    """Clear price/score history WITHOUT touching the JustTCG match, so the
    next ingest treats the card as a first-time fetch and re-requests the
    30-day priceHistory backfill — no extra search/resolution API call spent.

    Use this when a card's momentum looks flat (7D == 30D): that almost
    always means it only has 1-2 snapshot rows (e.g. from before the
    backfill feature existed, or from testing), so both windows fall back
    to the same single baseline price. body: {"card_id": int} or
    {"all": true} to reset every tracked card in one go."""
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Resetting history is admin-only")
    body = await request.json()
    reset_all = bool(body.get("all"))
    card_id = body.get("card_id")
    if not reset_all and not isinstance(card_id, int):
        raise HTTPException(status_code=400, detail="card_id (int) or all=true required")

    async with request.app.state.db.acquire() as conn:
        if reset_all:
            async with conn.transaction():
                await conn.execute("DELETE FROM card_scores")
                deleted = await conn.execute("DELETE FROM price_snapshots")
            count = await conn.fetchval("SELECT COUNT(*) FROM tracked_cards")
            logger.info("Card tracker: history reset for ALL cards by admin %s; %s",
                        user["id"], deleted)
            return JSONResponse({"ok": True, "cards_affected": count},
                                headers={"Cache-Control": "no-store"})

        card = await conn.fetchrow("SELECT id, name FROM tracked_cards WHERE id = $1", card_id)
        if card is None:
            raise HTTPException(status_code=404, detail="Card not found")
        async with conn.transaction():
            await conn.execute("DELETE FROM card_scores WHERE card_id = $1", card_id)
            deleted = await conn.execute("DELETE FROM price_snapshots WHERE card_id = $1", card_id)
    logger.info("Card tracker: history reset for %r (id %d) by admin %s; %s",
                card["name"], card_id, user["id"], deleted)
    return JSONResponse({"ok": True, "cards_affected": 1}, headers={"Cache-Control": "no-store"})

# ── Set import (admin-only): enumerate a set from the free catalog APIs,
#    preview, then import into tracked_cards. The import re-fetches from the
#    source API server-side — the client never supplies card data directly. ──

def _require_tracker_admin(request: Request) -> dict:
    user = require_staff(request)
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Importing is admin-only")
    return user

# pokemontcg.io can be slow even with trimmed payloads — generous read timeout
# plus a single retry on timeout.
_CATALOG_TIMEOUT = httpx.Timeout(60.0, connect=10.0)

async def _fetch_set_candidates(game: str, set_id: str, rarity: str,
                                limit: int | None = None) -> tuple:
    """(set_name, cards) from the free catalog APIs, rarity-filtered.

    `limit` caps how many cards come back and defaults to the tracker's import
    cap. The grading calculator passes None: that cap exists to protect the
    JustTCG budget when mass-adding a set, and silently truncating a large set
    would drop cards out of its picker.

    Retries transient failures: pokemontcg.io throws intermittent HTTP 500s,
    and both APIs occasionally time out. 5xx and timeouts are retried with a
    short backoff; 4xx (a real bad request / missing set) fails immediately.
    """
    last_exc = None
    attempts = 4
    for attempt in range(attempts):
        try:
            async with httpx.AsyncClient(timeout=_CATALOG_TIMEOUT) as client:
                if game == "pokemon":
                    set_name, cards = await set_import.fetch_pokemon_set_cards(client, set_id)
                else:
                    set_name, cards = await set_import.fetch_onepiece_set_cards(client, set_id)
            break
        except httpx.TimeoutException as e:
            last_exc = e
            logger.warning("Catalog fetch timed out (attempt %d/%d) for %s/%s",
                           attempt + 1, attempts, game, set_id)
        except httpx.HTTPStatusError as e:
            if e.response.status_code < 500:
                raise      # a real 4xx (bad set id etc.) — don't retry
            last_exc = e
            logger.warning("Catalog fetch got HTTP %d (attempt %d/%d) for %s/%s",
                           e.response.status_code, attempt + 1, attempts, game, set_id)
        if attempt < attempts - 1:
            await asyncio.sleep(0.8 * (attempt + 1))
    else:
        raise RuntimeError("The card catalog API is having a moment (repeated "
                           "errors) — try again in a minute.") from last_exc
    if rarity:
        cards = [c for c in cards if rarity.lower() in (c["variant"] or "").lower()]
    if limit is None:
        return set_name, cards
    return set_name, cards[:limit]

async def _existing_card_keys(conn) -> tuple:
    """Sets used for duplicate detection: exact identity and (game|set|number)."""
    rows = await conn.fetch("SELECT game, name, set_name, card_number FROM tracked_cards")
    exact = {(r["game"], r["name"].lower(), r["set_name"].lower(), r["card_number"].lower()) for r in rows}
    by_number = {(r["game"], r["set_name"].lower(), r["card_number"].lower())
                 for r in rows if r["card_number"]}
    return exact, by_number

def _validate_import_params(game: str, set_id: str, rarity: str) -> None:
    if game not in ("pokemon", "one_piece"):
        raise HTTPException(status_code=400, detail="Invalid game")
    if not set_id or len(set_id) > 40:
        raise HTTPException(status_code=400, detail="Invalid set")
    if len(rarity) > 60:
        raise HTTPException(status_code=400, detail="Invalid rarity")

@app.get("/api/card-tracker/import/sets")
@limiter.limit("30/hour")
async def api_import_sets(request: Request, game: str = "pokemon"):
    _require_tracker_admin(request)
    if game not in ("pokemon", "one_piece"):
        raise HTTPException(status_code=400, detail="Invalid game")
    try:
        async with httpx.AsyncClient(timeout=_CATALOG_TIMEOUT) as client:
            if game == "pokemon":
                sets = await set_import.fetch_pokemon_sets(client)
            else:
                sets = await set_import.fetch_onepiece_sets(client)
    except Exception as e:
        logger.exception("Set list fetch failed")
        raise HTTPException(status_code=502, detail=f"Couldn't fetch the set list: {e}")
    return JSONResponse(sets, headers={"Cache-Control": "no-store"})

@app.post("/api/card-tracker/import/preview")
@limiter.limit("20/hour")
async def api_import_preview(request: Request):
    _require_tracker_admin(request)
    body = await request.json()
    game = body.get("game", "")
    set_id = (body.get("set_id") or "").strip()
    rarity = (body.get("rarity") or "").strip()
    _validate_import_params(game, set_id, rarity)
    try:
        set_name, cards = await _fetch_set_candidates(game, set_id, rarity,
                                                      limit=set_import.MAX_CARDS_PER_IMPORT)
    except (ValueError, RuntimeError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Import preview failed")
        raise HTTPException(status_code=502, detail=f"Couldn't fetch that set: {e}")

    async with request.app.state.db.acquire() as conn:
        exact, by_number = await _existing_card_keys(conn)
        tracked_count = await conn.fetchval("SELECT COUNT(*) FROM tracked_cards")

    out = []
    for c in cards:
        key_exact = (game, c["name"].lower(), (set_name or "").lower(), c["card_number"].lower())
        key_num = (game, (set_name or "").lower(), c["card_number"].lower())
        dup = key_exact in exact or (bool(c["card_number"]) and key_num in by_number)
        out.append({**c, "already_tracked": dup})
    rarities = sorted({(c["variant"] or "").strip() for c in out if (c["variant"] or "").strip()})
    return JSONResponse({
        "set_name": set_name,
        "cards": out,
        "rarities": rarities,
        "tracked_count": tracked_count,
        "max_tracked": MAX_TRACKED_CARDS,
    }, headers={"Cache-Control": "no-store"})

@app.post("/api/card-tracker/import/run")
@limiter.limit("10/hour")
async def api_import_run(request: Request):
    _require_tracker_admin(request)
    body = await request.json()
    game = body.get("game", "")
    set_id = (body.get("set_id") or "").strip()
    rarity = (body.get("rarity") or "").strip()
    exclude = body.get("exclude_numbers") or []
    _validate_import_params(game, set_id, rarity)
    if not isinstance(exclude, list) or len(exclude) > 1000 or \
            any(not isinstance(x, str) or len(x) > 40 for x in exclude):
        raise HTTPException(status_code=400, detail="Invalid exclusion list")
    excluded = {x.lower() for x in exclude}

    # Server-side re-fetch — the client only says WHAT set/rarity to import
    # and which numbers to leave out; it can't inject card rows.
    try:
        set_name, cards = await _fetch_set_candidates(game, set_id, rarity,
                                                      limit=set_import.MAX_CARDS_PER_IMPORT)
    except (ValueError, RuntimeError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Import run fetch failed")
        raise HTTPException(status_code=502, detail=f"Couldn't fetch that set: {e}")
    cards = [c for c in cards if c["card_number"].lower() not in excluded and c["name"]]

    pool = request.app.state.db
    added, skipped = 0, 0
    async with pool.acquire() as conn:
        exact, by_number = await _existing_card_keys(conn)
        tracked_count = await conn.fetchval("SELECT COUNT(*) FROM tracked_cards")
        new_cards = []
        for c in cards:
            key_exact = (game, c["name"].lower(), (set_name or "").lower(), c["card_number"].lower())
            key_num = (game, (set_name or "").lower(), c["card_number"].lower())
            if key_exact in exact or (c["card_number"] and key_num in by_number):
                skipped += 1
                continue
            new_cards.append(c)
        if tracked_count + len(new_cards) > MAX_TRACKED_CARDS:
            raise HTTPException(
                status_code=400,
                detail=f"Import would take the tracker to {tracked_count + len(new_cards)} cards "
                       f"(cap {MAX_TRACKED_CARDS}, currently {tracked_count}). The cap protects the "
                       f"JustTCG monthly call budget — trim the selection or raise MAX_TRACKED_CARDS "
                       f"in card_tracker.py deliberately.")
        async with conn.transaction():
            for c in new_cards:
                result = await conn.execute(
                    """
                    INSERT INTO tracked_cards (name, game, set_name, card_number, variant, release_date)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (game, name, set_name, card_number) DO NOTHING
                    """,
                    c["name"], game, set_name or "", c["card_number"], c["variant"] or None,
                    datetime.strptime(c["release_date"], "%Y-%m-%d").date() if c.get("release_date") else None,
                )
                if result.endswith("1"):
                    added += 1
                else:
                    skipped += 1
        total = await conn.fetchval("SELECT COUNT(*) FROM tracked_cards")
    logger.info("Set import: %s/%s rarity=%r -> +%d added, %d skipped as duplicates (total %d)",
                game, set_id, rarity or "all", added, skipped, total)
    return JSONResponse({"ok": True, "set_name": set_name, "added": added,
                         "skipped_duplicates": skipped, "total_tracked": total},
                        headers={"Cache-Control": "no-store"})

# ---- Grading calculator ----
# Standalone page, deliberately not wired into the card tracker: it works for
# ANY card, not just the ~400 in tracked_cards, so it has no dependency on the
# JustTCG watchlist. Open to members (same gate as the map/dashboard); the
# vendor-backed lookups spend paid quota, so they keep a per-IP rate limit.

@app.get("/grading-calculator", response_class=HTMLResponse)
async def grading_calculator_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/login")
    if is_demo(request):
        return RedirectResponse("/sample")
    if not await terms_current(request, user):
        return RedirectResponse("/terms")
    is_admin = int(user["id"]) in ADMIN_USER_IDS
    return templates.TemplateResponse("grading_calculator.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "is_admin": is_admin,
        "is_mod": request.session.get("mod", False),
        "sources": price_sources.configured_sources(),
        "grade_labels": price_sources.GRADE_LABELS,
        "grade_levels": price_sources.GRADE_LEVEL,
        "grading_companies": grading_tiers.GRADING_COMPANIES,
        "grading_sets": grading_sets.GRADING_SETS,
    })


@app.get("/api/grading-calculator/quotes")
@limiter.limit("20/minute")
async def api_grading_quotes(request: Request, name: str, game: str = "pokemon",
                             set_name: str = "", card_number: str = "",
                             tcgplayer_id: str = "", language: str = "english",
                             user=Depends(get_current_user)):
    """Live graded prices for one card, merged across configured vendors.
    Open to members (like the rest of the grading calculator). Spends
    PokemonPriceTracker credits and eBay's daily budget, so it keeps the
    per-IP rate limit above — watch aggregate usage if member traffic grows."""
    name = (name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="A card name is required")
    if game not in ("pokemon", "one_piece"):
        raise HTTPException(status_code=400, detail="Unknown game")

    card = price_sources.CardRef(game=game, name=name[:120],
                                 set_name=(set_name or "").strip()[:120],
                                 card_number=(card_number or "").strip()[:40],
                                 tcgplayer_id=(tcgplayer_id or "").strip()[:32] or None,
                                 language=price_sources.ppt_language(language))
    try:
        result = await price_sources.fetch_all(card)
    except Exception:
        logger.exception("Graded quote lookup failed for %r", card.query())
        raise HTTPException(status_code=502, detail="Price lookup failed")

    return JSONResponse({
        "card_key": card.key(),
        "query": card.query(),
        "sources_used": result["sources"],
        "sources_configured": price_sources.configured_sources(),
        "quotes": {g: q.to_dict() for g, q in result["quotes"].items()},
        "all": [q.to_dict() for q in result["all"]],
    }, headers={"Cache-Control": "no-store"})


@app.post("/api/grading-calculator/calc")
@limiter.limit("120/minute")
async def api_grading_calc(request: Request, user=Depends(get_current_user)):
    """Net proceeds per grade — no network, no DB. Split from the quotes route
    so editing a price or a cost recalculates instantly and spends no quota."""
    body = await request.json()

    def _num(value, default=0.0):
        try:
            out = float(value)
        except (TypeError, ValueError):
            return default
        return out if math.isfinite(out) else default

    raw_price = _num(body.get("raw_price"))
    grade_prices = {g: _num(v) for g, v in (body.get("grade_prices") or {}).items()
                    if g in price_sources.GRADE_KEYS}
    c = body.get("costs") or {}
    costs = grading_roi.Costs(
        grading_fee=_num(c.get("grading_fee"), 25.0),
        ship_to=_num(c.get("ship_to"), 5.0),
        ship_return=_num(c.get("ship_return"), 20.0),
        insurance=_num(c.get("insurance"), 0.0),
        sale_fee_pct=min(max(_num(c.get("sale_fee_pct"), 0.0), 0.0), 0.9),
        sale_ship=_num(c.get("sale_ship"), 0.0),
    )
    # "What you'd net" reports on whichever grading company is picked in Your
    # costs — each company's own top-two grade keys, from grading_tiers.py.
    # Falls back to PSA 10/9 when no company is selected ("Custom / other").
    company = (body.get("company") or "").strip().lower()
    report_grades = grading_tiers.GRADING_COMPANIES.get(company, {}).get(
        "report_grades") or grading_roi.REPORTED_GRADES
    try:
        r = grading_roi.evaluate(raw_price, grade_prices, costs, grades=report_grades)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return JSONResponse({
        "raw_price": r.raw_price,
        "raw_sale_fee": r.raw_sale_fee,
        "raw_sale_ship": r.raw_sale_ship,
        "raw_net": r.raw_net,
        "submission_total": r.submission_total,
        "grades": [{"grade": g.grade,
                    "label": price_sources.GRADE_LABELS.get(g.grade, g.grade),
                    "price": g.price, "sale_fee": g.sale_fee,
                    "sale_ship": g.sale_ship,
                    "submission_total": g.submission_total,
                    "net": g.net, "vs_raw": g.vs_raw}
                   for g in r.grades],
        "warnings": r.warnings,
    }, headers={"Cache-Control": "no-store"})


# Cards-in-a-set are fetched from the free catalog APIs for the grading
# calculator's card picker. Cached in-process (a set's card list is static)
# so repeated picks don't re-hit the external API. Separate from the admin
# import endpoints: mod-accessible, no DB duplicate work, lighter rate limit.
_grading_set_cache: dict[tuple[str, str], list] = {}


@app.get("/api/grading-calculator/sets")
@limiter.limit("120/hour")
async def api_grading_sets(request: Request, game: str = "pokemon",
                           language: str = "english",
                           user=Depends(get_current_user)):
    """Set list for the picker.

    For Pokemon this comes from PokemonPriceTracker, so the set string sent
    back on a lookup is byte-identical to what their price API expects — that
    removes the set-name matching that caused every missed lookup. Falls back
    to the baked catalog list if PPT isn't configured or errors.
    """
    if game not in ("pokemon", "one_piece"):
        raise HTTPException(status_code=400, detail="Unknown game")
    language = price_sources.ppt_language(language)

    if game == "pokemon" and price_sources.pokemonpricetracker_available():
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                sets = await price_sources.fetch_ppt_sets(client, language=language)
            if sets:
                return JSONResponse({"source": "pokemonpricetracker",
                                     "language": language, "sets": sets},
                                    headers={"Cache-Control": "no-store"})
        except Exception:
            logger.exception("PPT set list failed — falling back to the baked list")
    # The baked list is English-only; a Japanese request with no PPT falls back
    # to it rather than showing nothing, so say which language actually came back.
    if language != "english":
        logger.info("Japanese sets unavailable (PPT off or empty) — serving the "
                    "English catalog list")

    return JSONResponse(
        {"source": "catalog", "language": "english",
         "sets": grading_sets.GRADING_SETS.get(game, [])},
        headers={"Cache-Control": "no-store"})


@app.get("/api/grading-calculator/set-cards")
@limiter.limit("60/hour")
async def api_grading_set_cards(request: Request, game: str, set_id: str,
                                language: str = "english",
                                user=Depends(get_current_user)):
    if game not in ("pokemon", "one_piece"):
        raise HTTPException(status_code=400, detail="Unknown game")
    set_id = (set_id or "").strip()
    # PPT set ids ARE set names, so this is longer than a catalog set code.
    if not set_id or len(set_id) > 120:
        raise HTTPException(status_code=400, detail="Invalid set")
    language = price_sources.ppt_language(language)

    # Pokemon: take the card list from PPT so each card carries its
    # tcgPlayerId. A lookup can then pin the exact printing instead of
    # matching on name and set.
    if game == "pokemon" and price_sources.pokemonpricetracker_available():
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                cards = await price_sources.fetch_ppt_set_cards(
                    client, set_id, language=language)
            if cards:
                return JSONResponse({"set_id": set_id, "source": "pokemonpricetracker",
                                     "language": language, "cards": cards},
                                    headers={"Cache-Control": "no-store"})
            logger.info("PPT returned no cards for set %r [%s] — falling back to catalog",
                        set_id, language)
        except Exception:
            logger.exception("PPT set-cards failed for %r — falling back to catalog", set_id)

    cache_key = (game, set_id)
    if cache_key not in _grading_set_cache:
        try:
            # No limit: the importer's 250-card cap protects the JustTCG budget
            # and has no business truncating a picker.
            set_name, cards = await _fetch_set_candidates(game, set_id, "")
        except (ValueError, RuntimeError) as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except Exception:
            logger.exception("Grading set-cards fetch failed for %s/%s", game, set_id)
            raise HTTPException(status_code=502, detail="Couldn't fetch that set's cards")
        _grading_set_cache[cache_key] = [
            {"name": c["name"], "card_number": c["card_number"],
             "variant": c["variant"], "set_name": set_name, "tcgplayer_id": ""}
            for c in cards
        ]
    return JSONResponse({"set_id": set_id, "source": "catalog",
                         "language": "english",
                         "cards": _grading_set_cache[cache_key]},
                        headers={"Cache-Control": "no-store"})


# ---- Analytics ----

@app.get("/analytics", response_class=HTMLResponse)
async def analytics_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/login")
    if not await terms_current(request, user):
        return RedirectResponse("/terms")
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Not authorized")
    return templates.TemplateResponse("analytics.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "is_admin": True,
        "is_mod": request.session.get("mod", False),
    })

@app.get("/api/analytics")
async def get_analytics(
    request: Request,
    days: int = 7,
    user=Depends(get_current_user)
):
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Not authorized")

    if days not in (7, 30, 60, 90):
        days = 7

    eastern = ZoneInfo("America/New_York")
    now = datetime.now(eastern)
    since = now - timedelta(days=days)

    async with request.app.state.db.acquire() as conn:

        # Active informants filter
        informant_rows = await conn.fetch("SELECT user_id FROM active_informants")
        informant_ids = {int(r["user_id"]) for r in informant_rows}

        # Restock events
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

        # Empty ping events
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

        # Plus one events
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

        # Username lookup — most recent username per user
        user_name_rows = await conn.fetch(
            """
            SELECT DISTINCT ON (user_id) user_id, username
            FROM dashboard_sessions
            ORDER BY user_id, logged_in_at DESC
            """
        )

    username_map = {int(r["user_id"]): r["username"] for r in user_name_rows}

    # Build date range list
    date_list = []
    d = since.date()
    while d <= now.date():
        date_list.append(d.isoformat())
        d += timedelta(days=1)

    # Collect all user_ids across all events, normalized to int
    all_user_ids = set()
    for r in restock_rows:
        all_user_ids.add(int(r["user_id"]))
    for r in empty_rows:
        all_user_ids.add(int(r["user_id"]))
    for r in plusone_rows:
        all_user_ids.add(int(r["user_id"]))

    # Filter to only active informants
    all_user_ids = all_user_ids & informant_ids

    # Build per-user per-day data
    user_daily = {
        uid: {d: {"restock_pts": 0.0, "empty_pts": 0.0, "plusone_pts": 0.0, "total": 0.0}
              for d in date_list}
        for uid in all_user_ids
    }

    user_activity = defaultdict(list)

    # Process restocks
    for r in restock_rows:
        uid = int(r["user_id"])
        if uid not in all_user_ids:
            continue
        date_str = r["local_date"].date().isoformat()
        pts = float(r["pts"])
        if uid in user_daily and date_str in user_daily[uid]:
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

    # Process empty pings
    for r in empty_rows:
        uid = int(r["user_id"])
        if uid not in all_user_ids:
            continue
        date_str = r["local_date"].date().isoformat()
        dow = int(r["dow"])  # 0=Sun, 6=Sat
        is_weekend = dow in (0, 6)
        pts = 0.05 if is_weekend else 0.1
        if uid in user_daily and date_str in user_daily[uid]:
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

    # Process plusones
    for r in plusone_rows:
        uid = int(r["user_id"])
        if uid not in all_user_ids:
            continue
        date_str = r["local_date"].date().isoformat()
        pts = float(r["pts"])
        if uid in user_daily and date_str in user_daily[uid]:
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

    # Anomaly detection
    def detect_flags(uid, daily, activity):
        daily_totals = [v["total"] for v in daily.values() if v["total"] > 0]
        avg = sum(daily_totals) / len(daily_totals) if daily_totals else 0
        spike_days = sum(1 for v in daily.values() if v["total"] >= max(avg * 3, 1.0))
        restock_count = sum(1 for a in activity if a["type"] == "Restock")
        empty_count = sum(1 for a in activity if a["type"].startswith("Empty"))
        empty_ratio = (empty_count / restock_count) if restock_count > 0 else (empty_count if empty_count > 0 else 0)
        loc_counts = defaultdict(int)
        for a in activity:
            if a["type"].startswith("Empty") and a["location"]:
                loc_counts[a["location"]] += 1
        repeat_max = max(loc_counts.values()) if loc_counts else 0
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

    # Assemble response
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

    users_out.sort(key=lambda x: x["total_pts"], reverse=True)

    return JSONResponse({
        "dates": date_list,
        "users": users_out,
    })

# ---- Store Status ----

@app.get("/status", response_class=HTMLResponse)
async def status_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/login")
    if is_demo(request):
        return RedirectResponse("/sample-status")
    if not await terms_current(request, user):
        return RedirectResponse("/terms")
    is_admin = int(user["id"]) in ADMIN_USER_IDS
    return templates.TemplateResponse("status.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "is_admin": is_admin,
        "is_mod": request.session.get("mod", False),
    })

@app.get("/api/status")
async def get_status(request: Request, user=Depends(get_current_user)):
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (cl.location)
                SPLIT_PART(cl.location, '|', 1) AS city,
                SPLIT_PART(cl.location, '|', 2) AS store,
                cl.command_used,
                cl.timestamp AT TIME ZONE 'America/New_York' AS local_time,
                COALESCE(u.username, ds.username) AS username
            FROM command_logs cl
            LEFT JOIN users u ON u.user_id = cl.user_id
            LEFT JOIN LATERAL (
                SELECT username FROM dashboard_sessions
                WHERE user_id = cl.user_id
                ORDER BY logged_in_at DESC
                LIMIT 1
            ) ds ON true
            WHERE cl.location IS NOT NULL
              AND cl.location LIKE '%|%'
              AND cl.command_used IN ('empty', 'remain', 'restock', 'hope')
            ORDER BY cl.location, cl.timestamp DESC
            """
        )
    return JSONResponse([
        {
            "city":     r["city"],
            "store":    r["store"],
            "status":   r["command_used"],
            "time":     r["local_time"].isoformat(),
            "username": r["username"] or "Unknown",
        }
        for r in rows
    ], headers={"Cache-Control": "no-store"})

# ---- Contributors ----

@app.get("/contributors", response_class=HTMLResponse)
async def contributors_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/login")
    if not await terms_current(request, user):
        return RedirectResponse("/terms")
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Not authorized")
    return templates.TemplateResponse("contributors.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "is_admin": True,
        "is_mod": request.session.get("mod", False),
    })

def _contributors_period_since(period: str):
    """'month' = calendar month-to-date (America/New_York); '3months' (default)
    = trailing 90 days from now. Replaces the old recency-decayed scoring,
    which silently EXCLUDED the current in-progress month (it anchored its
    30/60/90-day tiers to the start of the current month, not to now)."""
    eastern = ZoneInfo("America/New_York")
    now = datetime.now(eastern)
    if period == "month":
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return now - timedelta(days=90)

@app.get("/api/contributors/points")
async def get_contributor_points(request: Request, period: str = "3months",
                                 user=Depends(get_current_user)):
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Not authorized")
    if period not in ("month", "3months"):
        period = "3months"
    since = _contributors_period_since(period)

    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH restock_points AS (
                SELECT user_id,
                    SUM(CASE WHEN store_name IN ('Target','Walmart','5 Below','Barnes and Noble','Best Buy') THEN 1 ELSE 0.5 END) AS restock_pts,
                    COUNT(*) AS restock_count
                FROM restock_reports
                WHERE date >= $1
                  AND channel_name NOT IN ('online-restock-information','other-online-restocks','pokemon-center-drops')
                GROUP BY user_id
            ),
            empty_points AS (
                SELECT user_id,
                    SUM(CASE WHEN EXTRACT(DOW FROM timestamp AT TIME ZONE 'America/New_York') IN (0,6) THEN 0.05 ELSE 0.1 END) AS empty_pts,
                    COUNT(*) AS empty_count
                FROM command_logs
                WHERE command_used = 'empty'
                  AND timestamp >= $1
                  AND location NOT LIKE '%|Costco'
                  AND location NOT LIKE '%|Sam''s Club'
                  AND location NOT LIKE '%|CVS'
                  AND location NOT LIKE '%|Walgreens'
                GROUP BY user_id
            ),
            plusone_points AS (
                SELECT receiver_id AS user_id, COALESCE(SUM(value), 0) AS plusone_pts
                FROM plusones WHERE timestamp >= $1 GROUP BY receiver_id
            ),
            manual_points_cte AS (
                SELECT receiver_id AS user_id, COALESCE(SUM(value), 0) AS manual_pts
                FROM manual_points WHERE timestamp >= $1 GROUP BY receiver_id
            ),
            hope_points AS (
                SELECT user_id, COALESCE(SUM(value), 0) AS hope_pts
                FROM hope_contributions WHERE timestamp >= $1 GROUP BY user_id
            ),
            combined AS (
                SELECT
                    COALESCE(r.user_id, e.user_id, p.user_id, m.user_id, h.user_id) AS user_id,
                    COALESCE(r.restock_pts, 0) AS restock_pts,
                    COALESCE(r.restock_count, 0) AS restock_count,
                    COALESCE(e.empty_pts, 0) AS empty_pts,
                    COALESCE(e.empty_count, 0) AS empty_count,
                    COALESCE(p.plusone_pts, 0) AS plusone_pts,
                    COALESCE(m.manual_pts, 0) AS manual_pts,
                    COALESCE(h.hope_pts, 0) AS hope_pts,
                    COALESCE(r.restock_pts, 0) + COALESCE(e.empty_pts, 0) + COALESCE(p.plusone_pts, 0)
                        + COALESCE(m.manual_pts, 0) + COALESCE(h.hope_pts, 0) AS total_points
                FROM restock_points r
                FULL OUTER JOIN empty_points e ON r.user_id = e.user_id
                FULL OUTER JOIN plusone_points p ON COALESCE(r.user_id, e.user_id) = p.user_id
                FULL OUTER JOIN manual_points_cte m ON COALESCE(r.user_id, e.user_id, p.user_id) = m.user_id
                FULL OUTER JOIN hope_points h ON COALESCE(r.user_id, e.user_id, p.user_id, m.user_id) = h.user_id
            )
            SELECT c.*, COALESCE(u.username, ds.username) AS username
            FROM combined c
            LEFT JOIN users u ON u.user_id = c.user_id
            LEFT JOIN LATERAL (
                SELECT username FROM dashboard_sessions
                WHERE user_id = c.user_id ORDER BY logged_in_at DESC LIMIT 1
            ) ds ON true
            ORDER BY total_points DESC
            LIMIT 300
            """,
            since
        )

    return JSONResponse({
        "period": period,
        "since": since.isoformat(),
        "rows": [
            {
                "user_id": str(r["user_id"]),
                "username": r["username"] or f"User {r['user_id']}",
                "restock_pts": round(float(r["restock_pts"]), 2),
                "restock_count": int(r["restock_count"]),
                "empty_pts": round(float(r["empty_pts"]), 2),
                "empty_count": int(r["empty_count"]),
                "plusone_pts": round(float(r["plusone_pts"]), 2),
                "manual_pts": round(float(r["manual_pts"]), 2),
                "hope_pts": round(float(r["hope_pts"]), 2),
                "total_points": round(float(r["total_points"]), 2),
            }
            for r in rows
        ],
    }, headers={"Cache-Control": "no-store"})

@app.get("/api/contributors/invites")
async def get_contributor_invites(request: Request, period: str = "3months",
                                  user=Depends(get_current_user)):
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Not authorized")
    if period not in ("month", "3months"):
        period = "3months"
    since = _contributors_period_since(period)

    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH agg AS (
                SELECT inviter_id,
                    COUNT(*) FILTER (WHERE joined_at >= $1) AS invites_period,
                    COUNT(*) AS invites_alltime,
                    MAX(joined_at) AS last_invite_at
                FROM member_joins
                WHERE inviter_id IS NOT NULL AND inviter_id != 0
                GROUP BY inviter_id
            ),
            names AS (
                SELECT DISTINCT ON (inviter_id) inviter_id, inviter_name
                FROM member_joins
                WHERE inviter_id IS NOT NULL AND inviter_id != 0
                ORDER BY inviter_id, joined_at DESC
            )
            SELECT a.inviter_id, COALESCE(n.inviter_name, 'User ' || a.inviter_id) AS inviter_name,
                   a.invites_period, a.invites_alltime, a.last_invite_at
            FROM agg a
            LEFT JOIN names n ON n.inviter_id = a.inviter_id
            ORDER BY a.invites_period DESC, a.invites_alltime DESC
            LIMIT 300
            """,
            since
        )

    return JSONResponse({
        "period": period,
        "since": since.isoformat(),
        "rows": [
            {
                "user_id": str(r["inviter_id"]),
                "username": r["inviter_name"] or f"User {r['inviter_id']}",
                "invites_period": int(r["invites_period"]),
                "invites_alltime": int(r["invites_alltime"]),
                "last_invite_at": r["last_invite_at"].isoformat() if r["last_invite_at"] else None,
            }
            for r in rows
        ],
    }, headers={"Cache-Control": "no-store"})

@app.get("/api/contributors/regions")
async def get_contributor_regions(request: Request, region: str, period: str = "3months",
                                  user=Depends(get_current_user)):
    """Top contributors for callouts/restocks of stores located in ONE
    region — a user asked to pick a region and see who's active there,
    not a per-region rollup."""
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Not authorized")
    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")
    if period not in ("month", "3months"):
        period = "3months"
    since = _contributors_period_since(period)

    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH restock_pts_cte AS (
                SELECT rr.user_id,
                    SUM(CASE WHEN rr.store_name IN ('Target','Walmart','5 Below','Barnes and Noble','Best Buy') THEN 1 ELSE 0.5 END) AS restock_pts,
                    COUNT(*) AS restock_count
                FROM restock_reports rr
                JOIN locations l
                  ON LOWER(TRIM(l.location)) = LOWER(TRIM(rr.location))
                  AND LOWER(TRIM(l.store_type)) = LOWER(TRIM(rr.store_name))
                WHERE rr.date >= $1
                  AND l.state = $2
                  AND rr.channel_name NOT IN ('online-restock-information','other-online-restocks','pokemon-center-drops')
                GROUP BY rr.user_id
            ),
            empty_pts_cte AS (
                SELECT cl.user_id,
                    SUM(CASE WHEN EXTRACT(DOW FROM cl.timestamp AT TIME ZONE 'America/New_York') IN (0,6) THEN 0.05 ELSE 0.1 END) AS empty_pts,
                    COUNT(*) AS empty_count
                FROM command_logs cl
                JOIN locations l
                  ON LOWER(TRIM(l.location)) = LOWER(TRIM(SPLIT_PART(cl.location, '|', 1)))
                  AND LOWER(TRIM(l.store_type)) = LOWER(TRIM(SPLIT_PART(cl.location, '|', 2)))
                WHERE cl.command_used = 'empty'
                  AND cl.timestamp >= $1
                  AND l.state = $2
                GROUP BY cl.user_id
            ),
            combined AS (
                SELECT
                    COALESCE(r.user_id, e.user_id) AS user_id,
                    COALESCE(r.restock_pts, 0) AS restock_pts,
                    COALESCE(r.restock_count, 0) AS restock_count,
                    COALESCE(e.empty_pts, 0) AS empty_pts,
                    COALESCE(e.empty_count, 0) AS empty_count,
                    COALESCE(r.restock_pts, 0) + COALESCE(e.empty_pts, 0) AS total_points
                FROM restock_pts_cte r
                FULL OUTER JOIN empty_pts_cte e ON r.user_id = e.user_id
            )
            SELECT c.*, COALESCE(u.username, ds.username) AS username
            FROM combined c
            LEFT JOIN users u ON u.user_id = c.user_id
            LEFT JOIN LATERAL (
                SELECT username FROM dashboard_sessions
                WHERE user_id = c.user_id ORDER BY logged_in_at DESC LIMIT 1
            ) ds ON true
            ORDER BY total_points DESC
            LIMIT 300
            """,
            since, region
        )

    return JSONResponse({
        "period": period,
        "since": since.isoformat(),
        "region": region,
        "region_label": STATE_LABELS.get(region, region),
        "rows": [
            {
                "user_id": str(r["user_id"]),
                "username": r["username"] or f"User {r['user_id']}",
                "restock_pts": round(float(r["restock_pts"]), 2),
                "restock_count": int(r["restock_count"]),
                "empty_pts": round(float(r["empty_pts"]), 2),
                "empty_count": int(r["empty_count"]),
                "total_points": round(float(r["total_points"]), 2),
            }
            for r in rows
        ],
    }, headers={"Cache-Control": "no-store"})

# ---- Map ----

@app.get("/map", response_class=HTMLResponse)
async def map_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/login")
    if is_demo(request):
        return RedirectResponse("/sample-map")
    if not await terms_current(request, user):
        return RedirectResponse("/terms")
    is_admin = int(user["id"]) in ADMIN_USER_IDS
    return templates.TemplateResponse("map.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "is_admin": is_admin,
        "is_mod": request.session.get("mod", False),
        "google_maps_api_key": GOOGLE_MAPS_API_KEY,
    })

def require_all_mods(request: Request) -> dict:
    """Session gate for the invite-network page: admins or the all_mods
    role-management group (ALL_MODS_ROLE_IDS flag). Like require_staff, it
    deliberately does NOT require the premium member role — a role manager
    without premium still gets in, but demo users do not."""
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if int(user["id"]) not in ADMIN_USER_IDS and not request.session.get("all_mods", False):
        raise HTTPException(status_code=403, detail="Not authorized")
    return user


@app.get("/invite-network", response_class=HTMLResponse)
async def invite_network_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/login")
    if not await terms_current(request, user):
        return RedirectResponse("/terms")
    is_admin = int(user["id"]) in ADMIN_USER_IDS
    if not is_admin and not request.session.get("all_mods", False):
        raise HTTPException(status_code=403, detail="Not authorized")
    return templates.TemplateResponse("invite_network.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "is_admin": is_admin,
        "is_mod": request.session.get("mod", False),
    })


@app.get("/api/invite-network")
async def get_invite_network(request: Request, user=Depends(require_all_mods)):
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT user_id, username, joined_at, inviter_id, inviter_name
            FROM member_joins
            ORDER BY joined_at ASC
            """
        )

    # Count invites per inviter
    invite_counts: dict[str, int] = {}
    for row in rows:
        iid = str(row["inviter_id"]) if row["inviter_id"] else None
        if iid and iid != "0":
            invite_counts[iid] = invite_counts.get(iid, 0) + 1

    # Build username / joined maps
    username_map: dict[str, str] = {}
    joined_map: dict[str, str] = {}
    for row in rows:
        uid = str(row["user_id"])
        username_map[uid] = row["username"] or f"User {uid}"
        if row["joined_at"]:
            joined_map[uid] = row["joined_at"].isoformat()
        iid = str(row["inviter_id"]) if row["inviter_id"] else None
        if iid and iid != "0" and iid not in username_map:
            username_map[iid] = row["inviter_name"] or f"User {iid}"

    # Collect all unique node IDs (members + inviters without a member record)
    all_uids: list[str] = list(dict.fromkeys(
        [str(row["user_id"]) for row in rows]
        + [str(row["inviter_id"]) for row in rows if row["inviter_id"] and str(row["inviter_id"]) != "0"]
    ))

    # Phyllotaxis spiral layout
    GOLDEN_ANGLE = math.pi * (3.0 - math.sqrt(5.0))
    SCALE = 80

    def phyllotaxis(i: int) -> tuple[float, float]:
        r = SCALE * math.sqrt(i + 1)
        theta = i * GOLDEN_ANGLE
        return round(r * math.cos(theta), 2), round(r * math.sin(theta), 2)

    def node_color(invites: int) -> str:
        if invites >= 20:  return "#e74c3c"
        if invites >= 10:  return "#e67e22"
        if invites >= 5:   return "#f39c12"
        if invites >= 1:   return "#27ae60"
        return "#5865F2"

    nodes = []
    for i, uid in enumerate(all_uids):
        x, y = phyllotaxis(i)
        invites = invite_counts.get(uid, 0)
        nodes.append({
            "id":      uid,
            "label":   username_map.get(uid, f"User {uid}"),
            "uid":     uid,
            "invites": invites,
            "joined":  joined_map.get(uid),
            "color":   node_color(invites),
            "size":    max(8, min(30, 8 + invites * 1.5)),
            "x":       x,
            "y":       y,
        })

    # Build deduplicated directed edges
    edge_set: set[str] = set()
    edges = []
    for row in rows:
        uid = str(row["user_id"])
        iid = str(row["inviter_id"]) if row["inviter_id"] else None
        if iid and iid != "0" and uid != iid:
            key = f"{iid}->{uid}"
            if key not in edge_set:
                edge_set.add(key)
                edges.append({"id": key, "from": iid, "to": uid})

    return JSONResponse({"nodes": nodes, "edges": edges}, headers={"Cache-Control": "no-store"})


@app.post("/api/grading-calculator/scan")
@limiter.limit("10/minute")
async def api_grading_scan(request: Request, user=Depends(get_current_user)):
    """Identify a card from a photo via Claude Vision and return exactly the
    JSON it produced (game/name/number/set + One Piece variant flags) — no
    per-game catalog lookup here. The client matches that JSON against the
    same PPT/catalog set-and-card lists the picker already uses, so a scan
    result and a manual pick go through identical matching logic."""
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > 10 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Request body too large (max 10 MB)")

    body = await request.json()
    image_data = body.get("image")
    if not image_data:
        raise HTTPException(status_code=400, detail="No image provided")

    if "," in image_data:
        header, image_data = image_data.split(",", 1)
        media_type = header.split(";")[0].split(":")[1] if ":" in header else "image/jpeg"
    else:
        media_type = "image/jpeg"

    if media_type not in ALLOWED_MEDIA_TYPES:
        raise HTTPException(status_code=400, detail="Unsupported image type")

    try:
        image_bytes = base64.b64decode(image_data)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid base64 image")

    if anthropic_client is None:
        raise HTTPException(status_code=500, detail="Anthropic API key not configured")

    try:
        parsed = await _claude_identify(anthropic_client, image_bytes, media_type)
    except json.JSONDecodeError:
        return JSONResponse({"error": "Could not read the card — try a clearer photo."}, status_code=422)
    except Exception:
        logger.exception("Claude API error during card scan")
        return JSONResponse({"error": "Failed to process image — please try again."}, status_code=500)

    # Only normalization applied: the raw game string maps to the grading
    # calculator's own game-select values ("one piece" -> "one_piece"); every
    # other field is passed through as Claude produced it.
    raw_game = (parsed.get("game") or "other").lower().strip()
    game = {"pokemon": "pokemon", "one piece": "one_piece"}.get(raw_game, raw_game)
    card_name = parsed.get("name")
    card_number = parsed.get("number")
    is_sp = bool(parsed.get("is_sp", False))

    if card_number and re.match(r"^SP\s+", card_number, re.IGNORECASE):
        is_sp = True
        card_number = re.sub(r"^SP\s+", "", card_number, flags=re.IGNORECASE).strip()

    if not card_name:
        return JSONResponse({"error": "Could not identify a card name — try a clearer photo."}, status_code=422)

    return JSONResponse({
        "game": game,
        "name": card_name,
        "number": card_number,
        "set": parsed.get("set"),
        "is_sp": is_sp,
        "is_manga": bool(parsed.get("is_manga", False)),
        "is_alt_art": bool(parsed.get("is_alt_art", False)),
        "promo_stamp": parsed.get("promo_stamp") or None,
    }, headers={"Cache-Control": "no-store"})


# ---- Data APIs ----

@app.get("/api/regions")
async def get_regions(
    request: Request,
    user=Depends(get_current_user)
):
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT state FROM locations WHERE state IS NOT NULL ORDER BY state"
        )
    regions = []
    for r in rows:
        code = r["state"]
        label = STATE_LABELS.get(code, code)
        regions.append({"code": code, "label": label})
    return JSONResponse(regions)

@app.get("/api/restocks")
async def get_restocks(
    days: int = 7,
    request: Request = None,
    user=Depends(get_current_user)
):
    # Map slider positions to allowed day values
    _POSITION_DAYS = {1:7,2:14,3:21,4:28,5:35,6:42,7:49,8:56,9:91,10:120,11:150,12:180}
    is_admin = int(user["id"]) in ADMIN_USER_IDS
    max_position = 12 if is_admin else request.session.get("max_position", 1)
    max_days = _POSITION_DAYS.get(max_position, 7)
    # Silently clamp to the user's allowed max — no error, just cap it
    if days not in _POSITION_DAYS.values():
        days = 7
    days = min(days, max_days)

    eastern = ZoneInfo("America/New_York")
    now = datetime.now(eastern)
    since = now - timedelta(days=days)

    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                rr.location,
                rr.store_name,
                rr.channel_name,
                rr.date AT TIME ZONE 'America/New_York' AS local_date,
                l.state
            FROM restock_reports rr
            LEFT JOIN locations l
              ON LOWER(TRIM(l.location)) = LOWER(TRIM(rr.location))
              AND LOWER(TRIM(l.store_type)) = LOWER(TRIM(rr.store_name))
            WHERE rr.date >= $1
            AND (rr.channel_name IS NULL OR rr.channel_name NOT IN (
                'online-restock-information',
                'other-online-restocks',
                'pokemon-center-drops'
            ))
            ORDER BY rr.date ASC
            """,
            since
        )

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
            "region":   row["state"] or "VA",
            "date":     local_dt.strftime("%Y-%m-%d"),
            "datetime": local_dt.strftime("%b %d %I:%M %p"),
            "slot":     time_slot(local_dt),
            "via_hope": row["channel_name"] in ("hope-converted", "hope-converted-late"),
        })

    return JSONResponse(result, headers={"Cache-Control": "no-store"})

@app.get("/api/locations")
async def get_locations(
    request: Request,
    region: str = "VA",
    user=Depends(get_current_user)
):
    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")
    state = region

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

@app.get("/api/map")
async def get_map_data(
    request: Request,
    region: str = "VA",
    window: str = "day",
    user=Depends(get_current_user)
):
    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")
    if window not in ("day", "week"):
        window = "day"
    state = region

    eastern = ZoneInfo("America/New_York")
    now = datetime.now(eastern)

    if window == "week":
        since = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        since = now.replace(hour=0, minute=0, second=0, microsecond=0)

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
                rr.location,
                rr.store_name,
                rr.channel_name,
                rr.date AT TIME ZONE 'America/New_York' AS local_date
            FROM restock_reports rr
            JOIN locations l
              ON LOWER(TRIM(l.location)) = LOWER(TRIM(rr.location))
              AND LOWER(TRIM(l.store_type)) = LOWER(TRIM(rr.store_name))
            WHERE l.state = $2
              AND rr.date >= $1
              AND (rr.channel_name IS NULL OR rr.channel_name NOT IN (
                  'online-restock-information',
                  'other-online-restocks',
                  'pokemon-center-drops'
              ))
            ORDER BY rr.date ASC
            """,
            since, state
        )

    restock_map: dict[str, list] = {}
    for r in restocks:
        key = f"{r['location']}||{r['store_name']}"
        local_dt = r["local_date"]
        if key not in restock_map:
            restock_map[key] = []
        restock_map[key].append({
            "datetime": local_dt.strftime("%b %d %I:%M %p"),
            "slot": time_slot(local_dt),
            "via_hope": r["channel_name"] in ("hope-converted", "hope-converted-late"),
        })

    result = []
    for loc in locations:
        lat, lng = _extract_latlng(loc["location_link"])
        if lat is None:
            continue
        key = f"{loc['location']}||{loc['store_type']}"
        result.append({
            "location": loc["location"],
            "store":    loc["store_type"],
            "link":     loc["location_link"],
            "lat":      lat,
            "lng":      lng,
            "restocks": restock_map.get(key, []),
        })

    return JSONResponse(result, headers={"Cache-Control": "no-store"})

# ---- Preferences API ----

@app.get("/api/preferences")
async def get_preferences(
    request: Request,
    region: str = "VA",
    user=Depends(get_current_user)
):
    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")
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
    region = body.get("region", "VA")
    selected = body.get("selected", [])

    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")
    if not isinstance(selected, list):
        raise HTTPException(status_code=400, detail="Invalid payload")
    if len(selected) > 500:
        raise HTTPException(status_code=400, detail="Too many selections")
    if any(not isinstance(s, str) or len(s) > 300 for s in selected):
        raise HTTPException(status_code=400, detail="Invalid selection item")

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