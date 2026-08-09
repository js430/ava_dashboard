import os
import io
import csv
import time
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
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from anthropic import AsyncAnthropic
from PIL import Image
from dotenv import load_dotenv
from content_size_limit_asgi import ContentSizeLimitMiddleware

from card_tracker import (ensure_card_tracker_schema, sync_watchlist, run_ingest,
                          run_ppt_ingest, run_scoring, MAX_TRACKED_CARDS,
                          run_ppt_backdate, BACKDATE_DAY_CHOICES, BACKDATE_MAX_CARDS,
                          VALID_GAMES, MAX_USER_PORTFOLIO_CARDS, auto_backdate_new_card,
                          refresh_one_card, TRACKER_COLUMN_KEYS, DEFAULT_VISIBLE_COLUMNS)
import card_scoring
import set_import
import price_sources
import grading_roi
import grading_tiers
import grading_sets
import catalog
import card_eras
import population
import inventory
import tips

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


# ---- Card tracker: gap-filling price sweep ----
# The nightly ingest alone cannot guarantee a price per card per day: this is
# an in-process scheduler, so a Railway restart near 11pm silently skips that
# night, and a rate limit part-way through drops whatever came after it.
#
# The sweep closes both holes. run_ppt_ingest only prices cards with NO
# snapshot for the current UTC day, so running it repeatedly is idempotent and
# costs nothing extra — a card already priced today is skipped. Eight passes a
# day therefore cost the same credits as one, while recovering from restarts,
# throttles and transient per-card failures within hours instead of never.
CARD_SWEEP_INTERVAL_S = float(os.getenv("TRACKER_SWEEP_INTERVAL_S", str(3 * 3600)))
# Let the app finish booting (and the catalog backfill get a head start) before
# spending any quota.
CARD_SWEEP_START_DELAY_S = float(os.getenv("TRACKER_SWEEP_START_DELAY_S", "90"))


async def _card_price_sweep(app: FastAPI) -> None:
    """Fill today's price gaps, then re-score if anything was added."""
    pool = app.state.db
    result = await run_ppt_ingest(pool)
    if result["snapshots"]:
        # Scores are pure DB math — no quota — so keep them in step with the
        # prices the sweep just added rather than waiting for the nightly.
        await run_scoring(pool)
    return result


async def _card_price_sweep_scheduler(app: FastAPI) -> None:
    await asyncio.sleep(CARD_SWEEP_START_DELAY_S)
    while True:
        try:
            st = _tracker_refresh_state(app)
            if st["running"]:
                logger.info("Price sweep: a refresh is running — skipping this pass")
            else:
                result = await _card_price_sweep(app)
                logger.info("Price sweep: %d new snapshot(s), %d/%d Pokemon card(s) "
                            "priced today", result["snapshots"],
                            result["priced_today"], result["total"])
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Price sweep failed — retrying next interval")
        await asyncio.sleep(CARD_SWEEP_INTERVAL_S)


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
    try:
        await catalog.ensure_catalog_schema(app.state.db)
    except Exception:
        logger.exception("Catalog schema ensure failed — /catalog may be unavailable")
    try:
        await population.ensure_population_schema(app.state.db)
    except Exception:
        logger.exception("Population schema ensure failed — grading calculator population "
                         "reports may be unavailable")
    try:
        await inventory.ensure_inventory_schema(app.state.db)
    except Exception:
        logger.exception("Inventory schema ensure failed — /inventory may be unavailable")
    try:
        await tips.ensure_tips_schema(app.state.db)
    except Exception:
        logger.exception("Tips schema ensure failed — /tips may be unavailable")
    scheduler_task = asyncio.create_task(_card_tracker_daily_scheduler(app))
    # Background, never awaited: seeding the catalog must not delay startup or
    # take the app down if PPT is unreachable.
    catalog_backfill_task = asyncio.create_task(_catalog_backfill_startup(app))
    # Same reasoning: geocoding is an external call and shouldn't block boot,
    # or take the app down if Google Geocoding is unreachable/not yet enabled.
    tips_geocode_backfill_task = asyncio.create_task(_tips_geocode_backfill_startup(app))
    # Runs shortly after boot, which is also the catch-up for a night the
    # 11pm ingest missed because the process restarted.
    price_sweep_task = asyncio.create_task(_card_price_sweep_scheduler(app))
    catalog_price_sweep_task = asyncio.create_task(_catalog_price_sweep_scheduler(app))
    try:
        yield
    finally:
        scheduler_task.cancel()
        catalog_backfill_task.cancel()
        tips_geocode_backfill_task.cancel()
        price_sweep_task.cancel()
        catalog_price_sweep_task.cancel()
        await app.state.db.close()


app = FastAPI(lifespan=lifespan)

# ---- Static assets ----
# Brand assets (favicon, app icons, the Open Graph banner). Self-hosted, which
# the existing CSP already allows via img-src 'self' — no security change.
# Mounted only if the directory exists: StaticFiles raises on a missing
# directory, and a branding folder must never be able to stop the app booting.
# Absolute origin for Open Graph tags. og:image MUST be absolute — a relative
# path is silently ignored by unfurl crawlers — and request.base_url can't be
# trusted here because Railway terminates TLS at the proxy, so it can report
# http://. Env-overridable so a custom domain is config, not a code change.
PUBLIC_BASE_URL = os.getenv(
    "PUBLIC_BASE_URL", "https://www.nexuscardco.com").rstrip("/")
OG_TITLE = "Nexus Card Co — Restock Dashboard"
OG_DESCRIPTION = ("Historical restock data, store maps, card prices and analytics "
                  "for the TCG community, built around our Discord server.")

# link-unfurl bots (Discord, Slack, etc.) never carry a session cookie, so
# every protected page's normal 302-to-login chain runs all the way through
# to Discord's own OAuth authorize page — and the crawler renders THAT
# page's branding ("Discord — Group Chat That's All Fun & Games") instead of
# ours. login_redirect_or_preview() below intercepts known preview bots and
# hands back our own OG tags directly instead of redirecting them, so a
# shared dashboard link always previews as Nexus Card Co. Real visitors are
# unaffected — they still get the normal redirect to Discord OAuth.
LINK_PREVIEW_BOT_MARKERS = ("discordbot", "slackbot", "twitterbot", "facebookexternalhit",
                           "telegrambot", "whatsapp", "linkedinbot", "skypeuripreview",
                           "vkshare", "redditbot", "embedly")

def _is_link_preview_bot(request: Request) -> bool:
    ua = (request.headers.get("user-agent") or "").lower()
    return any(marker in ua for marker in LINK_PREVIEW_BOT_MARKERS)

def login_redirect_or_preview(request: Request, title: str = None, description: str = None):
    """Drop-in replacement for RedirectResponse("/login") on any route gated
    behind a Discord session. A detected link-preview bot gets our own OG
    tags rendered directly (no redirect followed); anyone else gets the
    normal login redirect, unchanged."""
    if _is_link_preview_bot(request):
        return templates.TemplateResponse("og_preview.html", {
            "request": request,
            "base_url": PUBLIC_BASE_URL,
            "og_title": title or OG_TITLE,
            "og_description": description or OG_DESCRIPTION,
            "og_url": f"{PUBLIC_BASE_URL}{request.url.path}",
        })
    return RedirectResponse("/login")

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
else:
    logger.warning("No static/ directory — brand assets will 404")

# ---- Rate limiter ----
def rate_limit_key(request: Request) -> str:
    """Key an authenticated request on its Discord user id — stable regardless
    of IP — rather than get_real_ip(). Railway's edge header behavior
    (X-Real-IP / X-Forwarded-For) isn't guaranteed trustworthy in every
    deployment/rollout state, so a limit keyed on IP alone can be bypassed by
    rotating the header on every request; a real Discord identity can't be
    rotated the same way. Guests have no identity yet, so they still fall
    back to IP here — the guest tier's actual cost control against IP
    spoofing is the scan endpoint's own global + per-IP daily counters
    (_guest_scan_take), not this rate limiter.
    """
    user = request.session.get("user")
    if user and user.get("id"):
        return f"user:{user['id']}"
    return f"ip:{get_real_ip(request)}"

limiter = Limiter(key_func=rate_limit_key)
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
            # tcgplayer-cdn: card images for the catalog's grid view. The URLs
            # are stored exactly as PokemonPriceTracker returns them
            # (price_sources._ppt_image_url, which accepts only https on this
            # host) — this entry and PPT_IMAGE_HOSTS must stay in step.
            "img-src 'self' data: blob: https://cdn.discordapp.com https://images.pokemontcg.io "
            "https://tcgplayer-cdn.tcgplayer.com "
            "https://api.scryfall.com https://db.ygoprodeck.com https://www.optcgapi.com "
            "https://maps.googleapis.com https://maps.gstatic.com https://assets.pokemon.com; "
            "connect-src 'self' https://maps.googleapis.com; "
            "font-src 'self' data:; "
            # frame-src back to 'none' — Tips, Tricks, and Guide's locations
            # used to embed Google's Maps Embed iframe per-location; that's
            # gone now in favor of one Maps JavaScript API map per module
            # (script-src/connect-src above), so nothing needs framing at all.
            "frame-ancestors 'none'; frame-src 'none'; "
            "object-src 'none'; base-uri 'self';"
        )
        return response

# ---- CSRF (double-submit cookie) ----
# Defense-in-depth: a security review found no working CSRF exploit today —
# every state-changing route reads a JSON body, which forces a CORS
# preflight on any cross-origin fetch, and there's no CORS allowlist
# configured at all, so the browser already blocks the real request before
# it's sent. This is the second layer in case either of those ever changes
# (a future CORS addition, a legacy browser, a same-site subdomain
# compromise). No server-side token storage needed: cross-origin JS can't
# read OR set this app's cookies (same-origin policy), so a forged request
# simply has no way to produce a header that matches the cookie. The cookie
# is minted client-side by static/csrf.js, which also attaches it as a
# header to every same-origin fetch automatically — no template needed to
# know this exists.
CSRF_COOKIE_NAME = "csrf_token"
CSRF_HEADER_NAME = "X-CSRF-Token"
CSRF_SAFE_METHODS = {"GET", "HEAD", "OPTIONS", "TRACE"}

class CSRFMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.method not in CSRF_SAFE_METHODS:
            cookie_token = request.cookies.get(CSRF_COOKIE_NAME)
            header_token = request.headers.get(CSRF_HEADER_NAME)
            if not cookie_token or not header_token or not secrets.compare_digest(cookie_token, header_token):
                return JSONResponse(
                    {"detail": "Missing or invalid CSRF token. Reload the page and try again."},
                    status_code=403)
        return await call_next(request)

app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(CSRFMiddleware)
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET"),
    max_age=60 * 60 * 24 * 7,   # 7 day session
    https_only=os.getenv("HTTPS_ONLY", "true").lower() != "false",
    same_site="lax",            # "lax" required for OAuth redirect flow
)


class RequestTooLarge(StarletteHTTPException):
    """ContentSizeLimitMiddleware instantiates its exception_cls with a single
    positional message string — this adapts that to a real HTTPException so
    FastAPI's default handling turns it into a proper 413 response, instead
    of an unhandled 500."""
    def __init__(self, message: str):
        super().__init__(status_code=413, detail=message)


MAX_REQUEST_BODY_BYTES = int(os.getenv("MAX_REQUEST_BODY_BYTES", str(10 * 1024 * 1024)))
# Global, every route — not just the photo scanner. A client-supplied
# Content-Length header was never authoritative (it can be false or absent
# entirely with chunked transfer-encoding); this counts actual bytes off the
# ASGI receive stream as they arrive and aborts before the body is fully
# buffered, regardless of what the request claims about its own size.
app.add_middleware(ContentSizeLimitMiddleware,
                   max_content_size=MAX_REQUEST_BODY_BYTES,
                   exception_cls=RequestTooLarge)

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
# Separate from every other role set on purpose: grants ONLY /inventory (the
# single-seller pick tool), not the card tracker or any admin page. Someone
# with this role but not an admin must not gain require_admin's wider reach.
INVENTORY_ROLE_IDS       = {r.strip() for r in os.getenv("INVENTORY_ROLE_IDS", "1528530585960710338").split(",") if r.strip()}
GOOGLE_MAPS_API_KEY      = os.getenv("GOOGLE_MAPS_API_KEY", "")
ANTHROPIC_API_KEY        = os.getenv("ANTHROPIC_API_KEY", "")
# Reused across requests instead of constructing a client per scan.
anthropic_client = AsyncAnthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None
ACTIVE_INFORMANT_ROLE_ID = os.getenv("ACTIVE_INFORMANT_ROLE_ID", "")
ADMIN_USER_IDS           = {
    int(uid) for uid in os.getenv("ADMIN_USER_IDS", "").split(",") if uid.strip()
}
# Where a demo/no-role Discord account goes to get the community role — the
# sample pages' existing "Get premium access" CTA, and now also the Nexus
# Playground guest-tier disclaimers extended to demo accounts.
DEMO_UPGRADE_URL = os.getenv("DEMO_UPGRADE_URL", "https://whop.com/avatcg")
# The community server itself — shown on the landing page's access-tiers breakdown.
DISCORD_INVITE_URL = os.getenv("DISCORD_INVITE_URL", "https://discord.gg/paRbvUmtVS")

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

# Guests get free but metered access to the photo scanner, since it's the one
# call in Nexus Playground that spends real Claude API money per request
# (~1-1.5 cents/scan at current Sonnet pricing). Capped per-IP per-day rather
# than per-session: a session (cookie) resets the moment a guest clears it,
# so it can't carry a cost control on its own. In-process like the catalog
# backfill's trackers — a redeploy resets it, which is the generous direction.
GUEST_SCAN_DAILY_LIMIT = int(os.getenv("GUEST_SCAN_DAILY_LIMIT", "5"))
# Independent backstop: bounds TOTAL guest-scan spend across every guest
# combined, regardless of source IP. The per-IP cap above only holds if the
# client's IP is trustworthy (see get_real_ip / rate_limit_key) — Railway's
# edge header behavior isn't guaranteed to hold across every deployment and
# rollout state, so this is the real ceiling on worst-case daily Anthropic
# spend if IP-keying is ever defeated: default 500/day * ~1-1.5c ≈ $5-7.50.
GUEST_SCAN_GLOBAL_DAILY_LIMIT = int(os.getenv("GUEST_SCAN_GLOBAL_DAILY_LIMIT", "500"))
_guest_scan_counts: dict[str, tuple[str, int]] = {}
_guest_scan_global_count = ["", 0]   # [day, count] — mutable holder, no `global` needed


def _guest_scan_take(ip: str) -> tuple[bool, str]:
    """Consume one of today's guest scans for `ip`, checked against both the
    per-IP cap and the global cap. Returns (allowed, reason) — reason is ""
    when allowed, else "global" or "per_ip" for the caller's error message.
    The global cap is checked first since it's the one that still holds even
    if `ip` isn't a trustworthy key at all.
    """
    today = datetime.utcnow().date().isoformat()

    if _guest_scan_global_count[0] != today:
        _guest_scan_global_count[0] = today
        _guest_scan_global_count[1] = 0
    if _guest_scan_global_count[1] >= GUEST_SCAN_GLOBAL_DAILY_LIMIT:
        return False, "global"

    day, count = _guest_scan_counts.get(ip, (today, 0))
    if day != today:
        count = 0
    if count >= GUEST_SCAN_DAILY_LIMIT:
        return False, "per_ip"

    _guest_scan_counts[ip] = (today, count + 1)
    _guest_scan_global_count[1] += 1
    return True, ""

# ---- OAuth state helpers (stateless HMAC — no session cookie required) ----
# This avoids browser SameSite/ITP issues where the session cookie is not sent
# after a cross-site OAuth redirect round-trip.
def _oauth_secret() -> bytes:
    return os.getenv("SESSION_SECRET", "").encode()

# 10 minutes comfortably covers a real login (Discord's consent screen plus
# slow typing), while keeping a validly-signed state from staying usable
# indefinitely — same reasoning as SCAN_TOKEN_MAX_AGE_S below, which this was
# missing until a security review flagged the gap.
OAUTH_STATE_MAX_AGE_S = int(os.getenv("OAUTH_STATE_MAX_AGE_S", str(10 * 60)))

def make_oauth_state() -> str:
    """Return a signed state token: '<timestamp>.<nonce>.<hmac-sha256-hex>'."""
    ts = str(int(time.time()))
    nonce = secrets.token_hex(24)
    msg = f"{ts}.{nonce}"
    sig = hmac.new(_oauth_secret(), msg.encode(), hashlib.sha256).hexdigest()
    return f"{msg}.{sig}"

def verify_oauth_state(state: str | None) -> bool:
    """Return True iff *state* carries a valid, non-expired HMAC signature."""
    if not state or state.count(".") != 2:
        return False
    try:
        ts, nonce, sig = state.split(".", 2)
        msg = f"{ts}.{nonce}"
        expected = hmac.new(_oauth_secret(), msg.encode(), hashlib.sha256).hexdigest()
        if not secrets.compare_digest(sig, expected):
            return False
        return (time.time() - int(ts)) <= OAUTH_STATE_MAX_AGE_S
    except Exception:
        return False

# ---- Scan-page proof token (lightweight anti-automation friction) ----
# Not a CAPTCHA — no external service or account to provision. A signed,
# short-lived token minted only when the Grading Calculator page itself
# renders, which the client echoes back on /scan. A script hitting /scan
# directly (never having rendered the page) has no valid token to send.
# Reuses SESSION_SECRET like the OAuth state token above, with a distinct
# purpose tag baked into the signed message so one token can never be
# replayed as the other. Deliberately reusable within its window (not
# single-use) — a real guest scanning several cards in one visit shouldn't
# need to reload between each one; the actual per-scan cost control is
# _guest_scan_take above, not this token.
SCAN_TOKEN_MAX_AGE_S = int(os.getenv("SCAN_TOKEN_MAX_AGE_S", str(30 * 60)))

def make_scan_token() -> str:
    ts = str(int(time.time()))
    sig = hmac.new(_oauth_secret(), f"scan.{ts}".encode(), hashlib.sha256).hexdigest()
    return f"{ts}.{sig}"

def verify_scan_token(token: str | None) -> bool:
    if not token or "." not in token:
        return False
    try:
        ts, sig = token.split(".", 1)
        expected = hmac.new(_oauth_secret(), f"scan.{ts}".encode(), hashlib.sha256).hexdigest()
        if not secrets.compare_digest(sig, expected):
            return False
        return (time.time() - int(ts)) <= SCAN_TOKEN_MAX_AGE_S
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


def get_current_discord_user(request: Request):
    """Any real Discord identity — full member OR role-less demo account —
    but not an anonymous guest. For features open to the whole Discord
    regardless of paid role (Tips, Tricks, and Guide), unlike get_current_user (members
    only, blocks demo) or get_current_user_or_guest (also allows anonymous
    guests)."""
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Sign in with Discord to use this.")
    return user

# ---- Guest access (Nexus Playground only) ----
# A guest is a visitor with NO Discord identity at all — distinct from
# `is_demo`, which is a Discord-authenticated member without the community
# role. Guests exist only to reach Grading Calculator + Card Catalog
# (Nexus Playground minus Tracker, which stays admin-only); every other route
# still requires real Discord auth. Session-only, no DB row — clearing
# cookies or hitting /logout ends it, same as any other session flag.
GUEST_USER = {"id": None, "username": "Guest", "avatar": None, "guest": True}


def is_guest(request: Request) -> bool:
    return bool(request.session.get("guest"))


def get_current_user_or_guest(request: Request):
    """Like `get_current_user`, but a guest session — OR a role-less Discord
    ("demo"/"sample") session — also passes, both at the guest tier.

    A demo session's real user dict is returned (not the anonymous
    GUEST_USER placeholder) with `guest: True` added, so every existing
    guest-tier restriction downstream (they all key off `user.get("guest")`)
    applies to demo accounts automatically, while logging/audit still sees
    their real Discord identity.

    Only wired into the Grading Calculator and Card Catalog APIs — every
    other endpoint keeps `get_current_user`, which still closes the door on
    both guest and demo sessions; the main dashboard stays members-only.
    """
    user = request.session.get("user")
    if user:
        if is_demo(request):
            return {**user, "guest": True}
        return user
    if is_guest(request):
        return GUEST_USER
    raise HTTPException(status_code=401, detail="Not authenticated")


def _viewer_context(request: Request, user) -> dict:
    """Common template vars for a page open to full members, guests (no
    Discord identity), and demo/role-less Discord accounts — the latter two
    both render as the guest tier (`is_guest: True`) here. `user` is the
    session's Discord user dict, or None for a guest.

    This only backs the two Nexus Playground pages, which extend guest-tier
    access to demo accounts — the main dashboard's `is_demo` check (redirect
    to /sample) is untouched and lives in its own routes.
    """
    if user is None:
        return {"username": GUEST_USER["username"], "avatar": None, "user_id": None,
                "is_admin": False, "is_mod": False, "is_guest": True,
                "upgrade_url": DEMO_UPGRADE_URL}
    if is_demo(request):
        return {"username": user["username"], "avatar": user.get("avatar"),
                "user_id": user["id"], "is_admin": False, "is_mod": False, "is_guest": True,
                "upgrade_url": DEMO_UPGRADE_URL}
    return {"username": user["username"], "avatar": user.get("avatar"),
            "user_id": user["id"], "is_admin": int(user["id"]) in ADMIN_USER_IDS,
            "is_mod": request.session.get("mod", False), "is_guest": False,
            "upgrade_url": DEMO_UPGRADE_URL}

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
        # Public landing page rather than an immediate bounce to Discord OAuth.
        # This is the ONLY page an anonymous request can reach, so it's the only
        # place Open Graph tags can live — without it the dashboard link unfurls
        # in Discord as whatever the OAuth redirect chain ends on. Signed-in
        # users never see it; every other route still redirects to /login.
        return templates.TemplateResponse("landing.html", {
            "request": request,
            "base_url": PUBLIC_BASE_URL,
            "og_title": OG_TITLE,
            "og_description": OG_DESCRIPTION,
            "discord_invite_url": DISCORD_INVITE_URL,
            "upgrade_url": DEMO_UPGRADE_URL,
            "guest_catalog_visible_sets": GUEST_CATALOG_VISIBLE_SETS,
        })
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
        return login_redirect_or_preview(request)
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
        "upgrade_url": DEMO_UPGRADE_URL,
    })

@app.get("/sample-status", response_class=HTMLResponse)
async def sample_status_page(request: Request):
    """Demo store-status page with fake data. Same audience as /sample."""
    user = request.session.get("user")
    if not user:
        return login_redirect_or_preview(request)
    is_staff = int(user["id"]) in ADMIN_USER_IDS or request.session.get("mod", False)
    if not is_demo(request) and not is_staff:
        return RedirectResponse("/status")
    return templates.TemplateResponse("sample_status.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "upgrade_url": DEMO_UPGRADE_URL,
    })

@app.get("/sample-map", response_class=HTMLResponse)
async def sample_map_page(request: Request):
    """Demo map page with fake data and a self-contained mock map (no Google
    Maps API). Same audience as /sample."""
    user = request.session.get("user")
    if not user:
        return login_redirect_or_preview(request)
    is_staff = int(user["id"]) in ADMIN_USER_IDS or request.session.get("mod", False)
    if not is_demo(request) and not is_staff:
        return RedirectResponse("/map")
    return templates.TemplateResponse("sample_map.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "upgrade_url": DEMO_UPGRADE_URL,
    })

@app.get("/terms", response_class=HTMLResponse)
async def terms_page(request: Request):
    user = request.session.get("user")
    if not user:
        return login_redirect_or_preview(request)
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

@app.get("/guest", response_class=HTMLResponse)
async def guest_entry(request: Request):
    """Public entry point: no Discord account needed. Marks the session as a
    guest and lands on a menu of the Nexus Playground tools guests may
    use — Grading Calculator, Card Catalog, and the read-only Card Tracker
    trial (require_trial_viewer allows guests too)."""
    request.session["guest"] = True
    return templates.TemplateResponse("guest_home.html", {
        "request": request,
        "guest_scan_daily_limit": GUEST_SCAN_DAILY_LIMIT,
    })

@app.get("/signin", response_class=HTMLResponse)
async def signin_placeholder(request: Request):
    """Standing entry point for a future non-Discord account system (likely
    subscription-gated eventually). No accounts exist yet — this just tells
    visitors that and points them at what does work today."""
    return templates.TemplateResponse("signin.html", {"request": request})

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
    request.session["inventory_access"] = bool(INVENTORY_ROLE_IDS & set(member_roles)) or is_admin_user
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
        return login_redirect_or_preview(request)
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

def require_trial_viewer(request: Request):
    """Session gate for the card-tracker trial (/sample-card-tracker and its
    read-only API): guests (no Discord identity), demo/no-role members, and
    staff previewing it — same guest-tier extension as the Grading
    Calculator/Catalog. A real premium member has the live tracker already
    and has no reason to be here. Returns None for a guest (the trial
    endpoints never look at the caller's own id, only
    TRIAL_PORTFOLIO_USER_ID's), or the session's user dict otherwise."""
    if is_guest(request):
        return None
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    is_staff = int(user["id"]) in ADMIN_USER_IDS or request.session.get("mod", False)
    if not is_demo(request) and not is_staff:
        raise HTTPException(status_code=403, detail="Not available to premium members")
    return user

def require_inventory_access(request: Request) -> dict:
    """Gate for /inventory: admins, plus INVENTORY_ROLE_IDS via the
    'inventory_access' session flag set at login. Deliberately separate from
    require_admin — this role must open ONLY the pick tool, not the card
    tracker or any admin page."""
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if int(user["id"]) not in ADMIN_USER_IDS and not request.session.get("inventory_access", False):
        raise HTTPException(status_code=403, detail="Not authorized")
    return user

# ---- Sports card inventory / eBay pick tool ----
# Unrelated to the restock-tracking data this app otherwise reads — a
# standalone single-seller tool sharing this app's DB and auth for
# convenience. See inventory.py for the SKU-as-address design. Manual intake
# only: eBay sync and photo-to-metadata extraction are not built yet, and
# nothing here fakes them.
#
# Access: admins, plus INVENTORY_ROLE_IDS (require_inventory_access). That
# role opens ONLY this page — not the card tracker or any admin page.

@app.get("/inventory", response_class=HTMLResponse)
async def inventory_page(request: Request):
    user = request.session.get("user")
    if not user:
        return login_redirect_or_preview(request)
    is_admin_user = int(user["id"]) in ADMIN_USER_IDS
    if not is_admin_user and not request.session.get("inventory_access", False):
        raise HTTPException(status_code=403, detail="Not authorized")
    return templates.TemplateResponse("inventory.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "is_admin": is_admin_user,
        "is_mod": request.session.get("mod", False),
        "statuses": inventory.STATUSES,
    })


@app.get("/api/inventory/zones")
async def api_inventory_zones(request: Request, user=Depends(require_inventory_access)):
    """Zones with their bins and computed occupancy — the overview grid."""
    zones = await inventory.list_zones_with_bins(request.app.state.db)
    return JSONResponse({"zones": zones}, headers={"Cache-Control": "no-store"})


@app.get("/api/inventory/cards")
async def api_inventory_cards(request: Request, status: str = "", bin_id: int = 0,
                              search: str = "", user=Depends(require_inventory_access)):
    """Card list, optionally filtered by status / bin / a loose text search
    across player, set and card number — card naming is chaos, per the design
    notes, so this is intentionally forgiving rather than an exact match."""
    where = ["1=1"]
    params = []
    if status:
        if status not in inventory.STATUSES:
            raise HTTPException(status_code=400, detail="Unknown status")
        params.append(status)
        where.append(f"c.status = ${len(params)}")
    if bin_id:
        params.append(bin_id)
        where.append(f"c.bin_id = ${len(params)}")
    if search:
        params.append(f"%{search.strip()[:80]}%")
        where.append(
            f"(c.player ILIKE ${len(params)} OR c.set_name ILIKE ${len(params)} "
            f"OR c.card_number ILIKE ${len(params)})")
    query = (
        "SELECT c.id, c.status, c.needs_location, c.cant_find, c.player, c.year, "
        "c.manufacturer, c.set_name, c.card_number, c.parallel, c.features, c.sport, "
        "c.is_graded, c.grading_company, c.grade, c.cert_number, c.price, "
        "c.ebay_listing_id, c.ebay_sku, c.notes, b.code AS bin_code, "
        "c.created_at, c.updated_at "
        "FROM inv_cards c LEFT JOIN inv_bins b ON b.id = c.bin_id "
        f"WHERE {' AND '.join(where)} ORDER BY c.updated_at DESC LIMIT 200")
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query, *params)

    def _row(r):
        d = dict(r)
        d["price"] = float(d["price"]) if d["price"] is not None else None
        d["created_at"] = d["created_at"].isoformat() if d["created_at"] else None
        d["updated_at"] = d["updated_at"].isoformat() if d["updated_at"] else None
        return d
    return JSONResponse({"cards": [_row(r) for r in rows]},
                        headers={"Cache-Control": "no-store"})


@app.post("/api/inventory/cards")
async def api_inventory_create_card(request: Request, user=Depends(require_inventory_access)):
    """Manual intake: create a card and immediately stow it into a zone's
    fill-pointer bin. One call, matching "capture location at photo time" —
    there's no unassigned/unstowed state to leave a card sitting in."""
    body = await request.json()
    zone_id = body.get("zone_id")
    if not zone_id:
        raise HTTPException(status_code=400, detail="zone_id is required")

    def _s(key, limit=120):
        return str(body.get(key) or "").strip()[:limit]

    price = body.get("price")
    try:
        price = float(price) if price not in (None, "") else None
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="price must be a number")

    pool = request.app.state.db
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO inv_cards (player, year, manufacturer, set_name, card_number,
                parallel, features, sport, is_graded, grading_company, grade,
                cert_number, price, notes)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
            RETURNING id
            """,
            _s("player"), _s("year", 10), _s("manufacturer"), _s("set_name"),
            _s("card_number", 40), _s("parallel"), _s("features", 300), _s("sport", 40),
            bool(body.get("is_graded")), _s("grading_company", 40), _s("grade", 20),
            _s("cert_number", 40), price, _s("notes", 500))
        card_id = row["id"]
        await inventory.log_event(conn, card_id, "created", to_status="available",
                                  actor=str(user["id"]))

    try:
        b = await inventory.stow_card(pool, card_id, int(zone_id), actor=str(user["id"]))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse({"id": card_id, "bin": b}, headers={"Cache-Control": "no-store"})


@app.post("/api/inventory/cards/{card_id}/status")
async def api_inventory_set_status(request: Request, card_id: int,
                                   user=Depends(require_inventory_access)):
    body = await request.json()
    to_status = str(body.get("status") or "").strip()
    try:
        await inventory.set_status(request.app.state.db, card_id, to_status,
                                   actor=str(user["id"]), note=str(body.get("note") or "")[:300])
    except inventory.TransitionError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return JSONResponse({"ok": True}, headers={"Cache-Control": "no-store"})


@app.post("/api/inventory/cards/{card_id}/cant-find")
async def api_inventory_cant_find(request: Request, card_id: int,
                                  user=Depends(require_inventory_access)):
    """Fast cancel path from the pick queue — flags the card for review rather
    than blocking or guessing."""
    body = await request.json()
    await inventory.flag_cant_find(request.app.state.db, card_id, actor=str(user["id"]),
                                   note=str(body.get("note") or "")[:300])
    return JSONResponse({"ok": True}, headers={"Cache-Control": "no-store"})


@app.get("/api/inventory/pick-queue")
async def api_inventory_pick_queue(request: Request, user=Depends(require_inventory_access)):
    groups = await inventory.pick_queue(request.app.state.db)
    return JSONResponse({"groups": groups}, headers={"Cache-Control": "no-store"})


@app.get("/api/inventory/needs-location")
async def api_inventory_needs_location(request: Request, user=Depends(require_inventory_access)):
    cards = await inventory.needs_location_inbox(request.app.state.db)
    return JSONResponse({"cards": cards}, headers={"Cache-Control": "no-store"})


@app.get("/card-tracker", response_class=HTMLResponse)
async def card_tracker_page(request: Request):
    # Same member/sample split as the real dashboard ("/") — any paid member
    # gets their own portfolio; admins additionally see the Full Catalog
    # admin tools. No longer admin-only.
    user = request.session.get("user")
    if not user:
        return login_redirect_or_preview(
            request, title="Card Tracker — Nexus Card Co",
            description=f"Track up to {MAX_USER_PORTFOLIO_CARDS} Pokémon or One Piece cards — "
                        "daily price history, momentum, and a profit-potential score. Sign in "
                        "with Discord to build your portfolio.")
    if is_demo(request):
        return RedirectResponse("/sample-card-tracker")
    return templates.TemplateResponse("card_tracker.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "is_admin": int(user["id"]) in ADMIN_USER_IDS,
        "is_mod": request.session.get("mod", False),
        "max_portfolio_cards": MAX_USER_PORTFOLIO_CARDS,
        "default_visible_columns": DEFAULT_VISIBLE_COLUMNS,
    })

# The trial's data source: a real, curated portfolio (not synthetic data like
# /sample's) shown read-only to demo members as a taste of the live tracker.
# Owned by this one Discord user deliberately — an admin curates it exactly
# like their own portfolio, via the same Add/Remove tools on the real page.
TRIAL_PORTFOLIO_USER_ID = 96718322170597376
TRIAL_PORTFOLIO_SAMPLE_SIZE = 10

@app.get("/sample-card-tracker", response_class=HTMLResponse)
async def sample_card_tracker_page(request: Request):
    """Read-only trial: guests (no Discord account), demo/no-role members,
    and staff previewing it — up to 10 random cards from
    TRIAL_PORTFOLIO_USER_ID's real portfolio, view + sort/filter only, no
    add/remove/anything else. Same guest-tier extension as the Grading
    Calculator/Catalog; a real premium member is sent to their own
    portfolio instead."""
    user = request.session.get("user")
    if user:
        is_staff = int(user["id"]) in ADMIN_USER_IDS or request.session.get("mod", False)
        if not is_demo(request) and not is_staff:
            return RedirectResponse("/card-tracker")
    elif not is_guest(request):
        return login_redirect_or_preview(
            request, title="Card Tracker Trial — Nexus Card Co",
            description="See the Card Tracker in action with a live sample portfolio. "
                        "Sign in with Discord or continue as a guest to try it.")
    return templates.TemplateResponse("sample_card_tracker.html", {
        "request": request,
        **_viewer_context(request, user),
        # The upsell copy quotes the portfolio size; passing it keeps that
        # promise in step with the real cap instead of drifting.
        "max_portfolio_cards": MAX_USER_PORTFOLIO_CARDS,
    })

def _f_num(x):
    return float(x) if x is not None else None

def _canon_ident(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())

def _serialize_tracker_row(r) -> dict:
    """Shared row->JSON shape for the admin Full Catalog list and a member's
    Portfolio list — same columns, only the SQL's WHERE/JOIN differs between
    the two callers."""
    return {
        "id": r["id"],
        "name": r["name"],
        "game": r["game"],
        "language": r["language"],
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
        "price_low": _f_num(r["price_low"]),
        "price_mid": _f_num(r["price_mid"]),
        "price_high": _f_num(r["price_high"]),
        "captured_at": r["captured_at"].isoformat() if r["captured_at"] else None,
        "momentum_7d": _f_num(r["momentum_7d"]),
        "momentum_30d": _f_num(r["momentum_30d"]),
        "momentum_180d": _f_num(r["momentum_180d"]),
        "liquidity_score": _f_num(r["liquidity_score"]),
        "age_days": r["age_days"],
        "potential_score": _f_num(r["potential_score"]),
        "computed_at": r["computed_at"].isoformat() if r["computed_at"] else None,
    }

_TRACKER_ROW_COLUMNS = """
    tc.id, tc.name, tc.game, tc.language, tc.set_name, tc.card_number, tc.variant,
    tc.release_date, tc.justtcg_name, tc.justtcg_set, tc.justtcg_number,
    ps.price_low, ps.price_mid, ps.price_high, ps.captured_at,
    cs.momentum_7d, cs.momentum_30d, cs.momentum_180d, cs.liquidity_score,
    cs.age_days, cs.potential_score, cs.computed_at
"""
_TRACKER_ROW_JOINS = """
    LEFT JOIN LATERAL (
        SELECT * FROM price_snapshots WHERE card_id = tc.id
        ORDER BY captured_at DESC LIMIT 1
    ) ps ON true
    LEFT JOIN LATERAL (
        SELECT * FROM card_scores WHERE card_id = tc.id
        ORDER BY computed_at DESC LIMIT 1
    ) cs ON true
"""

@app.get("/api/card-tracker/list")
async def api_card_tracker_list(request: Request, user=Depends(require_admin)):
    """Full shared catalog — admin-only 'Full Catalog' view."""
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT {_TRACKER_ROW_COLUMNS}
            FROM tracked_cards tc
            {_TRACKER_ROW_JOINS}
            ORDER BY cs.potential_score DESC NULLS LAST, tc.name ASC
            """
        )
    return JSONResponse([_serialize_tracker_row(r) for r in rows],
                        headers={"Cache-Control": "no-store"})

@app.get("/api/card-tracker/portfolio")
async def api_portfolio_list(request: Request, user=Depends(get_current_user)):
    """The caller's own tracked cards — any paid member, not just admins."""
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT {_TRACKER_ROW_COLUMNS}
            FROM user_tracked_cards utc
            JOIN tracked_cards tc ON tc.id = utc.card_id
            {_TRACKER_ROW_JOINS}
            WHERE utc.user_id = $1
            ORDER BY cs.potential_score DESC NULLS LAST, tc.name ASC
            """, int(user["id"]))
    return JSONResponse([_serialize_tracker_row(r) for r in rows],
                        headers={"Cache-Control": "no-store"})

@app.get("/api/card-tracker/trial")
async def api_trial_list(request: Request, user=Depends(require_trial_viewer)):
    """Up to TRIAL_PORTFOLIO_SAMPLE_SIZE random cards from
    TRIAL_PORTFOLIO_USER_ID's real portfolio — read-only, re-randomized on
    every call. Detail lookups (api_trial_history) aren't limited to this
    exact sample, so re-randomizing on reload never makes a card
    unreachable."""
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT {_TRACKER_ROW_COLUMNS}
            FROM user_tracked_cards utc
            JOIN tracked_cards tc ON tc.id = utc.card_id
            {_TRACKER_ROW_JOINS}
            WHERE utc.user_id = $1
            ORDER BY RANDOM()
            LIMIT $2
            """, TRIAL_PORTFOLIO_USER_ID, TRIAL_PORTFOLIO_SAMPLE_SIZE)
    return JSONResponse([_serialize_tracker_row(r) for r in rows],
                        headers={"Cache-Control": "no-store"})

@app.post("/api/card-tracker/portfolio/search")
@limiter.limit("60/hour")
async def api_portfolio_search(request: Request, user=Depends(get_current_user)):
    """Free (no PPT/JustTCG credits) — searches the already-populated
    catalog_cards table so a member can find a card to add without spending
    anything. If a card genuinely isn't in the catalog yet, it isn't
    findable here; the same limitation resolve_tcgplayer_ids already has.

    `language` is optional — English and Japanese are separate products
    (see catalog.py), so when the caller picks one (the card-tracker page's
    Pokemon-only language dropdown) results are filtered to it; omitted
    (One Piece, which has no language split in this app) returns matches
    across whatever languages exist, each labeled so they're distinguishable
    either way."""
    body = await request.json()
    game = body.get("game", "")
    query = (body.get("query") or "").strip()
    language = body.get("language")
    if game not in VALID_GAMES:
        raise HTTPException(status_code=400, detail="game must be 'pokemon' or 'one_piece'")
    if len(query) < 2:
        raise HTTPException(status_code=400, detail="query must be at least 2 characters")
    if language is not None:
        language = price_sources.ppt_language(language)
    async with request.app.state.db.acquire() as conn:
        if language:
            rows = await conn.fetch(
                "SELECT DISTINCT ON (card_name, set_name, card_number, rarity, language) "
                "card_name, set_name, card_number, rarity, tcgplayer_id, language "
                "FROM catalog_cards WHERE game = $1 AND language = $2 AND card_name ILIKE $3 "
                "ORDER BY card_name, set_name, card_number, rarity, language LIMIT 25",
                game, language, f"%{query}%")
        else:
            rows = await conn.fetch(
                "SELECT DISTINCT ON (card_name, set_name, card_number, rarity, language) "
                "card_name, set_name, card_number, rarity, tcgplayer_id, language "
                "FROM catalog_cards WHERE game = $1 AND card_name ILIKE $2 "
                "ORDER BY card_name, set_name, card_number, rarity, language LIMIT 25",
                game, f"%{query}%")
    return JSONResponse([
        {"name": r["card_name"], "set_name": r["set_name"], "card_number": r["card_number"],
         "variant": r["rarity"] or None, "tcgplayer_id": r["tcgplayer_id"] or None,
         "language": r["language"]}
        for r in rows
    ], headers={"Cache-Control": "no-store"})

@app.post("/api/card-tracker/portfolio/add")
@limiter.limit("60/hour")
async def api_portfolio_add(request: Request, user=Depends(get_current_user)):
    """Add one card (from a /portfolio/search result) to the caller's
    portfolio. Ensures the card exists in the shared catalog first (free
    insert if it's genuinely new — Pokemon adds already carry a
    tcgplayer_id from the search, skipping the usual free-resolve step
    entirely), then auto-backdates it exactly like any other brand-new
    catalog card if this member is the first to ever track it."""
    body = await request.json()
    game = body.get("game", "")
    name = (body.get("name") or "").strip()[:200]
    set_name = (body.get("set_name") or "").strip()[:200]
    card_number = (body.get("card_number") or "").strip()[:40]
    variant = (body.get("variant") or "").strip()[:200] or None
    tcgplayer_id = (body.get("tcgplayer_id") or "").strip()[:40] or None
    language = price_sources.ppt_language(body.get("language"))
    if game not in VALID_GAMES or not name:
        raise HTTPException(status_code=400, detail="game and name required")

    user_id = int(user["id"])
    pool = request.app.state.db
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM user_tracked_cards WHERE user_id = $1", user_id)
        if count >= MAX_USER_PORTFOLIO_CARDS:
            raise HTTPException(
                status_code=400,
                detail=f"Your portfolio is full ({MAX_USER_PORTFOLIO_CARDS} cards) "
                       f"— remove a card before adding another")

        async with conn.transaction():
            insert_row = await conn.fetchrow(
                "INSERT INTO tracked_cards (name, game, set_name, card_number, variant, "
                "tcgplayer_id, language) VALUES ($1, $2, $3, $4, $5, $6, $7) "
                "ON CONFLICT (game, name, set_name, card_number, language) DO NOTHING RETURNING id",
                name, game, set_name, card_number,
                variant, tcgplayer_id if game == "pokemon" else None, language)
            is_new = insert_row is not None
            if is_new:
                card_id = insert_row["id"]
                total = await conn.fetchval("SELECT COUNT(*) FROM tracked_cards")
                if total > MAX_TRACKED_CARDS:
                    raise HTTPException(
                        status_code=400,
                        detail=f"The shared card catalog is at its cap "
                               f"({MAX_TRACKED_CARDS} cards) — this particular "
                               f"card isn't tracked by anyone yet, so it can't be added right "
                               f"now. An admin can raise the cap in card_tracker.py.")
            else:
                existing = await conn.fetchrow(
                    "SELECT id FROM tracked_cards WHERE game=$1 AND name=$2 "
                    "AND set_name=$3 AND card_number=$4 AND language=$5",
                    game, name, set_name, card_number, language)
                card_id = existing["id"]
            await conn.execute(
                "INSERT INTO user_tracked_cards (user_id, card_id) VALUES ($1, $2) "
                "ON CONFLICT DO NOTHING", user_id, card_id)
        new_count = count + 1

    refresh = {"snapshot_added": False, "credits": 0, "scored": False}
    async with httpx.AsyncClient(timeout=30.0) as client:
        if is_new and game == "pokemon" and tcgplayer_id:
            await auto_backdate_new_card(client, pool, card_id, name, tcgplayer_id, language=language)
        # Whether new or already in the shared catalog — this fills in
        # today's price (if missing) and a fresh score right away, so the
        # 7d/30d/score columns aren't blank until the next scheduled sweep.
        try:
            refresh = await refresh_one_card(client, pool, card_id)
        except Exception:
            logger.exception("Portfolio: post-add refresh failed for %r (id %d)", name, card_id)

    logger.info("Portfolio: user %s added %r (card_id=%d, new_to_catalog=%s, refresh=%s)",
                user_id, name, card_id, is_new, refresh)
    return JSONResponse({"ok": True, "card_id": card_id, "portfolio_count": new_count,
                        "new_to_catalog": is_new, "scored": refresh["scored"]},
                       headers={"Cache-Control": "no-store"})

@app.post("/api/card-tracker/portfolio/remove")
@limiter.limit("60/hour")
async def api_portfolio_remove(request: Request, user=Depends(get_current_user)):
    """Unlink cards from the caller's OWN portfolio only — never touches
    tracked_cards/price_snapshots, so the card and its history survive for
    anyone else still tracking it. No cap; unlinking is cheap."""
    body = await request.json()
    ids = body.get("card_ids")
    if not isinstance(ids, list) or not ids or not all(isinstance(i, int) for i in ids):
        raise HTTPException(status_code=400, detail="card_ids (non-empty list of int) required")
    user_id = int(user["id"])
    async with request.app.state.db.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM user_tracked_cards WHERE user_id = $1 AND card_id = ANY($2::int[])",
            user_id, ids)
    removed = int(result.split()[-1]) if result else 0
    return JSONResponse({"ok": True, "removed": removed}, headers={"Cache-Control": "no-store"})

@app.get("/api/card-tracker/prefs")
async def api_tracker_prefs_get(request: Request, user=Depends(get_current_user)):
    """Saved column-visibility choice for the card-tracker table. null
    visible_columns means no preference saved yet — the page falls back to
    DEFAULT_VISIBLE_COLUMNS client-side."""
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT visible_columns FROM card_tracker_prefs WHERE user_id = $1",
            int(user["id"]))
    columns = None
    if row:
        columns = row["visible_columns"]
        if isinstance(columns, str):   # asyncpg hands back JSONB as text absent a registered codec
            try:
                columns = json.loads(columns)
            except (TypeError, ValueError):
                columns = None
    return JSONResponse(
        {"visible_columns": columns, "default_visible_columns": DEFAULT_VISIBLE_COLUMNS},
        headers={"Cache-Control": "no-store"})

@app.post("/api/card-tracker/prefs")
@limiter.limit("30/hour")
async def api_tracker_prefs_save(request: Request, user=Depends(get_current_user)):
    body = await request.json()
    columns = body.get("visible_columns")
    if (not isinstance(columns, list)
            or not all(isinstance(c, str) and c in TRACKER_COLUMN_KEYS for c in columns)):
        raise HTTPException(
            status_code=400,
            detail=f"visible_columns must be a list drawn from {TRACKER_COLUMN_KEYS}")
    # Dedupe while preserving order — a stray double-toggle client-side
    # shouldn't be able to write duplicate entries into storage.
    columns = list(dict.fromkeys(columns))
    async with request.app.state.db.acquire() as conn:
        await conn.execute(
            "INSERT INTO card_tracker_prefs (user_id, visible_columns, updated_at) "
            "VALUES ($1, $2::jsonb, NOW()) "
            "ON CONFLICT (user_id) DO UPDATE SET visible_columns = EXCLUDED.visible_columns, "
            "updated_at = NOW()",
            int(user["id"]), json.dumps(columns))
    return JSONResponse({"ok": True, "visible_columns": columns},
                        headers={"Cache-Control": "no-store"})

async def _card_history_payload(conn, card_id: int):
    """Shared body for api_card_tracker_history and api_trial_history — full
    price history + a live score explanation for one card. None if the card
    doesn't exist."""
    def _f(x):
        return float(x) if x is not None else None
    card = await conn.fetchrow(
        "SELECT id, name, game, language, set_name, card_number, variant, release_date, "
        "justtcg_name, justtcg_set, justtcg_number "
        "FROM tracked_cards WHERE id = $1", card_id)
    if card is None:
        return None
    snaps = await conn.fetch(
        "SELECT captured_at, price_low, price_mid, price_high FROM price_snapshots "
        "WHERE card_id = $1 ORDER BY captured_at ASC", card_id)
    # Recompute the full component breakdown live so the UI can show WHY the
    # card scores what it does (card_scores stores only the headline numbers).
    explain = card_scoring.score_card([dict(s) for s in snaps], card["release_date"]) if snaps else None
    return {
        "card": {
            "id": card["id"], "name": card["name"], "game": card["game"],
            "language": card["language"],
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
    }

@app.get("/api/card-tracker/history")
async def api_card_tracker_history(request: Request, card_id: int, user=Depends(get_current_user)):
    async with request.app.state.db.acquire() as conn:
        payload = await _card_history_payload(conn, card_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="Card not found")
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})

@app.get("/api/card-tracker/trial/history")
async def api_trial_history(request: Request, card_id: int, user=Depends(require_trial_viewer)):
    """Same payload as api_card_tracker_history, restricted to cards that are
    actually in TRIAL_PORTFOLIO_USER_ID's portfolio — a trial viewer can look
    up any card that's ever been in the demo portfolio, not just whichever
    TRIAL_PORTFOLIO_SAMPLE_SIZE the random sample happened to include on this
    particular load."""
    async with request.app.state.db.acquire() as conn:
        owned = await conn.fetchval(
            "SELECT 1 FROM user_tracked_cards WHERE user_id = $1 AND card_id = $2",
            TRIAL_PORTFOLIO_USER_ID, card_id)
        if not owned:
            raise HTTPException(status_code=404, detail="Card not found")
        payload = await _card_history_payload(conn, card_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="Card not found")
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})

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
        # Pokemon via PokemonPriceTracker (resolved free from catalog_cards),
        # One Piece via JustTCG. Split deliberately: see card_tracker.run_ingest.
        ppt = await run_ppt_ingest(pool)
        ingest = await run_ingest(pool)
        scoring = await run_scoring(pool)
        st["result"] = {
            "watchlist_added": added,
            # Both sources contribute snapshots; the totals are summed for the
            # headline and broken out below so a failure in one is still visible.
            "snapshots": ppt["snapshots"] + ingest["snapshots"],
            "resolved": ppt["resolved"] + ingest["resolved"],
            "backfilled": ingest.get("backfilled", 0),
            "failures": (ppt["failed"] + ingest["failed"])[:20],
            "justtcg_calls": ingest.get("justtcg_calls", 0),
            "ppt_credits": ppt["credits"],
            "ppt_snapshots": ppt["snapshots"],
            "ppt_cards": ppt["cards"],
            "ppt_rate_limited": ppt["rate_limited"],
            # The per-day guarantee, as a number rather than an assumption.
            "priced_today": ppt["priced_today"],
            "tracked_total": ppt["total"],
            "missing_today": ppt["missing"],
            "onepiece_snapshots": ingest["snapshots"],
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

def _validate_card_ids(body: dict) -> list:
    """No count cap — removal is just a DELETE, cheap regardless of how many
    cards are selected. The cap belongs to backdate (see
    _validate_backdate_card_ids), which spends PPT credits per card."""
    ids = body.get("card_ids")
    if not isinstance(ids, list) or not ids or not all(isinstance(i, int) for i in ids):
        raise HTTPException(status_code=400, detail="card_ids (non-empty list of int) required")
    return ids

def _validate_backdate_card_ids(body: dict) -> list:
    ids = _validate_card_ids(body)
    if len(ids) > BACKDATE_MAX_CARDS:
        raise HTTPException(status_code=400,
                            detail=f"Select at most {BACKDATE_MAX_CARDS} cards at a time for backdating")
    return ids

@app.post("/api/card-tracker/remove")
@limiter.limit("30/hour")
async def api_card_tracker_remove(request: Request, user=Depends(require_admin)):
    """Permanently purge selected cards from the SHARED catalog (cascades to
    their price history, scores, AND every member's portfolio — this is not
    the same as a member removing a card from their own portfolio via
    /api/card-tracker/portfolio/remove, which never touches the shared data).
    Cards seeded from watchlist.py reappear on the next refresh unless also
    removed from that file — sync_watchlist never deletes."""
    body = await request.json()
    card_ids = _validate_card_ids(body)
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name FROM tracked_cards WHERE id = ANY($1::int[])", card_ids)
        if not rows:
            raise HTTPException(status_code=404, detail="No matching cards found")
        await conn.execute("DELETE FROM tracked_cards WHERE id = ANY($1::int[])", card_ids)
    logger.info("Card tracker: removed %d card(s) by admin %s: %s",
                len(rows), user["id"], ", ".join(r["name"] for r in rows))
    return JSONResponse({"ok": True, "removed": len(rows),
                        "names": [r["name"] for r in rows]},
                       headers={"Cache-Control": "no-store"})

# Backdate runs as a background task, same reasoning as the refresh flow: PPT's
# rate-limit pacing across up to BACKDATE_MAX_CARDS cards could run long enough
# to be a bad fit for a single request/response cycle.
def _tracker_backdate_state(app) -> dict:
    st = getattr(app.state, "tracker_backdate", None)
    if st is None:
        st = {"running": False, "started_at": None, "finished_at": None,
              "result": None, "error": None}
        app.state.tracker_backdate = st
    return st

async def _run_tracker_backdate(app, card_ids: list, days: int) -> None:
    st = app.state.tracker_backdate
    try:
        st["result"] = await run_ppt_backdate(app.state.db, card_ids, days)
    except Exception as e:
        logger.exception("Card tracker backdate failed")
        st["error"] = str(e)
    finally:
        st["running"] = False
        st["finished_at"] = datetime.utcnow().isoformat() + "Z"

@app.post("/api/card-tracker/backdate")
@limiter.limit("10/hour")
async def api_card_tracker_backdate(request: Request, user=Depends(require_admin)):
    """Backfill day-by-day raw prices for selected Pokemon cards via PPT's
    includeHistory param (30/60/180 days). Costs 2 PPT credits per card."""
    body = await request.json()
    card_ids = _validate_backdate_card_ids(body)
    days = body.get("days")
    if days not in BACKDATE_DAY_CHOICES:
        raise HTTPException(status_code=400,
                            detail=f"days must be one of {BACKDATE_DAY_CHOICES}")
    st = _tracker_backdate_state(request.app)
    if st["running"]:
        raise HTTPException(status_code=409, detail="A backdate is already running")
    st.update(running=True, started_at=datetime.utcnow().isoformat() + "Z",
              finished_at=None, result=None, error=None)
    asyncio.create_task(_run_tracker_backdate(request.app, card_ids, days))
    logger.info("Card tracker: backdate (%d days) started by admin %s for %d card(s)",
                days, user["id"], len(card_ids))
    return JSONResponse({"ok": True, "started": True}, headers={"Cache-Control": "no-store"})

@app.get("/api/card-tracker/backdate/status")
async def api_card_tracker_backdate_status(request: Request, user=Depends(require_admin)):
    return JSONResponse(dict(_tracker_backdate_state(request.app)),
                        headers={"Cache-Control": "no-store"})

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
                    INSERT INTO tracked_cards
                        (name, game, set_name, card_number, variant, release_date, language)
                    VALUES ($1, $2, $3, $4, $5, $6, 'english')
                    ON CONFLICT (game, name, set_name, card_number, language) DO NOTHING
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

# ---- Tips, Tricks, and Guide ----
# Open to the whole Discord — paid and unpaid roles alike — but not anonymous
# guests, unlike Nexus Playground. See get_current_discord_user.

@app.get("/tips", response_class=HTMLResponse)
async def tips_page(request: Request):
    user = request.session.get("user")
    if not user:
        return login_redirect_or_preview(
            request, title="Tips, Tricks, and Guide — Nexus Card Co",
            description="Community-written tips, guides, and photos, organized by topic. "
                        "Sign in with Discord to read and contribute.")
    if not await terms_current(request, user):
        return RedirectResponse("/terms")
    return templates.TemplateResponse("tips.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "is_admin": int(user["id"]) in ADMIN_USER_IDS,
        "is_mod": request.session.get("mod", False),
        # For the per-module Google Maps JavaScript API map — same key/pattern
        # map.html already uses, client-visible by design (Maps API keys are
        # restricted by HTTP referrer in Google Cloud Console, not secrecy).
        "google_maps_api_key": GOOGLE_MAPS_API_KEY,
    })


async def _geocode_address(client: httpx.AsyncClient, address: str):
    """(lat, lng) for one address via Google's Geocoding API, or None if the
    key is missing, the address doesn't resolve, or the request fails —
    callers treat all three the same: leave the location's coordinates NULL
    and skip its pin, everything else about the location still works."""
    if not GOOGLE_MAPS_API_KEY:
        return None
    try:
        resp = await client.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"address": address, "key": GOOGLE_MAPS_API_KEY})
        data = resp.json()
    except Exception:
        logger.exception("Geocoding request failed for address=%r", address)
        return None
    if data.get("status") != "OK" or not data.get("results"):
        return None
    loc = data["results"][0]["geometry"]["location"]
    return (loc["lat"], loc["lng"])


TIPS_GEOCODE_BACKFILL_START_DELAY_S = float(os.getenv("TIPS_GEOCODE_BACKFILL_START_DELAY_S", "15"))


async def _tips_geocode_backfill_startup(app) -> None:
    """One-time (per location) catch-up for rows saved before geocode-on-save
    existed, or whose address failed to resolve at save time. Idempotent
    across deploys: once every location has coordinates, later boots find
    nothing to do."""
    try:
        await asyncio.sleep(TIPS_GEOCODE_BACKFILL_START_DELAY_S)
        missing = await tips.get_locations_missing_coords(app.state.db)
        if not missing:
            return
        async with httpx.AsyncClient(timeout=10.0) as client:
            for loc in missing:
                coords = await _geocode_address(client, loc["address"])
                if coords:
                    await tips.set_location_coords(app.state.db, loc["id"], coords[0], coords[1])
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Tips location geocode backfill failed")


def _require_tips_admin(user) -> None:
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Admins only.")


@app.get("/api/tips")
@limiter.limit("60/minute")
async def api_tips_list(request: Request, user=Depends(get_current_discord_user)):
    is_admin = int(user["id"]) in ADMIN_USER_IDS
    my_id = int(user["id"])
    tree = await tips.list_tree(request.app.state.db, viewer_user_id=my_id)
    # Whether THIS viewer may edit/delete each entry/location/photo —
    # resolved server-side so the client never has to (and can't be tricked
    # into) deciding its own permissions; the actual edit/delete routes
    # re-check independently.
    for cat in tree:
        for topic in cat["topics"]:
            for mod in topic["modules"]:
                for e in mod["entries"]:
                    e["can_edit"] = is_admin or int(e["user_id"]) == my_id
                for loc in mod["locations"]:
                    loc["can_edit"] = is_admin or int(loc["user_id"]) == my_id
                for p in mod["photos"]:
                    p["can_edit"] = is_admin or int(p["user_id"]) == my_id
    return JSONResponse({"categories": tree, "is_admin": is_admin},
                        headers={"Cache-Control": "no-store"})


@app.post("/api/tips/entries")
@limiter.limit("20/minute")
async def api_tips_create_entry(request: Request, user=Depends(get_current_discord_user)):
    body = await request.json()
    content = (body.get("content") or "").strip()
    if not content:
        raise HTTPException(status_code=400, detail="Tip can't be empty.")
    try:
        module_id = int(body.get("module_id"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid module.")
    result = await tips.create_entry(
        request.app.state.db, module_id, int(user["id"]), user["username"], content)
    return JSONResponse(result)


@app.put("/api/tips/entries/{entry_id}")
@limiter.limit("20/minute")
async def api_tips_update_entry(request: Request, entry_id: int,
                                user=Depends(get_current_discord_user)):
    owner_id = await tips.get_entry_owner(request.app.state.db, entry_id)
    if owner_id is None:
        raise HTTPException(status_code=404, detail="Tip not found.")
    if int(user["id"]) not in ADMIN_USER_IDS and int(owner_id) != int(user["id"]):
        raise HTTPException(status_code=403, detail="You can only edit your own tips.")
    body = await request.json()
    content = (body.get("content") or "").strip()
    if not content:
        raise HTTPException(status_code=400, detail="Tip can't be empty.")
    await tips.update_entry(request.app.state.db, entry_id, content)
    return JSONResponse({"ok": True})


@app.delete("/api/tips/entries/{entry_id}")
@limiter.limit("20/minute")
async def api_tips_delete_entry(request: Request, entry_id: int,
                                user=Depends(get_current_discord_user)):
    owner_id = await tips.get_entry_owner(request.app.state.db, entry_id)
    if owner_id is None:
        raise HTTPException(status_code=404, detail="Tip not found.")
    if int(user["id"]) not in ADMIN_USER_IDS and int(owner_id) != int(user["id"]):
        raise HTTPException(status_code=403, detail="You can only delete your own tips.")
    await tips.delete_entry(request.app.state.db, entry_id)
    return JSONResponse({"ok": True})


@app.post("/api/tips/entries/{entry_id}/like")
@limiter.limit("60/minute")
async def api_tips_like_entry(request: Request, entry_id: int,
                              user=Depends(get_current_discord_user)):
    result = await tips.toggle_entry_like(request.app.state.db, entry_id, int(user["id"]))
    return JSONResponse(result)


@app.post("/api/tips/locations")
@limiter.limit("20/minute")
async def api_tips_create_location(request: Request, user=Depends(get_current_discord_user)):
    body = await request.json()
    name = (body.get("name") or "").strip()
    address = (body.get("address") or "").strip()
    if not name or not address:
        raise HTTPException(status_code=400, detail="Location name and address are both required.")
    try:
        module_id = int(body.get("module_id"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid module.")
    async with httpx.AsyncClient(timeout=10.0) as client:
        coords = await _geocode_address(client, address)
    lat, lng = coords if coords else (None, None)
    result = await tips.create_location(
        request.app.state.db, module_id, int(user["id"]), user["username"], name, address, lat, lng)
    return JSONResponse(result)


@app.put("/api/tips/locations/{location_id}")
@limiter.limit("20/minute")
async def api_tips_update_location(request: Request, location_id: int,
                                   user=Depends(get_current_discord_user)):
    owner_id = await tips.get_location_owner(request.app.state.db, location_id)
    if owner_id is None:
        raise HTTPException(status_code=404, detail="Location not found.")
    if int(user["id"]) not in ADMIN_USER_IDS and int(owner_id) != int(user["id"]):
        raise HTTPException(status_code=403, detail="You can only edit your own locations.")
    body = await request.json()
    name = (body.get("name") or "").strip()
    address = (body.get("address") or "").strip()
    if not name or not address:
        raise HTTPException(status_code=400, detail="Location name and address are both required.")
    async with httpx.AsyncClient(timeout=10.0) as client:
        coords = await _geocode_address(client, address)
    lat, lng = coords if coords else (None, None)
    await tips.update_location(request.app.state.db, location_id, name, address, lat, lng)
    return JSONResponse({"ok": True})


@app.delete("/api/tips/locations/{location_id}")
@limiter.limit("20/minute")
async def api_tips_delete_location(request: Request, location_id: int,
                                   user=Depends(get_current_discord_user)):
    owner_id = await tips.get_location_owner(request.app.state.db, location_id)
    if owner_id is None:
        raise HTTPException(status_code=404, detail="Location not found.")
    if int(user["id"]) not in ADMIN_USER_IDS and int(owner_id) != int(user["id"]):
        raise HTTPException(status_code=403, detail="You can only delete your own locations.")
    await tips.delete_location(request.app.state.db, location_id)
    return JSONResponse({"ok": True})


@app.post("/api/tips/locations/{location_id}/like")
@limiter.limit("60/minute")
async def api_tips_like_location(request: Request, location_id: int,
                                 user=Depends(get_current_discord_user)):
    result = await tips.toggle_location_like(request.app.state.db, location_id, int(user["id"]))
    return JSONResponse(result)


@app.post("/api/tips/categories")
@limiter.limit("30/minute")
async def api_tips_create_category(request: Request, user=Depends(get_current_discord_user)):
    _require_tips_admin(user)
    body = await request.json()
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Category name required.")
    category_id = await tips.create_category(request.app.state.db, name)
    return JSONResponse({"id": category_id})


@app.patch("/api/tips/categories/{category_id}")
@limiter.limit("30/minute")
async def api_tips_update_category(request: Request, category_id: int,
                                   user=Depends(get_current_discord_user)):
    _require_tips_admin(user)
    body = await request.json()
    if "name" in body:
        name = (body.get("name") or "").strip()
        if not name:
            raise HTTPException(status_code=400, detail="Category name required.")
        await tips.rename_category(request.app.state.db, category_id, name)
    if body.get("move") in ("up", "down"):
        await tips.reorder_category(request.app.state.db, category_id, body["move"])
    return JSONResponse({"ok": True})


@app.delete("/api/tips/categories/{category_id}")
@limiter.limit("30/minute")
async def api_tips_delete_category(request: Request, category_id: int,
                                   user=Depends(get_current_discord_user)):
    _require_tips_admin(user)
    await tips.delete_category(request.app.state.db, category_id)
    return JSONResponse({"ok": True})


@app.post("/api/tips/topics")
@limiter.limit("30/minute")
async def api_tips_create_topic(request: Request, user=Depends(get_current_discord_user)):
    _require_tips_admin(user)
    body = await request.json()
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Topic name required.")
    try:
        category_id = int(body.get("category_id"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid category.")
    topic_id = await tips.create_topic(request.app.state.db, category_id, name)
    return JSONResponse({"id": topic_id})


@app.patch("/api/tips/topics/{topic_id}")
@limiter.limit("30/minute")
async def api_tips_update_topic(request: Request, topic_id: int,
                                user=Depends(get_current_discord_user)):
    _require_tips_admin(user)
    body = await request.json()
    if "name" in body:
        name = (body.get("name") or "").strip()
        if not name:
            raise HTTPException(status_code=400, detail="Topic name required.")
        await tips.rename_topic(request.app.state.db, topic_id, name)
    if body.get("move") in ("up", "down"):
        await tips.reorder_topic(request.app.state.db, topic_id, body["move"])
    return JSONResponse({"ok": True})


@app.delete("/api/tips/topics/{topic_id}")
@limiter.limit("30/minute")
async def api_tips_delete_topic(request: Request, topic_id: int,
                                user=Depends(get_current_discord_user)):
    _require_tips_admin(user)
    await tips.delete_topic(request.app.state.db, topic_id)
    return JSONResponse({"ok": True})


@app.post("/api/tips/modules")
@limiter.limit("30/minute")
async def api_tips_create_module(request: Request, user=Depends(get_current_discord_user)):
    _require_tips_admin(user)
    body = await request.json()
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Module name required.")
    try:
        topic_id = int(body.get("topic_id"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid topic.")
    module_id = await tips.create_module(request.app.state.db, topic_id, name)
    return JSONResponse({"id": module_id})


@app.patch("/api/tips/modules/{module_id}")
@limiter.limit("30/minute")
async def api_tips_update_module(request: Request, module_id: int,
                                 user=Depends(get_current_discord_user)):
    _require_tips_admin(user)
    body = await request.json()
    if "name" in body:
        name = (body.get("name") or "").strip()
        if not name:
            raise HTTPException(status_code=400, detail="Module name required.")
        await tips.rename_module(request.app.state.db, module_id, name)
    if body.get("move") in ("up", "down"):
        await tips.reorder_module(request.app.state.db, module_id, body["move"])
    return JSONResponse({"ok": True})


@app.delete("/api/tips/modules/{module_id}")
@limiter.limit("30/minute")
async def api_tips_delete_module(request: Request, module_id: int,
                                 user=Depends(get_current_discord_user)):
    _require_tips_admin(user)
    await tips.delete_module(request.app.state.db, module_id)
    return JSONResponse({"ok": True})


@app.post("/api/tips/photos")
@limiter.limit("10/minute")
async def api_tips_create_photo(request: Request, user=Depends(get_current_discord_user)):
    body = await request.json()
    try:
        module_id = int(body.get("module_id"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid module.")
    caption = (body.get("caption") or "").strip()

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

    if len(image_bytes) > MAX_IMAGE_BYTES:
        image_bytes, media_type = _compress_card_image(image_bytes)

    result = await tips.create_photo(
        request.app.state.db, module_id, int(user["id"]), user["username"],
        image_bytes, media_type, caption)
    return JSONResponse(result)


@app.put("/api/tips/photos/{photo_id}")
@limiter.limit("20/minute")
async def api_tips_update_photo(request: Request, photo_id: int,
                                user=Depends(get_current_discord_user)):
    owner_id = await tips.get_photo_owner(request.app.state.db, photo_id)
    if owner_id is None:
        raise HTTPException(status_code=404, detail="Photo not found.")
    if int(user["id"]) not in ADMIN_USER_IDS and int(owner_id) != int(user["id"]):
        raise HTTPException(status_code=403, detail="You can only edit your own photos.")
    body = await request.json()
    caption = (body.get("caption") or "").strip()
    await tips.update_photo_caption(request.app.state.db, photo_id, caption)
    return JSONResponse({"ok": True})


@app.delete("/api/tips/photos/{photo_id}")
@limiter.limit("20/minute")
async def api_tips_delete_photo(request: Request, photo_id: int,
                                user=Depends(get_current_discord_user)):
    owner_id = await tips.get_photo_owner(request.app.state.db, photo_id)
    if owner_id is None:
        raise HTTPException(status_code=404, detail="Photo not found.")
    if int(user["id"]) not in ADMIN_USER_IDS and int(owner_id) != int(user["id"]):
        raise HTTPException(status_code=403, detail="You can only delete your own photos.")
    await tips.delete_photo(request.app.state.db, photo_id)
    return JSONResponse({"ok": True})


@app.post("/api/tips/photos/{photo_id}/like")
@limiter.limit("60/minute")
async def api_tips_like_photo(request: Request, photo_id: int,
                              user=Depends(get_current_discord_user)):
    result = await tips.toggle_photo_like(request.app.state.db, photo_id, int(user["id"]))
    return JSONResponse(result)


@app.get("/api/tips/photos/{photo_id}/image")
@limiter.limit("120/minute")
async def api_tips_photo_image(request: Request, photo_id: int,
                               user=Depends(get_current_discord_user)):
    result = await tips.get_photo_image(request.app.state.db, photo_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Photo not found.")
    image_bytes, media_type = result
    return Response(content=image_bytes, media_type=media_type,
                    headers={"Cache-Control": "private, max-age=3600"})


# ---- Grading calculator ----
# Standalone page, deliberately not wired into the card tracker: it works for
# ANY card, not just the ~400 in tracked_cards, so it has no dependency on the
# JustTCG watchlist. Open to members (same gate as the map/dashboard); the
# vendor-backed lookups spend paid quota, so they keep a per-IP rate limit.

# Guest tier: same "last 3 main sets" ceiling as the Catalog, not a grade/
# grader restriction — every grade is shown for whatever card a guest is
# allowed to look up at all. Enforced at /sets, /set-cards, and /quotes so a
# manual card-name entry or the photo scanner can't reach a card outside the
# window just because the Set dropdown wasn't used to get there.
#
# One Piece's set list (grading_sets.GRADING_SETS["one_piece"]) is a small
# hand-maintained, already-newest-first list — unlike Pokemon there's no live
# PPT set feed to walk, so the guest window is just its first N entries, no
# network round trip needed. Checked against both the picker's short set id
# ("OP-16") and its display name ("The Time of Battle"): /set-cards passes
# the id, /quotes passes whichever the caller supplied (the picker sends the
# resolved display name; the manual "enter a card" box lets a guest type
# either), so matching only one field would silently let the other through.
def _guest_visible_one_piece_sets() -> list:
    return grading_sets.GRADING_SETS.get("one_piece", [])[:GUEST_GRADING_VISIBLE_SETS]


async def _guest_grading_allowed(pool, game: str, language: str, set_name: str) -> bool:
    if game == "one_piece":
        needle = _norm_set_name(set_name)
        return any(needle in (_norm_set_name(s.get("id")), _norm_set_name(s.get("name")))
                   for s in _guest_visible_one_piece_sets())
    if game != "pokemon":
        return True
    visible = await _guest_visible_set_ids(pool, language, limit=GUEST_GRADING_VISIBLE_SETS)
    return _norm_set_name(set_name) in {_norm_set_name(v) for v in visible}


def _norm_set_name(value: str) -> str:
    return (value or "").strip().casefold()


@app.get("/grading-calculator", response_class=HTMLResponse)
async def grading_calculator_page(request: Request):
    # Demo/role-less Discord accounts reach this page too (guest-tier access,
    # see _viewer_context) rather than bouncing to /sample like the main
    # dashboard — Nexus Playground is intentionally open wider than that.
    user = request.session.get("user")
    if user:
        if not await terms_current(request, user):
            return RedirectResponse("/terms")
    elif not is_guest(request):
        return login_redirect_or_preview(
            request, title="Grading Calculator — Nexus Card Co",
            description="Estimate a card's graded value across PSA, BGS, CGC, and more. "
                        "Sign in with Discord or continue as a guest to use it.")
    return templates.TemplateResponse("grading_calculator.html", {
        "request": request,
        **_viewer_context(request, user),
        "sources": price_sources.configured_sources(),
        "grade_labels": price_sources.GRADE_LABELS,
        "grade_levels": price_sources.GRADE_LEVEL,
        "grading_companies": grading_tiers.GRADING_COMPANIES,
        "grading_sets": grading_sets.GRADING_SETS,
        # Proof-of-page-load token for the guest-tier scan quota — see
        # make_scan_token. Harmless to hand to non-guests too; only a guest
        # request is actually checked against it.
        "scan_token": make_scan_token(),
        # Own window, decoupled from the Catalog's — the disclaimer text
        # needs the live number, not a hardcoded one that drifts the moment
        # the constant changes.
        "guest_grading_visible_sets": GUEST_GRADING_VISIBLE_SETS,
    })


# ---- Grading calculator: population report (GemRate via PPT) ----
# Business-plan only — see price_sources.fetch_ppt_population. A confirmed
# 403 means the account's PPT plan doesn't cover this endpoint at all, a
# standing account-level condition rather than a per-card one, so further
# attempts are suppressed for a while instead of retried on every lookup.
POPULATION_UNAVAILABLE_COOLDOWN_S = float(os.getenv("POPULATION_UNAVAILABLE_COOLDOWN_S", "3600"))
_population_unavailable_until = 0.0


async def _get_population(pool, tcgplayer_id: str, language: str):
    """Cached population report for one card, refreshing if the cache is
    missing or more than a week old (population.CACHE_MAX_AGE). None if
    unavailable for any reason — never raises, since this is a value-add on
    top of the price lookup, not something that should break it."""
    global _population_unavailable_until
    if not tcgplayer_id or not price_sources.pokemonpricetracker_available():
        return None

    cached, fresh = await population.get_cached(pool, tcgplayer_id, language)
    if fresh:
        return cached
    if time.time() < _population_unavailable_until:
        # Confirmed unavailable (403) recently — don't spend a call finding
        # that out again. Stale cache, if any, still beats showing nothing.
        return cached

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            data, status = await price_sources.fetch_ppt_population(client, tcgplayer_id, language)
    except Exception:
        logger.exception("Population lookup failed for tcgPlayerId=%s", tcgplayer_id)
        return cached

    if status == "ok" and data:
        await population.upsert(pool, tcgplayer_id, language, data)
        return data
    if status == "forbidden":
        _population_unavailable_until = time.time() + POPULATION_UNAVAILABLE_COOLDOWN_S
        logger.warning("Population data unavailable (PPT 403 — Business plan required). "
                       "Not retrying for %d minutes.", int(POPULATION_UNAVAILABLE_COOLDOWN_S // 60))
    return cached  # stale cache, if any, beats nothing


@app.get("/api/grading-calculator/quotes")
@limiter.limit("20/minute")
async def api_grading_quotes(request: Request, name: str, game: str = "pokemon",
                             set_name: str = "", card_number: str = "",
                             tcgplayer_id: str = "", language: str = "english",
                             user=Depends(get_current_user_or_guest)):
    """Live graded prices for one card, merged across configured vendors.
    Open to members and guests (like the rest of the grading calculator).
    Spends PokemonPriceTracker credits and eBay's daily budget, so it keeps
    the per-IP rate limit above — watch aggregate usage if traffic grows."""
    name = (name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="A card name is required")
    if game not in ("pokemon", "one_piece"):
        raise HTTPException(status_code=400, detail="Unknown game")

    tcg_id = (tcgplayer_id or "").strip()[:32]
    language = price_sources.ppt_language(language)
    set_name_clean = (set_name or "").strip()[:120]

    if user.get("guest"):
        # tcg_id pinning only ever happens for Pokemon — One Piece's picker
        # never populates a tcgplayer_id (see _grading_set_cache), so tcg_id
        # is always empty for a one_piece call and this branch is skipped.
        if game == "pokemon" and tcg_id:
            # A pinned tcgPlayerId skips name/set matching entirely inside
            # PPT's own lookup (see fetch_pokemonpricetracker) — trusting the
            # client's set_name here would let a guest claim an allowed set
            # while the id itself pulls real prices for a card in a
            # different one. Check the id's ACTUAL set from our own cache.
            real_set_id = await catalog.set_id_for_tcgplayer_id(
                request.app.state.db, game, language, tcg_id)
            allowed = bool(real_set_id) and await _guest_grading_allowed(
                request.app.state.db, game, language, real_set_id)
        else:
            allowed = await _guest_grading_allowed(
                request.app.state.db, game, language, set_name_clean)
        if not allowed:
            # Blocks the manual "enter a card" box and the photo scanner too —
            # neither goes through the Set dropdown, so this is the one place
            # that actually enforces the guest tier's set restriction.
            raise HTTPException(
                status_code=403,
                detail="That card's set isn't available on the guest tier — pick a card from the "
                       "Set dropdown, or sign in with Discord for the full set list.")

    card = price_sources.CardRef(game=game, name=name[:120],
                                 set_name=set_name_clean,
                                 card_number=(card_number or "").strip()[:40],
                                 tcgplayer_id=tcg_id or None,
                                 language=language)
    try:
        result = await price_sources.fetch_all(card)
    except Exception:
        logger.exception("Graded quote lookup failed for %r", card.query())
        raise HTTPException(status_code=502, detail="Price lookup failed")

    # Only when the card came from the PPT-backed picker (a real tcgPlayerId
    # pins the exact printing) and only for Pokemon — PPT has no One Piece
    # coverage at all, population included.
    pop = None
    if tcg_id and game == "pokemon":
        pop = await _get_population(request.app.state.db, tcg_id, language)

    quotes_out = {g: q.to_dict() for g, q in result["quotes"].items()}
    all_out = [q.to_dict() for q in result["all"]]

    return JSONResponse({
        "card_key": card.key(),
        "query": card.query(),
        "sources_used": result["sources"],
        "sources_configured": price_sources.configured_sources(),
        "quotes": quotes_out,
        "all": all_out,
        "population": pop,
    }, headers={"Cache-Control": "no-store"})


@app.post("/api/grading-calculator/calc")
@limiter.limit("120/minute")
async def api_grading_calc(request: Request, user=Depends(get_current_user_or_guest)):
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
    # "What you'd net" reports on whichever grading company is picked in
    # Your costs, filtered to the SAME "lowest grade shown" threshold as
    # the Market Prices slider — so the two panels always agree on which
    # grades are in view instead of the net cards being frozen at a fixed
    # top-two. Falls back to PSA's full range when no company is selected
    # ("Custom / other"), matching PSA 10/9's existing role as the
    # no-company default. Same for guests as for members — the guest tier's
    # restriction is which cards they can look up (see _guest_grading_allowed),
    # not which grades are shown for one they're allowed to.
    company = (body.get("company") or "").strip().lower()
    min_grade = _num(body.get("min_grade"), 8.0)
    min_grade = min(max(min_grade, 1.0), 8.0)
    all_grades = (grading_tiers.GRADING_COMPANIES.get(company, {}).get("all_grades")
                 or grading_tiers.GRADING_COMPANIES["psa"]["all_grades"])
    report_grades = [g for g in all_grades if price_sources.GRADE_LEVEL.get(g, 0) >= min_grade]
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
                           user=Depends(get_current_user_or_guest)):
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
            if user.get("guest"):
                # Guest tier is capped to the newest GUEST_GRADING_VISIBLE_SETS
                # sets — its own window, not the Catalog's. Return the
                # filtered list even if it's empty rather than falling
                # through to the unrestricted baked list below.
                visible = {_norm_set_name(v)
                          for v in await _guest_visible_set_ids(
                              request.app.state.db, language, limit=GUEST_GRADING_VISIBLE_SETS)}
                sets = [s for s in sets if _norm_set_name(s.get("id")) in visible]
                return JSONResponse({"source": "pokemonpricetracker",
                                     "language": language, "sets": sets},
                                    headers={"Cache-Control": "no-store"})
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

    baked = grading_sets.GRADING_SETS.get(game, [])
    if user.get("guest"):
        # This is also One Piece's ONLY path (no live PPT-equivalent feed for
        # it), so unlike Pokemon's PPT branch above, skipping this filter
        # would leave One Piece guests with no set restriction at all.
        baked = (_guest_visible_one_piece_sets() if game == "one_piece"
                 else baked[:GUEST_GRADING_VISIBLE_SETS])
    return JSONResponse(
        {"source": "catalog", "language": "english", "sets": baked},
        headers={"Cache-Control": "no-store"})


@app.get("/api/grading-calculator/set-cards")
@limiter.limit("60/hour")
async def api_grading_set_cards(request: Request, game: str, set_id: str,
                                language: str = "english",
                                user=Depends(get_current_user_or_guest)):
    if game not in ("pokemon", "one_piece"):
        raise HTTPException(status_code=400, detail="Unknown game")
    set_id = (set_id or "").strip()
    # PPT set ids ARE set names, so this is longer than a catalog set code.
    if not set_id or len(set_id) > 120:
        raise HTTPException(status_code=400, detail="Invalid set")
    language = price_sources.ppt_language(language)

    if user.get("guest") and not await _guest_grading_allowed(request.app.state.db, game, language, set_id):
        raise HTTPException(status_code=403, detail="That set isn't available on the guest tier.")

    # Pokemon: take the card list from PPT so each card carries its
    # tcgPlayerId. A lookup can then pin the exact printing instead of
    # matching on name and set.
    if game == "pokemon" and price_sources.pokemonpricetracker_available():
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                cards = await price_sources.fetch_ppt_set_cards(
                    client, set_id, language=language)
            if cards:
                # Free catalog fill. This response was billed per card and
                # already carries rarity and raw price for the whole set, so
                # stocking /catalog from it costs nothing extra. Wrapped so a
                # catalog write can never break the calculator's picker.
                try:
                    await catalog.upsert_set_cards(request.app.state.db, "pokemon",
                                                   language, set_id, cards)
                except Exception:
                    logger.exception("Catalog: couldn't cache set %r from the picker", set_id)
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


# ---- Card catalog ----
# Browse/filter every card the dashboard has seen, by set, rarity and raw
# price. Reads come from the local `catalog_cards` cache, never from a vendor:
# "every holo under $50 across all sets" spans ~36,000 cards and PPT bills per
# card returned, so that question is only affordable against local rows.
#
# The cache fills two ways, neither of which costs extra credits:
#   1. Lazily — every grading-calculator set-cards call already pays for a
#      whole set's rows (including prices), so the result is written here on
#      the way past. Browsing the calculator stocks the catalog for free.
#   2. Admin "stock this set" below, for seeding popular sets deliberately.
# A member browsing the catalog NEVER triggers a vendor call.
#
# Pokemon-only for now: One Piece's catalog source (optcgapi) returns rarity
# but no prices at all, so OP cards would sit in a price-filtered table with
# nothing to filter on. See MEMORY.md.
CATALOG_GAME = "pokemon"

# Guest tier: catalog browsing (and the grading calculator's set picker,
# which shares this same window — see _guest_grading_allowed) is capped to
# the newest N stocked sets, and the manual per-card refresh is
# Discord-members-only. Neither restriction applies to a real Discord
# session (member or admin) — only to `is_guest`.
#
# Was 3 (roughly 6-9 months of releases); widened to 8 (~1.5-2 years) since a
# cold visitor's first search landing on an older set read as broken rather
# than restricted — 3 sets wasn't enough for a search to plausibly succeed,
# which is the wrong first impression for someone converting from a cold
# link, not a returning member who already knows the restriction exists.
GUEST_CATALOG_VISIBLE_SETS = int(os.getenv("GUEST_CATALOG_VISIBLE_SETS", "8"))
# The grading calculator has its own, tighter guest window — decoupled from
# the Catalog's so tightening/widening one never silently changes the other.
GUEST_GRADING_VISIBLE_SETS = int(os.getenv("GUEST_GRADING_VISIBLE_SETS", "5"))
# Era keys that aren't a main-series expansion era — promos, novelty/kit
# groups, and the unclassified catch-all. The guest window should count only
# main sets (Mega Evolution, Scarlet & Violet, ...), never one of these.
GUEST_CATALOG_NON_MAIN_ERAS = {"promos", "mcdonalds", "pop", "trainer_kits", "other"}


async def _guest_visible_set_ids(pool, language: str, limit: int = None) -> list:
    """The newest `limit` (default GUEST_CATALOG_VISIBLE_SETS) RELEASED,
    STOCKED, main-era sets, newest-by-release first — the guest tier's
    ceiling for whichever page is asking. Same "walk PPT's own newest-first
    list, keep only what's actually stocked" pattern as
    select_backfill_window, just capped much smaller.

    `limit` lets a caller use a tighter window than the Catalog's default —
    the grading calculator passes GUEST_GRADING_VISIBLE_SETS, since its
    guest restriction is deliberately stricter and must never silently
    follow the Catalog's if that one changes.

    A set can show up stocked (preview/pre-release cards already priced)
    before its actual release date, so an undated or future-dated set is
    skipped here even if it's already in `have` — the guest window should
    never include a set that hasn't come out yet. Promos and other
    non-main-era groups (see GUEST_CATALOG_NON_MAIN_ERAS) are skipped too —
    the window is meant to read as "the last N main sets".
    """
    limit = GUEST_CATALOG_VISIBLE_SETS if limit is None else limit
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            sets = await price_sources.fetch_ppt_sets(client, language=language)
    except Exception:
        logger.exception("Guest catalog restriction: couldn't fetch the PPT set list")
        sets = []
    have = await catalog.cached_set_ids(pool, CATALOG_GAME, language)
    today = datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%d")
    out = []
    for s in sets:
        released = s.get("released") or ""
        if not released or released > today:
            continue
        if card_eras.era_for_set(s.get("name") or s.get("id")) in GUEST_CATALOG_NON_MAIN_ERAS:
            continue
        if s.get("id") in have:
            out.append(s["id"])
        if len(out) >= limit:
            break
    return out


def _language_list(raw: str) -> list:
    """Parse the catalog's `language` query param into a list.

    Accepts one language or a comma-separated pair ("english,japanese") — the
    catalog shows them as independent checkboxes. Unknown values are dropped
    and an empty result falls back to the default rather than becoming "no
    language filter", which would silently mix both printings together.
    """
    parts = [p.strip().lower() for p in str(raw or "").split(",") if p.strip()]
    out = [p for p in dict.fromkeys(parts) if p in price_sources.PPT_LANGUAGES]
    return out or [price_sources.PPT_DEFAULT_LANGUAGE]


async def _guest_visible_set_ids_multi(pool, languages) -> list:
    """The guest ceiling across several languages, unioned.

    The cap is per-language on purpose: "the newest 3 sets" means the newest
    3 English sets AND the newest 3 Japanese ones, not 3 shared between them.
    Anything else would make the guest window shrink just because a second
    language was ticked.
    """
    out = []
    for lang in languages:
        out.extend(await _guest_visible_set_ids(pool, lang))
    return list(dict.fromkeys(out))


def _csv_list(raw: str, *, max_items: int = 40, max_len: int = 120) -> list:
    """Split a comma-separated filter param, bounded in count and length so a
    hand-crafted query can't build an enormous ANY() array."""
    out = []
    for part in (raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        out.append(part[:max_len])
        if len(out) >= max_items:
            break
    return out


def _bool_flag(raw) -> bool:
    """A checkbox-style query param ('1'/'true'/'yes') as a real bool."""
    return str(raw).strip().lower() in ("1", "true", "yes")


def _opt_price(raw) -> float | None:
    """A price bound, or None when absent/unparseable. Negatives clamp to 0."""
    if raw is None or str(raw).strip() == "":
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    return max(0.0, value)


async def _catalog_stock_set(pool, set_id: str, language: str) -> dict:
    """Fetch one set from PPT and write it into the catalog cache.

    Spends credits (1 per card) unless price_sources' 24h set-cards cache
    covers it. Callers must be admin-gated or rate-limited accordingly.
    """
    if not price_sources.pokemonpricetracker_available():
        raise HTTPException(status_code=503,
                            detail="PokemonPriceTracker isn't configured, so sets can't be stocked.")
    async with httpx.AsyncClient(timeout=60.0) as client:
        cards, status = await price_sources.fetch_ppt_set_cards_detailed(
            client, set_id, language=language)
    if not cards:
        # 'empty' = PPT has no data for this set yet (normal for brand-new
        # releases); 'error' = the request failed. Passed up so the backfill
        # can tell a healthy run from a broken one.
        return {"cards": 0, "priced": 0, "status": status}
    stats = await catalog.upsert_set_cards(pool, CATALOG_GAME, language, set_id, cards)
    stats["status"] = "ok"
    return stats


# ---- Catalog backfill ----
# Seeds the catalog with the newest N sets so the page isn't empty on day one.
# Everything older fills in lazily as members open sets in the grading
# calculator.
#
# THE WINDOW IS "THE NEWEST N SETS", NOT "THE NEXT N UNSTOCKED SETS". Railway
# redeploys on every push and the in-process PPT cache dies with the process,
# so a rolling "next N unstocked" would re-spend the whole budget on every
# deploy, forever. Anchoring to the newest N and skipping what's already in
# `catalog_cards` (which survives deploys) makes the first boot pay once and
# every later boot a no-op — and a newly released set enters the window on its
# own and gets stocked without anyone doing anything.
CATALOG_BACKFILL_SETS = int(os.getenv("CATALOG_BACKFILL_SETS", "20"))
# Spread the calls out — this is a background seed, not something anyone is
# waiting on, and it shares PPT's budget with live member lookups.
CATALOG_BACKFILL_DELAY_S = float(os.getenv("CATALOG_BACKFILL_DELAY_S", "2"))
# Let the app finish booting and start serving before spending any quota.
CATALOG_BACKFILL_START_DELAY_S = float(os.getenv("CATALOG_BACKFILL_START_DELAY_S", "15"))
# Consecutive FAILED requests (HTTP error, transport error) that abort the run.
# Deliberately NOT triggered by empty sets: the newest sets are exactly the
# ones PokemonPriceTracker most often has no data for yet, so a brand-new
# release returning nothing is the normal case, not a fault. Those are skipped
# and the run continues. An empty result costs no per-card credits, and the
# set stays outside `catalog_cards`, so it's simply retried on a later run —
# self-healing once PPT publishes the data.
CATALOG_BACKFILL_MAX_ERRORS = 3
# How many additional sets the admin "Stock 5 more sets" button takes per
# click. Unlike the startup seed this walks the whole catalog rather than a
# fixed window — safe precisely because a person triggers each batch.
CATALOG_NEXT_BATCH_SETS = int(os.getenv("CATALOG_NEXT_BATCH_SETS", "10"))
# How many already-stocked sets the admin "Refresh" button re-fetches per
# click (mode='refresh') — bigger than CATALOG_NEXT_BATCH_SETS since the
# preflight in _run_catalog_backfill skips most of these for free (already
# complete, images present) and only pays for the ones that actually need it.
CATALOG_REFRESH_BATCH_SETS = int(os.getenv("CATALOG_REFRESH_BATCH_SETS", "25"))
# A 429 is temporary, so it is NOT counted as a failure — the set is retried
# after a wait, doubling each time. If it's still throttled after that, the run
# stops cleanly and can be resumed later with the button: sets already stocked
# are skipped, so nothing is lost or paid for twice.
CATALOG_BACKFILL_RATE_WAIT_S = float(os.getenv("CATALOG_BACKFILL_RATE_WAIT_S", "30"))
CATALOG_BACKFILL_RATE_RETRIES = int(os.getenv("CATALOG_BACKFILL_RATE_RETRIES", "2"))

# The manual "refresh these cards" action (/api/catalog/refresh-cards) is open
# to every viewer, not just admins — this is the floor that keeps it from
# being a free-for-all: a card priced more recently than this can't be
# selected (enforced both client-side, for the checkbox, and server-side,
# since the client can't be trusted). A card can only ever cost 1 credit per
# window, no matter how many different people try to refresh it.
CATALOG_MANUAL_REFRESH_MIN_AGE_HOURS = int(os.getenv("CATALOG_MANUAL_REFRESH_MIN_AGE_HOURS", "48"))


def _catalog_known_empty(app, language: str = "english") -> set:
    """Set ids PPT had no data for this process run, per language.

    Keyed by language: a set id that returned nothing in Japanese says
    nothing about its English printing, so one language's misses must not
    suppress retries in the other.

    Empty sets are never written to `catalog_cards`, so without remembering
    them "the next 5 unstocked sets" would return the same empty ones on
    every click and never advance. Deliberately in-process, not persisted: a
    redeploy clears it, so a set that was empty last week gets one more try —
    which is exactly what should happen once PPT publishes it.
    """
    store = getattr(app.state, "catalog_empty_sets", None)
    if store is None:
        store = {}
        app.state.catalog_empty_sets = store
    return store.setdefault(language, set())


def _catalog_refreshed_sets(app, language: str = "english") -> set:
    """Set ids re-stocked by mode='refresh' this process run, per language.

    Same shape as `_catalog_known_empty`: in-process and cleared by a
    redeploy, so it only needs to track "already handled since the PPT
    pagination fix shipped" for the lifetime of one run, not forever.
    """
    store = getattr(app.state, "catalog_refreshed_sets", None)
    if store is None:
        store = {}
        app.state.catalog_refreshed_sets = store
    return store.setdefault(language, set())


def _catalog_backfill_state(app) -> dict:
    st = getattr(app.state, "catalog_backfill", None)
    if st is None:
        st = {"running": False, "started_at": None, "finished_at": None,
              "planned": 0, "done": 0, "stocked": 0, "empty": 0, "skipped": 0,
              "cards": 0, "current": None, "waiting_s": 0, "error": None,
              "last_result": None, "mode": None, "language": None}
        app.state.catalog_backfill = st
    return st


async def _run_catalog_backfill(app, limit: int | None = None,
                                mode: str = "window",
                                language: str = "english") -> None:
    """Stock sets into the catalog. Never raises.

    mode='window'  — the newest `limit` sets, minus what's cached. Idempotent
    across redeploys; this is what the startup seed uses.
    mode='next'    — the next `limit` uncached sets, walking the whole catalog.
    Advances further back on each call, so it is only ever driven by an admin
    pressing the button.
    mode='refresh' — the next `limit` ALREADY-cached sets, re-fetched so any
    that were cached before the PPT /cards pagination fix pick up cards past
    the old 200-per-set ceiling. Also only ever driven by an admin press.
    """
    st = _catalog_backfill_state(app)
    if st["running"]:
        logger.info("Catalog backfill: already running — skipping this trigger")
        return
    limit = CATALOG_BACKFILL_SETS if limit is None else limit
    if limit <= 0:
        logger.info("Catalog backfill: disabled (CATALOG_BACKFILL_SETS=%s)", limit)
        return
    if not price_sources.pokemonpricetracker_available():
        logger.info("Catalog backfill: PokemonPriceTracker not configured — skipping")
        return

    st.update(running=True, started_at=datetime.utcnow().isoformat() + "Z",
              finished_at=None, planned=0, done=0, stocked=0, empty=0, skipped=0,
              cards=0, current=None, waiting_s=0, error=None, mode=mode,
              language=language)
    pool = app.state.db
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            sets = await price_sources.fetch_ppt_sets(client, language=language)
        if not sets:
            st["error"] = "PokemonPriceTracker returned no sets."
            logger.warning("Catalog backfill: no sets from PPT — nothing to do")
            return

        have = await catalog.cached_set_ids(pool, CATALOG_GAME, language)
        if mode == "next":
            todo = catalog.select_next_unstocked(
                sets, have, limit, skip=_catalog_known_empty(app, language))
            logger.info("Catalog backfill [next]: %d set(s) stocked so far, "
                        "%d known empty — taking the next %d",
                        len(have), len(_catalog_known_empty(app, language)), len(todo))
        elif mode == "refresh":
            todo = catalog.select_next_refresh(
                sets, have, _catalog_refreshed_sets(app, language), limit)
            logger.info("Catalog backfill [refresh]: %d set(s) cached, "
                        "%d already refreshed this run — taking the next %d",
                        len(have), len(_catalog_refreshed_sets(app, language)), len(todo))
        else:
            todo = catalog.select_backfill_window(sets, have, limit)
            logger.info("Catalog backfill [window]: newest %d set(s), %d already "
                        "stocked, %d to fetch", min(limit, len(sets)),
                        min(limit, len(sets)) - len(todo), len(todo))
        st["planned"] = len(todo)
        if not todo:
            if mode == "next":
                st["last_result"] = "Every set is already stocked."
            elif mode == "refresh":
                st["last_result"] = "Every cached set has already been refreshed this run."
            else:
                st["last_result"] = "Already stocked — nothing to fetch."
            return

        # For refresh, a 1-credit preflight (PPT's metadata.total on a
        # limit=1 lookup) tells us whether a cached set is already complete
        # — skipping it there avoids paying for a full re-fetch (~1
        # credit/card) that would only confirm nothing was missing. Not
        # worth doing for mode='next'/'window': those sets aren't cached yet,
        # so there's nothing to compare against.
        #
        # Card-count completeness alone isn't enough to skip, though: a set
        # stocked before catalog_cards.image_url (or printing_prices)
        # existed is "complete" here but every row's image_url/
        # printing_prices is NULL (the price-only refresh never backfilled
        # either — see cached_set_missing_image_counts /
        # cached_set_missing_variant_counts). Skipping those too would mean
        # the grid view's "No image yet" placeholder, and the "Variant
        # prices" toggle, would never have anything to show for a set
        # stocked before those columns shipped.
        cached_counts = (await catalog.cached_set_counts(pool, CATALOG_GAME, language)
                         if mode == "refresh" else {})
        missing_image_counts = (await catalog.cached_set_missing_image_counts(
                                    pool, CATALOG_GAME, language)
                                 if mode == "refresh" else {})
        missing_variant_counts = (await catalog.cached_set_missing_variant_counts(
                                    pool, CATALOG_GAME, language)
                                 if mode == "refresh" else {})
        preflight_client = httpx.AsyncClient(timeout=15.0) if mode == "refresh" else None

        error_streak = 0
        try:
            for entry in todo:
                st["current"] = entry.get("name") or entry["id"]

                if preflight_client is not None:
                    total = await price_sources.fetch_ppt_set_total(
                        preflight_client, entry["id"], language)
                    cached = cached_counts.get(entry["id"], 0)
                    missing_images = missing_image_counts.get(entry["id"], 0)
                    missing_variants = missing_variant_counts.get(entry["id"], 0)
                    if (total is not None and cached >= total
                            and missing_images == 0 and missing_variants == 0):
                        logger.info("Catalog backfill [refresh]: %r already complete "
                                   "(%d/%d card(s), images + variant prices present) — "
                                   "skipping the full re-fetch",
                                   entry["id"], cached, total)
                        st["done"] += 1
                        st["skipped"] += 1
                        error_streak = 0
                        _catalog_refreshed_sets(app, language).add(entry["id"])
                        await asyncio.sleep(CATALOG_BACKFILL_DELAY_S)
                        continue

                # Retry a throttled set in place, backing off, before giving up on
                # the run. A 429 says "later", not "never".
                for attempt in range(CATALOG_BACKFILL_RATE_RETRIES + 1):
                    try:
                        stats = await _catalog_stock_set(pool, entry["id"], language)
                    except Exception:
                        logger.exception("Catalog backfill: %r failed", entry["id"])
                        stats = {"cards": 0, "priced": 0, "status": "error"}
                    if stats.get("status") != "rate_limited" or attempt >= CATALOG_BACKFILL_RATE_RETRIES:
                        break
                    wait = CATALOG_BACKFILL_RATE_WAIT_S * (2 ** attempt)
                    st["waiting_s"] = wait
                    logger.warning("Catalog backfill: rate limited on %r — waiting %.0fs "
                                   "(retry %d of %d)", entry["id"], wait,
                                   attempt + 1, CATALOG_BACKFILL_RATE_RETRIES)
                    await asyncio.sleep(wait)
                st["waiting_s"] = 0

                if stats.get("status") == "rate_limited":
                    st["error"] = (
                        "PokemonPriceTracker is still rate-limiting after backing off, so "
                        "stocking stopped here. Nothing was lost — press “Stock 5 more "
                        "sets” later and it picks up where it left off. If this keeps "
                        "happening, check the daily-remaining figure in the logs.")
                    logger.warning("Catalog backfill: stopped — still rate limited at %r",
                                   entry["id"])
                    break

                st["done"] += 1

                if stats["cards"]:
                    error_streak = 0
                    st["stocked"] += 1
                    st["cards"] += stats["cards"]
                    if mode == "refresh":
                        _catalog_refreshed_sets(app, language).add(entry["id"])
                elif stats.get("status") == "empty":
                    # PPT has nothing for this set yet — expected for unreleased
                    # and just-released sets. Skip it and carry on; it'll be
                    # retried on a later run, for free, until data appears.
                    error_streak = 0
                    st["empty"] += 1
                    # Remembered so the "stock 5 more" button steps past it next
                    # click instead of re-offering the same empty set forever.
                    _catalog_known_empty(app, language).add(entry["id"])
                    if mode == "refresh":
                        _catalog_refreshed_sets(app, language).add(entry["id"])
                    logger.info("Catalog backfill: %r has no data yet — skipping",
                                entry["id"])
                else:
                    error_streak += 1
                    logger.warning("Catalog backfill: %r failed (%d in a row)",
                                   entry["id"], error_streak)
                    if error_streak >= CATALOG_BACKFILL_MAX_ERRORS:
                        st["error"] = (f"Stopped after {error_streak} failed requests in a "
                                       "row — PokemonPriceTracker looks unreachable. Sets "
                                       "with no data yet are skipped, not counted here.")
                        logger.warning("Catalog backfill: %s", st["error"])
                        break
                await asyncio.sleep(CATALOG_BACKFILL_DELAY_S)
        finally:
            if preflight_client is not None:
                await preflight_client.aclose()

        verb = "Refreshed" if mode == "refresh" else "Stocked"
        st["last_result"] = (f"{verb} {st['cards']:,} card(s) across "
                             f"{st['stocked']} set(s).")
        if st["skipped"]:
            st["last_result"] += f" {st['skipped']} already complete, skipped."
        if st["empty"]:
            st["last_result"] += (f" {st['empty']} set(s) had no data yet — "
                                  "they'll be picked up automatically once they do.")
        logger.info("Catalog backfill: done — %s", st["last_result"])
    except Exception:
        logger.exception("Catalog backfill failed")
        st["error"] = "Backfill failed — see logs."
    finally:
        st.update(running=False, current=None,
                  finished_at=datetime.utcnow().isoformat() + "Z")


# Languages the STARTUP seed auto-stocks. English only by default —
# deliberately unchanged from before Japanese support existed, because this
# runs unattended on every deploy and seeding a language costs real credits
# (~N sets x a few hundred cards, once). Japanese is stocked on purpose from
# the admin panel instead; set this to "english,japanese" to opt in to
# seeding it automatically as well.
CATALOG_SEED_LANGUAGES = [
    l for l in (s.strip().lower() for s in
                os.getenv("CATALOG_SEED_LANGUAGES", "english").split(","))
    if l in price_sources.PPT_LANGUAGES
] or ["english"]


async def _catalog_backfill_startup(app) -> None:
    """Startup seed. Idempotent across deploys: once the newest N sets are in
    `catalog_cards`, every later boot finds nothing to do and spends nothing."""
    try:
        await asyncio.sleep(CATALOG_BACKFILL_START_DELAY_S)
        for lang in CATALOG_SEED_LANGUAGES:
            await _run_catalog_backfill(app, language=lang)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Catalog backfill startup task failed")


# ---- Catalog nightly price sweep (11:15pm America/New_York) ----
# Spends whatever PPT daily credit budget is left over refreshing catalog
# prices, one card at a time via the cheapest lookup PPT offers (pinned by
# tcgplayer_id, 1 credit — see price_sources.fetch_ppt_card_prices). Runs
# 15 minutes after the card tracker's own 11pm ingest so "remaining budget"
# already reflects everything else that spent credits today: that ingest,
# its 3-hourly gap-fill sweep, live member usage, and any admin catalog
# actions — there's no way to know that total in advance, so this reads
# PPT's own X-RateLimit-Daily-Remaining header live after every call rather
# than tracking a locally-computed count that could drift from reality.
CATALOG_PRICE_SWEEP_HOUR = 23
CATALOG_PRICE_SWEEP_MINUTE = 15
# How many of the newest sets get first crack at the budget before anything
# else does, regardless of how stale those cards are (they're never LESS
# eligible for it, only more).
CATALOG_PRICE_SWEEP_RECENT_SETS = int(os.getenv("CATALOG_PRICE_SWEEP_RECENT_SETS", "8"))
# Backstop against a header-parsing miss or a runaway candidate list — not
# the real budget control, which is the live remaining-credits check below.
# 1 credit/card makes the daily allowance itself a natural ceiling.
CATALOG_PRICE_SWEEP_MAX_CARDS = int(os.getenv("CATALOG_PRICE_SWEEP_MAX_CARDS", "20000"))
# Stop once PPT reports this many credits or fewer left today. Default 0:
# this is the last scheduled consumer before the daily reset, so there's
# nothing later today to hold a reserve back for.
CATALOG_PRICE_SWEEP_RESERVE_CREDITS = int(os.getenv("CATALOG_PRICE_SWEEP_RESERVE_CREDITS", "0"))
CATALOG_PRICE_SWEEP_RATE_WAIT_S = float(os.getenv("CATALOG_PRICE_SWEEP_RATE_WAIT_S", "30"))
CATALOG_PRICE_SWEEP_RATE_RETRIES = int(os.getenv("CATALOG_PRICE_SWEEP_RATE_RETRIES", "2"))


async def _run_catalog_price_sweep(app) -> dict:
    """Refresh catalog card prices until the candidates or the daily PPT
    budget run out, whichever comes first. Never raises."""
    summary = {"candidates": 0, "refreshed": 0, "cleared": 0, "skipped": 0,
              "credits": 0, "stopped": None}
    if not price_sources.pokemonpricetracker_available():
        logger.info("Catalog price sweep: PokemonPriceTracker not configured — skipping")
        return summary
    pool = app.state.db

    try:
        # Both languages: a stocked Japanese set whose prices never refresh
        # would go stale forever. They share the one daily credit budget —
        # the reserve/stop guard below already bounds total spend, so adding
        # a language spreads the same budget rather than doubling it.
        candidates = []
        for lang in price_sources.PPT_LANGUAGES:
            async with httpx.AsyncClient(timeout=30.0) as client:
                sets = await price_sources.fetch_ppt_sets(client, language=lang)
            recent_set_ids = [s["id"] for s in sets[:CATALOG_PRICE_SWEEP_RECENT_SETS]]
            candidates.extend(await catalog.select_price_refresh_candidates(
                pool, CATALOG_GAME, lang, recent_set_ids,
                CATALOG_PRICE_SWEEP_MAX_CARDS))
        summary["candidates"] = len(candidates)
        if not candidates:
            logger.info("Catalog price sweep: no refreshable cards (need a "
                        "verified tcgplayer_id) — nothing to do")
            return summary

        async with httpx.AsyncClient(timeout=30.0) as client:
            for card in candidates:
                prices, status, daily_remaining = {}, "error", None
                for attempt in range(CATALOG_PRICE_SWEEP_RATE_RETRIES + 1):
                    prices, status, daily_remaining = await price_sources.fetch_ppt_card_prices(
                        client, card["tcgplayer_id"])
                    if status != "rate_limited" or attempt >= CATALOG_PRICE_SWEEP_RATE_RETRIES:
                        break
                    wait = CATALOG_PRICE_SWEEP_RATE_WAIT_S * (2 ** attempt)
                    logger.warning("Catalog price sweep: rate limited on %r — waiting "
                                   "%.0fs (retry %d/%d)", card["name"], wait,
                                   attempt + 1, CATALOG_PRICE_SWEEP_RATE_RETRIES)
                    await asyncio.sleep(wait)

                if status == "rate_limited":
                    summary["stopped"] = "rate_limited"
                    logger.warning("Catalog price sweep: stopped — still rate limited "
                                   "at %r after backing off", card["name"])
                    break

                # Every attempt that wasn't rate-limited spent the 1 credit
                # fetch_ppt_card_prices is built to cost, whether or not PPT
                # had a price to give back — same accounting the tracker's
                # own ingest uses for this same call.
                summary["credits"] += 1
                if status == "error":
                    # The call itself failed (no usable response) — unlike
                    # "empty", PPT hasn't actually told us anything, so the
                    # existing stored price is left alone rather than cleared.
                    summary["skipped"] += 1
                else:
                    raw_price = (prices or {}).get("market")
                    if raw_price is None:
                        raw_price = (prices or {}).get("low")
                    await catalog.update_card_price(
                        pool, card["id"], raw_price, "pokemonpricetracker",
                        printing_prices=(prices or {}).get("printing_prices"))
                    if raw_price is not None:
                        summary["refreshed"] += 1
                    else:
                        summary["cleared"] += 1

                if (daily_remaining is not None
                        and daily_remaining <= CATALOG_PRICE_SWEEP_RESERVE_CREDITS):
                    summary["stopped"] = "budget"
                    logger.info("Catalog price sweep: stopping — %d credit(s) left "
                                "today (reserve %d)", daily_remaining,
                                CATALOG_PRICE_SWEEP_RESERVE_CREDITS)
                    break
    except Exception:
        logger.exception("Catalog price sweep failed")
        summary["stopped"] = "error"
        return summary

    logger.info("Catalog price sweep: %d/%d candidate(s) processed — %d refreshed, "
                "%d cleared, %d skipped, ~%d credit(s) spent%s",
                summary["refreshed"] + summary["cleared"] + summary["skipped"],
                summary["candidates"], summary["refreshed"], summary["cleared"],
                summary["skipped"], summary["credits"],
                f" — stopped ({summary['stopped']})" if summary["stopped"] else "")
    return summary


def _seconds_until_next_catalog_price_sweep() -> float:
    eastern = ZoneInfo("America/New_York")
    now = datetime.now(eastern)
    target = now.replace(hour=CATALOG_PRICE_SWEEP_HOUR, minute=CATALOG_PRICE_SWEEP_MINUTE,
                         second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


async def _catalog_price_sweep_scheduler(app: FastAPI) -> None:
    while True:
        wait_s = _seconds_until_next_catalog_price_sweep()
        logger.info("Catalog price sweep: next run in %.0f min (11:15pm America/New_York)",
                    wait_s / 60)
        await asyncio.sleep(wait_s)
        try:
            await _run_catalog_price_sweep(app)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Catalog price sweep scheduler failed — retrying tomorrow")


@app.get("/catalog", response_class=HTMLResponse)
async def catalog_page(request: Request):
    # Demo/role-less Discord accounts reach this page too (guest-tier access,
    # see _viewer_context) rather than bouncing to /sample like the main
    # dashboard — Nexus Playground is intentionally open wider than that.
    user = request.session.get("user")
    if user:
        if not await terms_current(request, user):
            return RedirectResponse("/terms")
    elif not is_guest(request):
        return login_redirect_or_preview(
            request, title="Card Catalog — Nexus Card Co",
            description="Browse every stocked Pokémon and One Piece set with live prices. "
                        "Sign in with Discord or continue as a guest to use it.")
    return templates.TemplateResponse("catalog.html", {
        "request": request,
        **_viewer_context(request, user),
        # Drives rel="sponsored" and the disclosure line. Both must appear
        # only when the eBay links actually carry EPN tracking.
        "ebay_affiliate": catalog.ebay_affiliate_enabled(),
        "refresh_min_age_hours": CATALOG_MANUAL_REFRESH_MIN_AGE_HOURS,
        "guest_catalog_visible_sets": GUEST_CATALOG_VISIBLE_SETS,
        # Drives the admin Stock/Refresh button labels — read from the same
        # constants the backend defaults to, so a batch-size change can never
        # leave the button text stale again.
        "catalog_next_batch_sets": CATALOG_NEXT_BATCH_SETS,
        "catalog_refresh_batch_sets": CATALOG_REFRESH_BATCH_SETS,
    })


@app.get("/api/catalog/cards")
@limiter.limit("120/minute")
async def api_catalog_cards(request: Request, sets: str = "", rarities: str = "",
                            min_price: str = "", max_price: str = "",
                            search: str = "", priced_only: str = "", variant: str = "",
                            exclude_search: str = "", exclude_sets: str = "",
                            exclude_rarities: str = "", exclude_price: str = "",
                            exclude_variant: str = "",
                            sort: str = catalog.DEFAULT_SORT,
                            limit: int = 50, offset: int = 0,
                            language: str = "english",
                            user=Depends(get_current_user_or_guest)):
    """Filtered, paginated catalog rows. Pure DB read — spends no vendor quota,
    which is why it carries a generous rate limit."""
    languages = _language_list(language)
    restrict_set_ids = (await _guest_visible_set_ids_multi(request.app.state.db, languages)
                        if user.get("guest") else None)
    result = await catalog.query_cards(
        request.app.state.db,
        game=CATALOG_GAME,
        language=languages,
        set_ids=_csv_list(sets),
        rarities=_csv_list(rarities),
        min_price=_opt_price(min_price),
        max_price=_opt_price(max_price),
        search=(search or "").strip()[:80],
        priced_only=_bool_flag(priced_only),
        variant=(variant or "").strip()[:80],
        # Unknown sort keys fall back to the default inside query_cards; the
        # ORDER BY clause itself is never built from user input.
        sort=sort,
        limit=limit,
        offset=offset,
        # Facet counts ride along on the page request rather than a second
        # round-trip: one query set, so the options can never disagree with
        # the rows being shown.
        with_facets=True,
        # Guest tier's "newest N sets only" ceiling — None (no restriction)
        # for a real Discord session.
        restrict_set_ids=restrict_set_ids,
        # Per-filter "all but these" toggle — see catalog._build_filters.
        exclude={
            "search": _bool_flag(exclude_search),
            "sets": _bool_flag(exclude_sets),
            "rarities": _bool_flag(exclude_rarities),
            "price": _bool_flag(exclude_price),
            "variant": _bool_flag(exclude_variant),
        },
    )
    if result.get("facets"):
        card_eras.annotate(result["facets"]["sets"], name_key="set_name")
    return JSONResponse(result, headers={"Cache-Control": "no-store"})


@app.get("/api/catalog/export")
@limiter.limit("10/minute")
async def api_catalog_export(request: Request, sets: str = "", rarities: str = "",
                             min_price: str = "", max_price: str = "",
                             search: str = "", priced_only: str = "", variant: str = "",
                             exclude_search: str = "", exclude_sets: str = "",
                             exclude_rarities: str = "", exclude_price: str = "",
                             exclude_variant: str = "",
                             sort: str = catalog.DEFAULT_SORT,
                             language: str = "english",
                             user=Depends(get_current_user_or_guest)):
    """CSV download of every card matching the current filters — not just the
    page on screen. Same filter logic as /api/catalog/cards (down to sharing
    the guest-tier set ceiling), so an export can never show something the
    table itself wouldn't. Capped at catalog.EXPORT_MAX_ROWS rows; a lower
    rate limit than the table's own fetch since a full export is a heavier
    single request, even though it's still a pure DB read with no vendor cost."""
    languages = _language_list(language)
    restrict_set_ids = (await _guest_visible_set_ids_multi(request.app.state.db, languages)
                        if user.get("guest") else None)
    rows = await catalog.export_rows(
        request.app.state.db,
        game=CATALOG_GAME,
        language=languages,
        set_ids=_csv_list(sets),
        rarities=_csv_list(rarities),
        min_price=_opt_price(min_price),
        max_price=_opt_price(max_price),
        search=(search or "").strip()[:80],
        priced_only=_bool_flag(priced_only),
        variant=(variant or "").strip()[:80],
        sort=sort,
        restrict_set_ids=restrict_set_ids,
        exclude={
            "search": _bool_flag(exclude_search),
            "sets": _bool_flag(exclude_sets),
            "rarities": _bool_flag(exclude_rarities),
            "price": _bool_flag(exclude_price),
            "variant": _bool_flag(exclude_variant),
        },
    )

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Set", "Card Name", "Card Number", "Rarity", "Language", "Variant",
                     "Raw Price", "Price Source", "Last Priced", "Last Refreshed",
                     "TCGplayer URL", "eBay URL"])
    for r in rows:
        writer.writerow([
            r["set_name"] or r["set_id"], r["name"], r["card_number"], r["rarity"],
            r["language"], r["variant"] or "",
            r["raw_price"] if r["raw_price"] is not None else "",
            r["price_source"] or "", r["priced_at"] or "", r["refreshed_at"] or "",
            r["tcgplayer_url"] or "", r["ebay_url"] or "",
        ])

    stamp = datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="catalog_export_{stamp}.csv"'},
    )


@app.get("/api/catalog/facets")
@limiter.limit("60/minute")
async def api_catalog_facets(request: Request, language: str = "english",
                             user=Depends(get_current_user_or_guest)):
    """Filter options plus coverage.

    `sets` is what's stocked; `available_sets` is PPT's full set list, so the
    UI can show which sets have no data yet instead of pretending the catalog
    is complete. The set list is cached for a day in price_sources.
    """
    languages = _language_list(language)
    restrict_set_ids = (await _guest_visible_set_ids_multi(request.app.state.db, languages)
                        if user.get("guest") else None)
    data = await catalog.facets(request.app.state.db, CATALOG_GAME, languages,
                               restrict_set_ids=restrict_set_ids)
    data["is_guest_restricted"] = restrict_set_ids is not None
    data["languages"] = languages

    # PPT's set list is per-language, so fetch each and tag the rows — the
    # admin stock picker needs to know which language a set would be stocked
    # in, not just its name.
    available = []
    if price_sources.pokemonpricetracker_available():
        for lang in languages:
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    for s in await price_sources.fetch_ppt_sets(client, language=lang):
                        available.append({**s, "language": lang})
            except Exception:
                logger.exception("Catalog: PPT set list failed for %s — "
                                 "showing stocked sets only", lang)
    # Tag every set with its era so the pickers can group them. Done here
    # rather than in catalog.py: it's presentation, and catalog.py stays
    # schema + queries. era_order lets the client sort groups newest-era-first
    # without duplicating ERA_ORDER in JavaScript.
    card_eras.annotate(data["sets"], name_key="set_name")
    card_eras.annotate(available, name_key="name")
    data["available_sets"] = available
    data["era_order"] = [key for key, _ in card_eras.ERA_ORDER]
    data["era_labels"] = dict(card_eras.ERA_ORDER)
    data["language"] = language
    data["can_stock"] = price_sources.pokemonpricetracker_available()
    # Lets the page show seeding progress instead of looking broken while the
    # startup backfill is still working through the newest sets.
    data["backfill"] = dict(_catalog_backfill_state(request.app))
    data["backfill"]["window"] = CATALOG_BACKFILL_SETS
    return JSONResponse(data, headers={"Cache-Control": "no-store"})


@app.post("/api/catalog/stock")
@limiter.limit("20/hour")
async def api_catalog_stock(request: Request, user=Depends(require_admin)):
    """Admin: pull a set into the catalog cache.

    Admin-only because it spends PokemonPriceTracker credits (1 per card, so a
    large set is ~400), and the budget is shared with the grading calculator.
    """
    body = await request.json()
    set_id = str(body.get("set_id") or "").strip()
    if not set_id or len(set_id) > 120:
        raise HTTPException(status_code=400, detail="A set is required")
    language = price_sources.ppt_language(body.get("language"))
    try:
        stats = await _catalog_stock_set(request.app.state.db, set_id, language)
    except HTTPException:
        raise
    except Exception:
        logger.exception("Catalog stock failed for %r", set_id)
        raise HTTPException(status_code=502, detail="Couldn't stock that set")
    if not stats["cards"]:
        if stats.get("status") == "empty":
            raise HTTPException(
                status_code=404,
                detail=f"PokemonPriceTracker has no cards for {set_id} yet. Brand-new "
                       "sets often aren't published for a while — try again later.")
        if stats.get("status") == "rate_limited":
            raise HTTPException(
                status_code=429,
                detail="PokemonPriceTracker is rate-limiting us right now — "
                       "wait a bit and try again.")
        raise HTTPException(status_code=502,
                            detail=f"The request for {set_id} failed — see logs.")
    return JSONResponse({"set_id": set_id, "language": language, **stats},
                        headers={"Cache-Control": "no-store"})


@app.post("/api/catalog/refresh-cards")
@limiter.limit("30/hour")
async def api_catalog_refresh_cards(request: Request, user=Depends(get_current_user_or_guest)):
    """Refresh up to 10 specific cards' prices right now — open to Discord
    members and admins, but NOT the guest tier (guests get the catalog
    read-only, restricted to the newest few sets).

    Same 1-credit-per-card pinned lookup the nightly price sweep uses
    (price_sources.fetch_ppt_card_prices). For members, the cost-control
    isn't the Depends() gate — it's CATALOG_MANUAL_REFRESH_MIN_AGE_HOURS
    below: a card priced more recently than that can't be picked again, by
    anyone, so the total possible spend is capped by the size of the catalog
    rather than by how many people ask.
    """
    if user.get("guest"):
        raise HTTPException(status_code=403,
                            detail="Refreshing prices requires a Discord account — "
                                   "sign in to use this.")
    body = await request.json()
    raw_ids = body.get("ids") or []
    if not isinstance(raw_ids, list) or not raw_ids:
        raise HTTPException(status_code=400, detail="Pick at least one card")
    if len(raw_ids) > 10:
        raise HTTPException(status_code=400, detail="Refresh at most 10 cards at a time")
    try:
        ids = [int(i) for i in raw_ids]
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid card id")

    if not price_sources.pokemonpricetracker_available():
        raise HTTPException(status_code=503,
                            detail="PokemonPriceTracker isn't configured, so cards can't be refreshed.")

    # A list: with both languages shown at once the selection can span them,
    # and scoping to one would report every row from the other as not_found.
    languages = _language_list(body.get("language"))
    pool = request.app.state.db
    rows = await catalog.get_cards_by_id(pool, CATALOG_GAME, languages, ids)
    found = {r["id"]: r for r in rows}
    min_age = timedelta(hours=CATALOG_MANUAL_REFRESH_MIN_AGE_HOURS)

    results = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        for card_id in ids:
            row = found.get(card_id)
            if not row:
                results.append({"id": card_id, "status": "not_found"})
                continue
            if not row["tcgplayer_verified"] or not row["tcgplayer_id"]:
                # No safe pinned lookup available for this card (see
                # select_price_refresh_candidates) — it only picks up a fresh
                # price the next time its whole set is stocked or refreshed.
                results.append({"id": card_id, "status": "unverified", "name": row["name"]})
                continue
            priced_at = row["priced_at"]
            if priced_at is not None:
                # priced_at is stored tz-aware (TIMESTAMPTZ); compare in UTC.
                now = datetime.now(priced_at.tzinfo) if priced_at.tzinfo else datetime.utcnow()
                if now - priced_at < min_age:
                    # Never trust the client's checkbox-disabling alone — this
                    # is the actual enforcement, re-checked against the DB.
                    results.append({"id": card_id, "status": "too_recent", "name": row["name"]})
                    continue

            prices, status, _daily_remaining = await price_sources.fetch_ppt_card_prices(
                client, row["tcgplayer_id"])
            if status == "rate_limited":
                results.append({"id": card_id, "status": "rate_limited", "name": row["name"]})
                break
            if status == "error":
                results.append({"id": card_id, "status": "error", "name": row["name"]})
                continue

            raw_price = (prices or {}).get("market")
            if raw_price is None:
                raw_price = (prices or {}).get("low")
            await catalog.update_card_price(
                pool, card_id, raw_price, "pokemonpricetracker",
                printing_prices=(prices or {}).get("printing_prices"))
            results.append({
                "id": card_id, "status": status, "name": row["name"],
                "raw_price": raw_price,
                "priced_at": datetime.utcnow().isoformat() + "Z" if raw_price is not None else None,
            })

    return JSONResponse({"results": results}, headers={"Cache-Control": "no-store"})


@app.post("/api/catalog/backfill")
@limiter.limit("30/hour")
async def api_catalog_backfill(request: Request, user=Depends(require_admin)):
    """Admin: stock more sets by hand.

    mode='next' (default) takes the next N sets that aren't cached yet,
    walking steadily back through older sets on each press — one click, one
    batch, a person choosing to spend the credits. mode='window' re-runs the
    startup seed's newest-N pass instead. mode='refresh' re-fetches the next N
    ALREADY-cached sets, so ones cached before the PPT /cards pagination fix
    pick up cards past the old 200-per-set ceiling.

    Runs in the background and returns immediately; poll
    /api/catalog/facets for progress.
    """
    try:
        body = await request.json()
    except Exception:
        body = {}

    mode = str(body.get("mode") or "next").strip().lower()
    if mode not in ("next", "window", "refresh"):
        raise HTTPException(status_code=400, detail="mode must be 'next', 'window', or 'refresh'")

    limit = body.get("sets")
    default = (CATALOG_BACKFILL_SETS if mode == "window"
               else CATALOG_REFRESH_BATCH_SETS if mode == "refresh"
               else CATALOG_NEXT_BATCH_SETS)
    try:
        limit = default if limit is None else int(limit)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="'sets' must be a number")
    # Bounded so a typo can't queue the entire ~180-set catalog in one press.
    limit = max(0, min(limit, 50))

    # Bulk stocking is per-language: "the next 10 unstocked sets" means the
    # next 10 in ONE language, since a set id's English and Japanese
    # printings are independent rows.
    language = price_sources.ppt_language(body.get("language"))

    st = _catalog_backfill_state(request.app)
    if st["running"]:
        raise HTTPException(status_code=409, detail="A backfill is already running.")
    asyncio.create_task(_run_catalog_backfill(request.app, limit=limit, mode=mode,
                                              language=language))
    return JSONResponse({"started": True, "sets": limit, "mode": mode,
                         "language": language},
                        headers={"Cache-Control": "no-store"})


# ---- Analytics ----

@app.get("/analytics", response_class=HTMLResponse)
async def analytics_page(request: Request):
    user = request.session.get("user")
    if not user:
        return login_redirect_or_preview(request)
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
        return login_redirect_or_preview(
            request, title="Store Status — Nexus Card Co",
            description="Live restock status for tracked stores in the DMV. "
                        "Sign in with Discord to view it.")
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
        return login_redirect_or_preview(request)
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
        return login_redirect_or_preview(
            request, title="Store Map — Nexus Card Co",
            description="An interactive map of tracked stores across the DMV. "
                        "Sign in with Discord to view it.")
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
        return login_redirect_or_preview(request)
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
async def api_grading_scan(request: Request, user=Depends(get_current_user_or_guest)):
    """Identify a card from a photo via Claude Vision and return exactly the
    JSON it produced (game/name/number/set + One Piece variant flags) — no
    per-game catalog lookup here. The client matches that JSON against the
    same PPT/catalog set-and-card lists the picker already uses, so a scan
    result and a manual pick go through identical matching logic.

    Guests get a metered daily allowance (per-IP AND a global daily ceiling —
    see _guest_scan_take) instead of the member's per-minute limit — this is
    the one call in Nexus Playground that spends real Claude API money per
    request, and a guest has no Discord identity to attribute abuse to. A
    scan-page-proof token (see make_scan_token) is also required for guests,
    so a script hitting this endpoint directly without ever rendering the
    page can't spend the quota at all.

    Request body size is enforced globally by ContentSizeLimitMiddleware, not
    here — a client-supplied Content-Length header was never authoritative.
    """
    body = await request.json()

    if user.get("guest"):
        if not verify_scan_token(body.get("scan_token")):
            raise HTTPException(status_code=403,
                                detail="Please reload the page and try again.")
        allowed, reason = _guest_scan_take(get_real_ip(request))
        if not allowed:
            if reason == "global":
                raise HTTPException(
                    status_code=429,
                    detail="Free guest scans are fully booked for today — try again "
                           "tomorrow, or sign in with Discord for unlimited use.")
            raise HTTPException(
                status_code=429,
                detail=f"Guests get {GUEST_SCAN_DAILY_LIMIT} free scans a day — "
                       "sign in with Discord for unlimited use.")

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
            AND rr.non_tcg = FALSE
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