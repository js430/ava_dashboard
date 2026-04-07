import os
import io
import logging
import httpx
import asyncpg
import re
import json
import base64
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from anthropic import AsyncAnthropic
from PIL import Image
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
DATABASE_URL             = os.getenv("DATABASE_URL")
DISCORD_CLIENT_ID        = os.getenv("DISCORD_CLIENT_ID")
DISCORD_CLIENT_SECRET    = os.getenv("DISCORD_CLIENT_SECRET")
DISCORD_REDIRECT_URI     = os.getenv("DISCORD_REDIRECT_URI")
DISCORD_GUILD_ID         = os.getenv("DISCORD_GUILD_ID")
REQUIRED_ROLE_IDS        = {r.strip() for r in os.getenv("REQUIRED_ROLE_ID", "").split(",") if r.strip()}
GOOGLE_MAPS_API_KEY      = os.getenv("GOOGLE_MAPS_API_KEY", "")
ANTHROPIC_API_KEY        = os.getenv("ANTHROPIC_API_KEY", "")
ACTIVE_INFORMANT_ROLE_ID = os.getenv("ACTIVE_INFORMANT_ROLE_ID", "")
ADMIN_USER_IDS           = {
    int(uid) for uid in os.getenv("ADMIN_USER_IDS", "").split(",") if uid.strip()
}

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

# ---- Card Scanner Constants ----
MAX_IMAGE_BYTES = 3 * 1024 * 1024 + 500_000  # ~3.5 MB raw
MAX_DIMENSION   = 2048
POKEMON_API     = "https://api.pokemontcg.io/v2/cards"
SCRYFALL_API    = "https://api.scryfall.com/cards/named"
YGOPRODECK_API  = "https://db.ygoprodeck.com/api/v7/cardinfo.php"
OPTCG_API       = "https://www.optcgapi.com/api"

CONDITION_MULTIPLIERS = {
    "NM": 1.00, "LP": 0.85, "MP": 0.65, "HP": 0.45, "Poor": 0.25,
}

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


def _normalize_pokemon_name(name: str) -> str:
    name = re.sub(r"\s+(GX|EX|VMAX|VStar|VMax|V-UNION|VUnion)$", r"-\1", name)
    name = re.sub(r"\s+V$", "-V", name)
    if re.match(r"^M ", name) and not name.endswith("-EX"):
        if name.endswith(" EX"):
            name = name[:-3] + "-EX"
        elif not name.endswith(" ex"):
            name = name + "-EX"
    return name


def _name_variants(name: str) -> list[str]:
    variants = [name, _normalize_pokemon_name(name)]
    if name.endswith("-EX") or name.endswith(" EX"):
        base = re.sub(r"[-\s]EX$", "", name)
        variants.append(base + " ex")
    if name.endswith(" ex"):
        variants.append(name[:-3] + "-ex")
    v_suffixes = ("V", "VMAX", "VStar", "VUnion", "VSTAR")
    extra: list[str] = []
    for var in variants:
        for suf in v_suffixes:
            if var.endswith(f"-{suf}"):
                extra.append(var[: -len(suf) - 1] + f" {suf}")
                break
            if var.endswith(f" {suf}"):
                extra.append(var[: -len(suf) - 1] + f"-{suf}")
                break
    variants.extend(extra)
    return list(dict.fromkeys(variants))


def _find_op_variant(cards: list[dict], keywords: list[str]) -> dict | None:
    for card in cards:
        haystack = (
            (card.get("card_name") or "").lower()
            + " "
            + (card.get("card_image_id") or "").lower()
        )
        if any(kw in haystack for kw in keywords):
            return card
    return None


def _find_op_base(cards: list[dict], name: str) -> dict | None:
    name_lower = name.lower()
    for card in cards:
        cn = (card.get("card_name") or "").lower()
        if cn == name_lower or (name_lower in cn and "(" not in cn):
            return card
    return None


async def _claude_identify(client: AsyncAnthropic, image_bytes: bytes, media_type: str) -> dict:
    if len(image_bytes) > MAX_IMAGE_BYTES:
        image_bytes, media_type = _compress_card_image(image_bytes)
    b64_image = base64.standard_b64encode(image_bytes).decode("utf-8")
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
    text = response.content[0].text.strip()
    if text.startswith("```"):
        parts = text.split("```")
        text = parts[1].lstrip("json").strip() if len(parts) > 1 else text
    return json.loads(text)


async def _lookup_pokemon(name: str, number: str | None) -> dict | None:
    PROMO_RE = re.compile(r"^(SWSH|XY|SM|SV|BW|DP|HGSS)[- ]?P?\d+$", re.IGNORECASE)
    card_number = number
    printed_total: str | None = None
    is_promo_number = bool(number and PROMO_RE.match(number.strip()))
    pokemon_key = os.getenv("POKEMON_TCG_API_KEY", "")
    headers = {"X-Api-Key": pokemon_key} if pokemon_key else {}

    if is_promo_number:
        card_number = number.strip().upper()
    elif number and "/" in number and not is_promo_number:
        parts = number.split("/", 1)
        card_number = str(int(parts[0].strip())) if parts[0].strip().isdigit() else parts[0].strip()
        raw_total = parts[1].strip()
        printed_total = str(int(raw_total)) if raw_total.isdigit() else raw_total
    elif number and number.isdigit():
        card_number = str(int(number))

    attempts: list[tuple[str, str | None]] = []
    variants = _name_variants(name)
    if card_number:
        attempts += [(n, card_number) for n in variants]
    attempts += [(n, None) for n in variants]

    async with httpx.AsyncClient(timeout=15) as client:
        for attempt_name, attempt_number in attempts:
            query = f'name:"{attempt_name}"'
            if attempt_number:
                query += f" number:{attempt_number}"
            if attempt_number and printed_total:
                query += f" set.printedTotal:{printed_total}"
            resp = await client.get(POKEMON_API, params={"q": query, "pageSize": 1, "orderBy": "-set.releaseDate"}, headers=headers)
            if resp.status_code == 200:
                results = resp.json().get("data", [])
                if results:
                    return results[0]

        if printed_total and printed_total.isdigit() and int(printed_total) >= 250:
            raw_part = number.split("/")[0].strip() if number and "/" in number else None
            candidates: list[str] = []
            for prefix in ("SWSH", "SM", "XY", "BW", "SV"):
                if raw_part:
                    candidates.append(f"{prefix}{raw_part}")
                candidates.append(f"{prefix}{printed_total}")
            for attempt_name in variants:
                for cand_num in candidates:
                    query = f'name:"{attempt_name}" number:{cand_num}'
                    resp = await client.get(POKEMON_API, params={"q": query, "pageSize": 1, "orderBy": "-set.releaseDate"}, headers=headers)
                    if resp.status_code == 200:
                        results = resp.json().get("data", [])
                        if results:
                            return results[0]
    return None


async def _lookup_scryfall(name: str) -> dict | None:
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(SCRYFALL_API, params={"fuzzy": name})
        if resp.status_code == 200:
            return resp.json()
    return None


async def _lookup_yugioh(name: str) -> dict | None:
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(YGOPRODECK_API, params={"name": name, "num": 1, "offset": 0})
        if resp.status_code == 200:
            results = resp.json().get("data", [])
            return results[0] if results else None
    return None


async def _lookup_onepiece(name: str, number: str | None, is_sp: bool, is_alt_art: bool) -> tuple[dict | None, list[dict]]:
    async with httpx.AsyncClient(timeout=15) as client:
        all_results: list[dict] = []
        seen_ids: set[str] = set()

        def _merge(new_cards: list[dict]) -> None:
            for c in new_cards:
                cid = c.get("card_image_id")
                if cid not in seen_ids:
                    seen_ids.add(cid)
                    all_results.append(c)

        if number and "-" in number:
            resp = await client.get(f"{OPTCG_API}/sets/card/{number}/")
            if resp.status_code == 200:
                data = resp.json()
                batch = data if isinstance(data, list) else ([data] if data else [])
                _merge(batch)

        if not all_results:
            resp = await client.get(f"{OPTCG_API}/sets/filtered/", params={"card_name": name})
            if resp.status_code == 200:
                data = resp.json()
                _merge(data if isinstance(data, list) else [])

        if not all_results:
            return None, []

        if is_sp:
            sp = _find_op_variant(all_results, ["(sp)"])
            if sp:
                return sp, all_results
        if is_alt_art:
            alt = _find_op_variant(all_results, ["(alternate art)", "alternate art"])
            if alt:
                return alt, all_results

        base = _find_op_base(all_results, name)
        return (base or all_results[0]), all_results


def _format_pokemon_price(prices: dict) -> tuple[str | None, float | None, float | None]:
    priority = ["holofoil", "reverseHolofoil", "normal", "1stEditionHolofoil", "unlimitedHolofoil"]
    for key in priority:
        tier = prices.get(key, {})
        market = tier.get("market")
        if market is not None:
            low = float(tier["low"]) if tier.get("low") else None
            label = key.replace("H", " H").replace("1st", "1st Ed.").strip()
            return f"${market:.2f} ({label})", float(market), low
    return None, None, None


def _build_card_result(game: str, card_name: str, card_number: str | None, card_set: str | None,
                       card_data, promo_stamp=None, is_sp=False, is_manga=False, is_alt_art=False,
                       all_variants=None) -> dict:
    """Build a JSON-serializable card result dict for the web UI."""
    result = {
        "game": game,
        "name": card_name,
        "number": card_number,
        "set": card_set,
        "found": False,
        "image": None,
        "market_price": None,
        "low_price": None,
        "fields": [],
        "variants": [],
        "promo_stamp": promo_stamp,
        "is_sp": is_sp,
        "is_manga": is_manga,
        "is_alt_art": is_alt_art,
        "source": "Claude Vision only",
    }

    if game == "pokemon" and card_data:
        result["found"] = True
        result["source"] = "pokemontcg.io"
        si = card_data.get("set", {})
        api_number = card_data.get("number", card_number or "?")
        PROMO_RE = re.compile(r"^(SWSH|XY|SM|SV|BW|DP|HGSS)[- ]?P?\d+$", re.IGNORECASE)
        if PROMO_RE.match(str(api_number)):
            full_number = api_number
        else:
            full_number = f"{api_number}/{si.get('printedTotal', '?')}"
        tcg_prices = card_data.get("tcgplayer", {}).get("prices", {})
        tcg_url = card_data.get("tcgplayer", {}).get("url")
        cm_prices = card_data.get("cardmarket", {}).get("prices", {})
        price_str, market, low = _format_pokemon_price(tcg_prices)
        if not price_str and cm_prices:
            trend = cm_prices.get("trendPrice") or cm_prices.get("averageSellPrice")
            if trend:
                price_str = f"€{float(trend):.2f} (Cardmarket trend)"
                market = float(trend)
        if not price_str:
            price_str = "No price data"
        result["name"] = card_data.get("name", card_name)
        result["market_price"] = market
        result["low_price"] = low
        result["image"] = card_data.get("images", {}).get("large") or card_data.get("images", {}).get("small")
        set_name = si.get("name", "Unknown Set")
        set_series = si.get("series", "")
        result["set"] = set_name + (f" ({set_series})" if set_series else "")
        result["number"] = full_number
        result["fields"] = [
            {"label": "Set", "value": result["set"]},
            {"label": "Number", "value": full_number},
            {"label": "Rarity", "value": card_data.get("rarity", "Unknown")},
        ]
        if card_data.get("hp"):
            result["fields"].append({"label": "HP", "value": card_data["hp"]})
        types = ", ".join(card_data.get("types") or card_data.get("subtypes") or [])
        if types:
            result["fields"].append({"label": "Type", "value": types})
        if card_data.get("artist"):
            result["fields"].append({"label": "Artist", "value": card_data["artist"]})
        result["fields"].append({"label": "Price", "value": price_str})
        if tcg_url:
            result["fields"].append({"label": "TCGPlayer", "value": tcg_url, "link": True})

    elif game == "magic" and card_data and card_data.get("object") != "error":
        result["found"] = True
        result["source"] = "Scryfall"
        prices = card_data.get("prices", {})
        usd = prices.get("usd")
        usd_foil = prices.get("usd_foil")
        result["market_price"] = float(usd) if usd else None
        image_uris = card_data.get("image_uris") or {}
        if not image_uris and card_data.get("card_faces"):
            image_uris = card_data["card_faces"][0].get("image_uris", {})
        result["image"] = image_uris.get("normal") or image_uris.get("small")
        result["name"] = card_data.get("name", card_name)
        result["set"] = card_data.get("set_name", "Unknown")
        result["number"] = card_data.get("collector_number", "—")
        price_parts = []
        if usd:
            price_parts.append(f"${usd}")
        if usd_foil:
            price_parts.append(f"${usd_foil} foil")
        result["fields"] = [
            {"label": "Set", "value": result["set"]},
            {"label": "Number", "value": result["number"]},
            {"label": "Rarity", "value": card_data.get("rarity", "Unknown").title()},
            {"label": "Mana Cost", "value": card_data.get("mana_cost") or "—"},
            {"label": "Type", "value": card_data.get("type_line", "—")},
        ]
        if price_parts:
            result["fields"].append({"label": "Price", "value": " / ".join(price_parts)})

    elif game == "yugioh" and card_data:
        result["found"] = True
        result["source"] = "YGOProDeck"
        prices = card_data.get("card_prices", [{}])[0]
        tcg_price = prices.get("tcgplayer_price")
        result["market_price"] = float(tcg_price) if tcg_price and tcg_price != "0.00" else None
        images = card_data.get("card_images", [])
        result["image"] = images[0].get("image_url") if images else None
        result["name"] = card_data.get("name", card_name)
        result["fields"] = [
            {"label": "Type", "value": card_data.get("type", "—")},
            {"label": "Race", "value": card_data.get("race", "—")},
            {"label": "Attribute", "value": card_data.get("attribute", "—")},
        ]
        if card_data.get("level") is not None:
            result["fields"].append({"label": "Level", "value": str(card_data["level"])})
        if card_data.get("atk") is not None:
            result["fields"].append({"label": "ATK / DEF", "value": f"{card_data['atk']} / {card_data.get('def', '?')}"})
        if result["market_price"]:
            result["fields"].append({"label": "Price", "value": f"${tcg_price}"})

    elif game == "one piece" and card_data:
        result["found"] = True
        result["source"] = "optcgapi.com"
        mp = card_data.get("market_price")
        result["market_price"] = float(mp) if mp else None
        result["image"] = card_data.get("card_image")
        result["name"] = card_data.get("card_name", card_name)
        result["set"] = card_data.get("set_name", "—")
        result["number"] = card_data.get("card_set_id") or card_number or "—"
        result["fields"] = [
            {"label": "Set", "value": result["set"]},
            {"label": "Number", "value": result["number"]},
            {"label": "Rarity", "value": card_data.get("rarity", "—")},
            {"label": "Color", "value": card_data.get("card_color", "—")},
            {"label": "Card Type", "value": card_data.get("card_type", "—")},
        ]
        if card_data.get("card_cost") is not None:
            result["fields"].append({"label": "Cost", "value": str(card_data["card_cost"])})
        if card_data.get("card_power"):
            result["fields"].append({"label": "Power", "value": str(card_data["card_power"])})
        # Variant data
        all_v = all_variants or []
        if len(all_v) > 1:
            for vi, v in enumerate(all_v):
                vmp = v.get("market_price")
                is_selected = (v is card_data)
                variant_entry = {
                    "name": v.get("card_name", "—"),
                    "price": f"${float(vmp):.2f}" if vmp else "N/A",
                    "market_price": float(vmp) if vmp else None,
                    "image": v.get("card_image"),
                    "rarity": v.get("rarity", "—"),
                    "selected": is_selected,
                    "fields": [
                        {"label": "Set", "value": v.get("set_name", "—")},
                        {"label": "Number", "value": v.get("card_set_id") or "—"},
                        {"label": "Rarity", "value": v.get("rarity", "—")},
                        {"label": "Color", "value": v.get("card_color", "—")},
                        {"label": "Card Type", "value": v.get("card_type", "—")},
                    ],
                }
                if v.get("card_cost") is not None:
                    variant_entry["fields"].append({"label": "Cost", "value": str(v["card_cost"])})
                if v.get("card_power"):
                    variant_entry["fields"].append({"label": "Power", "value": str(v["card_power"])})
                if vmp:
                    variant_entry["fields"].append({"label": "Price", "value": f"${float(vmp):.2f}"})
                    variant_entry["conditions"] = {}
                    m = float(vmp)
                    for cond, mult in CONDITION_MULTIPLIERS.items():
                        variant_entry["conditions"][cond] = round(m * mult, 2)
                else:
                    variant_entry["conditions"] = {}
                result["variants"].append(variant_entry)
        elif result["market_price"]:
            result["fields"].append({"label": "Price", "value": f"${result['market_price']:.2f}"})
    else:
        # Other / Claude-only fallback
        if card_set:
            result["fields"].append({"label": "Set", "value": card_set})
        if card_number:
            result["fields"].append({"label": "Number", "value": card_number})

    # Condition prices
    if result["market_price"]:
        m = result["market_price"]
        lo = result["low_price"]
        conditions = {}
        for cond, mult in CONDITION_MULTIPLIERS.items():
            if cond == "NM":
                conditions[cond] = round(m, 2)
            elif cond == "LP" and lo:
                conditions[cond] = round((m + lo) / 2, 2)
            elif cond == "MP" and lo:
                conditions[cond] = round(lo, 2)
            else:
                conditions[cond] = round(m * mult, 2)
        result["conditions"] = conditions
    else:
        result["conditions"] = {}

    return result


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

# ---- Helpers ----
def get_real_ip(request: Request) -> str:
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    return request.client.host

def get_current_user(request: Request):
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
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
        has_role = bool(REQUIRED_ROLE_IDS & set(roles))

        return has_role, user

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
    if not await terms_current(request, user):
        return RedirectResponse("/terms")
    is_admin = int(user["id"]) in ADMIN_USER_IDS
    return templates.TemplateResponse("index.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "is_admin": is_admin,
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
    if not await terms_current(request, user):
        return RedirectResponse("/terms")
    is_admin = int(user["id"]) in ADMIN_USER_IDS
    return templates.TemplateResponse("status.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "is_admin": is_admin,
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
    ])

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
    })

@app.get("/api/contributors")
async def get_contributors(request: Request, user=Depends(get_current_user)):
    if int(user["id"]) not in ADMIN_USER_IDS:
        raise HTTPException(status_code=403, detail="Not authorized")

    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH
            cutoff_cte AS (
                SELECT DATE_TRUNC('month', NOW() AT TIME ZONE 'America/New_York') AS cutoff
            ),
            restock_points AS (
                SELECT user_id,
                    SUM(CASE WHEN date >= ((SELECT cutoff FROM cutoff_cte) - INTERVAL '30 days')::date AND date < (SELECT cutoff FROM cutoff_cte)::date
                        THEN CASE WHEN store_name IN ('Target','Walmart','5 Below','Barnes and Noble','Best Buy') THEN 1 ELSE 0.5 END ELSE 0 END) +
                    SUM(CASE WHEN date >= ((SELECT cutoff FROM cutoff_cte) - INTERVAL '60 days')::date AND date < ((SELECT cutoff FROM cutoff_cte) - INTERVAL '30 days')::date
                        THEN CASE WHEN store_name IN ('Target','Walmart','5 Below','Barnes and Noble','Best Buy') THEN 0.8 ELSE 0.4 END ELSE 0 END) +
                    SUM(CASE WHEN date >= ((SELECT cutoff FROM cutoff_cte) - INTERVAL '90 days')::date AND date < ((SELECT cutoff FROM cutoff_cte) - INTERVAL '60 days')::date
                        THEN CASE WHEN store_name IN ('Target','Walmart','5 Below','Barnes and Noble','Best Buy') THEN 0.6 ELSE 0.3 END ELSE 0 END)
                    AS restock_pts,
                    SUM(CASE WHEN date >= ((SELECT cutoff FROM cutoff_cte) - INTERVAL '30 days')::date AND date < (SELECT cutoff FROM cutoff_cte)::date
                        THEN CASE WHEN store_name IN ('Target','Walmart','5 Below','Barnes and Noble','Best Buy') THEN 1 ELSE 0.5 END ELSE 0 END) AS r_30,
                    SUM(CASE WHEN date >= ((SELECT cutoff FROM cutoff_cte) - INTERVAL '60 days')::date AND date < ((SELECT cutoff FROM cutoff_cte) - INTERVAL '30 days')::date
                        THEN CASE WHEN store_name IN ('Target','Walmart','5 Below','Barnes and Noble','Best Buy') THEN 1 ELSE 0.5 END ELSE 0 END) AS r_60,
                    SUM(CASE WHEN date >= ((SELECT cutoff FROM cutoff_cte) - INTERVAL '90 days')::date AND date < ((SELECT cutoff FROM cutoff_cte) - INTERVAL '60 days')::date
                        THEN CASE WHEN store_name IN ('Target','Walmart','5 Below','Barnes and Noble','Best Buy') THEN 1 ELSE 0.5 END ELSE 0 END) AS r_90
                FROM restock_reports
                WHERE channel_name NOT IN ('online-restock-information','other-online-restocks','pokemon-center-drops')
                GROUP BY user_id
            ),
            empty_points AS (
                SELECT user_id,
                    SUM(CASE WHEN timestamp >= (SELECT cutoff FROM cutoff_cte) - INTERVAL '30 days' AND timestamp < (SELECT cutoff FROM cutoff_cte)
                        THEN CASE WHEN EXTRACT(DOW FROM timestamp AT TIME ZONE 'America/New_York') IN (0,6) THEN 0.05 ELSE 0.1 END ELSE 0 END) +
                    SUM(CASE WHEN timestamp >= (SELECT cutoff FROM cutoff_cte) - INTERVAL '60 days' AND timestamp < (SELECT cutoff FROM cutoff_cte) - INTERVAL '30 days'
                        THEN CASE WHEN EXTRACT(DOW FROM timestamp AT TIME ZONE 'America/New_York') IN (0,6) THEN 0.05 ELSE 0.1 END ELSE 0 END) +
                    SUM(CASE WHEN timestamp >= (SELECT cutoff FROM cutoff_cte) - INTERVAL '90 days' AND timestamp < (SELECT cutoff FROM cutoff_cte) - INTERVAL '60 days'
                        THEN CASE WHEN EXTRACT(DOW FROM timestamp AT TIME ZONE 'America/New_York') IN (0,6) THEN 0.05 ELSE 0.1 END ELSE 0 END)
                    AS empty_pts,
                    SUM(CASE WHEN timestamp >= (SELECT cutoff FROM cutoff_cte) - INTERVAL '30 days' AND timestamp < (SELECT cutoff FROM cutoff_cte) THEN 1 ELSE 0 END) AS e_30,
                    SUM(CASE WHEN timestamp >= (SELECT cutoff FROM cutoff_cte) - INTERVAL '60 days' AND timestamp < (SELECT cutoff FROM cutoff_cte) - INTERVAL '30 days' THEN 1 ELSE 0 END) AS e_60,
                    SUM(CASE WHEN timestamp >= (SELECT cutoff FROM cutoff_cte) - INTERVAL '90 days' AND timestamp < (SELECT cutoff FROM cutoff_cte) - INTERVAL '60 days' THEN 1 ELSE 0 END) AS e_90
                FROM command_logs
                WHERE command_used = 'empty'
                  AND location NOT LIKE '%|Costco'
                  AND location NOT LIKE '%|Sam''s Club'
                  AND location NOT LIKE '%|CVS'
                  AND location NOT LIKE '%|Walgreens'
                GROUP BY user_id
            ),
            plusone_points AS (
                SELECT receiver_id AS user_id,
                    COALESCE(SUM(CASE WHEN timestamp >= (SELECT cutoff FROM cutoff_cte) - INTERVAL '30 days' AND timestamp < (SELECT cutoff FROM cutoff_cte) THEN value ELSE 0 END), 0) AS p_30,
                    COALESCE(SUM(CASE WHEN timestamp >= (SELECT cutoff FROM cutoff_cte) - INTERVAL '60 days' AND timestamp < (SELECT cutoff FROM cutoff_cte) - INTERVAL '30 days' THEN value ELSE 0 END), 0) AS p_60,
                    COALESCE(SUM(CASE WHEN timestamp >= (SELECT cutoff FROM cutoff_cte) - INTERVAL '90 days' AND timestamp < (SELECT cutoff FROM cutoff_cte) - INTERVAL '60 days' THEN value ELSE 0 END), 0) AS p_90
                FROM plusones
                GROUP BY receiver_id
            ),
            manual_points_cte AS (
                SELECT receiver_id AS user_id,
                    COALESCE(SUM(CASE WHEN timestamp >= (SELECT cutoff FROM cutoff_cte) - INTERVAL '30 days' AND timestamp < (SELECT cutoff FROM cutoff_cte) THEN value ELSE 0 END), 0) AS m_30,
                    COALESCE(SUM(CASE WHEN timestamp >= (SELECT cutoff FROM cutoff_cte) - INTERVAL '60 days' AND timestamp < (SELECT cutoff FROM cutoff_cte) - INTERVAL '30 days' THEN value ELSE 0 END), 0) AS m_60,
                    COALESCE(SUM(CASE WHEN timestamp >= (SELECT cutoff FROM cutoff_cte) - INTERVAL '90 days' AND timestamp < (SELECT cutoff FROM cutoff_cte) - INTERVAL '60 days' THEN value ELSE 0 END), 0) AS m_90
                FROM manual_points
                GROUP BY receiver_id
            ),
            hope_points AS (
                SELECT user_id,
                    COALESCE(SUM(CASE WHEN timestamp >= (SELECT cutoff FROM cutoff_cte) - INTERVAL '30 days' AND timestamp < (SELECT cutoff FROM cutoff_cte) THEN value ELSE 0 END), 0) AS h_30,
                    COALESCE(SUM(CASE WHEN timestamp >= (SELECT cutoff FROM cutoff_cte) - INTERVAL '60 days' AND timestamp < (SELECT cutoff FROM cutoff_cte) - INTERVAL '30 days' THEN value ELSE 0 END), 0) AS h_60,
                    COALESCE(SUM(CASE WHEN timestamp >= (SELECT cutoff FROM cutoff_cte) - INTERVAL '90 days' AND timestamp < (SELECT cutoff FROM cutoff_cte) - INTERVAL '60 days' THEN value ELSE 0 END), 0) AS h_90
                FROM hope_contributions
                GROUP BY user_id
            ),
            combined AS (
                SELECT
                    COALESCE(r.user_id, e.user_id, p.user_id, m.user_id, h.user_id) AS user_id,
                    COALESCE(r.restock_pts, 0) AS restock_pts,
                    COALESCE(e.empty_pts, 0) AS empty_pts,
                    COALESCE(p.p_30, 0) + COALESCE(p.p_60, 0) + COALESCE(p.p_90, 0) AS plusone_pts,
                    COALESCE(m.m_30, 0) + COALESCE(m.m_60, 0) + COALESCE(m.m_90, 0) AS manual_pts,
                    COALESCE(h.h_30, 0) + COALESCE(h.h_60, 0) + COALESCE(h.h_90, 0) AS hope_pts,
                    COALESCE(r.restock_pts, 0) + COALESCE(e.empty_pts, 0) +
                    COALESCE(p.p_30, 0) + COALESCE(p.p_60, 0) + COALESCE(p.p_90, 0) +
                    COALESCE(m.m_30, 0) + COALESCE(m.m_60, 0) + COALESCE(m.m_90, 0) +
                    COALESCE(h.h_30, 0) + COALESCE(h.h_60, 0) + COALESCE(h.h_90, 0) AS total_points,
                    COALESCE(r.r_30, 0) AS r_30, COALESCE(r.r_60, 0) AS r_60, COALESCE(r.r_90, 0) AS r_90,
                    COALESCE(e.e_30, 0) AS e_30, COALESCE(e.e_60, 0) AS e_60, COALESCE(e.e_90, 0) AS e_90,
                    COALESCE(p.p_30, 0) AS p_30, COALESCE(p.p_60, 0) AS p_60, COALESCE(p.p_90, 0) AS p_90,
                    COALESCE(m.m_30, 0) AS m_30, COALESCE(m.m_60, 0) AS m_60, COALESCE(m.m_90, 0) AS m_90,
                    COALESCE(h.h_30, 0) AS h_30, COALESCE(h.h_60, 0) AS h_60, COALESCE(h.h_90, 0) AS h_90
                FROM restock_points r
                FULL OUTER JOIN empty_points e ON r.user_id = e.user_id
                FULL OUTER JOIN plusone_points p ON COALESCE(r.user_id, e.user_id) = p.user_id
                FULL OUTER JOIN manual_points_cte m ON COALESCE(r.user_id, e.user_id, p.user_id) = m.user_id
                FULL OUTER JOIN hope_points h ON COALESCE(r.user_id, e.user_id, p.user_id, m.user_id) = h.user_id
            )
            SELECT c.*,
                COALESCE(u.username, ds.username) AS username
            FROM combined c
            LEFT JOIN users u ON u.user_id = c.user_id
            LEFT JOIN LATERAL (
                SELECT username FROM dashboard_sessions
                WHERE user_id = c.user_id
                ORDER BY logged_in_at DESC
                LIMIT 1
            ) ds ON true
            ORDER BY total_points DESC
            LIMIT 100
            """
        )

    return JSONResponse([
        {
            "user_id": str(r["user_id"]),
            "username": r["username"] or f"User {r['user_id']}",
            "restock_pts": round(float(r["restock_pts"]), 2),
            "empty_pts":   round(float(r["empty_pts"]), 2),
            "plusone_pts": round(float(r["plusone_pts"]), 2),
            "manual_pts":  round(float(r["manual_pts"]), 2),
            "hope_pts":    round(float(r["hope_pts"]), 2),
            "total_points":round(float(r["total_points"]), 2),
            "r_30": int(r["r_30"]), "r_60": int(r["r_60"]), "r_90": int(r["r_90"]),
            "e_30": int(r["e_30"]), "e_60": int(r["e_60"]), "e_90": int(r["e_90"]),
            "p_30": round(float(r["p_30"]), 2), "p_60": round(float(r["p_60"]), 2), "p_90": round(float(r["p_90"]), 2),
            "m_30": round(float(r["m_30"]), 2), "m_60": round(float(r["m_60"]), 2), "m_90": round(float(r["m_90"]), 2),
            "h_30": round(float(r["h_30"]), 2), "h_60": round(float(r["h_60"]), 2), "h_90": round(float(r["h_90"]), 2),
        }
        for r in rows
    ])

# ---- Map ----

@app.get("/map", response_class=HTMLResponse)
async def map_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/login")
    if not await terms_current(request, user):
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

@app.get("/scan", response_class=HTMLResponse)
async def scan_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse("/login")
    if not await terms_current(request, user):
        return RedirectResponse("/terms")
    is_admin = int(user["id"]) in ADMIN_USER_IDS
    return templates.TemplateResponse("scan.html", {
        "request": request,
        "username": user["username"],
        "avatar": user.get("avatar"),
        "user_id": user["id"],
        "is_admin": is_admin,
    })


@app.post("/api/scan")
async def scan_card(
    request: Request,
    user=Depends(get_current_user)
):
    body = await request.json()
    image_data = body.get("image")
    if not image_data:
        raise HTTPException(status_code=400, detail="No image provided")

    # Strip data URL prefix if present
    if "," in image_data:
        header, image_data = image_data.split(",", 1)
        media_type = header.split(";")[0].split(":")[1] if ":" in header else "image/jpeg"
    else:
        media_type = "image/jpeg"

    try:
        image_bytes = base64.b64decode(image_data)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid base64 image")

    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=500, detail="Anthropic API key not configured")

    claude_client = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

    try:
        parsed = await _claude_identify(claude_client, image_bytes, media_type)
    except json.JSONDecodeError:
        return JSONResponse({"error": "Could not read the card — try a clearer photo."}, status_code=422)
    except Exception as e:
        logger.exception("Claude API error during card scan")
        return JSONResponse({"error": f"Claude API error: {str(e)}"}, status_code=500)

    game        = (parsed.get("game") or "other").lower().strip()
    card_name   = parsed.get("name")
    card_number = parsed.get("number")
    card_set    = parsed.get("set")
    is_sp       = bool(parsed.get("is_sp", False))
    is_manga    = bool(parsed.get("is_manga", False))
    is_alt_art  = bool(parsed.get("is_alt_art", False))
    promo_stamp = parsed.get("promo_stamp") or None

    if card_number and re.match(r"^SP\s+", card_number, re.IGNORECASE):
        is_sp = True
        card_number = re.sub(r"^SP\s+", "", card_number, flags=re.IGNORECASE).strip()

    if not card_name:
        return JSONResponse({"error": "Could not identify a card name — try a clearer photo."}, status_code=422)

    # Lookup
    card_data = None
    all_variants: list[dict] = []
    try:
        if game == "pokemon":
            card_data = await _lookup_pokemon(card_name, card_number)
        elif game == "magic":
            card_data = await _lookup_scryfall(card_name)
        elif game == "yugioh":
            card_data = await _lookup_yugioh(card_name)
        elif game == "one piece":
            card_data, all_variants = await _lookup_onepiece(card_name, card_number, is_sp, is_alt_art)
    except Exception:
        logger.exception(f"API lookup error for {game}")

    result = _build_card_result(
        game, card_name, card_number, card_set, card_data,
        promo_stamp=promo_stamp, is_sp=is_sp, is_manga=is_manga, is_alt_art=is_alt_art,
        all_variants=all_variants,
    )
    return JSONResponse(result)


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
    if days not in (7, 14, 21, 28, 35, 42, 49, 56):
        raise HTTPException(status_code=400, detail="Invalid days value")

    eastern = ZoneInfo("America/New_York")
    now = datetime.now(eastern)
    since = now - timedelta(days=days)

    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                rr.location,
                rr.store_name,
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
        })

    return JSONResponse(result)

@app.get("/api/locations")
async def get_locations(
    request: Request,
    region: str = "VA",
    user=Depends(get_current_user)
):
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

    return JSONResponse(result)

# ---- Preferences API ----

@app.get("/api/preferences")
async def get_preferences(
    request: Request,
    region: str = "VA",
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
    region = body.get("region", "VA")
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