"""Set-import providers for the card tracker — enumerate a set's cards from
the FREE catalog APIs (pokemontcg.io, optcgapi) so mass-adding a set costs
zero JustTCG budget.

Every provider normalizes to:
    {"name", "card_number", "variant", "release_date"}  (+ set_name)

NOTE: the One Piece set-listing endpoints on optcgapi are unverified guesses
(the scanner only uses its single-card + name-search endpoints). If both
attempts fail the UI shows a clear error rather than importing garbage.
"""

import os
import re
import logging
from datetime import datetime

import httpx

logger = logging.getLogger("dashboard.set_import")

POKEMONTCG_BASE = "https://api.pokemontcg.io/v2"
OPTCG_BASE = "https://www.optcgapi.com/api"

# Hard caps — safeguards against a bad API response flooding the tracker.
MAX_CARDS_PER_IMPORT = 250
OP_SET_CODE_RE = re.compile(r"^(OP|EB|ST|PRB)-?\d{1,3}$", re.IGNORECASE)


def _pokemon_headers() -> dict:
    key = os.getenv("POKEMON_TCG_API_KEY", "")
    return {"X-Api-Key": key} if key else {}


def _parse_date(raw):
    if not raw or not isinstance(raw, str):
        return None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw[:10], fmt).date()
        except ValueError:
            continue
    return None


async def fetch_pokemon_sets(client: httpx.AsyncClient) -> list:
    """All Pokémon sets, newest first: [{id, name, release_date, total}]."""
    resp = await client.get(f"{POKEMONTCG_BASE}/sets",
                            params={"pageSize": 250, "orderBy": "-releaseDate"},
                            headers=_pokemon_headers())
    resp.raise_for_status()
    out = []
    for s in resp.json().get("data", []):
        out.append({
            "id": s.get("id"),
            "name": s.get("name"),
            "release_date": (_parse_date(s.get("releaseDate")) or "") and _parse_date(s.get("releaseDate")).isoformat(),
            "total": s.get("total"),
        })
    return out


async def fetch_pokemon_set_cards(client: httpx.AsyncClient, set_id: str) -> tuple:
    """(set_name, cards) for one Pokémon set. card_number is the full printed
    form (e.g. '223/197') to help JustTCG matching; variant is the rarity."""
    cards, page, set_name, release = [], 1, None, None
    while page <= 3:  # sets are <= ~600 cards; safety bound
        resp = await client.get(f"{POKEMONTCG_BASE}/cards",
                                params={"q": f"set.id:{set_id}", "pageSize": 250, "page": page},
                                headers=_pokemon_headers())
        resp.raise_for_status()
        batch = resp.json().get("data", [])
        if not batch:
            break
        for c in batch:
            s = c.get("set") or {}
            set_name = set_name or s.get("name")
            release = release or _parse_date(s.get("releaseDate"))
            number = str(c.get("number") or "")
            printed = s.get("printedTotal")
            full_number = f"{number}/{printed}" if printed and number.isdigit() else number
            cards.append({
                "name": c.get("name") or "",
                "card_number": full_number,
                "variant": c.get("rarity") or "",
                "release_date": release.isoformat() if release else None,
            })
        if len(batch) < 250:
            break
        page += 1
    return set_name, cards


_PAREN_RE = re.compile(r"\s*\(([^)]+)\)\s*$")


def _split_op_name(raw_name: str) -> tuple:
    """optcgapi names often carry the printing as a suffix, e.g.
    'Monkey.D.Luffy (Alternate Art)'. Split it into (name, suffix)."""
    m = _PAREN_RE.search(raw_name or "")
    if m:
        return raw_name[:m.start()].strip(), m.group(1).strip()
    return (raw_name or "").strip(), ""


async def fetch_onepiece_set_cards(client: httpx.AsyncClient, set_code: str) -> tuple:
    """(set_name, cards) for one One Piece set code (e.g. OP09, EB01).

    Tries two plausible optcgapi endpoints; neither is verified against docs,
    so failures raise with a clear message instead of guessing further.
    """
    code = set_code.strip().upper().replace(" ", "")
    if not OP_SET_CODE_RE.match(code):
        raise ValueError("Set code should look like OP09, EB01, ST13, or PRB01")
    code = code.replace("-", "")

    batch = None
    for url in (f"{OPTCG_BASE}/sets/{code}/", f"{OPTCG_BASE}/sets/filtered/"):
        try:
            params = {"set_id": code} if url.endswith("filtered/") else None
            resp = await client.get(url, params=params)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and data:
                    batch = data
                    break
        except Exception:
            logger.exception("optcgapi set fetch failed at %s", url)
    if not batch:
        raise RuntimeError(
            f"Couldn't enumerate One Piece set {code} from optcgapi — the set-listing "
            "endpoint may differ from what this trial assumes. Add these cards via "
            "watchlist.py for now and check the server logs.")

    set_name, cards = None, []
    for c in batch:
        num = str(c.get("card_set_id") or "")
        # Guard against a fuzzy endpoint returning cards from other sets.
        if code[:2] in ("OP", "EB", "ST") and num and not num.upper().startswith(code):
            continue
        set_name = set_name or c.get("set_name")
        name, suffix = _split_op_name(c.get("card_name") or "")
        rarity = (c.get("rarity") or "").strip()
        variant = f"{rarity} ({suffix})" if suffix else rarity
        release = _parse_date(c.get("release_date") or c.get("set_release_date"))
        cards.append({
            "name": name,
            "card_number": num,
            "variant": variant,
            "release_date": release.isoformat() if release else None,
        })
    return set_name or code, cards
