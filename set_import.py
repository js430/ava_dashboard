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
# Compact set codes like "OP13" normalize to optcgapi's hyphenated ids ("OP-13").
_OP_COMPACT_RE = re.compile(r"^(OP|EB|ST|PRB)-?(\d{1,3})$", re.IGNORECASE)
_OP_ID_RE = re.compile(r"^[A-Z0-9-]{2,20}$")


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
                            params={"pageSize": 250, "orderBy": "-releaseDate",
                                    "select": "id,name,releaseDate,total"},
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
        # select= keeps the payload tiny (full card objects are ~50x larger and
        # routinely push pokemontcg.io past read timeouts).
        resp = await client.get(f"{POKEMONTCG_BASE}/cards",
                                params={"q": f"set.id:{set_id}", "pageSize": 250, "page": page,
                                        "select": "name,number,rarity,set"},
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


async def fetch_onepiece_sets(client: httpx.AsyncClient) -> list:
    """All One Piece sets from optcgapi: [{id, name, release_date, total}].
    Verified shape (2026-07-12): GET /allSets/ -> [{"set_name","set_id"}, ...]
    with hyphenated ids like "OP-13". No release dates available."""
    resp = await client.get(f"{OPTCG_BASE}/allSets/")
    resp.raise_for_status()
    out = []
    for s in resp.json():
        sid = (s.get("set_id") or "").strip()
        if sid:
            out.append({"id": sid, "name": s.get("set_name") or sid,
                        "release_date": None, "total": None})
    return out


def _normalize_op_set_id(raw: str) -> str:
    """Accept dropdown ids as-is ("OP-13", "OP14-EB04") and normalize typed
    compact codes ("op13" -> "OP-13"). optcgapi requires the hyphenated form."""
    code = (raw or "").strip().upper().replace(" ", "")
    m = _OP_COMPACT_RE.match(code)
    if m:
        return f"{m.group(1)}-{m.group(2).zfill(2)}"
    if not _OP_ID_RE.match(code):
        raise ValueError("Set code should look like OP-13, EB-01, or PRB-01")
    return code


async def fetch_onepiece_set_cards(client: httpx.AsyncClient, set_code: str) -> tuple:
    """(set_name, cards) for one One Piece set.
    Verified shape (2026-07-12): GET /sets/{SET-ID}/ (hyphenated, e.g. OP-01)
    -> list of card objects with card_name/card_set_id/rarity/set_name.
    No release-date field exists, so release_date stays null for OP cards."""
    code = _normalize_op_set_id(set_code)
    resp = await client.get(f"{OPTCG_BASE}/sets/{code}/")
    if resp.status_code == 404:
        raise RuntimeError(f"Set {code} isn't on optcgapi (yet) — check the set list.")
    resp.raise_for_status()
    batch = resp.json()
    if not isinstance(batch, list) or not batch:
        raise RuntimeError(f"optcgapi returned no cards for set {code}.")

    set_name, cards = None, []
    for c in batch:
        num = str(c.get("card_set_id") or "")
        set_name = set_name or c.get("set_name")
        name, suffix = _split_op_name(c.get("card_name") or "")
        rarity = (c.get("rarity") or "").strip()
        variant = f"{rarity} ({suffix})" if suffix else rarity
        cards.append({
            "name": name,
            "card_number": num,
            "variant": variant,
            "release_date": None,
        })
    return set_name or code, cards
