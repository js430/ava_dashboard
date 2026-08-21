"""Import a Collectr portfolio: read names and quantities, match them to our
catalog, skip what doesn't match cleanly.

WHY THIS IS ITS OWN MODULE
    Collectr has no user-collection API — their published API is a product
    catalog only (see their API terms). So the data arrives either from a
    shared-portfolio payload or from CSV text the member pastes in. Both are
    shapes we do not control, so everything here is tolerant about input and
    strict about output: an item either matches something in our catalog well
    enough to trust, or it is SKIPPED and reported. A wrong match silently
    puts the wrong card in someone's portfolio with their money attached to
    it, which is worse than importing nothing.

MATCHING IS THE HARD PART, NOT PARSING
    "Charizard ex #223 Obsidian Flames" has to find one row among tens of
    thousands, and the same card exists in several printings at very
    different prices. So a match needs corroboration: a card number that
    agrees, or a set that agrees, or a name so close it cannot be anything
    else. Anything less is a skip.
"""

import csv
import io
import re
import logging
from difflib import SequenceMatcher

logger = logging.getLogger("dashboard.collectr")

# A match must beat this to be imported at all. Tuned so a set-or-number
# agreement carries an item through and a bare fuzzy name does not.
MIN_SCORE = 0.72
# Above this we stop looking — an exact-looking match is not improved by
# scanning the rest of the catalog.
GOOD_ENOUGH = 0.97
# Guardrail on a single paste, so a bad file can't spend an afternoon.
MAX_ITEMS = 2000

# Sealed product keywords. If a name contains one of these it is matched
# against sealed_products rather than the singles catalog — a "Booster Box"
# is never a card, and letting it fuzzy-match a card named "Box" would be a
# silent disaster.
SEALED_HINTS = (
    "booster box", "booster bundle", "elite trainer box", "etb", "booster pack",
    "blister", "tin", "collection box", "premium collection", "build & battle",
    "build and battle", "display", "case", "bundle", "gift set", "box set",
    "mini tin", "poster collection", "binder collection", "surprise box",
)

# Noise that appears in Collectr product names but not in ours, or vice
# versa. Removed from both sides before comparison so it can't sway a score.
_NOISE = re.compile(
    r"\b(?:pokemon|pokémon|tcg|trading card game|english|japanese|"
    r"factory sealed|sealed|new|nm|near mint|lp|mp|hp|raw|ungraded)\b", re.I)
_PUNCT = re.compile(r"[^\w\s#/-]")
_SPACES = re.compile(r"\s+")

# "#223", "223/197", "SV107", "TG12/TG30"
_NUMBER = re.compile(r"#\s*([\w-]+)|(?<![\w/])(\w{1,4}\d{1,4}[a-z]?)\s*/\s*(\w+)",
                     re.I)

# Grade markers: a graded card is a different holding from a raw one, and
# Collectr tracks them separately.
_GRADE = re.compile(
    r"\b(psa|bgs|cgc|sgc|tag|ace|ags)\s*[- ]?\s*(10|9\.5|9|8\.5|8|7|6|5|4|3|2|1)\b",
    re.I)


def normalize(text: str) -> str:
    """Lower-case, strip punctuation and boilerplate, collapse spaces."""
    if not text:
        return ""
    out = _NOISE.sub(" ", str(text))
    out = _PUNCT.sub(" ", out)
    return _SPACES.sub(" ", out).strip().lower()


# A name ENDING in one of these is sealed even without a keyword phrase:
# "One Piece Card Game Illustration Box", "Crown Zenith Tin Case". Cards
# essentially never end this way, and routing a box to the singles catalog
# is how it ends up skipped or, worse, matched to a card.
_SEALED_TAIL = ("box", "case", "collection", "bundle", "tin", "display",
                "pack", "blister")


def looks_sealed(name: str) -> bool:
    low = " " + (name or "").lower().strip() + " "
    if any(h in low for h in SEALED_HINTS):
        return True
    # Drop a trailing "[Variant]" or "(Exclusive)", then look at the last few
    # tokens rather than only the last one: "Illustration Box Vol. 1" ends in
    # "1", and a volume number should not decide whether something is sealed.
    trimmed = re.sub(r"[\[(][^\])]*[\])]\s*$", "", (name or "").strip()).strip()
    trimmed = re.sub(r"(?:vol\.?|volume|series|set)\s*\.?\s*\d+\s*$", "",
                     trimmed, flags=re.I).strip()
    words = [w.strip(".,-") for w in trimmed.lower().split() if w.strip(".,-")]
    return any(w in _SEALED_TAIL for w in words[-3:])


def extract_number(name: str) -> str:
    """The printed card number, if the name carries one.

    This is the single strongest matching signal we get: names vary wildly
    between catalogs, numbers do not.
    """
    m = _NUMBER.search(name or "")
    if not m:
        return ""
    if m.group(1):
        return m.group(1).strip().lstrip("0") or m.group(1).strip()
    return (m.group(2) or "").strip().lstrip("0") or (m.group(2) or "").strip()


def extract_grade(name: str) -> str:
    """"PSA 10" -> "psa_10", matching our own condition vocabulary."""
    m = _GRADE.search(name or "")
    if not m:
        return ""
    company = m.group(1).lower()
    grade = m.group(2).replace(".", "_")
    return f"{company}_{grade}"


def _similar(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def score_match(item: dict, candidate: dict) -> float:
    """How confident we are that `candidate` is `item`. 0..1.

    Name similarity alone tops out below the import threshold on purpose.
    Something else — the card number, or the set — has to agree before an
    item is imported, because "Charizard" matches a hundred rows and only one
    of them is the card somebody owns.
    """
    item_name = normalize(item.get("name"))
    cand_name = normalize(candidate.get("name"))
    if not item_name or not cand_name:
        return 0.0

    # Compare the name with the set stripped out of it: Collectr tends to
    # write "Obsidian Flames Charizard ex", we store name and set apart.
    cand_set = normalize(candidate.get("set_name"))
    bare = item_name
    if cand_set and cand_set in bare:
        bare = _SPACES.sub(" ", bare.replace(cand_set, " ")).strip()

    name_score = max(_similar(item_name, cand_name), _similar(bare, cand_name))
    score = name_score * 0.6

    # A number that agrees is the strongest signal available.
    item_number = item.get("card_number") or extract_number(item.get("name", ""))
    cand_number = str(candidate.get("card_number") or "").strip()
    if item_number and cand_number:
        left = item_number.split("/")[0].lstrip("0").lower()
        right = cand_number.split("/")[0].lstrip("0").lower()
        if left and left == right:
            score += 0.30
        else:
            # Numbers that disagree are evidence AGAINST, not neutral.
            score -= 0.25

    # A set that agrees corroborates; one that disagrees is not fatal, since
    # Collectr may not name sets the way we do.
    item_set = normalize(item.get("set_name")) or item_name
    if cand_set:
        if cand_set and cand_set in item_set:
            score += 0.18
        elif _similar(cand_set, normalize(item.get("set_name"))) > 0.85:
            score += 0.15

    return max(0.0, min(1.0, score))


def best_match(item: dict, candidates) -> tuple:
    """(candidate, score) for the best row, or (None, 0.0).

    Ties are broken toward the FIRST candidate, so callers should pass the
    catalog in a stable, sensible order (newest set first works well).
    """
    best, best_score = None, 0.0
    for cand in candidates:
        s = score_match(item, cand)
        if s > best_score:
            best, best_score = cand, s
            if s >= GOOD_ENOUGH:
                break
    return best, round(best_score, 3)


def classify(item: dict, singles, sealed) -> dict:
    """Match one item and say what happened.

    Returns a row the preview renders directly: what we read, what we think
    it is, how sure we are, and — when we are not sure enough — why it is
    being skipped, in words a member can act on.
    """
    name = (item.get("name") or "").strip()
    result = {
        "name": name,
        "quantity": item.get("quantity") or 1,
        "kind": "sealed" if looks_sealed(name) else "card",
        "grade": extract_grade(name),
        "unit_cost": item.get("unit_cost"),
        "match": None,
        "score": 0.0,
        "status": "skipped",
        "reason": "",
    }
    if not name:
        result["reason"] = "No product name."
        return result

    pool = sealed if result["kind"] == "sealed" else singles
    match, score = best_match(item, pool)
    result["score"] = score

    if not match:
        result["reason"] = ("Nothing in the catalog resembles this."
                            if pool else
                            "The catalog has no %s to match against yet."
                            % ("sealed product" if result["kind"] == "sealed" else "cards"))
        return result
    if score < MIN_SCORE:
        result["match"] = match
        result["reason"] = ("Closest is \"%s\"%s, but not close enough to be sure."
                            % (match.get("name", ""),
                               " (%s)" % match["set_name"] if match.get("set_name") else ""))
        return result

    result["match"] = match
    result["status"] = "matched"
    return result


def summarize(rows) -> dict:
    matched = [r for r in rows if r["status"] == "matched"]
    return {
        "items": len(rows),
        "matched": len(matched),
        "skipped": len(rows) - len(matched),
        "cards": sum(1 for r in matched if r["kind"] == "card"),
        "sealed": sum(1 for r in matched if r["kind"] == "sealed"),
        "units": sum(int(r["quantity"] or 0) for r in matched),
    }


# ── Reading what Collectr gives us ───────────────────────────────────────
# Two shapes: a shared-portfolio payload (JSON, structure not documented to
# us) and CSV text pasted in. Both are read tolerantly — see the sealed
# importer for the same reasoning.

_NAME_KEYS = ("product_name", "productName", "name", "title", "card_name",
              "cardName", "displayName")
_QTY_KEYS = ("quantity", "qty", "count", "amount", "owned", "numOwned")
_SET_KEYS = ("set_name", "setName", "set", "catalog_group", "catalogGroup",
             "expansion", "series")
_NUM_KEYS = ("card_number", "cardNumber", "number", "collector_number")
_COST_KEYS = ("purchase_price", "purchasePrice", "cost", "paid", "buy_price",
              "acquisition_price")


def _first(row: dict, keys):
    for k in keys:
        if isinstance(row, dict) and row.get(k) not in (None, ""):
            return row[k]
    return None


def _to_int(value, default=1):
    try:
        n = int(float(str(value).strip()))
        return n if n > 0 else default
    except (TypeError, ValueError):
        return default


def parse_items(payload):
    """Pull items out of a shared-portfolio payload, whatever it nests them in.

    Walks the structure for the first list of dicts that carries a name-ish
    key, so a wrapper like {"data": {"portfolio": {"items": [...]}}} works
    without us knowing the exact shape in advance.
    """
    found = []
    _collect(payload, found, 0)
    items = []
    for row in found[:MAX_ITEMS]:
        name = _first(row, _NAME_KEYS)
        if not name:
            continue
        items.append({
            "name": str(name).strip(),
            "quantity": _to_int(_first(row, _QTY_KEYS), 1),
            "set_name": str(_first(row, _SET_KEYS) or "").strip(),
            "card_number": str(_first(row, _NUM_KEYS) or "").strip(),
            "unit_cost": _first(row, _COST_KEYS),
        })
    return items


def _collect(node, out, depth):
    if depth > 6 or len(out) >= MAX_ITEMS:
        return
    if isinstance(node, dict):
        if any(k in node for k in _NAME_KEYS):
            out.append(node)
            return
        for value in node.values():
            _collect(value, out, depth + 1)
    elif isinstance(node, list):
        for value in node:
            _collect(value, out, depth + 1)


def parse_csv(text: str):
    """Items from pasted CSV text. Headers are matched loosely, because
    every exporter names its columns differently."""
    if not text or not text.strip():
        return []
    sample = text[:4000]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
    except csv.Error:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    items = []
    for row in reader:
        if len(items) >= MAX_ITEMS:
            break
        clean = {(k or "").strip().lower().replace(" ", "_"): v
                 for k, v in row.items() if k}
        name = _first(clean, _NAME_KEYS) or clean.get("product") or clean.get("item")
        if not name:
            continue
        items.append({
            "name": str(name).strip(),
            "quantity": _to_int(_first(clean, _QTY_KEYS), 1),
            "set_name": str(_first(clean, _SET_KEYS) or "").strip(),
            "card_number": str(_first(clean, _NUM_KEYS) or "").strip(),
            "unit_cost": _first(clean, _COST_KEYS),
        })
    return items
