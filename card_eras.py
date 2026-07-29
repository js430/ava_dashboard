"""Pokémon TCG set → era mapping, for grouping the catalog's set filter.

Source: TCGplayer, "Every Pokémon TCG Set in Order (Newest to Oldest)"
(published 2026-06-12), read 2026-07-28. Covers all 128 English expansions
plus the promotional and miscellaneous groups.

Promos are a STANDALONE group, deliberately. "SWSH Black Star Promos" sits
with the other promos rather than inside Sword & Shield — it isn't a
main-series expansion, and letting the series name in its title pull it into
that series would put a promo set among the boosters.

ADDING A NEW SET
----------------
Usually you don't have to. Sets are classified in five passes, and the first
four handle almost everything automatically:

  0. Anything whose name says "promo" -> the Promos group, before any other
     rule gets a chance to claim it.
  1. An explicit name in SET_ERAS below.
  2. A registered release code before a colon, a dash, or nothing but
     whitespace — vendors use all three: "SV10: Destined Rivals",
     "XY - Ancient Origins", "XY Base Set". ERA_PREFIXES resolves the code
     (SV, XY, SWSH, SM, ...) directly; the code wins over trying to look up
     whatever follows it, which is what correctly sends "XY Base Set" to the
     XY era instead of colliding with the unrelated vintage "Base Set"
     (1998) entry under `original`.
  3. Colon fallback for names #2 doesn't cover — a parent-set label
     ("Scarlet & Violet: Destined Rivals", "Generations: Radiant
     Collection") where the part before OR after the colon is a real,
     already-listed set name rather than a short code.
  4. A keyword rule for the McDonald's / POP / Trainer Kit groups.

Anything still unmatched falls into "Other" rather than being guessed at — a
set in the wrong era is worse than one in an obvious catch-all. If a new set
arrives with neither a prefix nor a recognisable name, add it to SET_ERAS.

Names are matched loosely (case, spacing and punctuation are ignored), so
"Scarlet & Violet—151", "Scarlet & Violet 151" and "SV: 151" all resolve.
"""

import re

# Newest first. This order IS the display order in the set filter.
ERA_ORDER = [
    ("mega_evolution",   "Mega Evolution"),
    ("scarlet_violet",   "Scarlet & Violet"),
    ("sword_shield",     "Sword & Shield"),
    ("sun_moon",         "Sun & Moon"),
    ("xy",               "XY"),
    ("black_white",      "Black & White"),
    ("hgss",             "HeartGold & SoulSilver"),
    ("platinum",         "Platinum"),
    ("diamond_pearl",    "Diamond & Pearl"),
    ("ex",               "EX"),
    ("ecard",            "e-Card"),
    ("legendary",        "Legendary Collection"),
    ("neo",              "Neo"),
    ("original",         "Original"),
    ("promos",           "Promos"),
    ("mcdonalds",        "McDonald's"),
    ("pop",              "POP Series"),
    ("trainer_kits",     "Trainer Kits"),
    ("other",            "Other"),
]
ERA_LABELS = dict(ERA_ORDER)
ERA_INDEX = {key: i for i, (key, _) in enumerate(ERA_ORDER)}
FALLBACK_ERA = "other"

# Set-name prefixes vendors use. PokemonPriceTracker names its sets this way
# ("SWSH: Crown Zenith"), which is what makes future sets self-classifying.
ERA_PREFIXES = {
    "me": "mega_evolution", "meg": "mega_evolution",
    "sv": "scarlet_violet", "svi": "scarlet_violet",
    "swsh": "sword_shield", "ssh": "sword_shield",
    "sm": "sun_moon",
    "xy": "xy",
    "bw": "black_white",
    "hgss": "hgss", "hs": "hgss",
    "pl": "platinum",
    "dp": "diamond_pearl",
    "ex": "ex",
    "pop": "pop",
}

# Explicit name → era. Subsets (Trainer Gallery, Shiny Vault, Galarian
# Gallery, Classic Collection) are listed with their parent set's era so they
# don't scatter into "Other".
_ERA_SETS = {
    "mega_evolution": [
        "Delta Reign", "30th Celebration", "Pitch Black", "Chaos Rising",
        "Perfect Order", "Ascended Heroes", "Phantasmal Flames", "Mega Evolution",
        "Mega Evolution Energies",
    ],
    "scarlet_violet": [
        "White Flare", "Black Bolt", "Destined Rivals", "Journey Together",
        "Prismatic Evolutions", "Surging Sparks", "Stellar Crown", "Shrouded Fable",
        "Twilight Masquerade", "Temporal Forces", "Paldean Fates", "Paradox Rift",
        "Scarlet & Violet—151", "Scarlet & Violet 151", "151",
        "Obsidian Flames", "Paldea Evolved", "Scarlet & Violet",
        "Scarlet & Violet Energies",
    ],
    "sword_shield": [
        "Crown Zenith", "Crown Zenith Galarian Gallery", "Silver Tempest",
        "Silver Tempest Trainer Gallery", "Lost Origin", "Lost Origin Trainer Gallery",
        "Pokémon GO", "Pokemon GO", "Pokémon TCG: Pokémon GO",
        "Astral Radiance", "Astral Radiance Trainer Gallery",
        "Brilliant Stars", "Brilliant Stars Trainer Gallery", "Fusion Strike",
        "Celebrations", "Celebrations: Classic Collection", "Evolving Skies",
        "Chilling Reign", "Battle Styles", "Shining Fates", "Shining Fates Shiny Vault",
        "Vivid Voltage", "Champion's Path", "Darkness Ablaze", "Rebel Clash",
        "Sword & Shield",
    ],
    "sun_moon": [
        "Cosmic Eclipse", "Hidden Fates", "Hidden Fates Shiny Vault", "Unified Minds",
        "Unbroken Bonds", "Detective Pikachu", "Team Up", "Lost Thunder",
        "Dragon Majesty", "Celestial Storm", "Forbidden Light", "Ultra Prism",
        "Crimson Invasion", "Shining Legends", "Burning Shadows", "Guardians Rising",
        "Sun & Moon",
    ],
    "xy": [
        "Evolutions", "Steam Siege", "Fates Collide", "Generations", "BREAKpoint",
        "BREAKthrough", "Ancient Origins", "Roaring Skies", "Double Crisis",
        "Primal Clash", "Phantom Forces", "Furious Fists", "Flashfire", "XY",
        "Kalos Starter Set",
    ],
    "black_white": [
        "Legendary Treasures", "Plasma Blast", "Plasma Freeze", "Plasma Storm",
        "Boundaries Crossed", "Dragon Vault", "Dragons Exalted", "Dark Explorers",
        "Next Destinies", "Noble Victories", "Emerging Powers", "Black & White",
    ],
    "hgss": [
        "Call of Legends", "Triumphant", "HS—Triumphant", "Undaunted", "HS—Undaunted",
        "Unleashed", "HS—Unleashed", "HeartGold & SoulSilver",
    ],
    "platinum": ["Arceus", "Supreme Victors", "Rising Rivals", "Platinum"],
    "diamond_pearl": [
        "Stormfront", "Legends Awakened", "Majestic Dawn", "Great Encounters",
        "Secret Wonders", "Mysterious Treasures", "Diamond & Pearl",
    ],
    "ex": [
        "Power Keepers", "Dragon Frontiers", "Crystal Guardians", "Holon Phantoms",
        "Legend Maker", "Delta Species", "Unseen Forces", "Emerald", "Deoxys",
        "Team Rocket Returns", "FireRed & LeafGreen", "Hidden Legends",
        "Team Magma vs Team Aqua", "Dragon", "Sandstorm", "Ruby & Sapphire",
    ],
    "ecard": ["Skyridge", "Aquapolis", "Expedition", "Expedition Base Set"],
    "legendary": ["Legendary Collection"],
    "neo": ["Neo Destiny", "Neo Revelation", "Neo Discovery", "Neo Genesis"],
    "original": [
        "Gym Challenge", "Gym Heroes", "Team Rocket", "Base Set 2", "Fossil",
        "Jungle", "Base Set", "Base",
    ],
    "other": [
        "Pokémon Trading Card Game Classic", "Pokémon Futsal",
        "Pokémon Futsal Collection", "Pikachu World Collection", "Pokémon Rumble",
        "Southern Islands", "Best of Game",
        "Trick or Trade 2024", "Trick or Trade 2023", "Trick or Trade 2022",
        "Battle Academy 2024", "Battle Academy 2022", "Battle Academy 2020",
        "My First Battle",
    ],
}


def _norm(value: str) -> str:
    """Lowercase, letters and digits only — so punctuation and spacing in a
    vendor's set name can't cause a miss ('Scarlet & Violet—151' == 'SV 151')."""
    return re.sub(r"[^a-z0-9]", "", (value or "").lower())


SET_ERAS = {}
for _era, _names in _ERA_SETS.items():
    for _n in _names:
        SET_ERAS[_norm(_n)] = _era

# A release code before a colon, a dash, or (rarer) nothing but whitespace —
# vendors use all three: "SV10: Destined Rivals", "XY - Ancient Origins",
# "XY Base Set". Tried in that order; trailing digits/dots after the code are
# consumed but not captured (ERA_PREFIXES keys on "sv"/"xy", not "sv10"). Each
# pattern requires the WHOLE remainder to be the set name (anchored with
# `$`), so a genuine full name like "Scarlet & Violet: Destined Rivals" or
# "Team Up" never matches here — "Scarlet" and "Team" aren't 2-5 letters
# followed immediately by one of these separators, they're followed by more
# letters. The colon fallback for those lives separately, below.
_CODE_SEPARATOR_RES = [
    re.compile(r"^([A-Za-z]{2,5})[\d.]*\s*:\s*(.+)$"),
    re.compile(r"^([A-Za-z]{2,5})[\d.]*\s*-\s*(.+)$"),
    re.compile(r"^([A-Za-z]{2,5})[\d.]*\s+(.+)$"),
]


def _era_from_code_prefix(raw: str):
    """Era key if `raw` starts with a REGISTERED era code before one of the
    three separators above, else None.

    Requiring the code to already be a key in ERA_PREFIXES is what keeps the
    permissive bare-space pattern from misfiring on an ordinary name that
    happens to start with a short word — "Team Up" extracts "Team", which
    isn't a registered code, so the whole function falls through to the
    other passes in era_for_set instead of guessing.

    The code is trusted directly rather than also checking what follows it
    against SET_ERAS — which is what correctly resolves "XY Base Set" /
    "SM Base Set" (that era's OWN foundational release) instead of colliding
    with the unrelated vintage "Base Set" (1998) entry filed under
    `original`.
    """
    for pattern in _CODE_SEPARATOR_RES:
        m = pattern.match(raw)
        if m and m.group(1).lower() in ERA_PREFIXES:
            return ERA_PREFIXES[m.group(1).lower()]
    return None

# Promos are matched BEFORE anything else (see era_for_set) so they stay their
# own group instead of being absorbed into the series named in their title.
_PROMO_NEEDLES = ("blackstarpromo", "promos", "promo")

# Remaining keyword rules, applied last. First hit wins, so the more specific
# patterns come first.
_KEYWORD_RULES = [
    ("mcdonalds", ("mcdonald",)),
    ("trainer_kits", ("trainerkit",)),
    ("pop", ("popseries",)),
]


def era_for_set(set_name: str) -> str:
    """Era key for a set name. Never raises; unknown names get FALLBACK_ERA."""
    raw = (set_name or "").strip()
    if not raw:
        return FALLBACK_ERA

    # Split on the FIRST colon, if any. PPT uses one for two different
    # things — "SV10: Destined Rivals" (release code : set name) and
    # "Scarlet & Violet: Destined Rivals" (parent set : subset) — and in
    # both cases the text after it is usually a real set/subset name, so
    # it's tried as its own lookup below regardless of what precedes it.
    left, _, right = raw.partition(":")
    left, right = left.strip(), right.strip()

    # 0. Promos are STANDALONE, ahead of everything else. A Black Star Promos
    #    set isn't a main-series expansion, so it doesn't belong inside one —
    #    "SWSH Black Star Promos" groups with the other promos, not with Sword
    #    & Shield. This runs first precisely so a series name in the title
    #    can't pull it back into that series.
    if any(n in _norm(right) or n in _norm(raw) for n in _PROMO_NEEDLES):
        return "promos"

    # 1. Whole name, as given.
    hit = SET_ERAS.get(_norm(raw))
    if hit:
        return hit

    # 2. A registered release code before a colon, dash, or bare space —
    #    "SV10: Destined Rivals", "XY - Ancient Origins", "XY Base Set" —
    #    which is what makes future sets in a known era self-classify
    #    without this file being touched.
    era = _era_from_code_prefix(raw)
    if era:
        return era

    # 3. Colon fallback for names #2 doesn't cover: a known set/subset name
    #    after the colon beats an unresolved guess — then (parent-set form,
    #    e.g. "Generations: Radiant Collection" where only the parent
    #    "Generations" is listed) the text before the colon.
    if right:
        hit = SET_ERAS.get(_norm(right))
        if hit:
            return hit
        hit = SET_ERAS.get(_norm(left))
        if hit:
            return hit

    # 4. Keyword rules for the remaining misc groups.
    flat = _norm(raw)
    for era, needles in _KEYWORD_RULES:
        if any(n in flat for n in needles):
            return era

    return FALLBACK_ERA


def era_label(era_key: str) -> str:
    return ERA_LABELS.get(era_key, ERA_LABELS[FALLBACK_ERA])


def era_sort_index(era_key: str) -> int:
    """Position in ERA_ORDER; unknown keys sort last."""
    return ERA_INDEX.get(era_key, len(ERA_ORDER))


def annotate(sets: list, name_key: str = "set_name") -> list:
    """Add `era` and `era_label` to each set dict, in place. Returns the list."""
    for entry in sets:
        key = era_for_set(entry.get(name_key) or entry.get("set_id") or "")
        entry["era"] = key
        entry["era_label"] = era_label(key)
    return sets


def group_by_era(sets: list, name_key: str = "set_name") -> list:
    """[{era, era_label, sets: [...]}] in ERA_ORDER, empty eras dropped.

    Set order WITHIN a group is preserved from the input, so whatever ordering
    the caller supplied (newest-first, alphabetical) survives grouping.
    """
    annotate(sets, name_key)
    buckets = {}
    for entry in sets:
        buckets.setdefault(entry["era"], []).append(entry)
    out = []
    for key, label in ERA_ORDER:
        if buckets.get(key):
            out.append({"era": key, "era_label": label, "sets": buckets[key]})
    # Anything with an era key not in ERA_ORDER (shouldn't happen, but don't
    # silently drop rows if it does).
    for key, rows in buckets.items():
        if key not in ERA_INDEX:
            out.append({"era": key, "era_label": era_label(key), "sets": rows})
    return out
