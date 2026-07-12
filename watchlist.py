"""Card-tracker watchlist (trial).

This file IS the watchlist — edit it and re-run the ingest to pick up changes.
Each entry syncs into the tracked_cards table on ingest (existing rows are
never deleted automatically; remove from the DB manually if needed).

Fields per entry:
  game        - "pokemon" or "one_piece" (required)
  name        - card name as printed (required; used to search JustTCG)
  set_name    - set/expansion name (helps disambiguate the search)
  card_number - printed number, e.g. "223/197" or "OP05-119" (optional but
                recommended once known — tightens matching)
  variant     - rarity/printing note, e.g. "Special Illustration Rare",
                "Alternate Art", "Secret Rare" (optional, display + matching hint)

NOTE: these seed entries are PLACEHOLDER EXAMPLES — a couple of obvious chase
cards to validate the pipeline. Card numbers are left None where unverified;
fill them in (and fix any set-name mismatches vs JustTCG's naming) after the
first ingest run logs what it matched.
"""

WATCHLIST = [
    # ── Pokémon examples ──
    {"game": "pokemon", "name": "Charizard ex",   "set_name": "Obsidian Flames", "card_number": None, "variant": "Special Illustration Rare"},
    {"game": "pokemon", "name": "Umbreon VMAX",   "set_name": "Evolving Skies",  "card_number": None, "variant": "Alternate Art Secret"},
    {"game": "pokemon", "name": "Giratina VSTAR", "set_name": "Lost Origin",     "card_number": None, "variant": "Alternate Art"},
    {"game": "pokemon", "name": "Iono",           "set_name": "Paldea Evolved",  "card_number": None, "variant": "Special Illustration Rare"},

    # ── One Piece examples ──
    {"game": "one_piece", "name": "Shanks",          "set_name": "Romance Dawn",             "card_number": None, "variant": "Secret Rare"},
    {"game": "one_piece", "name": "Monkey D. Luffy", "set_name": "Awakening of the New Era", "card_number": None, "variant": "Secret Rare"},

    # ─────────────────────────────────────────────────────────────────
    # ADD MORE CARDS HERE — copy a line above and edit. Aim for the
    # ~30-50 cards you actually care about; the free JustTCG tier is
    # 1,000 calls/month, so ~40 cards ingested daily fits the budget.
    # ─────────────────────────────────────────────────────────────────
]
