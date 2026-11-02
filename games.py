"""Every card game the dashboard knows, and which features each one has.

One list answers "which games does this page or endpoint offer?" — the
allowed-game checks, the game pickers on every page, and the names the photo
scanner returns all read from here. Before this, each page and endpoint kept
its own hard-coded list, and adding Riftbound meant finding every one.

ADDING A GAME'S PRICES (One Piece, once it has a price API)
-----------------------------------------------------------
One Piece is already here: the Grading Calculator, Tracker Management's set
import and the Card Tracker all work for it. What it lacks is a price feed,
which the Catalog, the Sealed tab and portfolio values need. To add one:

  1. Write its API module, modelled on riftbound.py: fetch sets, cards and
     sealed product, and parse them into the dicts catalog.upsert_set_cards
     and portfolios.import_sealed_products take. Record the API's id for each
     card as `source_id`.
  2. Give it a daily sync like main.py's _run_riftbound_sync, and tracked-card
     pricing like card_tracker.run_riftbound_snapshots.
  3. Set `catalog=True` and `priced_from_catalog=True` below. From there the
     Catalog's game switch, the Portfolios picker, every page's game list,
     the add-a-card paths (main._catalog_add_fields), Tracker Management's
     set import (main._catalog_import_*) and the Grading Calculator's set and
     card pickers (main._grading_catalog_*) and ungraded price all work for
     it, reading its rows from catalog_cards.
  4. Add its image host to the CSP img-src in main.py — deliberately not
     generated from this list, so the security policy stays readable on its
     own — and its price sources to card_scoring.SOURCE_FAMILIES.
  5. Fill in the branches main.py still dispatches on by game: its guest set
     window (_catalog_guest_ceiling), set grouping (_annotate_catalog_sets),
     release order (_catalog_set_releases — sets list alphabetically until
     then), the facets endpoint's set list, and pricing a just-imported set
     (_price_imported_cards).
"""

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Game:
    key: str                         # stored in every table's `game` column
    label: str                       # what members see
    languages: tuple = ("english",)  # first is the default
    # Has a priced catalog: the Catalog page, the Sealed tab, and portfolio
    # values. A game without one can still be tracked and graded.
    catalog: bool = False
    grading: bool = False            # Grading Calculator
    set_import: bool = False         # Tracker Management's "Import a whole set"
    # Cards are added by their catalog row (the browser sends catalog_id) and
    # priced by copying that row's daily price, instead of Pokemon's
    # name-and-number matching against PokemonPriceTracker.
    priced_from_catalog: bool = False
    scan_name: str = ""              # what the photo scanner's prompt calls it
    card_example: str = ""           # search-box placeholders
    sealed_example: str = ""


GAMES = (
    Game("pokemon", "Pokémon", languages=("english", "japanese"), catalog=True,
         grading=True, set_import=True, scan_name="pokemon",
         card_example="Charizard", sealed_example="Prismatic Evolutions"),
    Game("one_piece", "One Piece", grading=True, set_import=True,
         scan_name="one piece", card_example="Monkey.D.Luffy"),
    Game("riftbound", "Riftbound", catalog=True, grading=True, set_import=True,
         priced_from_catalog=True, scan_name="riftbound",
         card_example="Ahri", sealed_example="Origins Booster Box"),
)

KEYS = tuple(g.key for g in GAMES)
_BY_KEY = {g.key: g for g in GAMES}


def get(key):
    """The Game for a key, or None for anything unknown."""
    return _BY_KEY.get(str(key or "").strip().lower())


def keys_with(feature: str) -> tuple:
    """Keys of the games that have `feature` (a Game field), in list order."""
    return tuple(g.key for g in GAMES if getattr(g, feature))


def priced_from_catalog(key) -> bool:
    game = get(key)
    return bool(game and game.priced_from_catalog)


def from_scan(name):
    """The game key for the photo scanner's game name, or None."""
    text = str(name or "").strip().lower()
    return next((g.key for g in GAMES if g.scan_name and g.scan_name == text), None)


def for_templates() -> list:
    """Plain dicts for Jinja and for |tojson in page scripts."""
    return [asdict(g) for g in GAMES]
