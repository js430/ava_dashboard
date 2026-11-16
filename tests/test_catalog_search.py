"""Standalone checks for the Catalog's "name + number" search.

    python3 tests/test_catalog_search.py

Needs no database and no network. It builds the real SQL with
catalog._build_filters and checks its shape (every value bound, placeholders
numbered in order, no stray "$"), then evaluates the same word rules in
Python against sample cards. The SQL itself is NOT run here.
"""
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import catalog  # noqa: E402


def matches(game, search, name, number):
    """Python mirror of _search_term_clause for one card."""
    def word_ok(word):
        if word.lower() in name.lower():
            return True
        rule = catalog._number_word(word, game)
        if rule is None:
            return word.lower() in number.lower()
        kind, values = rule
        if kind == "plain":
            first, _, total = number.partition("/")
            ok = catalog._plain_number_key(first) == values[0]
            if len(values) == 2:
                ok = ok and ("/" not in number or catalog._plain_number_key(total) == values[1])
            return ok
        if kind == "exact":
            return number.upper() == values[0]
        pattern = "^" + re.escape(values[0]).replace("%", ".*") + "$"
        return re.match(pattern, number, re.I) is not None

    terms = [t.strip() for t in search.split(",") if t.strip()] or [search]
    return any(all(word_ok(w) for w in term.split()) for term in terms)


POKEMON = [("Pikachu", "25"), ("Pikachu", "025/198"), ("Pikachu", "25/102"), ("Pikachu", "125/198"),
           ("Pikachu", "250"), ("Pikachu V", "SWSH039"), ("Charizard", "TG03/TG30"),
           ("Charizard", "3/102"), ("Porygon2", "74"), ("Porygon", "2/64"), ("Blaine's Quiz #1", "97/132"),
           ("Mewtwo & Mew-GX", "71/236"), ("Mew", "151/165"), ("Gengar", "GG35/GG70"),
           ("Pokégear 3.0", "186/198")]
ONE_PIECE = [("Monkey.D.Luffy", "OP11-040"), ("Monkey.D.Luffy", "OP11-119"), ("Monkey.D.Luffy", "OP01-003"),
             ("Monkey.D.Luffy", "ST31-004"), ("Monkey.D.Luffy", "OP17-079"), ("Monkey.D.Luffy", "P-084"),
             ("Roronoa Zoro", "EB04-007"), ("Roronoa Zoro", "OP11-040"), ("Sweet 3 Generals", "OP17-114"),
             ("King", "OP10-040")]
GUNDAM = [("Gundam (GD01-001) (LR++)", "GD01-001"), ("Argama (SP) (R+)", "GD02-129"),
          ("Gundam Aerial", "ST01-001"), ("EX Base", "EXBP-002")]

CASES = [
    ("pokemon", POKEMON, "Pikachu 25", {("Pikachu", "25"), ("Pikachu", "025/198"), ("Pikachu", "25/102")}),
    ("pokemon", POKEMON, "Pikachu 025", {("Pikachu", "25"), ("Pikachu", "025/198"), ("Pikachu", "25/102")}),
    ("pokemon", POKEMON, "pikachu #25", {("Pikachu", "25"), ("Pikachu", "025/198"), ("Pikachu", "25/102")}),
    ("pokemon", POKEMON, "Pikachu 25/102", {("Pikachu", "25"), ("Pikachu", "25/102")}),
    ("pokemon", POKEMON, "25/102 Pikachu", {("Pikachu", "25"), ("Pikachu", "25/102")}),
    ("pokemon", POKEMON, "Charizard TG03", {("Charizard", "TG03/TG30")}),
    ("pokemon", POKEMON, "charizard tg3", {("Charizard", "TG03/TG30")}),
    ("pokemon", POKEMON, "Pikachu SWSH039", {("Pikachu V", "SWSH039")}),
    ("pokemon", POKEMON, "pikachu swsh39", {("Pikachu V", "SWSH039")}),
    ("pokemon", POKEMON, "Gengar GG35", {("Gengar", "GG35/GG70")}),
    ("pokemon", POKEMON, "Porygon2", {("Porygon2", "74")}),
    ("pokemon", POKEMON, "Porygon 2", {("Porygon2", "74"), ("Porygon", "2/64")}),
    ("pokemon", POKEMON, "Blaine's Quiz #1", {("Blaine's Quiz #1", "97/132")}),
    ("pokemon", POKEMON, "Mewtwo & Mew-GX 71", {("Mewtwo & Mew-GX", "71/236")}),
    ("pokemon", POKEMON, "Mew 151", {("Mew", "151/165")}),
    ("pokemon", POKEMON, "Pokégear 3.0", {("Pokégear 3.0", "186/198")}),
    ("pokemon", POKEMON, "pikachu", {c for c in POKEMON if "Pikachu" in c[0]}),
    ("pokemon", POKEMON, "25", {("Pikachu", "25"), ("Pikachu", "025/198"), ("Pikachu", "25/102")}),
    ("pokemon", POKEMON, "Gengar, Mew 151", {("Gengar", "GG35/GG70"), ("Mew", "151/165")}),
    ("one_piece", ONE_PIECE, "Monkey.D.Luffy OP11", {("Monkey.D.Luffy", "OP11-040"), ("Monkey.D.Luffy", "OP11-119")}),
    ("one_piece", ONE_PIECE, "monkey d luffy op11", {("Monkey.D.Luffy", "OP11-040"), ("Monkey.D.Luffy", "OP11-119")}),
    ("one_piece", ONE_PIECE, "Luffy OP-11", {("Monkey.D.Luffy", "OP11-040"), ("Monkey.D.Luffy", "OP11-119")}),
    ("one_piece", ONE_PIECE, "OP11 Luffy", {("Monkey.D.Luffy", "OP11-040"), ("Monkey.D.Luffy", "OP11-119")}),
    ("one_piece", ONE_PIECE, "Luffy OP11-040", {("Monkey.D.Luffy", "OP11-040")}),
    ("one_piece", ONE_PIECE, "luffy op11 040", {("Monkey.D.Luffy", "OP11-040")}),
    ("one_piece", ONE_PIECE, "Luffy op11040", {("Monkey.D.Luffy", "OP11-040")}),
    ("one_piece", ONE_PIECE, "Luffy OP11-040_p1", {("Monkey.D.Luffy", "OP11-040")}),
    ("one_piece", ONE_PIECE, "Luffy OP1", {("Monkey.D.Luffy", "OP01-003")}),
    ("one_piece", ONE_PIECE, "Luffy ST31", {("Monkey.D.Luffy", "ST31-004")}),
    ("one_piece", ONE_PIECE, "Luffy P-084", {("Monkey.D.Luffy", "P-084")}),
    ("one_piece", ONE_PIECE, "luffy p84", {("Monkey.D.Luffy", "P-084")}),
    ("one_piece", ONE_PIECE, "Zoro EB04-007_p2", {("Roronoa Zoro", "EB04-007")}),
    ("one_piece", ONE_PIECE, "Luffy 040", {("Monkey.D.Luffy", "OP11-040")}),
    ("one_piece", ONE_PIECE, "Sweet 3 Generals", {("Sweet 3 Generals", "OP17-114")}),
    ("one_piece", ONE_PIECE, "King", {("King", "OP10-040")}),
    ("one_piece", ONE_PIECE, "OP11", {c for c in ONE_PIECE if c[1].startswith("OP11-")}),
    ("gundam", GUNDAM, "Gundam GD01", {("Gundam (GD01-001) (LR++)", "GD01-001")}),
    ("gundam", GUNDAM, "argama gd02-129", {("Argama (SP) (R+)", "GD02-129")}),
    ("gundam", GUNDAM, "aerial st1", {("Gundam Aerial", "ST01-001")}),
    ("gundam", GUNDAM, "exbp-2", {("EX Base", "EXBP-002")}),
]

failed = 0
for game, cards, query, want in CASES:
    got = {c for c in cards if matches(game, query, *c)}
    if got != want:
        failed += 1
        print("FAIL", game, repr(query), "\n   extra:", sorted(got - want), "\n   missing:", sorted(want - got))

# The SQL the real builder produces: every value bound, numbered in order,
# nothing typed by the visitor in the text, and no "$" except placeholders.
for game, query in (("pokemon", "Pikachu 25/102, x' OR 1=1 --"), ("one_piece", "Luffy OP11-040 100%_"),
                    ("riftbound", "ahri 303"), ("lorcana", "Elsa 207")):
    where, params = catalog._build_filters(game, "english", search=query)
    nums = [int(n) for n in re.findall(r"\$(\d+)", where)]
    assert max(nums) == len(params) and set(nums) == set(range(1, len(params) + 1)), (where, params)
    assert "$" not in re.sub(r"\$\d+", "", where), where
    for bad in ("OR 1=1", "Luffy", "Pikachu", "ahri", "Elsa"):
        assert bad not in where, (bad, where)
    shifted = catalog._renumber(where, 10)
    assert sorted(set(int(n) for n in re.findall(r"\$(\d+)", shifted))) == [n + 10 for n in sorted(set(nums))]
    excl, _ = catalog._build_filters(game, "english", search=query, exclude={"search": True})
    assert "NOT (" in excl
where, params = catalog._build_filters("pokemon", "english", search="Pikachu 25/102")
print(where)
print(params)
print("cases:", len(CASES), "failed:", failed)
sys.exit(1 if failed else 0)
