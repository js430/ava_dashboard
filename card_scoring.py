"""Profit-potential scoring for tracked cards — pure Python, no DB access.

score_card() takes a card's price snapshots + release date and returns every
component alongside the composite, so you can always see WHY a card scored
what it did. All tuning knobs are the module-level constants below.

Run `python -m card_scoring` for a quick synthetic-data demo of the output.
"""

import math
from datetime import datetime, date, timedelta, timezone

# ────────────────────────── TUNING KNOBS ──────────────────────────
# Composite weights (must sum to 1.0). Liquidity is a multiplier, not a weight.
W_MOMENTUM_7D = 0.35    # short-term price momentum
W_MOMENTUM_30D = 0.25   # medium-term price momentum
W_AGE = 0.40            # "post-hype, pre-scarcity" age window bonus

# Momentum normalization: a move of +/- this fraction (0.30 = 30%) saturates
# the momentum component via tanh. Bigger moves add little beyond this.
MOMENTUM_FULL_SCALE = 0.30

# Age bell curve: bonus peaks at AGE_PEAK_DAYS after release and falls off
# with a width of AGE_WIDTH_DAYS (gaussian). ~8 months post-release peak.
AGE_PEAK_DAYS = 240
AGE_WIDTH_DAYS = 150

# Neutral component value used when data is missing (score can't be computed).
NEUTRAL = 0.5
# Age bonus used when release_date is unknown (slightly below neutral).
NO_RELEASE_AGE_BONUS = 0.4

# Liquidity (proxy until real sales volume exists): snapshot-to-snapshot moves
# of at least MEANINGFUL_MOVE_PCT in the last 30d, saturating at
# LIQUIDITY_TARGET_MOVES. Final multiplier spans LIQUIDITY_FLOOR..1.0 so an
# illiquid card can lose at most (1 - LIQUIDITY_FLOOR) of its score.
# 0.005 (0.5%) is calibrated for DAILY snapshots — a steady +20%/month trend
# (~0.6%/day) counts as movement, while a card flat for weeks scores 0.
MEANINGFUL_MOVE_PCT = 0.005
LIQUIDITY_TARGET_MOVES = 8
LIQUIDITY_FLOOR = 0.5
# ──────────────────────────────────────────────────────────────────


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def snapshot_price(snap: dict):
    """Representative price for a snapshot: mid, else mean of low/high, else
    whichever single bound exists."""
    mid = snap.get("price_mid")
    if mid:
        return float(mid)
    low, high = snap.get("price_low"), snap.get("price_high")
    if low and high:
        return (float(low) + float(high)) / 2
    if low or high:
        return float(low or high)
    return None


# Snapshot sources that measure the same thing and can safely share a series.
# JustTCG's live and history rows are the same estimator, so they group.
# PokemonPriceTracker's market price is a DIFFERENT estimator — mixing it with
# JustTCG mid would make momentum register a jump at the changeover that is
# purely a change of ruler, not of price.
SOURCE_FAMILIES = {
    "justtcg": "justtcg",
    "justtcg-history": "justtcg",
    "pokemonpricetracker": "pokemonpricetracker",
    "pokemonpricetracker-history": "pokemonpricetracker",
}
# Below this, a family isn't enough of a series to prefer over an established
# one — momentum needs at least two points to mean anything.
MIN_SERIES_POINTS = 2


def source_family(source) -> str:
    return SOURCE_FAMILIES.get((source or "").strip().lower(), (source or "unknown"))


def select_scoring_series(snapshots: list) -> list:
    """The largest self-consistent run of snapshots to score from.

    Scoring must never span two pricing sources. Picks the family with the
    most recent data, provided it has enough points to be a series; otherwise
    falls back to whichever family has the most. Older snapshots from the
    other source are kept in the database and still drawn on the graph — they
    just don't feed momentum, which would otherwise report a source change as
    a price move.
    """
    if not snapshots:
        return []
    families = {}
    for snap in snapshots:
        families.setdefault(source_family(snap.get("source")), []).append(snap)
    if len(families) == 1:
        return list(snapshots)

    def latest(rows):
        return max(r["captured_at"] for r in rows)

    newest = max(families.values(), key=latest)
    if len(newest) >= MIN_SERIES_POINTS:
        return sorted(newest, key=lambda r: r["captured_at"])
    biggest = max(families.values(), key=len)
    return sorted(biggest, key=lambda r: r["captured_at"])


def _priced(snapshots: list) -> list:
    """[(captured_at, price)] sorted ascending, snapshots without a price dropped."""
    out = []
    for s in snapshots:
        p = snapshot_price(s)
        if p and p > 0:
            out.append((s["captured_at"], p))
    out.sort(key=lambda t: t[0])
    return out


def momentum(snapshots: list, days: int, now: datetime = None):
    """Fractional price change over the trailing window (0.12 = +12%), or None
    if there's no snapshot old enough to act as a baseline."""
    now = now or _now_utc()
    pts = _priced(snapshots)
    if len(pts) < 2:
        return None
    cutoff = now - timedelta(days=days)
    baseline = None
    for ts, price in pts:                 # oldest -> newest
        if ts <= cutoff:
            baseline = price              # last snapshot at/before the window start
        else:
            if baseline is None:
                baseline = price          # window starts before our history does
            break
    if baseline is None:
        baseline = pts[0][1]
    latest = pts[-1][1]
    if baseline <= 0:
        return None
    return (latest - baseline) / baseline


def normalize_momentum(m) -> float:
    """Map a fractional move onto 0..1 (0.5 = flat) with tanh saturation."""
    if m is None:
        return NEUTRAL
    return 0.5 + 0.5 * math.tanh(m / MOMENTUM_FULL_SCALE)


def liquidity(snapshots: list, now: datetime = None) -> float:
    """0..1 proxy: how many meaningful (>= MEANINGFUL_MOVE_PCT) consecutive
    price moves happened in the last 30 days, vs LIQUIDITY_TARGET_MOVES."""
    now = now or _now_utc()
    cutoff = now - timedelta(days=30)
    pts = [(ts, p) for ts, p in _priced(snapshots) if ts >= cutoff]
    moves = 0
    for (_, a), (_, b) in zip(pts, pts[1:]):
        if a > 0 and abs(b - a) / a >= MEANINGFUL_MOVE_PCT:
            moves += 1
    return min(1.0, moves / LIQUIDITY_TARGET_MOVES)


def age_in_days(release_date, now: datetime = None):
    if release_date is None:
        return None
    now = (now or _now_utc()).date()
    if isinstance(release_date, datetime):
        release_date = release_date.date()
    return max(0, (now - release_date).days)


def age_bonus(age_days) -> float:
    """Gaussian bump peaking at AGE_PEAK_DAYS — rewards the window after hype
    fades but before supply dries up."""
    if age_days is None:
        return NO_RELEASE_AGE_BONUS
    return math.exp(-((age_days - AGE_PEAK_DAYS) ** 2) / (2 * AGE_WIDTH_DAYS ** 2))


def score_card(snapshots: list, release_date, now: datetime = None) -> dict:
    """Compute all components + composite for one card.

    snapshots: dicts with captured_at (tz-aware datetime) and price_low/mid/high.
    Returns every intermediate value so scores are inspectable, with
    momentum_*_pct as percentages (12.5 = +12.5%) for storage/display.
    """
    now = now or _now_utc()
    m7 = momentum(snapshots, 7, now)
    m30 = momentum(snapshots, 30, now)
    # 180d is display-only (card-tracker table + why-grid) — NOT in the
    # weighted composite below. Folding it in would rebalance W_MOMENTUM_7D/
    # 30D/AGE and silently change every existing card's potential_score;
    # that's a deliberate call for whoever tunes the formula, not a side
    # effect of adding a column.
    m180 = momentum(snapshots, 180, now)
    n7 = normalize_momentum(m7)
    n30 = normalize_momentum(m30)
    liq = liquidity(snapshots, now)
    age_d = age_in_days(release_date, now)
    age_b = age_bonus(age_d)
    liq_mult = LIQUIDITY_FLOOR + (1.0 - LIQUIDITY_FLOOR) * liq

    base = W_MOMENTUM_7D * n7 + W_MOMENTUM_30D * n30 + W_AGE * age_b
    potential = round(100.0 * base * liq_mult, 1)

    return {
        "momentum_7d_pct": None if m7 is None else round(m7 * 100, 2),
        "momentum_30d_pct": None if m30 is None else round(m30 * 100, 2),
        "momentum_180d_pct": None if m180 is None else round(m180 * 100, 2),
        "momentum_7d_norm": round(n7, 3),
        "momentum_30d_norm": round(n30, 3),
        "age_days": age_d,
        "age_bonus": round(age_b, 3),
        "liquidity_score": round(liq, 3),
        "liquidity_multiplier": round(liq_mult, 3),
        "weights": {"momentum_7d": W_MOMENTUM_7D, "momentum_30d": W_MOMENTUM_30D, "age": W_AGE},
        "potential_score": potential,
    }


if __name__ == "__main__":
    # Synthetic sanity demo: 30 days of snapshots trending up 20%, released ~8 months ago.
    now = _now_utc()
    snaps = [
        {"captured_at": now - timedelta(days=30 - i),
         "price_mid": 100 * (1 + 0.20 * i / 30), "price_low": None, "price_high": None}
        for i in range(31)
    ]
    rel = (now - timedelta(days=AGE_PEAK_DAYS)).date()
    from pprint import pprint
    print("Uptrending card at the age sweet spot:")
    pprint(score_card(snaps, rel, now))
    print("\nSame card, no release date, flat prices:")
    flat = [{"captured_at": s["captured_at"], "price_mid": 100, "price_low": None, "price_high": None} for s in snaps]
    pprint(score_card(flat, None, now))
