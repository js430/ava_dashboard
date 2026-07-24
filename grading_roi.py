"""Grading ROI math — pure Python, no DB, no network.

evaluate() takes a card's raw price, its price at each grade, the odds you
assign to each grade, and your costs, and returns every intermediate value
alongside the verdict — so the page can always show WHY it said what it said.

The baseline is deliberately "sell it raw today", NOT zero. Grading only wins
if it beats what you'd net by listing the raw card right now; comparing
against zero is how every online grading calculator flatters the decision.

Run `python -m grading_roi` for a synthetic-data demo.
"""

from dataclasses import dataclass, field

# Canonical grade keys. Every price source maps its own vocabulary into these
# (see price_sources.GRADE_KEYS) so the math never sees a vendor's naming.
GRADE_ORDER = ("psa_10", "bgs_10", "cgc_10", "bgs_9_5", "psa_9", "psa_8", "psa_low")

# Verdict thresholds, expressed as a fraction of the raw card's net value.
# Below FLOOR the graded path loses money vs just selling raw; between FLOOR
# and MARGIN the edge is inside the noise of any price estimate, so we say so
# rather than pretending a 4% edge is a decision.
VERDICT_FLOOR = 0.0
VERDICT_MARGIN = 0.15

# Gem rates the sensitivity strip is evaluated at.
SENSITIVITY_POINTS = (0.10, 0.20, 0.30, 0.40, 0.50)


@dataclass
class Costs:
    """Per-card costs. All dollars except sale_fee_pct (0.13 = 13%)."""
    grading_fee: float = 25.0      # service tier, per card
    ship_to: float = 5.0           # your share of shipping to the grader
    ship_return: float = 20.0      # your share of return shipping
    insurance: float = 0.0         # declared-value insurance, per card
    sale_fee_pct: float = 0.0      # marketplace + payment processing
    sale_ship: float = 0.0         # what shipping the sold card costs you

    @property
    def submission_total(self) -> float:
        """Everything you pay to get the card back in a slab."""
        return self.grading_fee + self.ship_to + self.ship_return + self.insurance


@dataclass
class Outcome:
    """One grade's contribution to the expected value."""
    grade: str
    probability: float
    price: float
    net: float                     # after sale fees + shipping
    contribution: float            # net * probability


@dataclass
class Result:
    raw_price: float
    raw_net: float
    ev_gross: float                # EV of the sale, before submission costs
    ev_net: float                  # after submission costs
    delta: float                   # ev_net - raw_net  <- the headline number
    break_even_gem_rate: float | None
    verdict: str                   # 'grade' | 'marginal' | 'sell_raw'
    outcomes: list[Outcome] = field(default_factory=list)
    sensitivity: list[dict] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def net_proceeds(price: float, costs: Costs) -> float:
    """What you actually keep after selling at `price`."""
    if price <= 0:
        return 0.0
    return price * (1.0 - costs.sale_fee_pct) - costs.sale_ship


def normalize_odds(odds: dict) -> tuple[dict, list[str]]:
    """Clamp negatives, drop zero/missing grades, and rescale to sum to 1.

    Returns (odds, warnings). An all-zero input is rejected by the caller —
    we never silently invent a distribution.
    """
    warnings = []
    clean = {}
    for grade, p in (odds or {}).items():
        try:
            p = float(p)
        except (TypeError, ValueError):
            continue
        if p < 0:
            warnings.append(f"Negative odds for {grade} treated as 0.")
            p = 0.0
        if p > 0:
            clean[grade] = p
    total = sum(clean.values())
    if total <= 0:
        return {}, warnings
    if abs(total - 1.0) > 0.001:
        warnings.append(f"Odds summed to {total:.0%} — rescaled to 100%.")
        clean = {g: p / total for g, p in clean.items()}
    return clean, warnings


def break_even_gem_rate(raw_net: float, price_10: float, price_9: float,
                        costs: Costs) -> float | None:
    """The P(10) at which grading exactly matches selling raw.

    Solved on the simplified two-outcome model (it comes back a 10 or a 9),
    which is what the plain-English readout on the page describes. Returns
    None when the inputs can't produce a crossover — e.g. a 9 already beats
    raw (any gem rate wins) or the 10 and 9 are priced the same.
    """
    if not price_10 or not price_9:
        return None
    net_10 = net_proceeds(price_10, costs)
    net_9 = net_proceeds(price_9, costs)
    spread = net_10 - net_9
    if spread <= 0:
        return None
    p = (raw_net + costs.submission_total - net_9) / spread
    if p <= 0 or p > 1:
        # <=0: even an all-9s outcome beats raw. >1: unreachable at any rate.
        return None
    return p


def evaluate(raw_price: float, grade_prices: dict, odds: dict,
             costs: Costs | None = None) -> Result:
    """Full ROI evaluation. See module docstring for the framing."""
    costs = costs or Costs()
    raw_price = float(raw_price or 0)
    raw_net = net_proceeds(raw_price, costs)

    clean_odds, warnings = normalize_odds(odds)
    if not clean_odds:
        raise ValueError("At least one grade needs a probability above zero.")

    priced = {g: float(p) for g, p in (grade_prices or {}).items()
              if p is not None and float(p) > 0}

    outcomes = []
    for grade in sorted(clean_odds, key=lambda g: GRADE_ORDER.index(g)
                        if g in GRADE_ORDER else 99):
        prob = clean_odds[grade]
        price = priced.get(grade, 0.0)
        if price <= 0:
            warnings.append(f"No price for {grade} — counted as $0, which "
                            f"drags the result down. Fill it in or set its odds to 0.")
        net = net_proceeds(price, costs)
        outcomes.append(Outcome(grade=grade, probability=prob, price=price,
                                net=net, contribution=net * prob))

    ev_gross = sum(o.contribution for o in outcomes)
    ev_net = ev_gross - costs.submission_total
    delta = ev_net - raw_net

    if raw_net > 0:
        margin = delta / raw_net
    else:
        margin = 1.0 if delta > 0 else -1.0
        warnings.append("No raw price set — the comparison against selling "
                        "raw isn't meaningful yet.")

    if margin <= VERDICT_FLOOR:
        verdict = "sell_raw"
    elif margin < VERDICT_MARGIN:
        verdict = "marginal"
    else:
        verdict = "grade"

    be = break_even_gem_rate(raw_net, priced.get("psa_10"), priced.get("psa_9"), costs)

    sensitivity = []
    if priced.get("psa_10") and priced.get("psa_9"):
        net_10 = net_proceeds(priced["psa_10"], costs)
        net_9 = net_proceeds(priced["psa_9"], costs)
        for p in SENSITIVITY_POINTS:
            ev = p * net_10 + (1 - p) * net_9 - costs.submission_total
            sensitivity.append({"gem_rate": p, "delta": ev - raw_net})

    return Result(raw_price=raw_price, raw_net=raw_net, ev_gross=ev_gross,
                  ev_net=ev_net, delta=delta, break_even_gem_rate=be,
                  verdict=verdict, outcomes=outcomes, sensitivity=sensitivity,
                  warnings=warnings)


if __name__ == "__main__":
    r = evaluate(
        raw_price=420.0,
        grade_prices={"psa_10": 1180.0, "psa_9": 505.0, "psa_8": 300.0},
        odds={"psa_10": 0.30, "psa_9": 0.55, "psa_8": 0.15},
    )
    print(f"raw net       ${r.raw_net:,.2f}")
    print(f"EV gross      ${r.ev_gross:,.2f}")
    print(f"EV net        ${r.ev_net:,.2f}")
    print(f"delta         ${r.delta:,.2f}   -> {r.verdict}")
    if r.break_even_gem_rate is not None:
        print(f"break-even    {r.break_even_gem_rate:.1%} PSA 10s")
    for o in r.outcomes:
        print(f"  {o.grade:8s} p={o.probability:.0%} ${o.price:>8,.2f} "
              f"net ${o.net:>8,.2f} contrib ${o.contribution:>8,.2f}")
    for w in r.warnings:
        print(f"  ! {w}")
