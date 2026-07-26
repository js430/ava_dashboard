"""Grading net-proceeds math — pure Python, no DB, no network.

Deliberately OBJECTIVE: this module never guesses what grade a card will get.
It answers one arithmetic question per grade — "if this card sells at the
market price for that grade, what do I actually keep after every cost?" — and
compares it against selling the card raw today.

No probabilities, no expected value, no break-even gem rate. Those all require
assuming an outcome the user cannot know, and a confident-looking number built
on a guessed input is worse than no number.

Run `python -m grading_roi` for a synthetic-data demo.
"""

from dataclasses import dataclass, field

# Canonical grade keys, best first. price_sources.GRADE_KEYS must stay in sync.
GRADE_ORDER = ("psa_10", "bgs_10", "cgc_10", "sgc_10", "tag_10", "bgs_9_5",
               "psa_9", "cgc_9", "tag_9", "psa_8", "psa_low")

# Default grades the calculator reports on when no grading company is picked.
# PSA 10/9 is where essentially all the graded-sale volume is, so it's the
# fallback; grading_tiers.GRADING_COMPANIES["report_grades"] overrides this
# per company (BGS reports bgs_10/bgs_9_5, etc) so the net-proceeds cards
# match the company actually selected in "Your costs".
REPORTED_GRADES = ("psa_10", "psa_9")


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
class GradeResult:
    """What one grade nets, itemised so every subtraction is visible."""
    grade: str
    price: float               # market price for this grade
    sale_fee: float            # marketplace + processing, on this price
    sale_ship: float           # shipping the sold card to the buyer
    submission_total: float    # grading + shipping both ways + insurance
    net: float                 # what you keep
    vs_raw: float              # net minus what selling raw nets


@dataclass
class Result:
    raw_price: float
    raw_sale_fee: float
    raw_sale_ship: float
    raw_net: float
    submission_total: float
    grades: list = field(default_factory=list)     # [GradeResult]
    warnings: list = field(default_factory=list)


def net_proceeds(price: float, costs: Costs) -> float:
    """What you keep after selling at `price` — before any grading costs."""
    if price <= 0:
        return 0.0
    return price * (1.0 - costs.sale_fee_pct) - costs.sale_ship


def evaluate(raw_price: float, grade_prices: dict, costs: Costs | None = None,
             grades: tuple = REPORTED_GRADES) -> Result:
    """Net proceeds per grade, and how each compares with selling raw.

    Every figure traces back to a price the user can see and a cost they
    entered — nothing is inferred.
    """
    costs = costs or Costs()
    raw_price = max(0.0, float(raw_price or 0))
    raw_sale_fee = raw_price * costs.sale_fee_pct
    raw_sale_ship = costs.sale_ship if raw_price > 0 else 0.0
    raw_net = net_proceeds(raw_price, costs)

    warnings = []
    if raw_price <= 0:
        warnings.append("No raw price, so there's nothing to compare against — "
                        "the 'vs selling raw' figures assume $0.")

    results = []
    for grade in grades:
        try:
            price = float(grade_prices.get(grade) or 0)
        except (TypeError, ValueError):
            price = 0.0
        if price <= 0:
            warnings.append(f"No price for {grade.replace('_', ' ').upper()} — skipped.")
            continue
        sale_fee = price * costs.sale_fee_pct
        net = net_proceeds(price, costs) - costs.submission_total
        results.append(GradeResult(
            grade=grade,
            price=round(price, 2),
            sale_fee=round(sale_fee, 2),
            sale_ship=round(costs.sale_ship, 2),
            submission_total=round(costs.submission_total, 2),
            net=round(net, 2),
            vs_raw=round(net - raw_net, 2),
        ))

    return Result(
        raw_price=round(raw_price, 2),
        raw_sale_fee=round(raw_sale_fee, 2),
        raw_sale_ship=round(raw_sale_ship, 2),
        raw_net=round(raw_net, 2),
        submission_total=round(costs.submission_total, 2),
        grades=results,
        warnings=warnings,
    )


if __name__ == "__main__":
    r = evaluate(
        raw_price=60.96,
        grade_prices={"psa_10": 1199.49, "psa_9": 489.99},
        costs=Costs(grading_fee=25.0, sale_fee_pct=0.13),
    )
    print(f"sell raw    ${r.raw_price:>9,.2f} -> nets ${r.raw_net:>9,.2f}")
    print(f"submission  ${r.submission_total:>9,.2f}")
    for g in r.grades:
        print(f"{g.grade:8s}    ${g.price:>9,.2f} -> nets ${g.net:>9,.2f} "
              f"({g.vs_raw:+,.2f} vs raw)")
    for w in r.warnings:
        print(f"  ! {w}")
