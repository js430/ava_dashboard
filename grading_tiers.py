"""Grading company / tier reference for the grading calculator.

Snapshot of PSA / BGS / CGC / TAG service tiers — fee, turnaround, and the
tier's insured declared-value ceiling ("coverage"). Prices and turnaround
change often; the UI shows a "verify at each company's site" caveat and every
fee stays editable, so this is a convenience default, not an authority.

Sourced from a card-grading community reference (2026 snapshot). Do NOT treat
as live pricing — re-check before relying on it.

Shape consumed by templates/grading_calculator.html (passed as JSON):
  coverage        insured declared-value ceiling in USD, or None for no limit
  coverage_rule   'upcharge'  -> exceeding coverage bumps you to a pricier tier
                  'capped'    -> coverage is a hard cap but the fee doesn't change
                  'none'      -> no declared-value limit
  fee_pct_fmv     optional %; fee = fee + pct * fair-market-value (CGC Unlimited)
  report_grades   the company's own top two grade keys (price_sources.GRADE_KEYS)
                  — what "What you'd net" reports on when this company is picked.
"""

GRADING_COMPANIES = {
    "psa": {
        "label": "PSA",
        "report_grades": ["psa_10", "psa_9"],
        "coverage_rule": "upcharge",
        "note": "PSA upcharges you to the correct tier if a card's value exceeds "
                "the tier's coverage. You can mix tiers in one submission.",
        "tiers": [
            {"id": "regular",       "label": "Regular",       "fee": 79.99,  "turnaround": "40–50 days", "coverage": 1500},
            {"id": "express",       "label": "Express",       "fee": 149.00, "turnaround": "20–30 days", "coverage": 2500},
            {"id": "super_express", "label": "Super Express", "fee": 349.00, "turnaround": "7–10 days",  "coverage": 5000},
            {"id": "walk_through",  "label": "Walk-Through",  "fee": 599.00, "turnaround": "5–7 days",   "coverage": 10000},
        ],
    },
    "bgs": {
        "label": "BGS / Beckett",
        "report_grades": ["bgs_10", "bgs_9_5"],
        "coverage_rule": "none",
        "note": "Turnaround-based, no declared-value limit. Whole batch is one "
                "tier (can't mix). Base tiers grade only or add all 4 subgrades.",
        "tiers": [
            {"id": "base_nosub", "label": "Base (grade only)",   "fee": 14.95,  "turnaround": "75+ days", "coverage": None},
            {"id": "base_sub",   "label": "Base (w/ subgrades)", "fee": 17.95,  "turnaround": "75+ days", "coverage": None},
            {"id": "standard",   "label": "Standard",            "fee": 34.95,  "turnaround": "45 days",  "coverage": None},
            {"id": "express",    "label": "Express",             "fee": 79.95,  "turnaround": "15 days",  "coverage": None},
            {"id": "priority",   "label": "Priority",            "fee": 124.95, "turnaround": "5 days",   "coverage": None},
        ],
    },
    "cgc": {
        "label": "CGC",
        "report_grades": ["cgc_10", "cgc_9"],
        "coverage_rule": "upcharge",
        "note": "CGC upcharges to a higher tier if value exceeds the cap — except "
                "Unlimited Value, which has no cap (fee is $300 + 1% of value).",
        "tiers": [
            {"id": "economy",    "label": "Economy",         "fee": 20.00,  "turnaround": "65 days", "coverage": 1000},
            {"id": "standard",   "label": "Standard",        "fee": 55.00,  "turnaround": "10 days", "coverage": 3000},
            {"id": "express",    "label": "Express",         "fee": 100.00, "turnaround": "5 days",  "coverage": 10000},
            {"id": "walkthrough","label": "WalkThrough",     "fee": 300.00, "turnaround": "2 days",  "coverage": 100000},
            {"id": "unlimited",  "label": "Unlimited Value", "fee": 300.00, "turnaround": "2 days",  "coverage": None, "fee_pct_fmv": 0.01},
        ],
    },
    "tag": {
        "label": "TAG",
        "report_grades": ["tag_10", "tag_9"],
        "coverage_rule": "capped",
        "note": "TAG does NOT upcharge for card value — coverage is simply a hard "
                "cap. Lower tiers are currently paused; only these two are active.",
        "tiers": [
            {"id": "priority",   "label": "Priority",    "fee": 149.00, "turnaround": "5 days",   "coverage": 2500},
            {"id": "walkthrough","label": "Walkthrough", "fee": 299.00, "turnaround": "2–3 days", "coverage": 5000},
        ],
    },
}
