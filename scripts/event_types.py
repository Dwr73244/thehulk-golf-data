"""Classify PGA Tour events for calibration stratification.

Three buckets:
  - "major"     — The 4 majors (Masters, US Open, Open Championship, PGA
                  Championship). Cut after R2, large field (~95-156),
                  competitive cut line, distinct difficulty profile.
  - "no_cut"    — Signature events, WGC, Tour Championship, match play,
                  team events. Limited field, no 36-hole cut. EXCLUDED
                  from calibration training because every "made cut"
                  outcome is trivially true (everyone makes cut).
  - "standard"  — Everything else — the bulk of the PGA Tour schedule.

Used by both backfill scripts (to tag pairs and drop no_cut) and the
serving scraper (to look up the right calibration table at runtime).
"""

from __future__ import annotations

import re

MAJOR_PATTERNS = [
    r"\bmasters tournament\b",
    r"\bu\.?s\.?\s+open\b",
    r"\bopen championship\b",
    r"\bpga championship\b",
    r"\bthe open\b",
]

# Events with NO 36-hole cut. These get excluded from calibration training
# because the made_cut outcome is degenerate (always true).
NO_CUT_PATTERNS = [
    r"\bmatch play\b",
    r"\bsentry\b",
    r"\btour championship\b",  # FedEx playoff finale, 30-man field
    r"\bwgc\b",
    r"\bworld golf championship\b",
    r"\bhero world challenge\b",
    r"\bzurich classic\b",  # team event (2-player teams)
    r"\bgrant thornton\b",  # mixed team event
    r"\bskins game\b",
    r"\bcj cup\b",  # limited field signature
    r"\bgenesis scottish open\b",  # sometimes — varies
]


def classify_event_type(event_name, cut_rate=None, field_size=None):
    """Classify a PGA event by name + observed cut metrics.

    Args:
        event_name: Tournament name (BDL ``name`` or ESPN ``event.name``)
        cut_rate:   Observed (made_cut / total) for this event. Optional.
                    When provided, used to catch limited-field events whose
                    names don't match a pattern.
        field_size: Number of competitors. Optional. Used as a secondary
                    signal for the cut_rate-based no_cut detection.

    Returns: "major" | "no_cut" | "standard"

    Decision logic, in order:
      1. Name matches a major pattern → "major"
      2. Name matches a known no_cut pattern → "no_cut"
      3. Empirically: cut_rate ≥ 0.93 AND field ≤ 90 → "no_cut"
         (signature events have ~70 players and no R2 cut, so make-cut rate
         is effectively 100%; this catches new signature events whose names
         we don't have hard-coded)
      4. Otherwise → "standard"
    """
    if not event_name:
        return "standard"
    name = event_name.lower()
    for pat in MAJOR_PATTERNS:
        if re.search(pat, name):
            return "major"
    for pat in NO_CUT_PATTERNS:
        if re.search(pat, name):
            return "no_cut"
    if cut_rate is not None and cut_rate >= 0.93 and (field_size is None or field_size <= 90):
        return "no_cut"
    return "standard"


def event_type_label(event_type):
    """Human-readable label for logs / UI."""
    return {
        "major": "Major",
        "no_cut": "No-cut",
        "standard": "Standard",
    }.get(event_type, event_type or "Unknown")
