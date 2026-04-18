#!/usr/bin/env python3
"""Validate golf-data.json before deploy. Fails CI if core data is missing
or if the schema has drifted (upstream API changed and silently broke us)."""
import json
import sys

with open("golf-data.json", encoding="utf-8") as f:
    d = json.load(f)

q = d.get("dataQuality", {})
ce = d.get("currentEvent") or {}
status = str(ce.get("status", "")).upper()
live = status in ("IN_PROGRESS", "IN PROGRESS", "STATUS_IN_PROGRESS")

print(f"Players: {len(d['players'])}, Generated: {d['generatedAt']}")
print(f"Event: {ce.get('name')} [{status}]")
print(f"Leaderboard: {q.get('leaderboardEntries')} entries from "
      f"{q.get('leaderboardSource')}, hasScores={q.get('leaderboardHasScores')}")
print(f"Odds coverage: {q.get('playersWithOdds')}/{q.get('playersTotal')} "
      f"({q.get('oddsCoverage')})")
print(f"Tee times: {q.get('teeTimesWithValues')}/{q.get('teeTimesTotal')} populated")
print(f"3-balls: {q.get('threeBallGroups', 0)} groups, "
      f"{q.get('threeBallEdges5pct', 0)} edges >5% EV")
print(f"Majors schedule: {len(d.get('majorsSchedule') or [])} entries")

errors = []

# --- Data freshness / completeness ---
# Detect if current event is a major (check name + venue)
evt_name = (ce.get("name") or "").lower()
evt_course = (ce.get("course") or "").lower()
is_major = (
    any(kw in evt_name for kw in
        ("masters", "pga championship", "u.s. open", "us open",
         "open championship", "british open"))
    or any(v in evt_course for v in
        ("augusta", "oak hill", "aronimink", "quail hollow", "oakmont",
         "shinnecock", "pinehurst", "royal portrush", "royal birkdale",
         "royal troon", "st andrews"))
)
warnings = []

# Player count floors — majors have ~156 starters, regular events have
# 120-156, Signature events have ~70. Bump majors threshold higher.
min_players = 120 if is_major else 40
if len(d["players"]) < min_players:
    errors.append(
        f"Player count {len(d['players'])} too low for "
        f"{'major (~156 expected)' if is_major else 'regular event'}"
    )

if live and not q.get("leaderboardHasScores"):
    errors.append("Tournament is live but leaderboard has no scores")
if q.get("oddsCoverage", 0) < 0.3 and len(d["players"]) >= 50:
    errors.append(f"Odds coverage too low: {q.get('oddsCoverage')}")

# Major-week specific: LIV players should be in the field. If we detect a
# major but zero LIV notes anywhere, that's a signal the whitelist failed.
if is_major:
    liv_count = sum(
        1 for p in d["players"]
        if "LIV" in (p.get("notes", "") or "")
    )
    if liv_count < 5:  # Masters normally has ~14+ LIV invitees
        warnings.append(
            f"Only {liv_count} LIV players during major week — expected 10+. "
            "Check fallback LIV roster + is_major detection."
        )

# Majors schedule emitted from BDL — if it's empty we've lost dynamic data
if not d.get("majorsSchedule"):
    warnings.append("majorsSchedule is empty — BDL tournaments fetch may have failed")

# --- Schema drift detection ---
# If any of these top-level keys disappear from the output, an upstream
# API probably changed. Fail loudly before the stale deploy goes live.
required_top_level = [
    "generatedAt", "players", "currentEvent", "courses", "propsByType",
    "threeBalls", "threeBallsSource", "dataQuality",
]
for key in required_top_level:
    if key not in d:
        errors.append(f"Schema drift: missing top-level key '{key}'")

# Every player should have at least these fields — if SG disappears from
# all players the fallback or DataGolf path has silently broken.
if d["players"]:
    required_player_fields = ["name", "rank", "sgTotal", "birdieAvg", "bogeyAvg"]
    missing = {f: 0 for f in required_player_fields}
    for p in d["players"]:
        for f in required_player_fields:
            if f not in p:
                missing[f] += 1
    for f, count in missing.items():
        if count == len(d["players"]):
            errors.append(f"Schema drift: every player missing '{f}'")

for w in warnings:
    print(f"::warning::{w}")

if errors:
    print("::error::Data quality checks failed:")
    for e in errors:
        print(f"::error::  - {e}")
    sys.exit(1)
print("Data quality OK" + (f" ({len(warnings)} warnings)" if warnings else ""))
