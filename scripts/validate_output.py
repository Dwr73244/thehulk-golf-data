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
if len(d["players"]) < 40:
    errors.append(f"Player count too low: {len(d['players'])}")
if live and not q.get("leaderboardHasScores"):
    errors.append("Tournament is live but leaderboard has no scores")
if q.get("oddsCoverage", 0) < 0.3 and len(d["players"]) >= 50:
    errors.append(f"Odds coverage too low: {q.get('oddsCoverage')}")

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

if errors:
    print("::error::Data quality checks failed:")
    for e in errors:
        print(f"::error::  - {e}")
    sys.exit(1)
print("Data quality OK")
