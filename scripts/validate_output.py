#!/usr/bin/env python3
"""Validate golf-data.json before deploy. Fails the CI run if core data is missing."""
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

errors = []
if len(d["players"]) < 50:
    errors.append(f"Player count too low: {len(d['players'])}")
if live and not q.get("leaderboardHasScores"):
    errors.append("Tournament is live but leaderboard has no scores")
if q.get("oddsCoverage", 0) < 0.3:
    errors.append(f"Odds coverage too low: {q.get('oddsCoverage')}")

if errors:
    print("::error::Data quality checks failed:")
    for e in errors:
        print(f"::error::  - {e}")
    sys.exit(1)
print("Data quality OK")
