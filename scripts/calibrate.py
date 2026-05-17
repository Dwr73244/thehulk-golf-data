"""Calibrate raw confScore against historical make-cut outcomes.

The confScore is a hand-weighted heuristic; without calibration its 0-100
range has no defined relationship to actual outcomes. This script walks the
history/ directory, pairs each pre-event confScore with the final make-cut
outcome from the same event, and fits an isotonic (pool-adjacent-violators)
regression so confScore becomes a calibrated make-cut probability.

The calibration is written to model_params.json as:

    "calibration": {
      "trainedAt": "ISO timestamp",
      "n": 1234,                       # training pairs used
      "events": ["Masters 2026", ...], # events sourced from
      "brier": 0.18,                   # achieved Brier (lower=better)
      "baselineBrier": 0.21,           # uncalibrated baseline
      "table": [                       # piecewise-linear lookup
        {"score": 0,   "prob": 0.05},
        {"score": 25,  "prob": 0.32},
        {"score": 50,  "prob": 0.58},
        {"score": 75,  "prob": 0.79},
        {"score": 100, "prob": 0.93}
      ]
    }

At scraper-run time, ``scraper.apply_confscore_calibration`` looks up each
player's raw confScore in this table and writes ``confScoreCalibratedMakeCutProb``
onto the player. The frontend can then display calibrated probabilities
instead of raw scores.

Usage:
    python scripts/calibrate.py
    python scripts/calibrate.py --dry-run
    python scripts/calibrate.py --min-pairs 50   # require this many pairs
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HISTORY_DIR = os.path.join(REPO_ROOT, "history")
PARAMS_PATH = os.path.join(REPO_ROOT, "model_params.json")


def load_history_pairs():
    """Walk history/*.json and return list of (event_key, player_name,
    pre_event_confScore, made_cut_bool).

    Strategy:
      1. Group snapshots by event name.
      2. For each event, the PRE-event prediction = the snapshot taken
         BEFORE the event went IN_PROGRESS (i.e., highest confScore taken
         from a NOT_STARTED status, falling back to the earliest IN_PROGRESS
         if no pre-event snap exists).
      3. The OUTCOME = the latest snapshot with currentEvent.status of
         COMPLETED, IN_PROGRESS (post-R2), or Final. Made cut iff the
         player's leaderboard row exists AND position not in {"CUT", "WD"}
         AND round2 score is populated (cut happens after R2).
    """
    if not os.path.isdir(HISTORY_DIR):
        return [], []
    files = sorted(f for f in os.listdir(HISTORY_DIR) if f.endswith(".json"))
    snaps_by_event = {}
    for fn in files:
        path = os.path.join(HISTORY_DIR, fn)
        try:
            with open(path, encoding="utf-8") as f:
                d = json.load(f)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as e:
            print(f"  [WARN] skipped {fn}: {e}")
            continue
        ce = d.get("currentEvent") or {}
        event_name = ce.get("name") or "Unknown"
        snaps_by_event.setdefault(event_name, []).append((fn, d))

    pairs = []
    events_used = []
    for event_name, snaps in snaps_by_event.items():
        # Predictions: prefer latest NOT_STARTED snapshot, else earliest IN_PROGRESS
        not_started = [s for s in snaps
                       if (s[1].get("currentEvent") or {}).get("status") == "NOT_STARTED"]
        in_progress = [s for s in snaps
                       if (s[1].get("currentEvent") or {}).get("status") == "IN_PROGRESS"]
        pred_snap = (not_started[-1] if not_started
                     else (in_progress[0] if in_progress else None))
        if not pred_snap:
            continue

        # IDENTIFY THE PRE-CUT FIELD: earliest IN_PROGRESS snapshot with a
        # full leaderboard (before missed-cut players get removed). A typical
        # PGA field is 130-156; if the snapshot has fewer than 100 players,
        # the cut has likely already happened and we can't tell who played.
        field_snap = None
        for fn, d in snaps:
            ce = d.get("currentEvent") or {}
            if ce.get("status") != "IN_PROGRESS":
                continue
            lb = ce.get("leaderboard") or []
            if len(lb) >= 100:
                field_snap = (fn, d)
                break  # earliest wins
        if not field_snap:
            # Fall back to whichever has the largest leaderboard
            best = max(snaps, key=lambda s: len(((s[1].get("currentEvent") or {}).get("leaderboard")) or []))
            best_lb = ((best[1].get("currentEvent") or {}).get("leaderboard")) or []
            if len(best_lb) < 60:  # too small to be a real field — skip event
                continue
            field_snap = best
        pre_cut_field = set()
        for row in (((field_snap[1].get("currentEvent") or {}).get("leaderboard")) or []):
            name = (row.get("name") or "").strip().lower()
            if name:
                pre_cut_field.add(name)
        if not pre_cut_field:
            continue

        # OUTCOME SNAPSHOT: latest snapshot where R2 is complete (every row
        # in the leaderboard has a non-zero round2 score). After R2, the
        # made-cut roster is final; players not in the leaderboard missed the cut.
        outcome_snap = None
        for fn, d in snaps:
            lb = ((d.get("currentEvent") or {}).get("leaderboard")) or []
            if not lb:
                continue
            # All present players have R2 done — this is post-cut
            all_r2 = all(isinstance(r.get("round2"), (int, float)) and r.get("round2", 0) > 0
                         for r in lb if r.get("round1"))
            if all_r2 and len(lb) < len(pre_cut_field):
                outcome_snap = (fn, d)  # keep updating to latest
        if not outcome_snap:
            continue
        made_cut_names = set()
        for row in (((outcome_snap[1].get("currentEvent") or {}).get("leaderboard")) or []):
            name = (row.get("name") or "").strip().lower()
            pos = (row.get("position") or "").upper()
            if name and pos not in ("CUT", "WD", "DQ", "MDF"):
                made_cut_names.add(name)

        # Pair predictions with outcomes — only players who actually played
        for p in (pred_snap[1].get("players") or []):
            name = (p.get("name") or "").strip()
            cs = p.get("confScore")
            if not name or not isinstance(cs, (int, float)):
                continue
            nlow = name.lower()
            if nlow not in pre_cut_field:
                continue  # player wasn't actually in this field
            outcome = nlow in made_cut_names
            pairs.append((event_name, name, float(cs), outcome))
        events_used.append(event_name)
    return pairs, events_used


def isotonic_pav(x_y_pairs):
    """Pool Adjacent Violators isotonic regression.

    Input: list of (x, y) pairs where y is 0/1 (or any real). Returns the
    fitted monotonic-non-decreasing y values for the sorted x values.

    The classic PAV algorithm: sort by x, walk through, whenever the running
    mean decreases pool with the previous block. Output preserves the count
    structure so we can build a piecewise-linear lookup afterwards.
    """
    if not x_y_pairs:
        return []
    pairs = sorted(x_y_pairs, key=lambda t: t[0])
    blocks = [{"x_min": x, "x_max": x, "sum": y, "n": 1} for x, y in pairs]
    i = 0
    while i < len(blocks) - 1:
        if blocks[i]["sum"] / blocks[i]["n"] > blocks[i + 1]["sum"] / blocks[i + 1]["n"]:
            # Pool block i+1 into block i
            blocks[i]["x_max"] = blocks[i + 1]["x_max"]
            blocks[i]["sum"] += blocks[i + 1]["sum"]
            blocks[i]["n"] += blocks[i + 1]["n"]
            del blocks[i + 1]
            # Walk back to re-check the new neighbor
            if i > 0:
                i -= 1
        else:
            i += 1
    return blocks


def build_lookup_table(blocks, granularity=5):
    """Convert PAV blocks into a piecewise-linear lookup at every Nth score.

    Output: list of {"score": int, "prob": float} for scores 0..100 at
    ``granularity`` steps. Frontend / scraper do linear interpolation
    between adjacent entries.
    """
    if not blocks:
        return []
    # Block representative: midpoint of x range, mean of y
    block_points = [(0.5 * (b["x_min"] + b["x_max"]), b["sum"] / b["n"]) for b in blocks]
    # Extend to full 0-100 range so lookups outside training data are safe
    first_x, first_y = block_points[0]
    last_x, last_y = block_points[-1]
    table = []
    for s in range(0, 101, granularity):
        if s <= first_x:
            prob = first_y
        elif s >= last_x:
            prob = last_y
        else:
            # Linear interp between bracketing block points
            for j in range(len(block_points) - 1):
                x0, y0 = block_points[j]
                x1, y1 = block_points[j + 1]
                if x0 <= s <= x1:
                    if x1 == x0:
                        prob = y1
                    else:
                        prob = y0 + (y1 - y0) * (s - x0) / (x1 - x0)
                    break
            else:
                prob = last_y
        table.append({"score": s, "prob": round(prob, 4)})
    return table


def lookup_calibrated_prob(score, table):
    """Linear-interpolation lookup against the calibration table."""
    if not table or score is None:
        return None
    if score <= table[0]["score"]:
        return table[0]["prob"]
    if score >= table[-1]["score"]:
        return table[-1]["prob"]
    for i in range(len(table) - 1):
        a, b = table[i], table[i + 1]
        if a["score"] <= score <= b["score"]:
            span = b["score"] - a["score"]
            if span == 0:
                return a["prob"]
            return a["prob"] + (b["prob"] - a["prob"]) * (score - a["score"]) / span
    return table[-1]["prob"]


def brier(predictions):
    if not predictions:
        return None
    return round(sum((p - o) ** 2 for p, o in predictions) / len(predictions), 4)


BACKFILL_FILES = ("backfill_calibration.json", "backfill_pga_public.json")


def load_backfill_pairs():
    """Load the bulk historical backfill produced by the two backfill scripts.

    Returns ``(pairs, events)`` where each pair is a dict with all metadata:
      {
        "event": str,         # e.g. "Masters Tournament 2024"
        "player": str,
        "score": float,       # the proxy_score
        "outcome": bool,
        "event_type": str,    # "major" | "standard" (no_cut already filtered)
        "event_date": str,    # YYYY-MM-DD when available (ESPN provides; BDL doesn't)
        "source": str,        # filename of origin
      }

    Cross-file dedup on (event, season, player) — BDL preferred over ESPN.
    """
    all_pairs = []
    all_events = set()
    seen = set()
    for fn in BACKFILL_FILES:
        backfill_path = os.path.join(HISTORY_DIR, fn)
        if not os.path.isfile(backfill_path):
            continue
        try:
            with open(backfill_path, encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            print(f"  [WARN] backfill load failed for {fn}: {e}")
            continue
        added = 0
        for p in data.get("pairs") or []:
            try:
                ev = p["event"]
                pl = p["player"]
                season = p.get("season")
                key = (ev, season, pl.lower().strip())
                if key in seen:
                    continue
                seen.add(key)
                event_with_season = f"{ev} {season}" if season else ev
                all_pairs.append({
                    "event": event_with_season,
                    "player": pl,
                    "score": float(p["proxy_score"]),
                    "outcome": bool(p["made_cut"]),
                    "event_type": p.get("event_type", "standard"),
                    "event_date": p.get("event_date", ""),
                    "source": fn,
                })
                all_events.add(event_with_season)
                added += 1
            except (KeyError, TypeError, ValueError):
                continue
        print(f"  {fn}: {added} pairs after dedup")
    return all_pairs, sorted(all_events)


def fit_calibration(pairs, label="all"):
    """Fit PAV + lookup table on a subset of pairs. Returns ({table, n,
    brier, baselineBrier, baseRate, brierLift}, fit_input) or None if
    insufficient data."""
    if not pairs:
        return None, None
    fit_input = [(p["score"], 1.0 if p["outcome"] else 0.0) for p in pairs]
    if len(fit_input) < 30:
        print(f"[CALIBRATE] {label}: only {len(fit_input)} pairs — skipped (need ≥30)")
        return None, fit_input
    blocks = isotonic_pav(fit_input)
    table = build_lookup_table(blocks, granularity=5)
    base_rate = sum(o for _, o in fit_input) / len(fit_input)
    cal_b = brier([(lookup_calibrated_prob(s, table), o) for s, o in fit_input])
    uncal_b = brier([(s / 100.0, o) for s, o in fit_input])
    base_b = brier([(base_rate, o) for _, o in fit_input])
    print(f"[CALIBRATE] {label}: n={len(fit_input)}, blocks={len(blocks)}, "
          f"baseRate={base_rate:.3f}, Brier base={base_b}, uncal={uncal_b}, cal={cal_b}")
    return {
        "n": len(fit_input),
        "table": table,
        "brier": cal_b,
        "uncalibratedBrier": uncal_b,
        "baselineBrier": base_b,
        "baseRate": round(base_rate, 4),
        "blocks": len(blocks),
    }, fit_input


def time_split_holdout(pairs, holdout_fraction=0.25):
    """Split pairs into (train, test) by event_date. Most-recent fraction
    becomes test. Pairs without a date go entirely into train (we can't
    place them temporally).

    Returns: (train_pairs, test_pairs)
    """
    dated = [p for p in pairs if p.get("event_date")]
    undated = [p for p in pairs if not p.get("event_date")]
    if not dated:
        return list(pairs), []
    dated.sort(key=lambda p: p["event_date"])
    split_idx = int(len(dated) * (1.0 - holdout_fraction))
    train = dated[:split_idx] + undated
    test = dated[split_idx:]
    return train, test


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--dry-run", action="store_true", help="Print results, don't write")
    ap.add_argument("--min-pairs", type=int, default=80,
                    help="Minimum training pairs required (default 80)")
    ap.add_argument("--skip-backfill", action="store_true",
                    help="Ignore backfill files (use live snapshots only)")
    ap.add_argument("--live-weight", type=int, default=3,
                    help="Effective weight for live-snapshot pairs (replicated N times). "
                         "Live pairs use the real multi-feature confScore vs the proxy "
                         "used in backfill, so they're higher-signal. Default 3.")
    ap.add_argument("--holdout-fraction", type=float, default=0.25,
                    help="Fraction of most-recent dated pairs to hold out for honest "
                         "out-of-sample Brier reporting. Default 0.25.")
    args = ap.parse_args()

    print(f"[CALIBRATE] Walking {HISTORY_DIR}...")
    live_pairs_raw, live_events = load_history_pairs()
    # Adapt live pairs to dict shape (load_history_pairs returns tuples)
    live_pairs = [{
        "event": e, "player": pl, "score": s, "outcome": o,
        "event_type": "standard",  # live snapshots are tour stops; no major-tagging yet
        "event_date": "", "source": "live_snapshot",
    } for (e, pl, s, o) in live_pairs_raw]
    print(f"[CALIBRATE] Live snapshots: {len(live_pairs)} pairs from {len(live_events)} events")

    backfill_pairs, backfill_events = ([], [])
    if not args.skip_backfill:
        backfill_pairs, backfill_events = load_backfill_pairs()
        if backfill_pairs:
            print(f"[CALIBRATE] Backfill data: {len(backfill_pairs)} pairs from {len(backfill_events)} events")

    # Effective training set: live pairs replicated N times (upweighting).
    # The PAV algorithm treats each pair equally, so replication is the
    # clean way to give live pairs more pull on the calibration curve.
    weighted_live = live_pairs * max(1, args.live_weight)
    pairs = list(weighted_live) + list(backfill_pairs)
    events = list(live_events) + list(backfill_events)
    print(f"[CALIBRATE] Combined: {len(pairs)} effective pairs "
          f"({len(live_pairs)} live × {args.live_weight} weight + {len(backfill_pairs)} backfill) "
          f"from {len(set(events))} events")

    if len(pairs) < args.min_pairs:
        print(f"[CALIBRATE] Not enough data ({len(pairs)} < {args.min_pairs}). Skipping.")
        return 1

    # --- TIME-SPLIT HOLDOUT for honest out-of-sample Brier reporting ---
    # Most-recent dated pairs become test set. Train calibration on the
    # remainder. Live pairs (no date) all go into train.
    train_pairs, test_pairs = time_split_holdout(pairs, args.holdout_fraction)
    print(f"[CALIBRATE] Time-split: {len(train_pairs)} train, {len(test_pairs)} test "
          f"(holdout fraction {args.holdout_fraction})")

    # --- STRATIFIED CALIBRATION BY EVENT TYPE ---
    # Train separate isotonic tables for {major, standard} so the calibration
    # curve matches the field-difficulty profile at serve time. Also fit a
    # "default" table over all training pairs as the fallback when serve-time
    # event type can't be classified.
    tables = {}
    print("\n[CALIBRATE] === STRATIFIED FIT (in-sample) ===")
    for event_type in ("standard", "major"):
        subset = [p for p in train_pairs if p.get("event_type") == event_type]
        result, _ = fit_calibration(subset, label=event_type)
        if result:
            # Out-of-sample Brier against held-out test subset of same type
            test_subset = [p for p in test_pairs if p.get("event_type") == event_type]
            if test_subset:
                test_input = [(p["score"], 1.0 if p["outcome"] else 0.0) for p in test_subset]
                oos_brier = brier([(lookup_calibrated_prob(s, result["table"]), o)
                                   for s, o in test_input])
                result["heldoutBrier"] = oos_brier
                result["heldoutN"] = len(test_subset)
                print(f"  → out-of-sample on {len(test_subset)} held-out {event_type} pairs: "
                      f"Brier {oos_brier}")
            tables[event_type] = result

    # Default table: all training pairs union — used at serve time when we
    # can't classify the event (or for legacy consumers expecting a single table)
    print("\n[CALIBRATE] === DEFAULT FIT (all training pairs) ===")
    default_result, _ = fit_calibration(train_pairs, label="default")
    if default_result:
        if test_pairs:
            test_input = [(p["score"], 1.0 if p["outcome"] else 0.0) for p in test_pairs]
            oos_brier = brier([(lookup_calibrated_prob(s, default_result["table"]), o)
                               for s, o in test_input])
            default_result["heldoutBrier"] = oos_brier
            default_result["heldoutN"] = len(test_pairs)
            print(f"  → out-of-sample on {len(test_pairs)} held-out pairs: Brier {oos_brier}")
        tables["default"] = default_result

    if not tables:
        print("[CALIBRATE] No tables fit (every stratum below min size). Aborting.")
        return 1

    calibration = {
        "trainedAt": datetime.now(timezone.utc).isoformat(),
        "n": len(pairs),
        "events": sorted(set(events)),
        "liveWeight": args.live_weight,
        "holdoutFraction": args.holdout_fraction,
        "tables": tables,
        # Back-compat: legacy consumers reading calibration.table get the
        # default stratum
        "table": tables["default"]["table"] if "default" in tables else next(iter(tables.values()))["table"],
        "brier": tables.get("default", {}).get("brier"),
        "baselineBrier": tables.get("default", {}).get("uncalibratedBrier"),
        "heldoutBrier": tables.get("default", {}).get("heldoutBrier"),
        "_note": "Stratified isotonic (PAV) calibration. tables[event_type] gives "
                 "the table for that event type; tables.default is the all-pairs "
                 "fallback. Each table includes heldoutBrier (out-of-sample on the "
                 "most-recent holdoutFraction of dated pairs). Trained by scripts/calibrate.py.",
    }

    if args.dry_run:
        print("[CALIBRATE] Dry run — calibration not written.")
        print(json.dumps(calibration, indent=2))
        return 0

    # Load existing params, merge calibration, persist
    existing = {}
    if os.path.isfile(PARAMS_PATH):
        try:
            with open(PARAMS_PATH, encoding="utf-8") as f:
                existing = json.load(f)
        except (OSError, json.JSONDecodeError):
            pass
    existing["calibration"] = calibration
    with open(PARAMS_PATH, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2)
    print(f"[CALIBRATE] Calibration written to {PARAMS_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
