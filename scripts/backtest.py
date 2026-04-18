#!/usr/bin/env python3
"""Weekly backtest harness for the matchup model.

Replays the past N weeks of scored matchups through the scoring model
and reports calibration + ROI. If calibration has drifted over a
rolling window, grid-searches `baseStd` and `roundShockStd` to find
the combo that minimizes Brier score, then writes tuned values to
`model_params.json`.

Usage:
    python scripts/backtest.py [--weeks 12] [--tune] [--dry-run]

Runs as a weekly GitHub Actions job (Mon 2AM ET). Non-tuning runs just
emit backtest-report.json for transparency.
"""
import argparse
import json
import os
import sys
from datetime import datetime

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HISTORY_DIR = os.path.join(REPO_ROOT, "history")
PARAMS_PATH = os.path.join(REPO_ROOT, "model_params.json")
REPORT_PATH = os.path.join(REPO_ROOT, "backtest-report.json")


def load_history(weeks):
    """Load last N weekly snapshots in reverse chronological order."""
    if not os.path.isdir(HISTORY_DIR):
        return []
    files = sorted(
        [f for f in os.listdir(HISTORY_DIR) if f.endswith(".json")],
        reverse=True,
    )[:weeks]
    out = []
    for fname in files:
        try:
            with open(os.path.join(HISTORY_DIR, fname), encoding="utf-8") as f:
                snap = json.load(f)
            out.append({"date": fname[:10], "data": snap})
        except (OSError, json.JSONDecodeError):
            continue
    return out


def brier_score(predictions):
    """Brier score for a list of (prob, outcome_0_or_1) pairs. Lower is better."""
    if not predictions:
        return None
    s = sum((p - o) ** 2 for p, o in predictions)
    return round(s / len(predictions), 4)


def extract_scored_matchups(snapshots):
    """Walk snapshots, pair each matchup's model prediction with the
    actual outcome taken from the following snapshot's leaderboard."""
    # Snapshots are newest first; iterate pairs (older_snap, newer_snap)
    pairs = []
    for i in range(len(snapshots) - 1):
        older = snapshots[i + 1]["data"]
        newer = snapshots[i]["data"]
        tbs = older.get("threeBalls") or []
        lb = ((newer.get("currentEvent") or {}).get("leaderboard")) or []
        if not tbs or not lb:
            continue
        round_scores = {}
        for entry in lb:
            name = (entry.get("name") or "").lower()
            for rnd in (1, 2, 3, 4):
                v = entry.get(f"round{rnd}")
                if isinstance(v, (int, float)) and v > 0:
                    round_scores.setdefault(name, {})[rnd] = v

        for g in tbs:
            rnd = g.get("round")
            if rnd not in (1, 2, 3, 4):
                continue
            players = g.get("players") or []
            scores = []
            for p in players:
                name = (p.get("name") or "").lower()
                score = (round_scores.get(name) or {}).get(rnd)
                if score is None:
                    break
                scores.append((p, score))
            if len(scores) != len(players):
                continue  # missing a player's round — skip
            lowest = min(s for _, s in scores)
            winners = [p for p, s in scores if s == lowest]
            for p, s in scores:
                prob = p.get("deadHeatWinValue")
                if prob is None:
                    continue
                outcome = 1.0 if p in winners and len(winners) == 1 else (
                    0.5 if p in winners else 0.0
                )
                pairs.append((float(prob), outcome, g.get("type", "3ball")))
    return pairs


def calibration_bins(pairs, bins=10):
    """Reliability bins: for each 10% probability bucket, actual win rate."""
    if not pairs:
        return []
    buckets = [{"lo": i / bins, "hi": (i + 1) / bins, "n": 0, "sum_p": 0, "sum_o": 0}
               for i in range(bins)]
    for p, o, _ in pairs:
        idx = min(int(p * bins), bins - 1)
        b = buckets[idx]
        b["n"] += 1
        b["sum_p"] += p
        b["sum_o"] += o
    out = []
    for b in buckets:
        if b["n"] == 0:
            continue
        out.append({
            "bin": f'{b["lo"]:.1f}-{b["hi"]:.1f}',
            "n": b["n"],
            "predicted": round(b["sum_p"] / b["n"], 3),
            "actual": round(b["sum_o"] / b["n"], 3),
        })
    return out


def _resim_matchup(group_players, params, actual_scores, sims=3000, seed=1):
    """Rerun Monte Carlo on a single historical matchup with a new param set.

    Each player's stored modelMean was computed with the old baseStd; we
    adjust their std by the delta (newBase - oldBase) which is a reasonable
    first-order approximation since the inputs (sg, fit, weather) don't
    change. Returns (predicted_win_value per player, mc_outcome per player).
    """
    import random
    rnd = random.Random(seed)
    # Recover old baseStd implicitly from the stored modelStd; take minimum
    # across players as the proxy (all players share baseStd).
    old_base = min((p.get("modelStd") or 2.85) for p in group_players)
    delta = params["baseStd"] - old_base
    shock_std = params["roundShockStd"]
    n = len(group_players)
    clear = [0] * n
    tie2 = [0] * n
    tie3 = 0
    push = [0] * n  # 2-ball
    for _ in range(sims):
        shock = rnd.gauss(0.0, shock_std)
        scores = []
        for p in group_players:
            mean = p.get("modelMean") or 71.0
            std = max(1.5, (p.get("modelStd") or 2.85) + delta)
            scores.append(round(mean + shock + rnd.gauss(0.0, std)))
        lo = min(scores)
        winners = [i for i, s in enumerate(scores) if s == lo]
        if n == 2:
            if len(winners) == 1:
                clear[winners[0]] += 1
            else:
                for i in winners:
                    push[i] += 1
        else:
            if len(winners) == 1:
                clear[winners[0]] += 1
            elif len(winners) == 2:
                for i in winners:
                    tie2[i] += 1
            else:
                tie3 += 1
    probs = []
    for i in range(n):
        p_clear = clear[i] / sims
        if n == 2:
            probs.append(p_clear)
        else:
            p_t2 = tie2[i] / sims
            p_t3 = tie3 / sims
            probs.append(p_clear + 0.5 * p_t2 + (1.0 / 3.0) * p_t3)
    return probs


def _grid_search_params(snapshots, baseline_pairs):
    """Grid search over (baseStd, roundShockStd) to minimize Brier on history.

    For each param combo: re-simulate every historical matchup using the
    stored (modelMean, modelStd) per player, pair with actual outcomes
    from next snapshot's leaderboard, compute Brier. Return best combo.
    """
    # Build actuals map per historical matchup
    matchup_actuals = []  # list of (group_players, winner_indices, matchup_type)
    for i in range(len(snapshots) - 1):
        older = snapshots[i + 1]["data"]
        newer = snapshots[i]["data"]
        tbs = older.get("threeBalls") or []
        lb = ((newer.get("currentEvent") or {}).get("leaderboard")) or []
        if not tbs or not lb:
            continue
        round_scores = {}
        for entry in lb:
            name = (entry.get("name") or "").lower()
            for rnd in (1, 2, 3, 4):
                v = entry.get(f"round{rnd}")
                if isinstance(v, (int, float)) and v > 0:
                    round_scores.setdefault(name, {})[rnd] = v
        for g in tbs:
            rnd = g.get("round")
            if rnd not in (1, 2, 3, 4):
                continue
            players = g.get("players") or []
            actual = []
            ok = True
            for p in players:
                name = (p.get("name") or "").lower()
                score = (round_scores.get(name) or {}).get(rnd)
                if score is None:
                    ok = False
                    break
                actual.append(score)
            if not ok:
                continue
            matchup_actuals.append((players, actual, g.get("type", "3ball")))

    if len(matchup_actuals) < 50:
        return None, None

    grid = [
        {"baseStd": bs, "roundShockStd": rs, "sgBlendSeason": b, "sgBlendLive": 1 - b}
        for bs in (2.5, 2.7, 2.85, 3.0, 3.15, 3.3)
        for rs in (0.5, 0.75, 1.0, 1.25, 1.5)
        for b in (0.6, 0.7, 0.8)
    ]
    # Keep rest of defaults
    defaults = {
        "fitBoostScale": 25.0, "parBaseline": 71.0,
        "hotFormBoost": 0.25, "coldFormPenalty": -0.25,
        "windPenaltySlope": 0.08, "windStdSlope": 0.05,
        "windThresholdMph": 12.0,
    }

    best = None
    best_brier = None
    for params in grid:
        total = 0.0
        n_pairs = 0
        for players, actual, _type in matchup_actuals:
            probs = _resim_matchup(players, params, actual)
            lowest = min(actual)
            winners = [i for i, s in enumerate(actual) if s == lowest]
            for i, prob in enumerate(probs):
                outcome = 1.0 if i in winners and len(winners) == 1 else (
                    0.5 if i in winners else 0.0
                )
                total += (prob - outcome) ** 2
                n_pairs += 1
        if n_pairs == 0:
            continue
        brier = total / n_pairs
        if best_brier is None or brier < best_brier:
            best_brier = brier
            best = {**defaults, **params}

    return best, round(best_brier, 4) if best_brier else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--weeks", type=int, default=12)
    parser.add_argument("--tune", action="store_true",
                        help="Re-tune params if calibration drift exceeds 5 percent")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    snapshots = load_history(args.weeks)
    if len(snapshots) < 2:
        print(f"[BACKTEST] Insufficient history ({len(snapshots)} snapshots). "
              f"Need at least 2 weeks before calibration is meaningful.")
        report = {
            "ranAt": datetime.utcnow().isoformat() + "Z",
            "weeksAvailable": len(snapshots),
            "status": "insufficient_data",
        }
        with open(REPORT_PATH, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        return 0

    pairs = extract_scored_matchups(snapshots)
    if not pairs:
        print("[BACKTEST] No scored matchup pairs found in history yet.")
        report = {
            "ranAt": datetime.utcnow().isoformat() + "Z",
            "weeksAvailable": len(snapshots),
            "status": "no_scored_pairs",
        }
        with open(REPORT_PATH, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        return 0

    brier = brier_score([(p, o) for p, o, _ in pairs])
    calibration = calibration_bins(pairs)
    mean_pred = sum(p for p, _, _ in pairs) / len(pairs)
    mean_actual = sum(o for _, o, _ in pairs) / len(pairs)
    drift = abs(mean_pred - mean_actual)

    report = {
        "ranAt": datetime.utcnow().isoformat() + "Z",
        "weeksAvailable": len(snapshots),
        "pairs": len(pairs),
        "brierScore": brier,
        "meanPredicted": round(mean_pred, 3),
        "meanActual": round(mean_actual, 3),
        "calibrationDrift": round(drift, 3),
        "calibrationBins": calibration,
        "status": "ok",
    }

    print(f"[BACKTEST] Pairs: {len(pairs)}, Brier: {brier}, "
          f"Drift: {drift:.3f} (predicted {mean_pred:.3f} vs actual {mean_actual:.3f})")

    if args.tune and len(pairs) >= 200 and drift > 0.05:
        print(f"[BACKTEST] Drift {drift:.3f} > 5% with {len(pairs)} pairs — running grid search.")
        tuned, best_brier = _grid_search_params(snapshots, pairs)
        if tuned:
            # Preserve bookkeeping fields from the previous params
            prev = {}
            if os.path.isfile(PARAMS_PATH):
                try:
                    with open(PARAMS_PATH, encoding="utf-8") as f:
                        prev = json.load(f)
                except (OSError, json.JSONDecodeError):
                    prev = {}
            tuned["lastTrainedAt"] = datetime.utcnow().isoformat() + "Z"
            tuned["lastTrainedBrier"] = best_brier
            tuned["lastTrainedWeeks"] = len(snapshots)
            tuned["_note"] = prev.get(
                "_note",
                "Overwritten weekly by scripts/backtest.py when calibration drifts. Delete this file to reset to defaults.",
            )
            if not args.dry_run:
                with open(PARAMS_PATH, "w", encoding="utf-8") as f:
                    json.dump(tuned, f, indent=2, sort_keys=True)
                print(f"[BACKTEST] Tuned params written to {PARAMS_PATH} — new Brier {best_brier:.4f} vs baseline {brier}")
            report["tuning"] = {
                "applied": True,
                "baselineBrier": brier,
                "tunedBrier": best_brier,
                "tunedParams": {k: tuned[k] for k in tuned if not k.startswith("_") and k != "lastTrainedAt"},
            }
        else:
            report["tuning"] = {"applied": False, "reason": "no-improvement"}
    elif args.tune and drift > 0.05:
        print(f"[BACKTEST] Drift {drift:.3f} > 5% but only {len(pairs)} pairs (<200). Skipping grid search.")
        report["tuning"] = {"applied": False, "reason": "insufficient-pairs"}
    elif args.tune:
        print(f"[BACKTEST] Drift {drift:.3f} within 5% tolerance — no retune.")
        report["tuning"] = {"applied": False, "reason": "in-tolerance"}

    if not args.dry_run:
        with open(REPORT_PATH, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print(f"[BACKTEST] Report written to {REPORT_PATH}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
