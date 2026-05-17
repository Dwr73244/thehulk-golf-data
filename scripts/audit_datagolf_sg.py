"""DataGolf trust check: are DataGolf's season SG numbers field-adjusted?

We've been consuming DataGolf SG as if it were strength-of-schedule
corrected. This script independently verifies by joining DataGolf SG (in
``golf-data.json`` PLAYERS array) against BDL's ``/player_season_stats``
endpoint and computing:

  1. Per-stat correlation (DG vs BDL SG total, off-tee, approach, putting)
  2. Per-stat mean absolute difference
  3. Players with the largest divergence (could indicate one source isn't
     adjusting properly — investigate manually)
  4. A binary verdict: TRUSTABLE / SUSPECT / FAIL

Both sources SHOULD be field-adjusted; high correlation + low MAD = trust.
Low correlation or systematic bias on a subset = problem.

Usage:
    python scripts/audit_datagolf_sg.py
    python scripts/audit_datagolf_sg.py --season 2026
    python scripts/audit_datagolf_sg.py --json    # machine-readable output

Requires BDL_API_KEY in env. Read-only — never writes back.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(REPO_ROOT, "golf-data.json")


def _import_scraper_helpers():
    """Import the BDL helpers from scraper.py for endpoint access."""
    sys.path.insert(0, REPO_ROOT)
    from scraper import bdl_fetch_all, normalize_name, SEASON_STAT_KEY_MAP  # noqa: E402
    return bdl_fetch_all, normalize_name, SEASON_STAT_KEY_MAP


def pearson_correlation(xs, ys):
    """Pearson r for two equal-length lists. Returns None on degenerate input."""
    n = len(xs)
    if n < 2:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    dx2 = sum((x - mx) ** 2 for x in xs)
    dy2 = sum((y - my) ** 2 for y in ys)
    if dx2 == 0 or dy2 == 0:
        return None
    return num / ((dx2 ** 0.5) * (dy2 ** 0.5))


def fetch_bdl_season_sg(season, bdl_fetch_all, key_map):
    """Pull BDL season SG by stat_name → return { normalized_name: {sg_total, sg_app, sg_ott, sg_putt} }."""
    print(f"[AUDIT] Fetching BDL player_season_stats for season {season}...")
    rows = bdl_fetch_all("player_season_stats", {"season": str(season), "per_page": "100"}, max_pages=20)
    print(f"  Got {len(rows)} season-stat rows")
    # Build name → stat → numeric_value map. BDL season-stats stat_value is
    # a list of {statName, statValue} pairs; first entry is the canonical.
    out = {}
    matched_keys = set()
    for r in rows:
        player = r.get("player") or {}
        pname = player.get("display_name") or (
            f"{player.get('first_name','')} {player.get('last_name','')}".strip()
        )
        if not pname:
            continue
        stat_name = (r.get("stat_name") or "").lower().strip()
        # Map BDL stat_name → our internal key via SEASON_STAT_KEY_MAP
        our_key = None
        for ours, variants in key_map:
            if any(v in stat_name for v in variants):
                our_key = ours
                break
        if our_key not in ("sgTotal", "sgOtt", "sgApp", "sgPutt", "sgArg"):
            continue
        matched_keys.add(our_key)
        # Extract value
        sv = r.get("stat_value") or []
        val = None
        if isinstance(sv, list) and sv:
            first = sv[0]
            if isinstance(first, dict):
                raw = first.get("statValue") or first.get("value")
                try:
                    val = float(raw)
                except (TypeError, ValueError):
                    pass
        if val is None:
            continue
        key = normalize_name_simple(pname)
        out.setdefault(key, {})[our_key] = val
    print(f"  Matched BDL stat names for keys: {sorted(matched_keys)}")
    return out


def normalize_name_simple(name):
    """Same shape as scraper.normalize_name (lowercase, single-space). Inlined
    here so the audit can run with or without scraper available."""
    return " ".join((name or "").strip().lower().split())


def load_dg_players():
    """Pull players (DataGolf-fed) from golf-data.json. Returns dict keyed by name."""
    if not os.path.isfile(DATA_PATH):
        print(f"[AUDIT] golf-data.json not found at {DATA_PATH}")
        return {}
    with open(DATA_PATH, encoding="utf-8") as f:
        data = json.load(f)
    out = {}
    for p in data.get("players") or []:
        name = (p.get("name") or "").strip()
        if not name:
            continue
        out[normalize_name_simple(name)] = {
            "sgTotal": p.get("sgTotal"),
            "sgOtt":   p.get("sgOtt"),
            "sgApp":   p.get("sgApp"),
            "sgArg":   p.get("sgArg"),
            "sgPutt":  p.get("sgPutt"),
            "owgr":    p.get("owgr"),
            "displayName": name,
        }
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--season", type=int, default=None, help="BDL season year (default: current)")
    ap.add_argument("--json", action="store_true", help="Machine-readable JSON output")
    ap.add_argument("--min-pairs", type=int, default=30, help="Minimum overlapping players to call a verdict")
    args = ap.parse_args()

    from datetime import datetime
    season = args.season or datetime.utcnow().year

    try:
        bdl_fetch_all, _ignore_norm, key_map = _import_scraper_helpers()
    except Exception as e:
        print(f"[AUDIT] Could not import scraper helpers: {e}")
        return 4  # exit 4 = configuration error (CI ignores)
    if not os.environ.get("BDL_API_KEY"):
        print("[AUDIT] BDL_API_KEY env var not set — cannot fetch live BDL data.")
        return 4

    dg = load_dg_players()
    print(f"[AUDIT] DataGolf players (from golf-data.json): {len(dg)}")
    bdl = fetch_bdl_season_sg(season, bdl_fetch_all, key_map)
    print(f"[AUDIT] BDL players with season SG: {len(bdl)}")

    overlap = sorted(set(dg.keys()) & set(bdl.keys()))
    print(f"[AUDIT] Overlap (in both sources): {len(overlap)} players")
    if len(overlap) < args.min_pairs:
        print(f"[AUDIT] Not enough overlap ({len(overlap)} < {args.min_pairs}) — cannot trust verdict.")
        return 3  # INDETERMINATE

    # Per-stat correlation + MAD
    stats = ["sgTotal", "sgOtt", "sgApp", "sgArg", "sgPutt"]
    report = {"season": season, "overlap": len(overlap), "stats": {}, "divergent": []}
    for stat in stats:
        xs, ys, names = [], [], []
        for nm in overlap:
            dv = dg[nm].get(stat)
            bv = bdl[nm].get(stat)
            if isinstance(dv, (int, float)) and isinstance(bv, (int, float)):
                xs.append(float(dv))
                ys.append(float(bv))
                names.append(dg[nm]["displayName"])
        if len(xs) < args.min_pairs:
            print(f"  {stat}: only {len(xs)} pairs — skipped")
            continue
        r = pearson_correlation(xs, ys)
        mad = sum(abs(xs[i] - ys[i]) for i in range(len(xs))) / len(xs)
        report["stats"][stat] = {
            "pairs": len(xs),
            "correlation": round(r, 3) if r is not None else None,
            "mad": round(mad, 3),
            "dg_mean": round(sum(xs) / len(xs), 3),
            "bdl_mean": round(sum(ys) / len(ys), 3),
        }
        print(f"  {stat:10s} n={len(xs)} corr={r:.3f} mad={mad:.3f} dg_mean={sum(xs)/len(xs):+.3f} bdl_mean={sum(ys)/len(ys):+.3f}")

    # Top divergent players on sgTotal
    if "sgTotal" in report["stats"]:
        divergent_total = []
        for nm in overlap:
            dv = dg[nm].get("sgTotal")
            bv = bdl[nm].get("sgTotal")
            if isinstance(dv, (int, float)) and isinstance(bv, (int, float)):
                divergent_total.append((dg[nm]["displayName"], dv, bv, abs(dv - bv)))
        divergent_total.sort(key=lambda t: -t[3])
        print("\n[AUDIT] Top 10 divergent players on sgTotal (|DG - BDL|):")
        for nm, dv, bv, diff in divergent_total[:10]:
            print(f"  {nm:25s} DG={dv:+.2f}  BDL={bv:+.2f}  diff={diff:.2f}")
            report["divergent"].append(
                {"name": nm, "dgSgTotal": dv, "bdlSgTotal": bv, "diff": round(diff, 2)}
            )

    # Verdict
    sg_total_r = report["stats"].get("sgTotal", {}).get("correlation")
    sg_total_mad = report["stats"].get("sgTotal", {}).get("mad")
    if sg_total_r is None:
        verdict = "INDETERMINATE"
    elif sg_total_r >= 0.85 and sg_total_mad <= 0.5:
        verdict = "TRUSTABLE"
    elif sg_total_r >= 0.7 and sg_total_mad <= 0.8:
        verdict = "SUSPECT"
    else:
        verdict = "FAIL"
    report["verdict"] = verdict
    print(f"\n[AUDIT] VERDICT: {verdict}")
    if verdict == "TRUSTABLE":
        print("  DataGolf SG correlates strongly with BDL's field-adjusted SG. Both appear to apply equivalent strength correction. Continue using DataGolf SG as input without further adjustment.")
    elif verdict == "SUSPECT":
        print("  DataGolf and BDL SG correlate but with notable spread. Inspect the divergent-player list above for systematic patterns (e.g. LIV players, players with weak-tour-heavy schedules).")
    elif verdict == "FAIL":
        print("  Sources diverge significantly. DO NOT trust either as an inputs-ready field-adjusted measure without further investigation.")
    else:
        print("  Not enough data to call a verdict yet.")

    if args.json:
        print()
        print(json.dumps(report, indent=2))

    # Persist machine-readable report so CI can read + alert
    report_path = os.path.join(REPO_ROOT, "datagolf-audit.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(f"[AUDIT] Report written to {report_path}")

    # Exit codes: 0 = TRUSTABLE, 1 = SUSPECT, 2 = FAIL, 3 = INDETERMINATE.
    # CI uses these to drive Discord alerts.
    return {"TRUSTABLE": 0, "SUSPECT": 1, "FAIL": 2, "INDETERMINATE": 3}.get(verdict, 0)


if __name__ == "__main__":
    sys.exit(main())
