"""Backfill calibration training data from 2 years of BDL history.

Our live calibration trains on history/*.json snapshots, which is small
(~3-4 events at any given time). This script extends the training set
to 2024 + 2025 completed PGA Tour events — ~40-60 events × ~150 players
= thousands of additional (player_score, made_cut) pairs.

For each historical event we fetch:
  - /tournament_results — final positions (made cut iff position not in
    {CUT, WD, DQ, MDF})
  - /player_season_stats — that season's sgTotal per player

We can't reconstruct the FULL confScore (would need contemporaneous course
fit, recent form, weather, etc. — all expensive). Instead we use a
proxy "confScore-equivalent" derived from season SG only:

    proxy = 50 + sgTotal * 15  (range roughly 20-95)

The calibration table maps this proxy to make-cut probability. At
serving time the existing confScore consumes this same proxy as its
primary skill component, so the calibration extends cleanly.

Writes pairs to ``history/backfill_calibration.json`` in the same
``(score, outcome)`` shape that ``scripts/calibrate.py`` consumes from
the live history snapshots. The calibrate script then refits isotonic
on the combined corpus.

API budget: ~3 BDL calls per event + 2 season-stat calls. For ~50 events
that's ~150 calls — well within the rate limit. Run once.

Usage:
    python scripts/backfill_calibration.py [--seasons 2024 2025]
"""

from __future__ import annotations

import argparse
import json
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HISTORY_DIR = os.path.join(REPO_ROOT, "history")
OUTPUT_PATH = os.path.join(HISTORY_DIR, "backfill_calibration.json")

# Local import — sits in the same scripts/ folder
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from event_types import classify_event_type  # noqa: E402


def _import_scraper():
    sys.path.insert(0, REPO_ROOT)
    from scraper import bdl_fetch_all, normalize_name, SEASON_STAT_KEY_MAP  # noqa: E402
    return bdl_fetch_all, normalize_name, SEASON_STAT_KEY_MAP


def fetch_completed_events(bdl_fetch_all, season):
    """List completed PGA events for a season.

    Tries three query patterns in order, since BDL's `/tournaments`
    listing is inconsistent for historical PGA data:

      1. ``season=<year> & status=COMPLETED`` (the obvious query — usually 0)
      2. ``status=COMPLETED`` alone, filter client-side by start_date prefix
      3. **Per-course enumeration**: fetch every course, then query
         ``course_ids[]=<id> & status=COMPLETED`` for each. This is the
         pattern that empirically works (already in production via
         ``compute_learned_course_fit``). Filters by start_date prefix.

    Pattern 3 is API-heavier (one call per course = ~50-200 calls) but is
    the only one that consistently returns historical PGA data.
    """
    print(f"[BACKFILL] Fetching completed events for season {season}...")
    rows = bdl_fetch_all(
        "tournaments",
        {"season": str(season), "status": "COMPLETED", "per_page": "100"},
        max_pages=3,
    )
    out = []
    for t in (rows or []):
        tid = t.get("id")
        if not tid:
            continue
        out.append({
            "id": tid,
            "name": t.get("name", ""),
            "course": t.get("course_name", ""),
            "season": season,
        })
    if out:
        print(f"  {len(out)} completed events in season={season} (via season filter)")
        return out

    # Fallback 1: no season filter, filter client-side by start_date year
    print(f"  Season filter returned 0; trying status-only listing...")
    rows = bdl_fetch_all(
        "tournaments",
        {"status": "COMPLETED", "per_page": "100"},
        max_pages=10,
    )
    target_prefix = str(season)
    for t in (rows or []):
        tid = t.get("id")
        sd = t.get("start_date") or ""
        if not tid or not sd.startswith(target_prefix):
            continue
        out.append({
            "id": tid,
            "name": t.get("name", ""),
            "course": t.get("course_name", ""),
            "season": season,
        })
    if out:
        print(f"  {len(out)} events via status-only listing")
        return out

    # Fallback 2: per-course enumeration — this is the one that works
    print(f"  Status-only listing returned 0; falling back to per-course enumeration...")
    out = _enumerate_via_courses(bdl_fetch_all, season)
    print(f"  {len(out)} events via per-course enumeration")
    return out


def _enumerate_via_courses(bdl_fetch_all, season, max_courses=200):
    """List historical events by walking every course in /courses and
    querying its completed tournaments. This is the query pattern that
    empirically works in production (compute_learned_course_fit uses it).
    """
    target_prefix = str(season)
    print(f"  Fetching course list...")
    courses = bdl_fetch_all("courses", {"per_page": "100"}, max_pages=5)
    if not courses:
        print(f"  /courses returned 0; can't enumerate.")
        return []
    print(f"  {len(courses)} courses to walk")
    seen_tids = set()
    out = []
    for i, c in enumerate(courses[:max_courses]):
        cid = c.get("id") or c.get("course_id")
        if not cid:
            continue
        try:
            past_raw = bdl_fetch_all(
                "tournaments",
                {"course_ids[]": str(cid), "status": "COMPLETED", "per_page": "100"},
                max_pages=2,
            )
        except Exception as e:
            print(f"    [WARN] course {cid} tournaments fetch failed: {e}")
            continue
        added = 0
        for t in (past_raw or []):
            tid = t.get("id")
            if not tid or tid in seen_tids:
                continue
            sd = t.get("start_date") or ""
            if not sd.startswith(target_prefix):
                continue
            seen_tids.add(tid)
            out.append({
                "id": tid,
                "name": t.get("name", ""),
                "course": t.get("name") or c.get("name") or "",
                "season": season,
            })
            added += 1
        if added and (i + 1) % 10 == 0:
            print(f"    [{i+1}/{len(courses)}] courses walked, {len(out)} events so far")
    return out


def fetch_season_sg_lookup(bdl_fetch_all, normalize_name, key_map, season, fallback_seasons=None):
    """Build {normalized_name: sgTotal} for a season.

    BDL's /player_season_stats sometimes returns sparse data for older
    seasons. We try the requested season first, then fall back through
    ``fallback_seasons`` (typically current and adjacent years). The
    fallback isn't perfectly contemporaneous but is far better than no
    data — player skill carries year-to-year.
    """
    candidates = [season] + (list(fallback_seasons) if fallback_seasons else [])
    for s in candidates:
        print(f"[BACKFILL] Fetching player season SG for {s}...")
        rows = bdl_fetch_all(
            "player_season_stats",
            {"season": str(s), "per_page": "100"},
            max_pages=30,
        )
        sg_by_name = _parse_season_sg(rows, normalize_name, key_map)
        if sg_by_name:
            if s != season:
                print(f"  (using season {s} as proxy for {season} — no direct data)")
            return sg_by_name
    return {}


def _parse_season_sg(rows, normalize_name, key_map):
    """Helper: extract {normalized_name: sg_total_float} from BDL rows.

    Uses SEASON_STAT_KEY_MAP from scraper.py — the same matching logic used
    everywhere else in the codebase. (Earlier bug: hardcoded substrings missed
    the "sg: total" variant, producing 0 rows even when data existed.)
    """
    # Find the variant list for sgTotal in the shared key map.
    sg_total_variants = []
    for ours, variants in key_map:
        if ours == "sgTotal":
            sg_total_variants = variants
            break
    sg_by_name = {}
    for r in (rows or []):
        stat_name = (r.get("stat_name") or "").lower().strip()
        if not any(v in stat_name for v in sg_total_variants):
            continue
        player = r.get("player") or {}
        pname = player.get("display_name") or (
            f"{player.get('first_name','')} {player.get('last_name','')}".strip()
        )
        if not pname:
            continue
        sv = r.get("stat_value") or []
        if not isinstance(sv, list) or not sv:
            continue
        first = sv[0] if isinstance(sv[0], dict) else {}
        raw = first.get("statValue") or first.get("value")
        try:
            sg = float(raw)
        except (TypeError, ValueError):
            continue
        sg_by_name[normalize_name(pname)] = sg
    print(f"  {len(sg_by_name)} players with sg_total in this response")
    return sg_by_name


def fetch_event_results(bdl_fetch_all, tid):
    """Fetch tournament_results for one event. Returns list of
    (player_display_name, made_cut_bool).
    """
    rows = bdl_fetch_all(
        "tournament_results",
        {"tournament_ids[]": str(tid), "per_page": "100"},
        max_pages=4,
    )
    out = []
    for r in (rows or []):
        player = r.get("player") or {}
        pname = player.get("display_name") or (
            f"{player.get('first_name','')} {player.get('last_name','')}".strip()
        )
        if not pname:
            continue
        position = (r.get("position") or "").upper().strip()
        # Made cut iff position is a numeric place ("1", "T5", "T20", ...)
        # rather than CUT / WD / DQ / MDF.
        missed = position in ("CUT", "WD", "DQ", "MDF", "")
        out.append((pname, not missed))
    return out


def _load_dg_sg_from_local():
    """Read DataGolf-fed sgTotal from the current golf-data.json snapshot.

    Used as long-tail SG coverage when BDL's /player_season_stats returns
    only a handful of players (typical for older seasons). DataGolf's
    current-season rankings are NOT temporally aligned with the historical
    event, but skill is reasonably stationary — a #20-ranked player today
    was likely also a top-30 player 18 months ago.
    """
    path = os.path.join(REPO_ROOT, "golf-data.json")
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    out = {}
    for p in data.get("players") or []:
        name = (p.get("name") or "").strip()
        sg = p.get("sgTotal")
        if not name or not isinstance(sg, (int, float)):
            continue
        out[" ".join(name.lower().split())] = float(sg)
    return out


def sg_to_proxy_score(sg_total):
    """Convert season sgTotal to a proxy confScore-equivalent in [10, 95].

    confScore is normally built from many signals weighted ~20% by sgTotal.
    For backfill we proxy with the linear map below, which roughly matches
    the empirical distribution of confScore observed in current snapshots
    (top players land in the 70-95 range, average 45-55, weak 25-40).
    """
    if not isinstance(sg_total, (int, float)):
        return None
    # Clip extremes; linear map
    s = max(-2.5, min(3.0, float(sg_total)))
    return round(50.0 + 15.0 * s, 1)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--seasons", type=int, nargs="+", default=[2024, 2025])
    ap.add_argument("--max-events-per-season", type=int, default=60)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not os.environ.get("BDL_API_KEY"):
        print("[BACKFILL] BDL_API_KEY not set. Cannot fetch live BDL data.")
        return 1

    bdl_fetch_all, normalize_name, key_map = _import_scraper()

    # Load DataGolf snapshot once — used as the long-tail SG fallback for
    # every season. Year-specific BDL values overwrite per-player when
    # available (BDL is contemporaneous for the season being processed).
    dg_sg = _load_dg_sg_from_local()
    print(f"[BACKFILL] DataGolf snapshot: {len(dg_sg)} players (long-tail fallback)")

    all_pairs = []
    season_summary = []
    for season in args.seasons:
        events = fetch_completed_events(bdl_fetch_all, season)
        if not events:
            print(f"  No completed events found for {season}")
            continue
        # Year-specific BDL SG first (no cross-year fallback — temporal
        # correctness matters for calibration). Union with DataGolf snapshot
        # as long-tail coverage; BDL values overwrite DataGolf when both
        # have a player.
        sg_by_name = dict(dg_sg)
        bdl_only = fetch_season_sg_lookup(
            bdl_fetch_all, normalize_name, key_map, season, fallback_seasons=None
        )
        sg_by_name.update(bdl_only)
        print(f"  Combined SG for {season}: {len(sg_by_name)} players "
              f"({len(bdl_only)} year-specific BDL + DataGolf fallback)")
        if not sg_by_name:
            print(f"  No SG data available — skipping season")
            continue

        events_processed = 0
        pairs_this_season = 0
        skipped_no_cut = 0
        for ev in events[: args.max_events_per_season]:
            results = fetch_event_results(bdl_fetch_all, ev["id"])
            if not results:
                continue
            # Compute cut metrics for event-type classification (we need the
            # full results before knowing what to do with the event)
            field_size = len(results)
            cut_count = sum(1 for _, mc in results if mc)
            cut_rate = cut_count / field_size if field_size else None
            event_type = classify_event_type(ev["name"], cut_rate=cut_rate, field_size=field_size)
            if event_type == "no_cut":
                skipped_no_cut += 1
                continue
            n_paired = 0
            for pname, made_cut in results:
                key = normalize_name(pname)
                sg = sg_by_name.get(key)
                if sg is None:
                    continue  # player not in season-stats lookup
                proxy = sg_to_proxy_score(sg)
                if proxy is None:
                    continue
                all_pairs.append({
                    "event": ev["name"],
                    "season": season,
                    "event_type": event_type,
                    "tournament_id": ev["id"],
                    "player": pname,
                    "proxy_score": proxy,
                    "season_sg": round(sg, 2),
                    "made_cut": bool(made_cut),
                })
                n_paired += 1
            events_processed += 1
            pairs_this_season += n_paired
            if events_processed % 10 == 0:
                print(f"  [{season}] processed {events_processed} events, {pairs_this_season} pairs")
        season_summary.append({
            "season": season,
            "events": events_processed,
            "pairs": pairs_this_season,
            "skippedNoCut": skipped_no_cut,
        })
        print(f"[BACKFILL] {season}: {events_processed} events → {pairs_this_season} pairs "
              f"({skipped_no_cut} no-cut events excluded)")

    print(f"\n[BACKFILL] Total: {len(all_pairs)} (player, event) pairs from {sum(s['events'] for s in season_summary)} events")
    base_rate = sum(1 for p in all_pairs if p["made_cut"]) / max(len(all_pairs), 1)
    print(f"[BACKFILL] Field-wide make-cut rate: {base_rate:.3f}")

    # Sanity: score-quantile vs make-cut rate
    if all_pairs:
        sorted_pairs = sorted(all_pairs, key=lambda p: p["proxy_score"])
        n = len(sorted_pairs)
        print("[BACKFILL] Quintile spot-check (lower→higher proxy_score):")
        for i in range(5):
            lo = (i * n) // 5
            hi = ((i + 1) * n) // 5
            bucket = sorted_pairs[lo:hi]
            mc = sum(1 for p in bucket if p["made_cut"]) / max(len(bucket), 1)
            score_lo = bucket[0]["proxy_score"] if bucket else 0
            score_hi = bucket[-1]["proxy_score"] if bucket else 0
            print(f"  Q{i+1}: scores {score_lo}-{score_hi}, n={len(bucket)}, make-cut rate={mc:.3f}")

    if args.dry_run:
        print("[BACKFILL] Dry run — not writing.")
        return 0

    os.makedirs(HISTORY_DIR, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump({
            "generatedAt": __import__("datetime").datetime.utcnow().isoformat() + "Z",
            "seasons": args.seasons,
            "summary": season_summary,
            "totalPairs": len(all_pairs),
            "baseMakeCutRate": round(base_rate, 4),
            "_note": "Backfilled (proxy_score, made_cut) pairs for calibration. Consumed by scripts/calibrate.py — see --use-backfill flag.",
            "pairs": all_pairs,
        }, f, indent=2)
    print(f"[BACKFILL] Wrote {len(all_pairs)} pairs to {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
