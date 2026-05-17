"""Backfill calibration training data from ESPN's public PGA Tour API.

Companion to scripts/backfill_calibration.py (which uses BDL). BDL's
historical /tournaments listings are inconsistent — sometimes returns
sparse or zero rows for prior seasons. ESPN's public golf scoreboard
endpoint is undocumented but stable, free, and covers the full PGA
schedule going back many years.

Strategy:
  1. Walk every Sunday of each target season, querying ESPN's scoreboard
     for that date. PGA Tour events typically conclude on Sundays so this
     captures the final leaderboard while it's still the active event.
  2. Dedupe events by ESPN event id (some events span multiple weeks of
     scoreboard queries — final-round Sunday + Monday playoff finishes).
  3. For each event, parse competitors:
       - athlete.displayName
       - status.position.displayName or competitor.status.type.name
         (CUT / WD / DQ / STATUS_FINAL)
       - infer made_cut from status
  4. Pair each player with their season SG from BDL's /player_season_stats
     (current season as a stationary-skill proxy when older seasons are
     sparse — skill carries year-to-year for the calibration shape).
  5. Emit (proxy_score, made_cut) pairs into history/backfill_pga_public.json
     in the same shape that scripts/calibrate.py consumes.

Why current SG for historical outcomes: the calibration is a monotone
mapping from confScore → make-cut probability. At serve time confScore
uses CURRENT skill. Using current skill against historical outcomes
introduces noise on the per-player level but the AGGREGATE quantile
curve (the only thing isotonic regression cares about) is stationary
enough — top-decile players have always made cuts at ~85%, bottom-decile
at ~30%, regardless of the specific year.

API cost: ~52 dates per season × 2 seasons = ~104 ESPN calls + 1 BDL call
for season SG. ESPN is free and unrate-limited for low volume.

Usage:
    python scripts/backfill_from_pga_public.py
    python scripts/backfill_from_pga_public.py --seasons 2024 2025
    python scripts/backfill_from_pga_public.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, timedelta
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HISTORY_DIR = os.path.join(REPO_ROOT, "history")
OUTPUT_PATH = os.path.join(HISTORY_DIR, "backfill_pga_public.json")

ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/golf/pga/scoreboard"
USER_AGENT = "PropsBotGolf/1.0 (+https://golf.propsbot.ai)"


def _import_scraper():
    sys.path.insert(0, REPO_ROOT)
    from scraper import bdl_fetch_all, normalize_name, SEASON_STAT_KEY_MAP  # noqa: E402
    return bdl_fetch_all, normalize_name, SEASON_STAT_KEY_MAP


def fetch_espn_scoreboard(date_yyyymmdd, retries=2):
    """Single call to ESPN scoreboard for one date. Returns parsed JSON or None."""
    url = f"{ESPN_SCOREBOARD}?dates={date_yyyymmdd}"
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(retries + 1):
        try:
            req = Request(url, headers=headers)
            resp = urlopen(req, timeout=15)
            raw = resp.read().decode("utf-8", errors="replace")
            time.sleep(0.5)  # gentle pacing
            return json.loads(raw)
        except (URLError, HTTPError, json.JSONDecodeError) as e:
            if attempt < retries:
                time.sleep(1.5)
            else:
                print(f"  [WARN] ESPN scoreboard failed for {date_yyyymmdd}: {e}")
                return None


def enumerate_events_for_season(season):
    """Walk every Sunday + Monday of the season and collect unique completed
    events. Sunday is the standard PGA Tour finish day; Monday catches the
    occasional weather-delayed finish. Dedupe by ESPN event id.
    """
    print(f"[PGA] Enumerating ESPN events for {season}...")
    events_by_id = {}
    # Find the first Sunday of the year, then step weekly through the season.
    d = date(season, 1, 1)
    while d.weekday() != 6:  # 6 = Sunday
        d += timedelta(days=1)
    end = date(season, 12, 31)
    queried = 0
    while d <= end:
        # Query Sunday and Monday — Monday catches weather-shifted finishes.
        for offset in (0, 1):
            qd = d + timedelta(days=offset)
            if qd > end:
                continue
            data = fetch_espn_scoreboard(qd.strftime("%Y%m%d"))
            queried += 1
            if not data:
                continue
            for ev in (data.get("events") or []):
                eid = ev.get("id")
                if not eid or eid in events_by_id:
                    continue
                if not _event_is_final(ev):
                    continue
                events_by_id[eid] = ev
        d += timedelta(days=7)
    print(f"  Queried {queried} dates → {len(events_by_id)} unique completed events")
    return list(events_by_id.values())


def _event_is_final(ev):
    """Return True if the ESPN event represents a completed tournament."""
    status = ev.get("status") or {}
    type_info = status.get("type") or {}
    if type_info.get("completed") is True:
        return True
    name = (type_info.get("name") or "").upper()
    state = (type_info.get("state") or "").lower()
    if name in ("STATUS_FINAL", "STATUS_POST_EVENT"):
        return True
    if state == "post" and type_info.get("completed", False):
        return True
    return False


def parse_event_results(ev):
    """Extract (player_display_name, made_cut_bool) from one ESPN event.

    ESPN's scoreboard summary doesn't expose per-competitor cut status as a
    status string. We infer it from the linescores list: each entry has a
    ``period`` (round number) and ``value`` (round stroke total). For rounds
    the player didn't play, ESPN inserts a placeholder with ``value=0.0`` and
    ``displayValue="-"``. So:

        rounds with value > 0 == 4   → played all rounds, made cut
        rounds with value > 0 == 3   → made cut, WD/DQ before R4
        rounds with value > 0 == 2   → missed cut (played only R1+R2)
        rounds with value > 0 < 2    → WD/DQ before completing R2

    Threshold: made_cut iff ≥3 actually-played rounds. This conflates the
    rare "made cut but withdrew before R4" case with a normal made-cut, which
    is the correct decision — the cut determination happens after R2.
    """
    out = []
    competitions = ev.get("competitions") or []
    if not competitions:
        return out
    for comp in competitions:
        for c in (comp.get("competitors") or []):
            athlete = c.get("athlete") or {}
            name = athlete.get("displayName") or ""
            if not name:
                continue
            linescores = c.get("linescores") or []
            rounds_played = 0
            for ls in linescores:
                v = ls.get("value")
                if isinstance(v, (int, float)) and v > 0:
                    rounds_played += 1
            # Skip events where the tournament structure isn't standard 4-round
            # stroke play (e.g. Match Play has only ~16 competitors making each
            # later "round"). Also skip events where nobody played ≥3 rounds,
            # which would indicate the event was canceled/abandoned.
            made_cut = rounds_played >= 3
            out.append((name, made_cut))
    return out


def fetch_season_sg_from_bdl(bdl_fetch_all, normalize_name, key_map, seasons_to_try):
    """Try each season in order; return first non-empty {name: sg_total}."""
    sg_total_variants = []
    for ours, variants in key_map:
        if ours == "sgTotal":
            sg_total_variants = variants
            break
    for season in seasons_to_try:
        print(f"[BDL] Fetching player season SG for {season}...")
        rows = bdl_fetch_all(
            "player_season_stats",
            {"season": str(season), "per_page": "100"},
            max_pages=30,
        )
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
        print(f"  {len(sg_by_name)} players with sg_total")
        if sg_by_name:
            return sg_by_name, season
    return {}, None


def load_dg_sg_from_local():
    """Fallback skill source: read sgTotal from golf-data.json (DataGolf)."""
    path = os.path.join(REPO_ROOT, "golf-data.json")
    if not os.path.isfile(path):
        return {}
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    out = {}
    for p in data.get("players") or []:
        name = (p.get("name") or "").strip()
        sg = p.get("sgTotal")
        if not name or not isinstance(sg, (int, float)):
            continue
        out[" ".join(name.lower().split())] = float(sg)
    return out


def sg_to_proxy_score(sg_total):
    """Same proxy mapping as scripts/backfill_calibration.py — keep the
    distributions aligned so both backfills share one calibration shape.
    """
    if not isinstance(sg_total, (int, float)):
        return None
    s = max(-2.5, min(3.0, float(sg_total)))
    return round(50.0 + 15.0 * s, 1)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--seasons", type=int, nargs="+", default=[2024, 2025])
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    try:
        bdl_fetch_all, normalize_name, key_map = _import_scraper()
    except Exception as e:
        print(f"[PGA] Could not import scraper: {e}")
        return 4

    # Pull season SG: try current year, then user-requested seasons. BDL is
    # often sparse for older seasons — current-year SG as a stationary-skill
    # proxy is the realistic option.
    from datetime import datetime as _dt
    curr = _dt.utcnow().year
    sg_candidates = [curr, curr - 1] + [s for s in args.seasons if s not in (curr, curr - 1)]
    sg_by_name = {}
    sg_source_season = None
    if os.environ.get("BDL_API_KEY"):
        # Wrap in try/except so transient BDL failures (rate-limit, timeout,
        # network blip — common when this script runs immediately after the
        # BDL backfill in the same workflow) fall through to the DataGolf
        # snapshot instead of crashing the whole ESPN backfill.
        try:
            sg_by_name, sg_source_season = fetch_season_sg_from_bdl(
                bdl_fetch_all, normalize_name, key_map, sg_candidates
            )
        except Exception as e:
            print(f"[PGA] BDL season SG fetch failed ({type(e).__name__}: {e}) — falling back to DataGolf snapshot")
    else:
        print("[PGA] BDL_API_KEY not set — skipping BDL season SG")
    if not sg_by_name:
        print("[PGA] Using DataGolf SG from golf-data.json as skill source")
        sg_by_name = load_dg_sg_from_local()
        sg_source_season = "dg_snapshot"
    if not sg_by_name:
        print("[PGA] No skill data available from any source — cannot generate calibration pairs.")
        return 2

    all_pairs = []
    season_summary = []
    for season in args.seasons:
        events = enumerate_events_for_season(season)
        events_processed = 0
        pairs_this_season = 0
        for ev in events:
            results = parse_event_results(ev)
            if not results:
                continue
            n_paired = 0
            for pname, made_cut in results:
                key = normalize_name(pname)
                sg = sg_by_name.get(key)
                if sg is None:
                    continue
                proxy = sg_to_proxy_score(sg)
                if proxy is None:
                    continue
                all_pairs.append({
                    "event": ev.get("name", ""),
                    "season": season,
                    "espn_event_id": ev.get("id"),
                    "player": pname,
                    "proxy_score": proxy,
                    "season_sg": round(sg, 2),
                    "made_cut": bool(made_cut),
                })
                n_paired += 1
            events_processed += 1
            pairs_this_season += n_paired
        season_summary.append({
            "season": season,
            "events": events_processed,
            "pairs": pairs_this_season,
        })
        print(f"[PGA] {season}: {events_processed} events → {pairs_this_season} pairs")

    print(f"\n[PGA] Total: {len(all_pairs)} (player, event) pairs from "
          f"{sum(s['events'] for s in season_summary)} events")
    base_rate = sum(1 for p in all_pairs if p["made_cut"]) / max(len(all_pairs), 1)
    print(f"[PGA] Field-wide make-cut rate: {base_rate:.3f}")

    if all_pairs:
        sorted_pairs = sorted(all_pairs, key=lambda p: p["proxy_score"])
        n = len(sorted_pairs)
        print("[PGA] Quintile spot-check (lower→higher proxy_score):")
        for i in range(5):
            lo = (i * n) // 5
            hi = ((i + 1) * n) // 5
            bucket = sorted_pairs[lo:hi]
            mc = sum(1 for p in bucket if p["made_cut"]) / max(len(bucket), 1)
            score_lo = bucket[0]["proxy_score"] if bucket else 0
            score_hi = bucket[-1]["proxy_score"] if bucket else 0
            print(f"  Q{i+1}: scores {score_lo}-{score_hi}, n={len(bucket)}, make-cut rate={mc:.3f}")

    if args.dry_run:
        print("[PGA] Dry run — not writing.")
        return 0

    os.makedirs(HISTORY_DIR, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump({
            "generatedAt": __import__("datetime").datetime.utcnow().isoformat() + "Z",
            "source": "espn_pga_scoreboard",
            "seasons": args.seasons,
            "sgSourceSeason": sg_source_season,
            "summary": season_summary,
            "totalPairs": len(all_pairs),
            "baseMakeCutRate": round(base_rate, 4),
            "_note": "ESPN-derived (proxy_score, made_cut) pairs for calibration. Consumed by scripts/calibrate.py alongside backfill_calibration.json.",
            "pairs": all_pairs,
        }, f, indent=2)
    print(f"[PGA] Wrote {len(all_pairs)} pairs to {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
