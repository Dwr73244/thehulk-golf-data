#!/usr/bin/env python3
"""
PropsBot Golf Data Scraper
==========================
Pulls player stats, course data, and scoring info from free public sources.
Outputs a single golf-data.json file consumed by the PropsBot Golf Intelligence frontend.

Free Sources Used:
  - PGA Tour Stats (pgatour.com/stats) — official player statistics
  - DataGolf (datagolf.com) — hole-level scoring, SG breakdowns, rankings
  - ESPN PGA Leaderboard — current tournament results

Run manually:   python scraper.py
Run via cron:    GitHub Actions (see .github/workflows/weekly-scrape.yml)

Output: golf-data.json (single file, ~50-100KB)
"""

import json
import re
import sys
import time
import os
import math
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError


def normalize_name(name):
    """Strip accents/diacritics and lowercase for fuzzy name matching.
    'Ludvig Åberg' → 'ludvig aberg', 'José María Olazábal' → 'jose maria olazabal'
    """
    if not name:
        return ""
    # Decompose unicode, strip combining marks (accents), re-compose
    nfkd = unicodedata.normalize('NFKD', name)
    stripped = ''.join(c for c in nfkd if not unicodedata.combining(c))
    return stripped.lower().strip()

# ============================================================
# CONFIG
# ============================================================
OUTPUT_FILE = "golf-data.json"
USER_AGENT = "PropsBot-Golf-Scraper/1.0 (Educational Research Tool)"

# API Keys (from environment — set in GitHub Secrets)
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
BDL_API_KEY = os.getenv("BDL_API_KEY", "")
BDL_BASE = "https://api.balldontlie.io/pga/v1"

# Module-level cache of the last BDL tournaments response (populated by
# bdl_get_current_tournament), consumed by run_pipeline to emit a dynamic
# majors schedule without a second API call.
_LAST_TOURNAMENTS_RAW = None

# Request timeout in seconds
TIMEOUT = 15

# Delay between requests to be respectful
REQUEST_DELAY = 2.0

# Neutral course-fit default used when a player has no curated/computed fit
# for the current venue. 75 = neutral on our 60-100 scale, producing a
# zero-stroke boost via the `(fit - 75) / 25.0` formula.
DEFAULT_COURSE_FIT = 75

# ============================================================
# HTTP HELPERS
# ============================================================
def fetch_url(url, retries=2):
    """Fetch a URL with retries and respectful delays."""
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(retries + 1):
        try:
            req = Request(url, headers=headers)
            resp = urlopen(req, timeout=TIMEOUT)
            data = resp.read().decode("utf-8", errors="replace")
            time.sleep(REQUEST_DELAY)
            return data
        except (URLError, HTTPError) as e:
            print(f"  [WARN] Attempt {attempt+1} failed for {url}: {e}")
            if attempt < retries:
                time.sleep(REQUEST_DELAY * 2)
            else:
                print(f"  [ERROR] All attempts failed for {url}")
                return None


def fetch_json(url, retries=2):
    """Fetch a URL and parse as JSON."""
    raw = fetch_url(url, retries)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"  [ERROR] JSON decode failed for {url}: {e}")
        return None


# ============================================================
# DATA GOLF — FREE PUBLIC PAGES
# ============================================================
# DataGolf's public pages (no API key needed) expose:
#   - Rankings / skill ratings
#   - Past event hole-level stats
#   - Course history
#
# We scrape the HTML pages and extract embedded JSON data.
# DataGolf embeds data in <script> tags as __NEXT_DATA__ (Next.js app).

def scrape_datagolf_rankings():
    """
    Scrape the DataGolf rankings page for current player skill estimates.
    Returns a list of player dicts with SG breakdowns.
    """
    print("[1/4] Scraping DataGolf rankings...")
    url = "https://datagolf.com/rankings"
    html = fetch_url(url)
    if not html:
        print("  Could not fetch DataGolf rankings. Using fallback data.")
        return None

    # DataGolf uses Next.js — data is in __NEXT_DATA__ script tag
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
    if not match:
        print("  Could not find __NEXT_DATA__ in rankings page. Using fallback.")
        return None

    try:
        next_data = json.loads(match.group(1))
        # Navigate the Next.js data structure to find player rankings
        # This structure may change — add error handling
        props = next_data.get("props", {}).get("pageProps", {})
        rankings_data = props.get("rankings", props.get("data", []))

        if not rankings_data:
            print("  Rankings data empty or structure changed. Using fallback.")
            return None

        players = []
        for i, row in enumerate(rankings_data[:150]):  # Top 150 players
            player = {
                "id": i + 1,
                "name": row.get("player_name", row.get("name", "Unknown")),
                "rank": row.get("dg_rank", row.get("rank", i + 1)),
                "sgTotal": safe_float(row.get("sg_total", row.get("skill_estimate", 0))),
                "sgOtt": safe_float(row.get("sg_ott", 0)),
                "sgApp": safe_float(row.get("sg_app", 0)),
                "sgArg": safe_float(row.get("sg_arg", 0)),
                "sgPutt": safe_float(row.get("sg_putt", 0)),
                "country": row.get("country", ""),
            }
            players.append(player)

        print(f"  Found {len(players)} players from DataGolf rankings.")
        return players

    except (KeyError, TypeError, json.JSONDecodeError) as e:
        print(f"  Error parsing DataGolf data: {e}. Using fallback.")
        return None


def scrape_datagolf_event_holes(event_slug=None):
    """
    Scrape hole-level stats from the most recent completed DataGolf event page.
    Returns per-hole scoring data by round.
    """
    print("[2/4] Scraping DataGolf event hole stats...")
    # Use past results page — the most recent completed event
    url = "https://datagolf.com/past-results"
    html = fetch_url(url)
    if not html:
        print("  Could not fetch DataGolf past results. Skipping hole data.")
        return None

    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
    if not match:
        print("  Could not find hole data. Skipping.")
        return None

    try:
        next_data = json.loads(match.group(1))
        props = next_data.get("props", {}).get("pageProps", {})
        # Extract whatever event/hole data is available
        return props
    except Exception as e:
        print(f"  Error parsing event data: {e}")
        return None


# ============================================================
# PGA TOUR STATS — FREE PUBLIC PAGES
# ============================================================

def scrape_pgatour_stats():
    """
    Scrape key stats from PGA Tour's public stats pages.
    These pages are freely accessible and provide season-level stats.
    """
    print("[3/4] Scraping PGA Tour stats pages...")

    stats_collected = {}

    # Scoring Average (stat ID 108)
    stat_pages = [
        ("scoring_avg", "https://www.pgatour.com/stats/stat.108.html"),
        ("sg_total", "https://www.pgatour.com/stats/stat.02675.html"),
        ("sg_ott", "https://www.pgatour.com/stats/stat.02567.html"),
        ("sg_app", "https://www.pgatour.com/stats/stat.02568.html"),
        ("sg_arg", "https://www.pgatour.com/stats/stat.02569.html"),
        ("sg_putt", "https://www.pgatour.com/stats/stat.02564.html"),
        ("birdie_avg", "https://www.pgatour.com/stats/stat.352.html"),
        ("gir_pct", "https://www.pgatour.com/stats/stat.103.html"),
    ]

    for stat_key, url in stat_pages:
        html = fetch_url(url)
        if html:
            # PGA Tour pages use React hydration — data may be in script tags
            # or in table HTML. We attempt both approaches.
            players = parse_pgatour_stat_table(html, stat_key)
            if players:
                stats_collected[stat_key] = players
                print(f"  Found {len(players)} entries for {stat_key}")
            else:
                print(f"  Could not parse {stat_key} — page structure may have changed")

    return stats_collected if stats_collected else None


def parse_pgatour_stat_table(html, stat_key):
    """
    Parse a PGA Tour stats HTML page.
    Returns list of (player_name, stat_value) tuples.
    """
    # PGA Tour renders stats in table rows — try to extract from HTML
    # The site has changed formats multiple times, so we try multiple patterns
    results = []

    # Pattern 1: Look for JSON-LD or embedded data
    json_match = re.search(r'"statRows"\s*:\s*(\[.*?\])', html, re.DOTALL)
    if json_match:
        try:
            rows = json.loads(json_match.group(1))
            for row in rows:
                name = row.get("playerName", "")
                value = row.get("statValue", row.get("value", ""))
                if name and value:
                    results.append({"name": name, "value": safe_float(value)})
        except:
            pass

    # Pattern 2: Simple table scraping fallback
    if not results:
        # Look for player names and numbers in table-like structures
        rows = re.findall(r'class="[^"]*player[^"]*"[^>]*>([^<]+)</.*?class="[^"]*stat[^"]*"[^>]*>([\d.+-]+)', html, re.DOTALL)
        for name, value in rows:
            results.append({"name": name.strip(), "value": safe_float(value)})

    return results[:150] if results else None


# ============================================================
# ESPN — CURRENT TOURNAMENT / RECENT RESULTS
# ============================================================

def _annotate_tie_positions(leaderboard):
    """Group leaderboard entries by score and apply "T<n>" labels on ties.

    Assumes leaderboard is already in finish order. Non-numeric statuses
    (CUT, WD, DQ) are preserved as-is and not re-labeled.
    """
    if not leaderboard:
        return leaderboard

    # Parse each entry's numeric "score to par" for tie detection. Score is
    # already a string like "-14", "-7", "E", or "+3".
    def score_key(entry):
        s = str(entry.get("score", "")).strip().upper()
        if s in ("E", ""):
            return 0
        try:
            return int(s)
        except ValueError:
            return None  # can't group non-numeric

    # Walk the sorted list, group runs of identical scores, relabel with T
    i = 0
    n = len(leaderboard)
    place = 0
    while i < n:
        entry = leaderboard[i]
        pos_raw = str(entry.get("position", "")).upper()
        if pos_raw in ("CUT", "WD", "DQ", "MDF"):
            place += 1
            i += 1
            continue
        k = score_key(entry)
        if k is None:
            place += 1
            entry["position"] = str(place)
            i += 1
            continue
        # Find how many subsequent entries share this score
        j = i
        while j < n and score_key(leaderboard[j]) == k:
            j += 1
        tied_count = j - i
        place += 1  # tie group starts at this place
        start_place = place
        if tied_count == 1:
            entry["position"] = str(start_place)
        else:
            for m in range(i, j):
                leaderboard[m]["position"] = f"T{start_place}"
            place += tied_count - 1  # advance place over the tie group
        i = j
    return leaderboard


def scrape_espn_leaderboard():
    """
    Fetch current/recent PGA Tour leaderboard from ESPN's public API.
    This is a real, free JSON endpoint.
    """
    print("[4/4] Fetching ESPN leaderboard...")
    url = "https://site.api.espn.com/apis/site/v2/sports/golf/pga/scoreboard"
    data = fetch_json(url)
    if not data:
        print("  Could not fetch ESPN data.")
        return None

    try:
        events = data.get("events", [])
        if not events:
            print("  No current events found.")
            return None

        event = events[0]
        event_info = {
            "name": event.get("name", ""),
            "course": "",
            "startDate": event.get("date", ""),
            "status": event.get("status", {}).get("type", {}).get("description", ""),
        }

        # Get course info
        competitions = event.get("competitions", [])
        if competitions:
            venue = competitions[0].get("venue", {})
            event_info["course"] = venue.get("fullName", "")
            event_info["city"] = venue.get("address", {}).get("city", "")
            event_info["state"] = venue.get("address", {}).get("state", "")

        # Get leaderboard — ESPN now returns score on the competitor directly,
        # and per-round scores in linescores[].value (stroke total per round).
        leaderboard = []
        for comp in competitions:
            for competitor in comp.get("competitors", []):
                athlete = competitor.get("athlete", {}) or {}
                status = competitor.get("status") or {}
                linescores = competitor.get("linescores") or []

                rounds = [0.0, 0.0, 0.0, 0.0]
                total_strokes = 0.0
                for ls in linescores:
                    period = ls.get("period")
                    value = ls.get("value")
                    if period and 1 <= period <= 4 and isinstance(value, (int, float)) and value > 0:
                        rounds[period - 1] = float(value)
                        total_strokes += float(value)

                score = competitor.get("score", "")
                position = (
                    (status.get("position") or {}).get("displayName")
                    if isinstance(status, dict) else None
                ) or str(competitor.get("order", "") or "")

                entry = {
                    "name": athlete.get("displayName", ""),
                    "position": position,
                    "score": score,
                    "totalStrokes": total_strokes,
                    "round1": rounds[0],
                    "round2": rounds[1],
                    "round3": rounds[2],
                    "round4": rounds[3],
                    "thru": (status.get("thru", "") if isinstance(status, dict) else ""),
                }
                leaderboard.append(entry)

        # ESPN returns leaderboard in finish order already — preserve it
        # (earliest rounds list the leader first via `order`).

        # Apply T-tie labels: players sharing the same numeric score get "T<pos>"
        # where <pos> is the position of the first player in the tie group.
        # Example: 13 players at -7 labeled T25 instead of 25..37 sequentially.
        leaderboard = _annotate_tie_positions(leaderboard)

        event_info["leaderboard"] = leaderboard
        print(f"  Found event: {event_info['name']} with {len(leaderboard)} players")
        return event_info

    except (KeyError, TypeError) as e:
        print(f"  Error parsing ESPN data: {e}")
        return None


# ============================================================
# BALLDONTLIE PGA API — Premium data source (GOAT tier)
# ============================================================

def bdl_fetch(endpoint, params=None):
    """Fetch from BallDontLie PGA API with auth header."""
    if not BDL_API_KEY:
        return None
    url = f"{BDL_BASE}/{endpoint}"
    if params:
        url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
    headers = {"User-Agent": USER_AGENT}
    try:
        req = Request(url, headers=headers)
        req.add_header("Authorization", BDL_API_KEY)
        resp = urlopen(req, timeout=TIMEOUT)
        data = json.loads(resp.read().decode("utf-8", errors="replace"))
        time.sleep(0.5)  # Rate limiting
        return data
    except (URLError, HTTPError) as e:
        print(f"  [WARN] BDL API error for {endpoint}: {e}")
        return None


def bdl_fetch_all(endpoint, params=None, max_pages=10):
    """Fetch all pages from a paginated BDL endpoint."""
    all_data = []
    cursor = None
    for _ in range(max_pages):
        p = dict(params or {})
        p["per_page"] = "100"
        if cursor:
            p["cursor"] = str(cursor)
        result = bdl_fetch(endpoint, p)
        if not result or not result.get("data"):
            break
        all_data.extend(result["data"])
        cursor = result.get("meta", {}).get("next_cursor")
        if not cursor:
            break
    return all_data


def bdl_get_current_tournament():
    """Find the current/next tournament from BDL."""
    print("\n[BDL] Finding current tournament...")
    season = datetime.utcnow().year
    data = bdl_fetch("tournaments", {"season": str(season), "per_page": "50"})
    if not data:
        # Retry with previous season for early-January weeks
        data = bdl_fetch("tournaments", {"season": str(season - 1), "per_page": "50"})
    # Cache the raw tournaments response on the module so run_pipeline can use it
    # to emit a dynamic majors schedule without a second API call.
    global _LAST_TOURNAMENTS_RAW
    _LAST_TOURNAMENTS_RAW = data
    if not data:
        return None

    # Find IN_PROGRESS first, then NOT_STARTED (next up)
    tournaments = data.get("data", [])
    for t in tournaments:
        if t.get("status") == "IN_PROGRESS":
            print(f"  Found live tournament: {t['name']} (id={t['id']})")
            return t
    for t in tournaments:
        if t.get("status") == "NOT_STARTED":
            print(f"  Found upcoming tournament: {t['name']} (id={t['id']})")
            return t
    return None


def bdl_get_tournament_field(tournament_id):
    """Get the full field for a tournament."""
    print(f"[BDL] Fetching tournament field (id={tournament_id})...")
    entries = bdl_fetch_all("tournament_field", {"tournament_id": str(tournament_id)})
    print(f"  Got {len(entries)} players in field")
    return entries


EMPTY_FUTURES = {"winner": {}, "top5": {}, "top10": {}, "top20": {}, "makeCut": {}, "r1Leader": {}}


def _classify_futures_market(market_type):
    """Map BDL `market_type` strings to our internal market keys.

    Substring-based so we tolerate variants like top_5_finish / top_5 / top5.
    Order matters — check the more specific tokens before less specific ones
    (top_20 before top_2, top_10 before top_1).
    Returns None for unknown markets so we can log them.
    """
    if not market_type:
        return None
    m = str(market_type).lower()
    if m == "tournament_winner" or "outright" in m:
        return "winner"
    if "top_20" in m or "top20" in m or "top 20" in m:
        return "top20"
    if "top_10" in m or "top10" in m or "top 10" in m:
        return "top10"
    if "top_5" in m or "top5" in m or "top 5" in m:
        return "top5"
    if "make_cut" in m or "makecut" in m or "make cut" in m:
        return "makeCut"
    if (
        "first_round_leader" in m
        or "round_1_leader" in m
        or "r1_leader" in m
        or "first round leader" in m
    ):
        return "r1Leader"
    return None


def bdl_get_futures_odds(tournament_id, event_status=None):
    """Get all futures markets from BDL: winner + top 5/10/20 + make cut + R1 leader.

    Returns a market-keyed dict — each value is `{player_name: {vendor: odds_str}}`:

        {
          "winner":   {"Scottie Scheffler": {"dk": "+800", "fd": "+850", ...}, ...},
          "top5":     {...},
          "top10":    {...},
          "top20":    {...},
          "makeCut":  {...},
          "r1Leader": {...},
        }

    Phantom "market closed" odds are stripped — books park eliminated /
    non-contender players at inflated values instead of removing the market.

    Threshold scales with event status:
      - IN_PROGRESS: |american| >= 8000 (8x stricter than idle).
        During live play, anything 10000+ is almost always a closed-market
        placeholder, not a bettable longshot. +5000 stays in.
      - NOT_STARTED / unknown: |american| >= 50000 (idle threshold).
        Some books legitimately open longshots at +20000-30000 pre-tournament.
      - COMPLETED / other: idle threshold.

    Previous version filtered to `tournament_winner` only, which dropped every
    placement market on the floor. That broke top5/top10/top20/makeCut Discord
    alerts because their consumer reads from output["propsByType"][market],
    which had nothing in it.
    """
    print(f"[BDL] Fetching futures odds (tournament_id={tournament_id})...")
    odds = bdl_fetch_all("futures", {"tournament_ids[]": str(tournament_id)})
    out = {k: {} for k in EMPTY_FUTURES}
    if not odds:
        return out

    # Tighten phantom threshold while play is live; books park closed markets
    # at +10000+ during in-progress events, but real longshots stay below +8000.
    if str(event_status or "").upper() == "IN_PROGRESS":
        PHANTOM_ODDS_THRESHOLD = 8000
    elif str(event_status or "").upper() == "NOT_STARTED":
        # Pre-tournament: books open longshots at +15000-+25000 (legitimate),
        # but anything >+25000 is almost certainly a "we haven't priced this
        # player" placeholder. PGA Championship feed showed DJ +21000, Smith
        # +29000, Mickelson +30000 — all garbage.
        PHANTOM_ODDS_THRESHOLD = 25000
    else:
        PHANTOM_ODDS_THRESHOLD = 50000  # completed / unknown
    skipped_phantom = 0
    unknown_markets = {}
    vendor_short = {"fanduel": "fd", "draftkings": "dk", "betmgm": "mgm",
                    "caesars": "czr", "pointsbet": "pb", "bet365": "365"}

    for o in odds:
        market = _classify_futures_market(o.get("market_type"))
        if market is None:
            mt = o.get("market_type") or "<empty>"
            unknown_markets[mt] = unknown_markets.get(mt, 0) + 1
            continue

        player = o.get("player", {})
        name = player.get("display_name", "")
        vendor = o.get("vendor", "")
        american = o.get("american_odds", 0)
        if not name:
            continue
        try:
            if abs(int(american)) >= PHANTOM_ODDS_THRESHOLD:
                skipped_phantom += 1
                continue
        except (ValueError, TypeError):
            continue

        short = vendor_short.get(vendor, vendor[:3])
        bucket = out[market]
        if name not in bucket:
            bucket[name] = {}
        bucket[name][short] = f"+{american}" if american > 0 else str(american)

    parts = [f"{k}={len(v)}" for k, v in out.items() if v]
    summary = ", ".join(parts) if parts else "no markets parsed"
    suffix = (
        f" ({skipped_phantom} phantom/closed-market entries skipped, "
        f"threshold=|{PHANTOM_ODDS_THRESHOLD}|)"
    ) if skipped_phantom else ""
    if unknown_markets:
        top_unknown = sorted(unknown_markets.items(), key=lambda kv: -kv[1])[:3]
        suffix += " (ignored market_types: " + ", ".join(
            f"{k}×{v}" for k, v in top_unknown
        ) + ")"
    print(f"  {summary}{suffix}")
    return out


def bdl_get_player_props(tournament_id):
    """Get player props (birdies, bogeys, round scores) from BDL."""
    print(f"[BDL] Fetching player props (tournament_id={tournament_id})...")
    data = bdl_fetch("odds/player_props", {"tournament_id": str(tournament_id)})
    if not data:
        return {}

    # Group by player
    props_map = {}
    for prop in data.get("data", []):
        player = prop.get("player", {})
        name = player.get("display_name", "")
        if not name:
            continue
        if name not in props_map:
            props_map[name] = []
        props_map[name].append({
            "type": prop.get("prop_type", ""),
            "line": prop.get("line"),
            "over_odds": prop.get("over_odds"),
            "under_odds": prop.get("under_odds"),
            "vendor": prop.get("vendor", ""),
        })

    print(f"  Got props for {len(props_map)} players")
    return props_map


def bdl_get_tournament_results(tournament_id):
    """Get leaderboard / results for a tournament."""
    print(f"[BDL] Fetching tournament results (id={tournament_id})...")
    results = bdl_fetch_all("tournament_results", {"tournament_ids[]": str(tournament_id)})
    print(f"  Got {len(results)} result entries")
    return results


def bdl_get_player_round_stats(tournament_id):
    """Get per-round SG stats for all players in a tournament."""
    print(f"[BDL] Fetching player round stats (tournament_id={tournament_id})...")
    stats = bdl_fetch_all("player_round_stats", {"tournament_ids[]": str(tournament_id)})
    print(f"  Got {len(stats)} round stat entries")
    return stats


def bdl_get_course_holes(course_id):
    """Get hole-by-hole data for a course."""
    print(f"[BDL] Fetching course holes (course_id={course_id})...")
    holes = bdl_fetch_all("course_holes", {"course_ids[]": str(course_id)})
    print(f"  Got {len(holes)} holes")
    return holes


def bdl_get_player_round_results(tournament_id, max_pages=20):
    """Per-player per-round par-relative scores for a tournament.
    Returns rows: {tournament, player, round_number, score, par_relative_score}
    """
    print(f"[BDL] Fetching player round results (tournament_id={tournament_id})...")
    rows = bdl_fetch_all("player_round_results", {"tournament_ids[]": str(tournament_id)}, max_pages=max_pages)
    print(f"  Got {len(rows)} round result entries")
    return rows


def bdl_build_player_round_results(raw_rows, keep_names=None):
    """Reshape to { normalized_name: [{round, score, parRelative}] }."""
    if not raw_rows:
        return {}
    out = {}
    for row in raw_rows:
        player = row.get("player") or {}
        pname = player.get("display_name") or f"{player.get('first_name','')} {player.get('last_name','')}".strip()
        if not pname:
            continue
        key = normalize_name(pname)
        if keep_names is not None and key not in keep_names:
            continue
        rnum = row.get("round_number")
        score = row.get("score")
        par_rel = row.get("par_relative_score")
        if rnum is None or score is None:
            continue
        entry = out.setdefault(key, [])
        entry.append({
            "round": int(rnum),
            "score": int(score),
            "parRelative": int(par_rel) if par_rel is not None else None,
        })
    for pkey in out:
        out[pkey].sort(key=lambda r: r["round"] or 99)
    return out


# Season stats — BDL stat_name string → our internal key. Substring matched.
SEASON_STAT_KEY_MAP = [
    ("sgTotal", ["sg total", "strokes gained total", "sg: total"]),
    ("sgOtt",   ["sg off the tee", "sg: off-the-tee", "strokes gained off"]),
    ("sgApp",   ["sg approach", "sg: approach", "strokes gained approach"]),
    ("sgArg",   ["sg around the green", "sg: around-the-green", "strokes gained around"]),
    ("sgPutt",  ["sg putting", "sg: putting", "strokes gained putting"]),
    ("scoringAvg", ["scoring average", "scoring avg"]),
    ("drivingDistance", ["driving distance"]),
    ("drivingAccuracy", ["driving accuracy"]),
    ("gir",     ["greens in regulation"]),
    ("birdieAvg", ["birdie average", "birdies per round"]),
]


def bdl_get_player_season_stats(season, max_pages=20):
    """Season stats with rankings for every player who has any. Powers the
    'ranked #5 in SG Approach this season' authority badges in Course Fit.
    """
    print(f"[BDL] Fetching player season stats (season={season})...")
    rows = bdl_fetch_all("player_season_stats", {"season": str(season), "per_page": "100"}, max_pages=max_pages)
    print(f"  Got {len(rows)} season stat entries")
    return rows


def bdl_build_season_ranks(raw_rows, keep_names=None):
    """Build { normalized_name: { sgTotal: rank, sgApp: rank, ... } }"""
    if not raw_rows:
        return {}
    out = {}
    for row in raw_rows:
        player = row.get("player") or {}
        pname = player.get("display_name") or f"{player.get('first_name','')} {player.get('last_name','')}".strip()
        if not pname:
            continue
        key = normalize_name(pname)
        if keep_names is not None and key not in keep_names:
            continue
        stat_name = (row.get("stat_name") or "").lower().strip()
        rank = row.get("rank")
        if not stat_name or rank is None:
            continue
        our_key = None
        for ours, variants in SEASON_STAT_KEY_MAP:
            if any(v in stat_name for v in variants):
                our_key = ours
                break
        if not our_key:
            continue
        out.setdefault(key, {})[our_key] = int(rank)
    return out


def bdl_get_player_scorecards(tournament_id, max_pages=120):
    """Fetch per-player per-hole scorecards for a tournament.

    Returns raw BDL rows: {tournament, player, course, round_number, hole_number, par, score}
    Volume is high — ~150 players × 4 rounds × 18 holes = ~10,800 rows per
    tournament — so we cap pages aggressively and let callers filter to the
    top N players downstream.
    """
    print(f"[BDL] Fetching player scorecards (tournament_id={tournament_id})...")
    rows = bdl_fetch_all(
        "player_scorecards",
        {"tournament_ids[]": str(tournament_id)},
        max_pages=max_pages,
    )
    print(f"  Got {len(rows)} scorecard rows")
    return rows


def bdl_build_player_scorecards(raw_rows, keep_names=None):
    """Reshape raw scorecard rows into a frontend-friendly nested dict.

    Output: { normalized_name: { round_number: [{hole, par, score, toPar}, ...] } }

    Only retains players in ``keep_names`` (set of normalized names) when
    provided — keeps the output JSON manageable. We typically pass the
    leaderboard + top model picks (~50 players).
    """
    if not raw_rows:
        return {}
    out = {}
    for row in raw_rows:
        player = row.get("player") or {}
        pname = player.get("display_name") or f"{player.get('first_name','')} {player.get('last_name','')}".strip()
        if not pname:
            continue
        key = normalize_name(pname)
        if keep_names is not None and key not in keep_names:
            continue
        rnum = row.get("round_number")
        hole = row.get("hole_number")
        par = row.get("par")
        score = row.get("score")
        if rnum is None or hole is None or par is None or score is None:
            continue
        if score == 0:  # ESPN/BDL uses 0 for "not played yet"
            continue
        try:
            par_i = int(par)
            score_i = int(score)
        except (TypeError, ValueError):
            continue
        to_par = score_i - par_i
        p_entry = out.setdefault(key, {})
        r_entry = p_entry.setdefault(str(rnum), [])
        r_entry.append({"hole": int(hole), "par": par_i, "score": score_i, "toPar": to_par})
    # Sort each round's holes ascending
    for pkey in out:
        for rkey in out[pkey]:
            out[pkey][rkey].sort(key=lambda h: h["hole"])
    return out


# ============================================================
# FALLBACK PLAYER DATA
# ============================================================
# If scraping fails, we use this curated dataset based on real stats.
# Updated manually as a safety net.

def _load_fallback_overrides():
    """Load self-healed fallback stats from fallback_dynamic.json.

    The scraper writes fresh DataGolf/PGA-Tour/BDL values back to this file
    every run, keyed by normalized player name. Next run picks them up —
    so the fallback tier automatically reflects whatever live data we
    last successfully fetched, even if both DataGolf and PGA Tour sites
    are down today.
    """
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fallback_dynamic.json")
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f) or {}
    except (OSError, json.JSONDecodeError):
        return {}


def _save_fallback_overrides(overrides):
    """Persist self-healed fallback stats. No-op if overrides dict is empty."""
    if not overrides:
        return
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fallback_dynamic.json")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(overrides, f, indent=2, sort_keys=True, ensure_ascii=False)
    except OSError as e:
        print(f"  [WARN] Could not write fallback_dynamic.json: {e}")


# Dynamic fields that self-heal — refreshed from live sources every run,
# written back to fallback_dynamic.json so stale hardcoded values don't
# persist when primary sources are healthy.
_FALLBACK_DYNAMIC_FIELDS = (
    "sgTotal", "sgOtt", "sgApp", "sgArg", "sgPutt",
    "birdieAvg", "bogeyAvg", "scoringAvg", "gir", "fairways", "scramble",
    "proxAvg", "rank",
)


def get_fallback_players():
    """Return player data based on real Tour stats.
    Static metadata (courseFit, notes, missDir, flight, augustaHistory, liv
    flag) stays hardcoded here. Dynamic stats (SG splits, birdie/bogey
    averages, rank) self-heal via fallback_dynamic.json — the scraper writes
    every run's fresh values back, so next run's fallback is as recent as
    our last successful scrape.
    """
    static_players = _fallback_static()
    overrides = _load_fallback_overrides()
    if not overrides:
        return static_players
    # Merge: override dynamic fields when we have fresher values
    merged = []
    for p in static_players:
        key = p["name"].lower()
        fresh = overrides.get(key) or {}
        if fresh:
            merged_player = dict(p)
            for field in _FALLBACK_DYNAMIC_FIELDS:
                if field in fresh:
                    merged_player[field] = fresh[field]
            merged.append(merged_player)
        else:
            merged.append(p)
    return merged


def _fallback_static():
    """Hardcoded static seed. Treated as the baseline — dynamic fields get
    overridden by fallback_dynamic.json when available."""
    return [
        {"id":1,"name":"Scottie Scheffler","rank":1,"sgTotal":2.45,"sgOtt":0.62,"sgApp":1.18,"sgArg":0.35,"sgPutt":0.30,"birdieAvg":5.1,"bogeyAvg":2.2,"scoringAvg":68.8,"gir":72.5,"fairways":63.2,"scramble":65.0,"proxAvg":29.5,"missDir":"right","flight":"high_fade","courseFit":{"augusta":95,"tpc_sawgrass":82,"pebble":80,"torrey_south":85,"riviera":88,"valhalla":92,"pinehurst_2":88,"royal_troon":75,"quail_hollow":85,"east_lake":90,"bay_hill":82,"harbour_town":72,"colonial":78,"memorial":88,"tpc_scottsdale":80},"notes":"Elite ball-striker. Best SG:Approach on Tour. Premium plays: strokes under, birdies over."},
        {"id":2,"name":"Xander Schauffele","rank":2,"sgTotal":2.10,"sgOtt":0.55,"sgApp":0.80,"sgArg":0.40,"sgPutt":0.35,"birdieAvg":4.8,"bogeyAvg":2.1,"scoringAvg":69.1,"gir":70.8,"fairways":66.1,"scramble":63.5,"proxAvg":31.2,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":88,"tpc_sawgrass":85,"pebble":82,"torrey_south":90,"riviera":84,"valhalla":85,"pinehurst_2":82,"royal_troon":80,"quail_hollow":82,"east_lake":88,"bay_hill":80,"harbour_town":78,"colonial":80,"memorial":82,"tpc_scottsdale":82},"notes":"Most well-rounded player on Tour. No weakness. Excels at Torrey Pines."},
        {"id":3,"name":"Rory McIlroy","rank":3,"sgTotal":1.95,"sgOtt":0.85,"sgApp":0.72,"sgArg":0.18,"sgPutt":0.20,"birdieAvg":5.0,"bogeyAvg":2.5,"scoringAvg":69.2,"gir":69.5,"fairways":58.5,"scramble":58.0,"proxAvg":32.8,"missDir":"left","flight":"high_draw","courseFit":{"augusta":82,"tpc_sawgrass":80,"pebble":78,"torrey_south":75,"riviera":80,"valhalla":90,"pinehurst_2":82,"royal_troon":88,"quail_hollow":92,"east_lake":85,"bay_hill":82,"harbour_town":68,"colonial":72,"memorial":80,"tpc_scottsdale":78},"notes":"Best driver on Tour. High birdie ceiling but volatile. Great for matchups."},
        {"id":4,"name":"Collin Morikawa","rank":4,"sgTotal":1.75,"sgOtt":0.30,"sgApp":1.05,"sgArg":0.22,"sgPutt":0.18,"birdieAvg":4.5,"bogeyAvg":2.0,"scoringAvg":69.4,"gir":71.2,"fairways":70.5,"scramble":60.5,"proxAvg":30.5,"missDir":"right","flight":"low_fade","courseFit":{"augusta":70,"tpc_sawgrass":88,"pebble":85,"torrey_south":78,"riviera":92,"valhalla":80,"pinehurst_2":85,"royal_troon":90,"quail_hollow":78,"east_lake":82,"bay_hill":80,"harbour_town":88,"colonial":90,"memorial":85,"tpc_scottsdale":78},"notes":"Second-best iron player. Low ball flight suits firm/windy. Premium bogey under play."},
        {"id":5,"name":"Ludvig Aberg","rank":5,"sgTotal":1.65,"sgOtt":0.70,"sgApp":0.68,"sgArg":0.15,"sgPutt":0.12,"birdieAvg":4.9,"bogeyAvg":2.4,"scoringAvg":69.5,"gir":68.5,"fairways":61.0,"scramble":57.5,"proxAvg":33.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":85,"tpc_sawgrass":78,"pebble":72,"torrey_south":80,"riviera":82,"valhalla":85,"pinehurst_2":78,"royal_troon":72,"quail_hollow":82,"east_lake":80,"bay_hill":78,"harbour_town":68,"colonial":72,"memorial":80,"tpc_scottsdale":78},"notes":"Young star with elite power. Short game still developing. High upside."},
        {"id":6,"name":"Patrick Cantlay","rank":6,"sgTotal":1.55,"sgOtt":0.32,"sgApp":0.55,"sgArg":0.28,"sgPutt":0.40,"birdieAvg":4.2,"bogeyAvg":1.8,"scoringAvg":69.6,"gir":69.0,"fairways":68.8,"scramble":66.5,"proxAvg":32.0,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":72,"tpc_sawgrass":82,"pebble":80,"torrey_south":78,"riviera":90,"valhalla":75,"pinehurst_2":80,"royal_troon":75,"quail_hollow":78,"east_lake":85,"bay_hill":78,"harbour_town":82,"colonial":85,"memorial":82,"tpc_scottsdale":80},"notes":"Elite putter. Lowest bogey rate. Best bogey under play on Tour. Riviera specialist."},
        {"id":7,"name":"Wyndham Clark","rank":7,"sgTotal":1.42,"sgOtt":0.60,"sgApp":0.50,"sgArg":0.20,"sgPutt":0.12,"birdieAvg":4.6,"bogeyAvg":2.5,"scoringAvg":69.8,"gir":67.5,"fairways":59.5,"scramble":59.0,"proxAvg":33.5,"missDir":"right","flight":"high_fade","courseFit":{"augusta":75,"tpc_sawgrass":72,"pebble":82,"torrey_south":80,"riviera":75,"valhalla":78,"pinehurst_2":72,"royal_troon":68,"quail_hollow":78,"east_lake":75,"bay_hill":72,"harbour_town":65,"colonial":68,"memorial":75,"tpc_scottsdale":78},"notes":"Powerful driver. Higher bogey rate offsets birdies. Volatile for props."},
        {"id":8,"name":"Viktor Hovland","rank":8,"sgTotal":1.35,"sgOtt":0.48,"sgApp":0.65,"sgArg":-0.05,"sgPutt":0.27,"birdieAvg":4.7,"bogeyAvg":2.6,"scoringAvg":69.9,"gir":69.8,"fairways":62.0,"scramble":52.0,"proxAvg":31.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":65,"tpc_sawgrass":75,"pebble":68,"torrey_south":72,"riviera":78,"valhalla":80,"pinehurst_2":68,"royal_troon":72,"quail_hollow":78,"east_lake":80,"bay_hill":75,"harbour_town":65,"colonial":70,"memorial":78,"tpc_scottsdale":75},"notes":"Great ball-striker but short game liability. Negative SG:ARG. Risky bogey unders."},
        {"id":9,"name":"Tommy Fleetwood","rank":9,"sgTotal":1.25,"sgOtt":0.35,"sgApp":0.52,"sgArg":0.20,"sgPutt":0.18,"birdieAvg":4.3,"bogeyAvg":2.1,"scoringAvg":70.0,"gir":68.2,"fairways":67.0,"scramble":62.0,"proxAvg":33.8,"missDir":"left","flight":"low_draw","courseFit":{"augusta":68,"tpc_sawgrass":80,"pebble":85,"torrey_south":75,"riviera":80,"valhalla":72,"pinehurst_2":82,"royal_troon":92,"quail_hollow":72,"east_lake":75,"bay_hill":75,"harbour_town":85,"colonial":82,"memorial":75,"tpc_scottsdale":72},"notes":"Links-style player. Low ball flight suits windy conditions. Good bogey under candidate."},
        {"id":10,"name":"Sahith Theegala","rank":10,"sgTotal":1.18,"sgOtt":0.52,"sgApp":0.42,"sgArg":0.10,"sgPutt":0.14,"birdieAvg":4.8,"bogeyAvg":2.7,"scoringAvg":70.1,"gir":66.5,"fairways":57.0,"scramble":56.5,"proxAvg":34.2,"missDir":"left","flight":"high_draw","courseFit":{"augusta":70,"tpc_sawgrass":72,"pebble":72,"torrey_south":68,"riviera":82,"valhalla":75,"pinehurst_2":68,"royal_troon":65,"quail_hollow":75,"east_lake":72,"bay_hill":70,"harbour_town":62,"colonial":68,"memorial":72,"tpc_scottsdale":78},"notes":"High birdie upside but high bogey rate. Very aggressive. Best for birdie overs."},
        {"id":11,"name":"Hideki Matsuyama","rank":11,"sgTotal":1.12,"sgOtt":0.38,"sgApp":0.52,"sgArg":0.12,"sgPutt":0.10,"birdieAvg":4.4,"bogeyAvg":2.2,"scoringAvg":70.0,"gir":69.0,"fairways":64.0,"scramble":60.0,"proxAvg":32.5,"missDir":"left","flight":"high_draw","courseFit":{"augusta":92,"tpc_sawgrass":78,"pebble":75,"torrey_south":78,"riviera":80,"valhalla":78,"pinehurst_2":75,"royal_troon":72,"quail_hollow":78,"east_lake":82,"bay_hill":78,"harbour_town":72,"colonial":75,"memorial":82,"tpc_scottsdale":85},"notes":"Masters champion. Elite approach play with high ball flight. Streaky putter — hot weeks can win anywhere."},
        {"id":12,"name":"Max Homa","rank":12,"sgTotal":1.05,"sgOtt":0.42,"sgApp":0.38,"sgArg":0.15,"sgPutt":0.10,"birdieAvg":4.2,"bogeyAvg":2.3,"scoringAvg":70.3,"gir":67.0,"fairways":64.5,"scramble":61.0,"proxAvg":34.5,"missDir":"left","flight":"high_draw","courseFit":{"augusta":65,"tpc_sawgrass":70,"pebble":78,"torrey_south":82,"riviera":88,"valhalla":68,"pinehurst_2":65,"royal_troon":62,"quail_hollow":72,"east_lake":70,"bay_hill":68,"harbour_town":72,"colonial":75,"memorial":72,"tpc_scottsdale":72},"augustaHistory":{"appearances":4,"bestFinish":12,"cuts":3,"top10":0,"avgScore":72.3},"notes":"Solid all-around. Riviera and Torrey specialist. Reliable at home courses."},
        {"id":13,"name":"Shane Lowry","rank":13,"sgTotal":1.02,"sgOtt":0.30,"sgApp":0.42,"sgArg":0.18,"sgPutt":0.12,"birdieAvg":4.1,"bogeyAvg":2.1,"scoringAvg":70.2,"gir":67.5,"fairways":65.5,"scramble":63.0,"proxAvg":34.0,"missDir":"left","flight":"low_draw","courseFit":{"augusta":68,"tpc_sawgrass":78,"pebble":82,"torrey_south":72,"riviera":75,"valhalla":82,"pinehurst_2":80,"royal_troon":95,"quail_hollow":72,"east_lake":72,"bay_hill":72,"harbour_town":82,"colonial":78,"memorial":72,"tpc_scottsdale":68},"augustaHistory":{"appearances":6,"bestFinish":4,"cuts":5,"top10":2,"avgScore":71.2},"notes":"Links specialist. Open champion. Low ball flight dominates in wind. Best in coastal/windy conditions."},
        {"id":14,"name":"Sungjae Im","rank":14,"sgTotal":0.95,"sgOtt":0.28,"sgApp":0.40,"sgArg":0.15,"sgPutt":0.12,"birdieAvg":4.0,"bogeyAvg":2.0,"scoringAvg":70.4,"gir":68.5,"fairways":69.0,"scramble":61.5,"proxAvg":35.0,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":72,"tpc_sawgrass":75,"pebble":70,"torrey_south":70,"riviera":72,"valhalla":72,"pinehurst_2":70,"royal_troon":68,"quail_hollow":75,"east_lake":72,"bay_hill":72,"harbour_town":75,"colonial":78,"memorial":75,"tpc_scottsdale":75},"notes":"Iron man — plays every week. Rarely misses cuts. Great for MC and top 20 props."},
        {"id":15,"name":"Sam Burns","rank":15,"sgTotal":0.92,"sgOtt":0.45,"sgApp":0.35,"sgArg":0.08,"sgPutt":0.04,"birdieAvg":4.5,"bogeyAvg":2.4,"scoringAvg":70.3,"gir":67.0,"fairways":60.0,"scramble":58.5,"proxAvg":34.5,"missDir":"right","flight":"high_fade","courseFit":{"augusta":72,"tpc_sawgrass":78,"pebble":70,"torrey_south":75,"riviera":72,"valhalla":78,"pinehurst_2":72,"royal_troon":65,"quail_hollow":78,"east_lake":78,"bay_hill":75,"harbour_town":72,"colonial":75,"memorial":78,"tpc_scottsdale":82},"augustaHistory":{"appearances":4,"bestFinish":8,"cuts":3,"top10":1,"avgScore":71.8},"notes":"Talented ball-striker with streaky putting. When putter is hot, can contend anywhere."},
        {"id":16,"name":"Tony Finau","rank":16,"sgTotal":0.90,"sgOtt":0.55,"sgApp":0.30,"sgArg":0.05,"sgPutt":0.00,"birdieAvg":4.4,"bogeyAvg":2.3,"scoringAvg":70.3,"gir":67.5,"fairways":60.5,"scramble":58.0,"proxAvg":34.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":72,"tpc_sawgrass":70,"pebble":72,"torrey_south":78,"riviera":72,"valhalla":80,"pinehurst_2":72,"royal_troon":68,"quail_hollow":78,"east_lake":75,"bay_hill":75,"harbour_town":65,"colonial":68,"memorial":75,"tpc_scottsdale":82},"notes":"Elite power but inconsistent approach and flat-stick. Best at bombers courses. Fade candidate at short tracks."},
        {"id":17,"name":"Keegan Bradley","rank":17,"sgTotal":0.88,"sgOtt":0.35,"sgApp":0.32,"sgArg":0.12,"sgPutt":0.09,"birdieAvg":4.1,"bogeyAvg":2.2,"scoringAvg":70.4,"gir":67.5,"fairways":65.0,"scramble":60.5,"proxAvg":34.2,"missDir":"left","flight":"high_draw","courseFit":{"augusta":70,"tpc_sawgrass":72,"pebble":68,"torrey_south":72,"riviera":72,"valhalla":75,"pinehurst_2":72,"royal_troon":68,"quail_hollow":75,"east_lake":72,"bay_hill":72,"harbour_town":75,"colonial":78,"memorial":75,"tpc_scottsdale":72},"augustaHistory":{"appearances":10,"bestFinish":14,"cuts":7,"top10":0,"avgScore":72.5},"notes":"Steady veteran. Consistent without being flashy. Good MC and top-20 candidate."},
        {"id":18,"name":"Justin Thomas","rank":18,"sgTotal":0.85,"sgOtt":0.48,"sgApp":0.42,"sgArg":0.05,"sgPutt":-0.10,"birdieAvg":4.6,"bogeyAvg":2.6,"scoringAvg":70.5,"gir":68.0,"fairways":60.0,"scramble":56.0,"proxAvg":33.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":80,"tpc_sawgrass":78,"pebble":72,"torrey_south":75,"riviera":78,"valhalla":88,"pinehurst_2":78,"royal_troon":72,"quail_hollow":85,"east_lake":82,"bay_hill":78,"harbour_town":68,"colonial":72,"memorial":82,"tpc_scottsdale":80},"notes":"Former world #1 in a slump. Ball-striking still elite but putter has gone cold. High ceiling, low floor."},
        {"id":19,"name":"Jason Day","rank":19,"sgTotal":0.82,"sgOtt":0.40,"sgApp":0.28,"sgArg":0.08,"sgPutt":0.06,"birdieAvg":4.2,"bogeyAvg":2.3,"scoringAvg":70.5,"gir":66.5,"fairways":62.0,"scramble":60.0,"proxAvg":34.5,"missDir":"right","flight":"high_fade","courseFit":{"augusta":80,"tpc_sawgrass":82,"pebble":72,"torrey_south":78,"riviera":72,"valhalla":78,"pinehurst_2":72,"royal_troon":68,"quail_hollow":82,"east_lake":78,"bay_hill":85,"harbour_town":72,"colonial":72,"memorial":78,"tpc_scottsdale":78},"augustaHistory":{"appearances":14,"bestFinish":2,"cuts":12,"top10":5,"avgScore":71.2},"notes":"Resurgent veteran. Short game wizard when healthy. Bay Hill specialist. Good at TPC Sawgrass."},
        {"id":20,"name":"Russell Henley","rank":20,"sgTotal":0.80,"sgOtt":0.22,"sgApp":0.38,"sgArg":0.12,"sgPutt":0.08,"birdieAvg":3.9,"bogeyAvg":2.0,"scoringAvg":70.4,"gir":68.0,"fairways":68.5,"scramble":62.0,"proxAvg":34.0,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":68,"tpc_sawgrass":78,"pebble":75,"torrey_south":72,"riviera":75,"valhalla":72,"pinehurst_2":78,"royal_troon":75,"quail_hollow":72,"east_lake":75,"bay_hill":72,"harbour_town":82,"colonial":85,"memorial":75,"tpc_scottsdale":72},"augustaHistory":{"appearances":8,"bestFinish":5,"cuts":7,"top10":2,"avgScore":71.0},"notes":"Extremely accurate. Low bogey rate. Great at precision courses like Harbour Town and Colonial."},
        {"id":21,"name":"Brian Harman","rank":21,"sgTotal":0.78,"sgOtt":0.15,"sgApp":0.30,"sgArg":0.20,"sgPutt":0.13,"birdieAvg":3.8,"bogeyAvg":1.9,"scoringAvg":70.5,"gir":66.5,"fairways":70.5,"scramble":65.0,"proxAvg":35.5,"missDir":"left","flight":"low_draw","courseFit":{"augusta":62,"tpc_sawgrass":75,"pebble":80,"torrey_south":68,"riviera":72,"valhalla":68,"pinehurst_2":78,"royal_troon":90,"quail_hollow":68,"east_lake":72,"bay_hill":68,"harbour_town":88,"colonial":85,"memorial":72,"tpc_scottsdale":68},"notes":"Open champion. Lefty with great short game. Not a power player. Thrives on accuracy courses and links."},
        {"id":22,"name":"Cameron Young","rank":22,"sgTotal":0.75,"sgOtt":0.65,"sgApp":0.18,"sgArg":-0.02,"sgPutt":-0.06,"birdieAvg":4.5,"bogeyAvg":2.6,"scoringAvg":70.6,"gir":66.0,"fairways":56.0,"scramble":55.0,"proxAvg":34.8,"missDir":"left","flight":"high_draw","courseFit":{"augusta":72,"tpc_sawgrass":68,"pebble":65,"torrey_south":75,"riviera":72,"valhalla":78,"pinehurst_2":68,"royal_troon":65,"quail_hollow":78,"east_lake":72,"bay_hill":72,"harbour_town":58,"colonial":62,"memorial":72,"tpc_scottsdale":75},"notes":"Huge power but accuracy issues. Multiple runner-up finishes. Volatile scorer — birdie overs at long courses."},
        {"id":23,"name":"Denny McCarthy","rank":23,"sgTotal":0.72,"sgOtt":0.05,"sgApp":0.15,"sgArg":0.18,"sgPutt":0.34,"birdieAvg":3.6,"bogeyAvg":1.8,"scoringAvg":70.6,"gir":65.0,"fairways":70.0,"scramble":68.0,"proxAvg":36.0,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":58,"tpc_sawgrass":72,"pebble":75,"torrey_south":65,"riviera":72,"valhalla":62,"pinehurst_2":75,"royal_troon":72,"quail_hollow":65,"east_lake":68,"bay_hill":65,"harbour_town":85,"colonial":88,"memorial":70,"tpc_scottsdale":72},"notes":"Best putter on Tour. Short off the tee. Needs accuracy courses where length doesn't matter. Elite bogey under."},
        {"id":24,"name":"Byeong Hun An","rank":24,"sgTotal":0.70,"sgOtt":0.32,"sgApp":0.28,"sgArg":0.06,"sgPutt":0.04,"birdieAvg":4.0,"bogeyAvg":2.2,"scoringAvg":70.6,"gir":67.0,"fairways":64.0,"scramble":59.0,"proxAvg":34.5,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":68,"tpc_sawgrass":72,"pebble":70,"torrey_south":72,"riviera":72,"valhalla":72,"pinehurst_2":70,"royal_troon":68,"quail_hollow":72,"east_lake":72,"bay_hill":72,"harbour_town":72,"colonial":75,"memorial":72,"tpc_scottsdale":72},"augustaHistory":{"appearances":5,"bestFinish":12,"cuts":4,"top10":0,"avgScore":72.0},"notes":"Consistent ball-striker. No elite category but no major weakness. Reliable for top-20 finishes."},
        {"id":25,"name":"Adam Scott","rank":25,"sgTotal":0.68,"sgOtt":0.35,"sgApp":0.25,"sgArg":0.05,"sgPutt":0.03,"birdieAvg":4.0,"bogeyAvg":2.2,"scoringAvg":70.6,"gir":67.5,"fairways":63.0,"scramble":59.5,"proxAvg":34.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":85,"tpc_sawgrass":78,"pebble":72,"torrey_south":75,"riviera":82,"valhalla":75,"pinehurst_2":72,"royal_troon":72,"quail_hollow":75,"east_lake":78,"bay_hill":78,"harbour_town":72,"colonial":72,"memorial":78,"tpc_scottsdale":72},"augustaHistory":{"appearances":23,"bestFinish":1,"cuts":20,"top10":4,"avgScore":71.4},"notes":"Veteran with elite swing. Augusta specialist. Still competitive but ceiling has lowered."},
        {"id":26,"name":"Aaron Rai","rank":26,"sgTotal":0.65,"sgOtt":0.18,"sgApp":0.32,"sgArg":0.10,"sgPutt":0.05,"birdieAvg":3.9,"bogeyAvg":2.1,"scoringAvg":70.7,"gir":68.0,"fairways":67.0,"scramble":61.0,"proxAvg":34.5,"missDir":"right","flight":"low_fade","courseFit":{"augusta":62,"tpc_sawgrass":75,"pebble":78,"torrey_south":70,"riviera":75,"valhalla":68,"pinehurst_2":78,"royal_troon":82,"quail_hollow":68,"east_lake":70,"bay_hill":68,"harbour_town":82,"colonial":82,"memorial":72,"tpc_scottsdale":70},"augustaHistory":{"appearances":1,"bestFinish":25,"cuts":0,"top10":0,"avgScore":74.0},"notes":"Precise iron player. Accuracy over power. Good at courses that demand shotmaking. Steady for props."},
        {"id":27,"name":"Billy Horschel","rank":27,"sgTotal":0.62,"sgOtt":0.28,"sgApp":0.22,"sgArg":0.08,"sgPutt":0.04,"birdieAvg":3.9,"bogeyAvg":2.2,"scoringAvg":70.7,"gir":66.5,"fairways":65.0,"scramble":60.0,"proxAvg":35.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":62,"tpc_sawgrass":82,"pebble":72,"torrey_south":68,"riviera":72,"valhalla":72,"pinehurst_2":70,"royal_troon":68,"quail_hollow":72,"east_lake":88,"bay_hill":78,"harbour_town":72,"colonial":72,"memorial":72,"tpc_scottsdale":72},"notes":"East Lake specialist. TPC Sawgrass has history. Emotional player — performs well when locked in."},
        {"id":28,"name":"Tom Kim","rank":28,"sgTotal":0.60,"sgOtt":0.30,"sgApp":0.22,"sgArg":0.05,"sgPutt":0.03,"birdieAvg":4.2,"bogeyAvg":2.4,"scoringAvg":70.7,"gir":66.0,"fairways":62.0,"scramble":57.5,"proxAvg":34.5,"missDir":"left","flight":"high_draw","courseFit":{"augusta":68,"tpc_sawgrass":72,"pebble":68,"torrey_south":72,"riviera":72,"valhalla":72,"pinehurst_2":68,"royal_troon":65,"quail_hollow":78,"east_lake":75,"bay_hill":72,"harbour_town":68,"colonial":70,"memorial":72,"tpc_scottsdale":78},"augustaHistory":{"appearances":3,"bestFinish":15,"cuts":2,"top10":0,"avgScore":72.5},"notes":"Young talent with flair. Aggressive player with high birdie ceiling. Inconsistent but fun for props."},
        {"id":29,"name":"Corey Conners","rank":29,"sgTotal":0.58,"sgOtt":0.20,"sgApp":0.45,"sgArg":0.02,"sgPutt":-0.09,"birdieAvg":3.8,"bogeyAvg":2.1,"scoringAvg":70.7,"gir":70.0,"fairways":68.0,"scramble":57.0,"proxAvg":33.0,"missDir":"right","flight":"low_fade","courseFit":{"augusta":75,"tpc_sawgrass":78,"pebble":78,"torrey_south":72,"riviera":78,"valhalla":72,"pinehurst_2":80,"royal_troon":78,"quail_hollow":72,"east_lake":72,"bay_hill":72,"harbour_town":80,"colonial":82,"memorial":78,"tpc_scottsdale":72},"augustaHistory":{"appearances":6,"bestFinish":4,"cuts":6,"top10":3,"avgScore":70.6},"notes":"Elite iron player but can't putt. Highest GIR with lowest conversion. Great approach stats, fade putting props."},
        {"id":30,"name":"Sepp Straka","rank":30,"sgTotal":0.55,"sgOtt":0.30,"sgApp":0.18,"sgArg":0.05,"sgPutt":0.02,"birdieAvg":4.0,"bogeyAvg":2.2,"scoringAvg":70.8,"gir":67.0,"fairways":63.0,"scramble":59.0,"proxAvg":34.5,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":68,"tpc_sawgrass":72,"pebble":70,"torrey_south":72,"riviera":72,"valhalla":72,"pinehurst_2":70,"royal_troon":68,"quail_hollow":75,"east_lake":72,"bay_hill":72,"harbour_town":72,"colonial":72,"memorial":72,"tpc_scottsdale":72},"augustaHistory":{"appearances":4,"bestFinish":6,"cuts":4,"top10":1,"avgScore":71.4},"notes":"Steady ball-striker from Austria. No standout category. Reliable for MC and top-20 at mid-strength events."},
        {"id":31,"name":"Chris Kirk","rank":31,"sgTotal":0.52,"sgOtt":0.22,"sgApp":0.18,"sgArg":0.08,"sgPutt":0.04,"birdieAvg":3.8,"bogeyAvg":2.1,"scoringAvg":70.8,"gir":66.5,"fairways":66.0,"scramble":61.0,"proxAvg":35.0,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":65,"tpc_sawgrass":72,"pebble":72,"torrey_south":68,"riviera":72,"valhalla":72,"pinehurst_2":72,"royal_troon":70,"quail_hollow":72,"east_lake":72,"bay_hill":72,"harbour_town":78,"colonial":78,"memorial":72,"tpc_scottsdale":72},"augustaHistory":{"appearances":4,"bestFinish":18,"cuts":3,"top10":0,"avgScore":72.3},"notes":"Comeback story. Steady and consistent. Good at shorter accuracy courses. Low variance player."},
        {"id":32,"name":"Taylor Pendrith","rank":32,"sgTotal":0.50,"sgOtt":0.55,"sgApp":0.10,"sgArg":-0.05,"sgPutt":-0.10,"birdieAvg":4.3,"bogeyAvg":2.5,"scoringAvg":70.9,"gir":65.5,"fairways":57.0,"scramble":55.0,"proxAvg":35.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":68,"tpc_sawgrass":65,"pebble":62,"torrey_south":75,"riviera":68,"valhalla":78,"pinehurst_2":65,"royal_troon":60,"quail_hollow":78,"east_lake":68,"bay_hill":72,"harbour_town":58,"colonial":60,"memorial":68,"tpc_scottsdale":75},"notes":"Bomber who relies on length. Weak short game. Best at wide-open bombers courses. Fade at precision tracks."},
        {"id":33,"name":"Matt Fitzpatrick","rank":33,"sgTotal":0.48,"sgOtt":0.10,"sgApp":0.30,"sgArg":0.05,"sgPutt":0.03,"birdieAvg":3.7,"bogeyAvg":2.1,"scoringAvg":70.9,"gir":67.5,"fairways":69.0,"scramble":60.0,"proxAvg":34.0,"missDir":"left","flight":"low_draw","courseFit":{"augusta":65,"tpc_sawgrass":78,"pebble":78,"torrey_south":68,"riviera":78,"valhalla":68,"pinehurst_2":85,"royal_troon":80,"quail_hollow":68,"east_lake":72,"bay_hill":68,"harbour_town":82,"colonial":85,"memorial":75,"tpc_scottsdale":68},"notes":"US Open champion. Precision player. Not long but very accurate. Thrives on tight, demanding courses."},
        {"id":34,"name":"Robert MacIntyre","rank":34,"sgTotal":0.45,"sgOtt":0.32,"sgApp":0.15,"sgArg":0.02,"sgPutt":-0.04,"birdieAvg":4.0,"bogeyAvg":2.3,"scoringAvg":71.0,"gir":66.0,"fairways":62.0,"scramble":58.0,"proxAvg":35.0,"missDir":"left","flight":"low_draw","courseFit":{"augusta":62,"tpc_sawgrass":68,"pebble":72,"torrey_south":68,"riviera":72,"valhalla":72,"pinehurst_2":72,"royal_troon":85,"quail_hollow":72,"east_lake":68,"bay_hill":68,"harbour_town":75,"colonial":72,"memorial":68,"tpc_scottsdale":68},"notes":"Scottish lefty with links pedigree. Gritty competitor. Good in wind. Putter can go cold."},
        {"id":35,"name":"Akshay Bhatia","rank":35,"sgTotal":0.42,"sgOtt":0.38,"sgApp":0.15,"sgArg":0.02,"sgPutt":-0.13,"birdieAvg":4.3,"bogeyAvg":2.5,"scoringAvg":71.0,"gir":66.0,"fairways":58.0,"scramble":56.0,"proxAvg":35.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":68,"tpc_sawgrass":65,"pebble":62,"torrey_south":72,"riviera":68,"valhalla":72,"pinehurst_2":62,"royal_troon":58,"quail_hollow":72,"east_lake":68,"bay_hill":68,"harbour_town":58,"colonial":62,"memorial":68,"tpc_scottsdale":75},"augustaHistory":{"appearances":2,"bestFinish":10,"cuts":2,"top10":1,"avgScore":71.8},"notes":"Young lefty talent. Aggressive. Putting holds him back. High birdie ceiling with high floor."},
        {"id":36,"name":"Min Woo Lee","rank":36,"sgTotal":0.40,"sgOtt":0.42,"sgApp":0.12,"sgArg":0.00,"sgPutt":-0.14,"birdieAvg":4.2,"bogeyAvg":2.4,"scoringAvg":71.0,"gir":66.0,"fairways":59.0,"scramble":57.0,"proxAvg":35.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":65,"tpc_sawgrass":68,"pebble":68,"torrey_south":72,"riviera":70,"valhalla":72,"pinehurst_2":65,"royal_troon":72,"quail_hollow":72,"east_lake":68,"bay_hill":68,"harbour_town":65,"colonial":68,"memorial":68,"tpc_scottsdale":72},"augustaHistory":{"appearances":2,"bestFinish":18,"cuts":2,"top10":0,"avgScore":72.8},"notes":"Australian talent with power. Sister Minjee is LPGA star. Inconsistent but can go low. Good for T20 at mid-tier."},
        {"id":37,"name":"Nicolai Hojgaard","rank":37,"sgTotal":0.38,"sgOtt":0.35,"sgApp":0.12,"sgArg":0.00,"sgPutt":-0.09,"birdieAvg":4.1,"bogeyAvg":2.3,"scoringAvg":71.1,"gir":66.0,"fairways":61.0,"scramble":57.5,"proxAvg":35.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":65,"tpc_sawgrass":68,"pebble":65,"torrey_south":70,"riviera":68,"valhalla":72,"pinehurst_2":65,"royal_troon":72,"quail_hollow":72,"east_lake":68,"bay_hill":68,"harbour_town":65,"colonial":68,"memorial":68,"tpc_scottsdale":72},"notes":"Danish twin (brother Rasmus also on Tour). Athletic and powerful. Still adapting to PGA Tour courses."},
        {"id":38,"name":"Davis Thompson","rank":38,"sgTotal":0.35,"sgOtt":0.25,"sgApp":0.12,"sgArg":0.02,"sgPutt":-0.04,"birdieAvg":3.9,"bogeyAvg":2.2,"scoringAvg":71.1,"gir":66.5,"fairways":64.0,"scramble":58.5,"proxAvg":35.0,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":62,"tpc_sawgrass":72,"pebble":68,"torrey_south":68,"riviera":68,"valhalla":68,"pinehurst_2":68,"royal_troon":65,"quail_hollow":72,"east_lake":72,"bay_hill":68,"harbour_town":72,"colonial":75,"memorial":72,"tpc_scottsdale":72},"notes":"Young steady player. Won his first Tour event. No standout skill but consistent. Good for MC props."},
        {"id":39,"name":"Austin Eckroat","rank":39,"sgTotal":0.33,"sgOtt":0.30,"sgApp":0.08,"sgArg":0.00,"sgPutt":-0.05,"birdieAvg":4.0,"bogeyAvg":2.3,"scoringAvg":71.2,"gir":66.0,"fairways":62.0,"scramble":57.0,"proxAvg":35.5,"missDir":"right","flight":"high_fade","courseFit":{"augusta":62,"tpc_sawgrass":65,"pebble":65,"torrey_south":72,"riviera":65,"valhalla":72,"pinehurst_2":62,"royal_troon":60,"quail_hollow":72,"east_lake":65,"bay_hill":68,"harbour_town":62,"colonial":65,"memorial":68,"tpc_scottsdale":72},"notes":"Oklahoma product with power. Still developing consistency. Better at longer courses. Thin field value play."},
        {"id":40,"name":"Harris English","rank":40,"sgTotal":0.30,"sgOtt":0.22,"sgApp":0.12,"sgArg":0.02,"sgPutt":-0.06,"birdieAvg":3.8,"bogeyAvg":2.2,"scoringAvg":71.2,"gir":66.0,"fairways":65.0,"scramble":59.0,"proxAvg":35.5,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":65,"tpc_sawgrass":72,"pebble":70,"torrey_south":72,"riviera":68,"valhalla":72,"pinehurst_2":68,"royal_troon":65,"quail_hollow":72,"east_lake":78,"bay_hill":72,"harbour_town":75,"colonial":72,"memorial":72,"tpc_scottsdale":72},"notes":"Steady veteran. Good all-around game without elite skill. East Lake familiarity. Reliable for MC at full fields."},
        {"id":41,"name":"Jake Knapp","rank":41,"sgTotal":0.28,"sgOtt":0.58,"sgApp":0.00,"sgArg":-0.12,"sgPutt":-0.18,"birdieAvg":4.2,"bogeyAvg":2.7,"scoringAvg":71.3,"gir":64.0,"fairways":54.0,"scramble":52.0,"proxAvg":36.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":60,"tpc_sawgrass":58,"pebble":55,"torrey_south":68,"riviera":62,"valhalla":72,"pinehurst_2":55,"royal_troon":50,"quail_hollow":72,"east_lake":60,"bay_hill":65,"harbour_town":48,"colonial":50,"memorial":62,"tpc_scottsdale":72},"notes":"Longest hitter on Tour. Accuracy is a problem. Short game is Tour-worst tier. Only play at bomber-friendly tracks."},
        {"id":42,"name":"Keith Mitchell","rank":42,"sgTotal":0.25,"sgOtt":0.48,"sgApp":0.02,"sgArg":-0.10,"sgPutt":-0.15,"birdieAvg":4.1,"bogeyAvg":2.6,"scoringAvg":71.3,"gir":65.0,"fairways":56.0,"scramble":53.0,"proxAvg":35.5,"missDir":"right","flight":"high_fade","courseFit":{"augusta":62,"tpc_sawgrass":60,"pebble":58,"torrey_south":72,"riviera":62,"valhalla":75,"pinehurst_2":58,"royal_troon":55,"quail_hollow":75,"east_lake":62,"bay_hill":68,"harbour_town":52,"colonial":55,"memorial":62,"tpc_scottsdale":72},"notes":"Power player with accuracy issues. Thrives at wide courses. Short game liability."},
        {"id":43,"name":"Stephan Jaeger","rank":43,"sgTotal":0.22,"sgOtt":0.15,"sgApp":0.10,"sgArg":0.02,"sgPutt":-0.05,"birdieAvg":3.7,"bogeyAvg":2.2,"scoringAvg":71.3,"gir":66.0,"fairways":65.0,"scramble":58.0,"proxAvg":35.5,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":58,"tpc_sawgrass":68,"pebble":68,"torrey_south":65,"riviera":68,"valhalla":65,"pinehurst_2":68,"royal_troon":65,"quail_hollow":68,"east_lake":65,"bay_hill":65,"harbour_town":72,"colonial":72,"memorial":68,"tpc_scottsdale":68},"notes":"German journeyman having a solid stretch. Shot 58 on mini-tour. Reliable for MC at weaker fields."},
        {"id":44,"name":"Eric Cole","rank":44,"sgTotal":0.20,"sgOtt":0.42,"sgApp":0.00,"sgArg":-0.08,"sgPutt":-0.14,"birdieAvg":4.0,"bogeyAvg":2.5,"scoringAvg":71.4,"gir":65.0,"fairways":57.0,"scramble":54.0,"proxAvg":35.5,"missDir":"right","flight":"high_fade","courseFit":{"augusta":62,"tpc_sawgrass":60,"pebble":58,"torrey_south":68,"riviera":62,"valhalla":72,"pinehurst_2":58,"royal_troon":55,"quail_hollow":72,"east_lake":62,"bay_hill":65,"harbour_town":55,"colonial":58,"memorial":62,"tpc_scottsdale":68},"augustaHistory":{"appearances":2,"bestFinish":20,"cuts":1,"top10":0,"avgScore":73.2},"notes":"Big hitter with inconsistent short game. Best at wide-open courses. Avoid at precision tracks."},
        {"id":45,"name":"Christiaan Bezuidenhout","rank":45,"sgTotal":0.18,"sgOtt":0.15,"sgApp":0.12,"sgArg":0.00,"sgPutt":-0.09,"birdieAvg":3.7,"bogeyAvg":2.2,"scoringAvg":71.4,"gir":66.5,"fairways":66.0,"scramble":58.0,"proxAvg":35.0,"missDir":"left","flight":"low_draw","courseFit":{"augusta":62,"tpc_sawgrass":72,"pebble":72,"torrey_south":65,"riviera":72,"valhalla":65,"pinehurst_2":72,"royal_troon":75,"quail_hollow":65,"east_lake":68,"bay_hill":65,"harbour_town":75,"colonial":78,"memorial":68,"tpc_scottsdale":65},"notes":"South African with smooth swing. Accuracy player. Good in wind. Putting holds him back from contending."},
        {"id":46,"name":"Jordan Spieth","rank":46,"sgTotal":0.15,"sgOtt":0.10,"sgApp":0.08,"sgArg":0.05,"sgPutt":-0.08,"birdieAvg":3.8,"bogeyAvg":2.4,"scoringAvg":71.5,"gir":65.0,"fairways":60.0,"scramble":62.0,"proxAvg":35.5,"missDir":"left","flight":"high_draw","courseFit":{"augusta":88,"tpc_sawgrass":72,"pebble":82,"torrey_south":72,"riviera":72,"valhalla":75,"pinehurst_2":72,"royal_troon":82,"quail_hollow":72,"east_lake":82,"bay_hill":72,"harbour_town":72,"colonial":85,"memorial":72,"tpc_scottsdale":78},"notes":"3x major champ in a slump. Course history still matters — elite at Augusta, Colonial, Pebble. Buy low candidate."},
        {"id":47,"name":"Michael Thorbjornsen","rank":47,"sgTotal":0.12,"sgOtt":0.25,"sgApp":0.05,"sgArg":-0.05,"sgPutt":-0.13,"birdieAvg":3.9,"bogeyAvg":2.4,"scoringAvg":71.5,"gir":65.5,"fairways":61.0,"scramble":55.0,"proxAvg":35.5,"missDir":"right","flight":"high_fade","courseFit":{"augusta":58,"tpc_sawgrass":62,"pebble":60,"torrey_south":65,"riviera":62,"valhalla":68,"pinehurst_2":60,"royal_troon":58,"quail_hollow":68,"east_lake":62,"bay_hill":65,"harbour_town":58,"colonial":60,"memorial":65,"tpc_scottsdale":68},"notes":"Promising rookie from Stanford. Athletic and long. Raw but talented. Watch for breakout at long courses."},
        {"id":48,"name":"Nick Dunlap","rank":48,"sgTotal":0.10,"sgOtt":0.28,"sgApp":0.02,"sgArg":-0.08,"sgPutt":-0.12,"birdieAvg":4.0,"bogeyAvg":2.5,"scoringAvg":71.5,"gir":65.0,"fairways":60.0,"scramble":54.0,"proxAvg":36.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":58,"tpc_sawgrass":60,"pebble":58,"torrey_south":65,"riviera":62,"valhalla":68,"pinehurst_2":58,"royal_troon":55,"quail_hollow":68,"east_lake":62,"bay_hill":62,"harbour_town":55,"colonial":58,"memorial":62,"tpc_scottsdale":72},"notes":"Won as amateur on Tour. Young talent still developing. High variance — DFS dart throw at big courses."},
        {"id":49,"name":"Ben Griffin","rank":49,"sgTotal":0.08,"sgOtt":0.18,"sgApp":0.00,"sgArg":-0.02,"sgPutt":-0.08,"birdieAvg":3.7,"bogeyAvg":2.3,"scoringAvg":71.5,"gir":65.5,"fairways":63.0,"scramble":57.0,"proxAvg":35.5,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":55,"tpc_sawgrass":62,"pebble":62,"torrey_south":62,"riviera":62,"valhalla":65,"pinehurst_2":62,"royal_troon":60,"quail_hollow":65,"east_lake":62,"bay_hill":62,"harbour_town":65,"colonial":68,"memorial":62,"tpc_scottsdale":65},"notes":"Steady mid-tier player. No outstanding skill. MC candidate at weaker fields. Fade at marquee events."},
        {"id":50,"name":"Maverick McNealy","rank":50,"sgTotal":0.05,"sgOtt":0.15,"sgApp":0.00,"sgArg":-0.02,"sgPutt":-0.08,"birdieAvg":3.6,"bogeyAvg":2.3,"scoringAvg":71.6,"gir":65.0,"fairways":64.0,"scramble":57.0,"proxAvg":36.0,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":55,"tpc_sawgrass":62,"pebble":68,"torrey_south":68,"riviera":65,"valhalla":62,"pinehurst_2":62,"royal_troon":60,"quail_hollow":62,"east_lake":62,"bay_hill":62,"harbour_town":65,"colonial":68,"memorial":62,"tpc_scottsdale":68},"augustaHistory":{"appearances":1,"bestFinish":40,"cuts":0,"top10":0,"avgScore":75.0},"notes":"Stanford product. West Coast familiarity helps. Pebble and Torrey specialist. Thin field MC candidate."},
        # ---- PAST MASTERS CHAMPIONS (still active/invited) ----
        {"id":52,"name":"Sergio Garcia","rank":75,"sgTotal":0.25,"sgOtt":0.30,"sgApp":0.10,"sgArg":0.05,"sgPutt":-0.20,"birdieAvg":3.9,"bogeyAvg":2.2,"scoringAvg":71.2,"gir":66.0,"fairways":62.0,"scramble":59.0,"proxAvg":34.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":80,"tpc_sawgrass":68,"pebble":72,"torrey_south":68,"riviera":72,"valhalla":72,"pinehurst_2":70,"royal_troon":75,"quail_hollow":68,"east_lake":72,"bay_hill":68,"harbour_town":68,"colonial":68,"memorial":70,"tpc_scottsdale":68},"augustaHistory":{"appearances":24,"bestFinish":1,"cuts":21,"top10":6,"avgScore":71.6},"notes":"2017 champion. One of Augusta's great history makers — 23 prior top-10s. Putter remains the weakness. Respect the track record."},
        {"id":53,"name":"Danny Willett","rank":88,"sgTotal":0.10,"sgOtt":0.15,"sgApp":0.05,"sgArg":0.00,"sgPutt":-0.10,"birdieAvg":3.7,"bogeyAvg":2.2,"scoringAvg":71.6,"gir":65.0,"fairways":63.0,"scramble":57.0,"proxAvg":35.0,"missDir":"left","flight":"mid_draw","courseFit":{"augusta":72,"tpc_sawgrass":65,"pebble":68,"torrey_south":65,"riviera":68,"valhalla":68,"pinehurst_2":65,"royal_troon":72,"quail_hollow":68,"east_lake":65,"bay_hill":65,"harbour_town":65,"colonial":65,"memorial":65,"tpc_scottsdale":65},"augustaHistory":{"appearances":8,"bestFinish":1,"cuts":5,"top10":1,"avgScore":72.1},"notes":"2016 champion in stunning Jordan Spieth collapse. Inconsistent since. Pedigree at Augusta — can't fully fade the champion."},
        {"id":54,"name":"Phil Mickelson","rank":200,"sgTotal":-0.50,"sgOtt":0.10,"sgApp":-0.30,"sgArg":0.20,"sgPutt":-0.50,"birdieAvg":3.5,"bogeyAvg":3.0,"scoringAvg":73.0,"gir":62.0,"fairways":52.0,"scramble":62.0,"proxAvg":37.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":82,"tpc_sawgrass":65,"pebble":80,"torrey_south":68,"riviera":75,"valhalla":70,"pinehurst_2":68,"royal_troon":72,"quail_hollow":65,"east_lake":68,"bay_hill":68,"harbour_town":65,"colonial":68,"memorial":68,"tpc_scottsdale":72},"augustaHistory":{"appearances":30,"bestFinish":1,"cuts":26,"top10":9,"avgScore":71.8},"notes":"3x Masters champion (2004, 2006, 2010). Physically declining but Augusta knowledge is unmatched. Career MC prop only."},
        {"id":55,"name":"Bubba Watson","rank":250,"sgTotal":-0.80,"sgOtt":0.30,"sgApp":-0.60,"sgArg":-0.30,"sgPutt":-0.20,"birdieAvg":3.2,"bogeyAvg":3.2,"scoringAvg":73.5,"gir":60.0,"fairways":55.0,"scramble":55.0,"proxAvg":38.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":78,"tpc_sawgrass":60,"pebble":65,"torrey_south":60,"riviera":65,"valhalla":68,"pinehurst_2":62,"royal_troon":60,"quail_hollow":65,"east_lake":60,"bay_hill":62,"harbour_town":58,"colonial":60,"memorial":62,"tpc_scottsdale":65},"augustaHistory":{"appearances":14,"bestFinish":1,"cuts":11,"top10":3,"avgScore":71.9},"notes":"2x champion (2012, 2014). Retired from PGA Tour. Invited as champion. Big fade — not competitive anymore."},
        {"id":56,"name":"Mike Weir","rank":300,"sgTotal":-1.20,"sgOtt":-0.20,"sgApp":-0.50,"sgArg":-0.30,"sgPutt":-0.20,"birdieAvg":2.8,"bogeyAvg":3.0,"scoringAvg":74.0,"gir":58.0,"fairways":63.0,"scramble":55.0,"proxAvg":38.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":65,"tpc_sawgrass":58,"pebble":65,"torrey_south":60,"riviera":62,"valhalla":62,"pinehurst_2":62,"royal_troon":65,"quail_hollow":58,"east_lake":58,"bay_hill":60,"harbour_town":60,"colonial":62,"memorial":60,"tpc_scottsdale":62},"augustaHistory":{"appearances":18,"bestFinish":1,"cuts":12,"top10":2,"avgScore":72.8},"notes":"2003 champion. Playing on Champions Tour. Augusta invite is honorary at this stage. MC prop only."},
        {"id":57,"name":"Zach Johnson","rank":180,"sgTotal":-0.40,"sgOtt":-0.10,"sgApp":-0.10,"sgArg":-0.05,"sgPutt":-0.15,"birdieAvg":3.3,"bogeyAvg":2.5,"scoringAvg":72.5,"gir":64.0,"fairways":68.0,"scramble":58.0,"proxAvg":35.5,"missDir":"neutral","flight":"low_fade","courseFit":{"augusta":72,"tpc_sawgrass":70,"pebble":72,"torrey_south":65,"riviera":68,"valhalla":68,"pinehurst_2":70,"royal_troon":75,"quail_hollow":65,"east_lake":68,"bay_hill":65,"harbour_town":70,"colonial":72,"memorial":68,"tpc_scottsdale":65},"augustaHistory":{"appearances":20,"bestFinish":1,"cuts":17,"top10":4,"avgScore":71.5},"notes":"2007 champion. Precision iron player who thrives at Augusta. Former Ryder Cup captain. Fade for outright; MC solid."},
        {"id":58,"name":"Tiger Woods","rank":999,"sgTotal":-1.50,"sgOtt":-0.20,"sgApp":-0.80,"sgArg":-0.20,"sgPutt":-0.30,"birdieAvg":2.5,"bogeyAvg":3.5,"scoringAvg":74.5,"gir":58.0,"fairways":58.0,"scramble":55.0,"proxAvg":38.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":38,"tpc_sawgrass":35,"pebble":38,"torrey_south":35,"riviera":35,"valhalla":33,"pinehurst_2":35,"royal_troon":32,"quail_hollow":33,"east_lake":33,"bay_hill":36,"harbour_town":30,"colonial":30,"memorial":35,"tpc_scottsdale":33},"augustaHistory":{"appearances":24,"bestFinish":1,"cuts":22,"top10":14,"avgScore":70.5},"notes":"5x champion (1997, 2001, 2002, 2005, 2019). NOT PLAYING 2026 — ongoing injury recovery. Course fit reflects current physical condition, not peak-era legacy. Do not use for any props or sim."},
        {"id":59,"name":"Fred Couples","rank":500,"sgTotal":-2.0,"sgOtt":-0.30,"sgApp":-1.0,"sgArg":-0.40,"sgPutt":-0.30,"birdieAvg":2.2,"bogeyAvg":3.8,"scoringAvg":76.0,"gir":55.0,"fairways":58.0,"scramble":50.0,"proxAvg":40.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":75,"tpc_sawgrass":60,"pebble":68,"torrey_south":62,"riviera":70,"valhalla":62,"pinehurst_2":62,"royal_troon":65,"quail_hollow":60,"east_lake":62,"bay_hill":65,"harbour_town":62,"colonial":65,"memorial":62,"tpc_scottsdale":62},"augustaHistory":{"appearances":33,"bestFinish":1,"cuts":30,"top10":8,"avgScore":71.8},"notes":"1992 champion. Champions Tour legend. Ball never short of 12th hole (famous). Playing as a past champ — for the love of the game."},
        # ---- ACTIVE PGA TOUR — MASTERS FIELD QUALIFIERS ----
        {"id":66,"name":"Si Woo Kim","rank":38,"sgTotal":0.45,"sgOtt":0.20,"sgApp":0.18,"sgArg":0.05,"sgPutt":0.02,"birdieAvg":4.0,"bogeyAvg":2.2,"scoringAvg":70.9,"gir":66.5,"fairways":63.0,"scramble":59.0,"proxAvg":33.5,"missDir":"left","flight":"mid_draw","courseFit":{"augusta":72,"tpc_sawgrass":70,"pebble":68,"torrey_south":68,"riviera":70,"valhalla":72,"pinehurst_2":70,"royal_troon":68,"quail_hollow":70,"east_lake":70,"bay_hill":68,"harbour_town":68,"colonial":68,"memorial":70,"tpc_scottsdale":68},"augustaHistory":{"appearances":7,"bestFinish":8,"cuts":5,"top10":1,"avgScore":71.9},"notes":"Versatile Korean player with multiple wins. Solid all-around game. Steady at Augusta but ceiling is top-10. Reliable MC prop."},
        {"id":69,"name":"Davis Riley","rank":42,"sgTotal":0.38,"sgOtt":0.28,"sgApp":0.15,"sgArg":0.02,"sgPutt":-0.07,"birdieAvg":3.9,"bogeyAvg":2.2,"scoringAvg":71.0,"gir":66.5,"fairways":63.0,"scramble":57.0,"proxAvg":34.5,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":70,"tpc_sawgrass":68,"pebble":68,"torrey_south":70,"riviera":68,"valhalla":72,"pinehurst_2":68,"royal_troon":65,"quail_hollow":72,"east_lake":70,"bay_hill":70,"harbour_town":68,"colonial":70,"memorial":70,"tpc_scottsdale":70},"augustaHistory":{"appearances":2,"bestFinish":22,"cuts":1,"top10":0,"avgScore":72.8},"notes":"Alabama native still adjusting to major speed. Solid iron player. Mid-tier Augusta floor. Better at more forgiving venues."},
        {"id":72,"name":"Taylor Montgomery","rank":53,"sgTotal":0.42,"sgOtt":0.30,"sgApp":0.15,"sgArg":0.05,"sgPutt":-0.08,"birdieAvg":4.0,"bogeyAvg":2.3,"scoringAvg":71.0,"gir":66.0,"fairways":63.0,"scramble":57.0,"proxAvg":34.0,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":68,"tpc_sawgrass":68,"pebble":68,"torrey_south":72,"riviera":68,"valhalla":70,"pinehurst_2":68,"royal_troon":65,"quail_hollow":70,"east_lake":68,"bay_hill":70,"harbour_town":65,"colonial":68,"memorial":68,"tpc_scottsdale":75},"augustaHistory":{"appearances":1,"bestFinish":28,"cuts":1,"top10":0,"avgScore":73.1},"notes":"Consistent mid-tier player. Augusta debut was rough. Power game but needs precision Augusta demands. Watch form going in."},
        {"id":76,"name":"Seamus Power","rank":62,"sgTotal":0.28,"sgOtt":0.20,"sgApp":0.12,"sgArg":0.05,"sgPutt":-0.09,"birdieAvg":3.8,"bogeyAvg":2.2,"scoringAvg":71.3,"gir":66.0,"fairways":63.0,"scramble":58.0,"proxAvg":34.5,"missDir":"left","flight":"mid_draw","courseFit":{"augusta":70,"tpc_sawgrass":68,"pebble":70,"torrey_south":68,"riviera":68,"valhalla":70,"pinehurst_2":70,"royal_troon":75,"quail_hollow":68,"east_lake":68,"bay_hill":68,"harbour_town":70,"colonial":70,"memorial":68,"tpc_scottsdale":65},"augustaHistory":{"appearances":3,"bestFinish":16,"cuts":2,"top10":0,"avgScore":72.4},"notes":"Irish power player. Better on links and European-style tracks. Augusta is not his best fit but competitive."},
        {"id":77,"name":"Adam Hadwin","rank":65,"sgTotal":0.25,"sgOtt":0.15,"sgApp":0.12,"sgArg":0.05,"sgPutt":-0.07,"birdieAvg":3.7,"bogeyAvg":2.1,"scoringAvg":71.4,"gir":67.0,"fairways":67.0,"scramble":58.0,"proxAvg":35.0,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":68,"tpc_sawgrass":68,"pebble":70,"torrey_south":72,"riviera":70,"valhalla":68,"pinehurst_2":70,"royal_troon":68,"quail_hollow":65,"east_lake":68,"bay_hill":68,"harbour_town":68,"colonial":68,"memorial":65,"tpc_scottsdale":72},"augustaHistory":{"appearances":4,"bestFinish":24,"cuts":3,"top10":0,"avgScore":72.5},"notes":"Canadian accuracy player. Very consistent but lacks the elite ballstriking Augusta demands at the top. MC value."},
        {"id":78,"name":"Mackenzie Hughes","rank":68,"sgTotal":0.22,"sgOtt":0.10,"sgApp":0.12,"sgArg":0.05,"sgPutt":-0.05,"birdieAvg":3.7,"bogeyAvg":2.1,"scoringAvg":71.5,"gir":67.0,"fairways":67.0,"scramble":59.0,"proxAvg":35.0,"missDir":"neutral","flight":"low_fade","courseFit":{"augusta":68,"tpc_sawgrass":68,"pebble":70,"torrey_south":68,"riviera":68,"valhalla":68,"pinehurst_2":70,"royal_troon":70,"quail_hollow":65,"east_lake":68,"bay_hill":65,"harbour_town":68,"colonial":68,"memorial":65,"tpc_scottsdale":68},"augustaHistory":{"appearances":3,"bestFinish":19,"cuts":2,"top10":0,"avgScore":72.6},"notes":"Canadian precision player. Limited Augusta experience. Consistent MC candidate. No contender ceiling yet."},
        {"id":79,"name":"J.T. Poston","rank":70,"sgTotal":0.20,"sgOtt":0.08,"sgApp":0.10,"sgArg":0.05,"sgPutt":-0.03,"birdieAvg":3.7,"bogeyAvg":2.1,"scoringAvg":71.5,"gir":67.0,"fairways":67.0,"scramble":59.0,"proxAvg":35.0,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":68,"tpc_sawgrass":68,"pebble":68,"torrey_south":65,"riviera":68,"valhalla":68,"pinehurst_2":68,"royal_troon":65,"quail_hollow":68,"east_lake":70,"bay_hill":68,"harbour_town":70,"colonial":70,"memorial":68,"tpc_scottsdale":65},"augustaHistory":{"appearances":3,"bestFinish":21,"cuts":2,"top10":0,"avgScore":72.5},"notes":"Ball-striking specialist. Consistent mid-field at Augusta. No major breakthrough yet. Solid MC prop."},
        {"id":80,"name":"Lucas Glover","rank":72,"sgTotal":0.18,"sgOtt":0.10,"sgApp":0.08,"sgArg":0.02,"sgPutt":-0.02,"birdieAvg":3.6,"bogeyAvg":2.1,"scoringAvg":71.6,"gir":66.5,"fairways":67.0,"scramble":59.0,"proxAvg":35.5,"missDir":"neutral","flight":"low_fade","courseFit":{"augusta":70,"tpc_sawgrass":70,"pebble":68,"torrey_south":65,"riviera":68,"valhalla":70,"pinehurst_2":70,"royal_troon":68,"quail_hollow":68,"east_lake":72,"bay_hill":68,"harbour_town":70,"colonial":70,"memorial":70,"tpc_scottsdale":65},"augustaHistory":{"appearances":12,"bestFinish":8,"cuts":9,"top10":1,"avgScore":71.8},"notes":"US Open champion (2009). Augusta veteran who plays with grinder mentality. Consistent MC maker. T8 here proves he can compete."},
        {"id":82,"name":"Rasmus Hojgaard","rank":74,"sgTotal":0.22,"sgOtt":0.18,"sgApp":0.12,"sgArg":0.05,"sgPutt":-0.13,"birdieAvg":3.8,"bogeyAvg":2.3,"scoringAvg":71.3,"gir":66.5,"fairways":63.0,"scramble":57.0,"proxAvg":34.5,"missDir":"right","flight":"mid_fade","courseFit":{"augusta":68,"tpc_sawgrass":65,"pebble":68,"torrey_south":65,"riviera":65,"valhalla":70,"pinehurst_2":68,"royal_troon":75,"quail_hollow":68,"east_lake":65,"bay_hill":65,"harbour_town":65,"colonial":65,"memorial":68,"tpc_scottsdale":65},"augustaHistory":{"appearances":1,"bestFinish":14,"cuts":1,"top10":0,"avgScore":72.2},"notes":"Danish twin with DP World Tour pedigree. Solid debut at Augusta. European-style patience suits the course. Rising stock."},
        {"id":84,"name":"Erik van Rooyen","rank":78,"sgTotal":0.18,"sgOtt":0.30,"sgApp":0.08,"sgArg":-0.05,"sgPutt":-0.15,"birdieAvg":3.9,"bogeyAvg":2.4,"scoringAvg":71.5,"gir":65.5,"fairways":61.0,"scramble":56.0,"proxAvg":34.5,"missDir":"left","flight":"high_draw","courseFit":{"augusta":68,"tpc_sawgrass":65,"pebble":65,"torrey_south":68,"riviera":65,"valhalla":70,"pinehurst_2":65,"royal_troon":68,"quail_hollow":70,"east_lake":65,"bay_hill":65,"harbour_town":62,"colonial":62,"memorial":65,"tpc_scottsdale":68},"augustaHistory":{"appearances":3,"bestFinish":22,"cuts":2,"top10":0,"avgScore":72.7},"notes":"South African power player. Long off the tee but short game lets him down. Augusta demands scrambling he doesn't have."},
        {"id":85,"name":"Callum Shinkwin","rank":80,"sgTotal":0.15,"sgOtt":0.22,"sgApp":0.08,"sgArg":-0.05,"sgPutt":-0.10,"birdieAvg":3.7,"bogeyAvg":2.2,"scoringAvg":71.6,"gir":65.5,"fairways":62.0,"scramble":57.0,"proxAvg":35.0,"missDir":"right","flight":"mid_fade","courseFit":{"augusta":65,"tpc_sawgrass":62,"pebble":65,"torrey_south":65,"riviera":65,"valhalla":68,"pinehurst_2":65,"royal_troon":72,"quail_hollow":65,"east_lake":62,"bay_hill":62,"harbour_town":65,"colonial":62,"memorial":62,"tpc_scottsdale":62},"augustaHistory":{"appearances":1,"bestFinish":30,"cuts":0,"top10":0,"avgScore":74.5},"notes":"English DP World Tour player. Augusta debut was rough. Links-style game. Not Augusta-suited."},
        {"id":86,"name":"Thriston Lawrence","rank":82,"sgTotal":0.15,"sgOtt":0.18,"sgApp":0.08,"sgArg":0.00,"sgPutt":-0.11,"birdieAvg":3.7,"bogeyAvg":2.2,"scoringAvg":71.7,"gir":66.0,"fairways":64.0,"scramble":57.0,"proxAvg":35.0,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":65,"tpc_sawgrass":62,"pebble":65,"torrey_south":62,"riviera":65,"valhalla":65,"pinehurst_2":65,"royal_troon":70,"quail_hollow":62,"east_lake":62,"bay_hill":62,"harbour_town":65,"colonial":62,"memorial":62,"tpc_scottsdale":62},"augustaHistory":{"appearances":1,"bestFinish":35,"cuts":0,"top10":0,"avgScore":75.0},"notes":"South African DP World Tour player. Augusta debut. Solid European game but long road to contend here."},
        {"id":87,"name":"Ryan Fox","rank":84,"sgTotal":0.18,"sgOtt":0.28,"sgApp":0.08,"sgArg":-0.05,"sgPutt":-0.13,"birdieAvg":3.8,"bogeyAvg":2.3,"scoringAvg":71.6,"gir":65.5,"fairways":62.0,"scramble":56.0,"proxAvg":34.5,"missDir":"left","flight":"high_draw","courseFit":{"augusta":65,"tpc_sawgrass":62,"pebble":68,"torrey_south":65,"riviera":65,"valhalla":68,"pinehurst_2":65,"royal_troon":72,"quail_hollow":65,"east_lake":62,"bay_hill":62,"harbour_town":65,"colonial":62,"memorial":62,"tpc_scottsdale":62},"augustaHistory":{"appearances":2,"bestFinish":28,"cuts":1,"top10":0,"avgScore":73.5},"notes":"New Zealand bomber. Son of Grant Fox. Huge off the tee but Augusta short game demands more. Better on longer bombers tracks."},
        {"id":88,"name":"Lee Hodges","rank":86,"sgTotal":0.12,"sgOtt":0.15,"sgApp":0.05,"sgArg":0.00,"sgPutt":-0.08,"birdieAvg":3.6,"bogeyAvg":2.2,"scoringAvg":71.8,"gir":66.0,"fairways":65.0,"scramble":58.0,"proxAvg":35.5,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":62,"tpc_sawgrass":62,"pebble":62,"torrey_south":62,"riviera":62,"valhalla":65,"pinehurst_2":62,"royal_troon":60,"quail_hollow":65,"east_lake":65,"bay_hill":62,"harbour_town":65,"colonial":65,"memorial":62,"tpc_scottsdale":65},"augustaHistory":{"appearances":2,"bestFinish":32,"cuts":1,"top10":0,"avgScore":73.8},"notes":"Alabama native making his Masters bones. Good ballstriker still learning major speeds. Development player."},
        {"id":89,"name":"Bernhard Langer","rank":999,"sgTotal":-3.0,"sgOtt":-1.0,"sgApp":-1.2,"sgArg":-0.5,"sgPutt":-0.3,"birdieAvg":2.0,"bogeyAvg":4.0,"scoringAvg":77.0,"gir":52.0,"fairways":60.0,"scramble":50.0,"proxAvg":42.0,"missDir":"neutral","flight":"low_fade","courseFit":{"augusta":72,"tpc_sawgrass":55,"pebble":60,"torrey_south":55,"riviera":58,"valhalla":58,"pinehurst_2":60,"royal_troon":65,"quail_hollow":55,"east_lake":55,"bay_hill":55,"harbour_town":58,"colonial":60,"memorial":58,"tpc_scottsdale":55},"augustaHistory":{"appearances":40,"bestFinish":1,"cuts":36,"top10":10,"avgScore":72.0},"notes":"2x champion (1985, 1993). Augusta legend. Playing as a 68-year-old past champion. Beloved but not a betting proposition. Avoid all props."},
        {"id":90,"name":"Larry Mize","rank":999,"sgTotal":-3.5,"sgOtt":-1.2,"sgApp":-1.5,"sgArg":-0.5,"sgPutt":-0.3,"birdieAvg":1.8,"bogeyAvg":4.5,"scoringAvg":78.0,"gir":50.0,"fairways":58.0,"scramble":48.0,"proxAvg":43.0,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":68,"tpc_sawgrass":50,"pebble":55,"torrey_south":50,"riviera":52,"valhalla":52,"pinehurst_2":55,"royal_troon":55,"quail_hollow":50,"east_lake":52,"bay_hill":52,"harbour_town":55,"colonial":55,"memorial":52,"tpc_scottsdale":50},"augustaHistory":{"appearances":38,"bestFinish":1,"cuts":28,"top10":3,"avgScore":73.2},"notes":"1987 champion — famous chip-in on 11 in playoff vs Norman. Augusta resident. Honorary start only at this stage. Beloved local."},
        # ---- LIV GOLF — MAJOR INVITEES (Past champions + OWGR qualifiers) ----
        # These players only compete in majors/select events; must be included manually.
        # liv=True flag ensures they are always merged in when DataGolf is primary source.
        {"id":91,"name":"Bryson DeChambeau","rank":8,"liv":True,"sgTotal":1.55,"sgOtt":1.10,"sgApp":0.48,"sgArg":0.10,"sgPutt":-0.13,"birdieAvg":5.2,"bogeyAvg":2.8,"scoringAvg":69.3,"gir":67.0,"fairways":52.0,"scramble":58.0,"proxAvg":30.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":80,"tpc_sawgrass":72,"pebble":85,"torrey_south":82,"riviera":75,"valhalla":80,"pinehurst_2":78,"royal_troon":68,"quail_hollow":82,"east_lake":80,"bay_hill":78,"harbour_town":60,"colonial":65,"memorial":80,"tpc_scottsdale":82},"augustaHistory":{"appearances":7,"bestFinish":2,"cuts":6,"top10":2,"avgScore":71.0},"notes":"2024 US Open champion. Power game. LIV — major invitee via past champion/OWGR."},
        {"id":92,"name":"Brooks Koepka","rank":12,"liv":True,"sgTotal":1.30,"sgOtt":0.60,"sgApp":0.55,"sgArg":0.15,"sgPutt":0.00,"birdieAvg":4.4,"bogeyAvg":2.2,"scoringAvg":69.8,"gir":68.5,"fairways":60.0,"scramble":61.0,"proxAvg":32.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":82,"tpc_sawgrass":75,"pebble":78,"torrey_south":80,"riviera":78,"valhalla":85,"pinehurst_2":88,"royal_troon":80,"quail_hollow":82,"east_lake":82,"bay_hill":78,"harbour_town":68,"colonial":70,"memorial":82,"tpc_scottsdale":80},"augustaHistory":{"appearances":8,"bestFinish":2,"cuts":7,"top10":3,"avgScore":71.3},"notes":"4x major champion. LIV — major invitee."},
        {"id":93,"name":"Dustin Johnson","rank":18,"liv":True,"sgTotal":1.10,"sgOtt":0.80,"sgApp":0.30,"sgArg":0.05,"sgPutt":-0.05,"birdieAvg":4.6,"bogeyAvg":2.4,"scoringAvg":69.8,"gir":68.0,"fairways":58.0,"scramble":59.0,"proxAvg":32.5,"missDir":"right","flight":"high_fade","courseFit":{"augusta":88,"tpc_sawgrass":80,"pebble":82,"torrey_south":85,"riviera":80,"valhalla":85,"pinehurst_2":80,"royal_troon":78,"quail_hollow":85,"east_lake":82,"bay_hill":82,"harbour_town":72,"colonial":72,"memorial":85,"tpc_scottsdale":85},"augustaHistory":{"appearances":13,"bestFinish":1,"cuts":12,"top10":5,"avgScore":70.8},"notes":"2020 Masters champion. LIV — major invitee."},
        {"id":94,"name":"Jon Rahm","rank":6,"liv":True,"sgTotal":1.60,"sgOtt":0.55,"sgApp":0.72,"sgArg":0.22,"sgPutt":0.11,"birdieAvg":4.9,"bogeyAvg":2.3,"scoringAvg":69.1,"gir":69.5,"fairways":62.0,"scramble":62.0,"proxAvg":31.5,"missDir":"left","flight":"mid_draw","courseFit":{"augusta":90,"tpc_sawgrass":82,"pebble":88,"torrey_south":90,"riviera":88,"valhalla":85,"pinehurst_2":82,"royal_troon":85,"quail_hollow":88,"east_lake":85,"bay_hill":82,"harbour_town":78,"colonial":78,"memorial":88,"tpc_scottsdale":85},"augustaHistory":{"appearances":7,"bestFinish":1,"cuts":7,"top10":4,"avgScore":70.2},"notes":"2023 Masters champion. LIV — major invitee."},
        {"id":95,"name":"Patrick Reed","rank":45,"liv":True,"sgTotal":0.55,"sgOtt":0.30,"sgApp":0.20,"sgArg":0.15,"sgPutt":-0.10,"birdieAvg":4.0,"bogeyAvg":2.3,"scoringAvg":70.7,"gir":66.5,"fairways":62.0,"scramble":60.0,"proxAvg":34.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":82,"tpc_sawgrass":72,"pebble":72,"torrey_south":70,"riviera":72,"valhalla":75,"pinehurst_2":72,"royal_troon":70,"quail_hollow":72,"east_lake":78,"bay_hill":72,"harbour_town":68,"colonial":68,"memorial":72,"tpc_scottsdale":72},"augustaHistory":{"appearances":9,"bestFinish":1,"cuts":8,"top10":2,"avgScore":71.5},"notes":"2018 Masters champion. LIV — major invitee."},
        {"id":98,"name":"Cameron Smith","rank":15,"liv":True,"sgTotal":1.25,"sgOtt":0.55,"sgApp":0.50,"sgArg":0.25,"sgPutt":0.30,"birdieAvg":4.8,"bogeyAvg":2.2,"scoringAvg":69.5,"gir":67.5,"fairways":60.0,"scramble":62.0,"proxAvg":32.0,"missDir":"left","flight":"mid_draw","courseFit":{"augusta":82,"tpc_sawgrass":80,"pebble":85,"torrey_south":78,"riviera":80,"valhalla":80,"pinehurst_2":75,"royal_troon":88,"quail_hollow":80,"east_lake":80,"bay_hill":78,"harbour_town":82,"colonial":75,"memorial":80,"tpc_scottsdale":80},"notes":"2022 Open Championship winner. Elite putter. LIV — major invitee via past champion/OWGR."},
        {"id":99,"name":"Tyrrell Hatton","rank":22,"liv":True,"sgTotal":1.15,"sgOtt":0.40,"sgApp":0.58,"sgArg":0.18,"sgPutt":0.20,"birdieAvg":4.5,"bogeyAvg":2.3,"scoringAvg":69.8,"gir":68.0,"fairways":62.0,"scramble":60.0,"proxAvg":33.0,"missDir":"left","flight":"mid_draw","courseFit":{"augusta":75,"tpc_sawgrass":72,"pebble":80,"torrey_south":78,"riviera":80,"valhalla":78,"pinehurst_2":72,"royal_troon":82,"quail_hollow":78,"east_lake":75,"bay_hill":78,"harbour_town":75,"colonial":72,"memorial":75,"tpc_scottsdale":75},"notes":"Ryder Cup star, multiple DP World Tour wins. LIV — major invitee via OWGR."},
        {"id":100,"name":"Joaquin Niemann","rank":28,"liv":True,"sgTotal":1.20,"sgOtt":0.65,"sgApp":0.45,"sgArg":0.12,"sgPutt":0.10,"birdieAvg":4.7,"bogeyAvg":2.3,"scoringAvg":69.6,"gir":67.5,"fairways":62.0,"scramble":59.0,"proxAvg":32.5,"missDir":"left","flight":"high_draw","courseFit":{"augusta":78,"tpc_sawgrass":72,"pebble":78,"torrey_south":80,"riviera":80,"valhalla":78,"pinehurst_2":72,"royal_troon":75,"quail_hollow":78,"east_lake":75,"bay_hill":78,"harbour_town":70,"colonial":70,"memorial":78,"tpc_scottsdale":80},"notes":"Chilean bomber, long off tee. LIV — major invitee via OWGR."},
        {"id":101,"name":"Phil Mickelson","rank":150,"liv":True,"sgTotal":-0.50,"sgOtt":-0.10,"sgApp":-0.15,"sgArg":0.05,"sgPutt":-0.30,"birdieAvg":3.2,"bogeyAvg":2.8,"scoringAvg":71.5,"gir":62.0,"fairways":55.0,"scramble":58.0,"proxAvg":36.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":75,"tpc_sawgrass":65,"pebble":78,"torrey_south":68,"riviera":70,"valhalla":72,"pinehurst_2":68,"royal_troon":70,"quail_hollow":68,"east_lake":65,"bay_hill":68,"harbour_town":65,"colonial":68,"memorial":68,"tpc_scottsdale":70},"notes":"6x major champion. 54 years old, declining stats. Past champion exemption. Betting prop avoid."},
        {"id":102,"name":"Louis Oosthuizen","rank":55,"liv":True,"sgTotal":0.65,"sgOtt":0.40,"sgApp":0.25,"sgArg":0.08,"sgPutt":0.02,"birdieAvg":4.0,"bogeyAvg":2.3,"scoringAvg":70.8,"gir":66.0,"fairways":62.0,"scramble":58.0,"proxAvg":34.0,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":80,"tpc_sawgrass":70,"pebble":75,"torrey_south":72,"riviera":72,"valhalla":75,"pinehurst_2":72,"royal_troon":85,"quail_hollow":72,"east_lake":72,"bay_hill":72,"harbour_town":70,"colonial":68,"memorial":72,"tpc_scottsdale":70},"notes":"2010 Open champion. Silky ball-striker, multiple runner-up finishes in majors. LIV — major invitee."},
        # ---- ADDITIONAL FIELD QUALIFIERS ----
        {"id":96,"name":"Marco Penge","rank":120,"sgTotal":0.05,"sgOtt":0.12,"sgApp":0.02,"sgArg":-0.02,"sgPutt":-0.07,"birdieAvg":3.6,"bogeyAvg":2.3,"scoringAvg":71.8,"gir":65.0,"fairways":63.0,"scramble":56.0,"proxAvg":35.5,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":58,"tpc_sawgrass":60,"pebble":62,"torrey_south":60,"riviera":60,"valhalla":62,"pinehurst_2":62,"royal_troon":65,"quail_hollow":60,"east_lake":58,"bay_hill":58,"harbour_town":62,"colonial":62,"memorial":60,"tpc_scottsdale":60},"notes":"English DP World Tour graduate. Steady but unspectacular. Still building PGA Tour résumé."},
        {"id":97,"name":"Jacob Bridgeman","rank":115,"sgTotal":0.08,"sgOtt":0.15,"sgApp":0.05,"sgArg":-0.03,"sgPutt":-0.09,"birdieAvg":3.7,"bogeyAvg":2.3,"scoringAvg":71.7,"gir":65.5,"fairways":62.0,"scramble":56.0,"proxAvg":35.5,"missDir":"left","flight":"mid_draw","courseFit":{"augusta":60,"tpc_sawgrass":62,"pebble":60,"torrey_south":62,"riviera":62,"valhalla":65,"pinehurst_2":62,"royal_troon":60,"quail_hollow":65,"east_lake":62,"bay_hill":62,"harbour_town":62,"colonial":65,"memorial":62,"tpc_scottsdale":65},"notes":"Former Clemson standout. Athletic young player building tour experience. High ceiling but still raw."},
    ]


# ============================================================
# COURSE DATA (Curated — based on real historic scoring)
# ============================================================
# This data is manually curated from free sources: DataGolf past results,
# PGA Tour hole-by-hole scoring, and GolfStats.com archives.
# Update annually or when you add new courses.

def get_course_data():
    """Return curated course data for major 2025-2026 PGA Tour venues."""
    ts = datetime.now().strftime("%Y-%m-%d")
    src = "Compiled from DataGolf, PGATour.com, and GolfStats.com"
    return {
        "augusta": {"name": "Augusta National", "event": "The Masters", "par": 72, "yards": 7545, "lastUpdated": ts, "source": src},
        "tpc_sawgrass": {"name": "TPC Sawgrass", "event": "THE PLAYERS Championship", "par": 72, "yards": 7256, "lastUpdated": ts, "source": src},
        "pebble": {"name": "Pebble Beach Golf Links", "event": "AT&T Pebble Beach Pro-Am", "par": 72, "yards": 6972, "lastUpdated": ts, "source": src},
        "torrey_south": {"name": "Torrey Pines South", "event": "Farmers Insurance Open", "par": 72, "yards": 7765, "lastUpdated": ts, "source": src},
        "riviera": {"name": "Riviera Country Club", "event": "Genesis Invitational", "par": 71, "yards": 7349, "lastUpdated": ts, "source": src},
        "valhalla": {"name": "Valhalla Golf Club", "event": "PGA Championship", "par": 72, "yards": 7456, "lastUpdated": ts, "source": src},
        "pinehurst_2": {"name": "Pinehurst No. 2", "event": "US Open", "par": 70, "yards": 7548, "lastUpdated": ts, "source": src},
        "royal_troon": {"name": "Royal Troon", "event": "The Open Championship", "par": 71, "yards": 7385, "lastUpdated": ts, "source": src},
        "quail_hollow": {"name": "Quail Hollow Club", "event": "Wells Fargo Championship", "par": 71, "yards": 7554, "lastUpdated": ts, "source": src},
        "east_lake": {"name": "East Lake Golf Club", "event": "TOUR Championship", "par": 72, "yards": 7346, "lastUpdated": ts, "source": src},
        "bay_hill": {"name": "Bay Hill Club & Lodge", "event": "Arnold Palmer Invitational", "par": 72, "yards": 7466, "lastUpdated": ts, "source": src},
        "harbour_town": {"name": "Harbour Town Golf Links", "event": "RBC Heritage", "par": 71, "yards": 7188, "lastUpdated": ts, "source": src},
        "colonial": {"name": "Colonial Country Club", "event": "Charles Schwab Challenge", "par": 70, "yards": 7209, "lastUpdated": ts, "source": src},
        "memorial": {"name": "Muirfield Village Golf Club", "event": "Memorial Tournament", "par": 72, "yards": 7543, "lastUpdated": ts, "source": src},
        "tpc_scottsdale": {"name": "TPC Scottsdale", "event": "WM Phoenix Open", "par": 71, "yards": 7261, "lastUpdated": ts, "source": src},
        # --- 2026 Major venues (Course tab now shows real metadata even
        # when BDL hole intel for these courses hasn't been ingested yet) ---
        "aronimink": {
            "name": "Aronimink Golf Club", "event": "2026 PGA Championship",
            "par": 70, "yards": 7267, "designer": "Donald Ross",
            "location": "Newtown Square, PA", "yearBuilt": 1926,
            "notes": "Donald Ross design (1926 layout, restored 2018). Classic par-70 with small undulating greens and demanding approach shots. Hosted 1962 PGA Championship and 2010 AT&T National. Premium on iron play, putting, and scrambling — accuracy off the tee less critical than at most majors.",
            "keyHoles": [
                {"hole": 16, "par": 4, "yards": 462, "note": "Long par-4 dogleg-left; one of the toughest holes in the closing stretch."},
                {"hole": 18, "par": 4, "yards": 489, "note": "Uphill finisher with a wide fairway but demanding second shot to a small green."}
            ],
            "lastUpdated": ts, "source": src,
        },
        "shinnecock": {
            "name": "Shinnecock Hills Golf Club", "event": "2026 U.S. Open",
            "par": 70, "yards": 7445, "designer": "William Flynn (1931 redesign)",
            "location": "Southampton, NY", "yearBuilt": 1891,
            "notes": "Links-style major venue, five-time U.S. Open host. Premium on wind management, accuracy off the tee, and creative short game. Firm fast greens, revetted bunkers, fescue rough.",
            "lastUpdated": ts, "source": src,
        },
        "royal_birkdale": {
            "name": "Royal Birkdale Golf Club", "event": "2026 The Open Championship",
            "par": 70, "yards": 7165, "designer": "George Lowe / F.G. Hawtree redesign",
            "location": "Southport, England", "yearBuilt": 1889,
            "notes": "Traditional links among the dunes. Tenth Open Championship host. Punishes wayward driving and rewards controlled ball flight. Strong putters and shotmakers historically rise here.",
            "lastUpdated": ts, "source": src,
        },
    }


# ============================================================
# HELPERS
# ============================================================

def safe_float(val, default=0.0):
    """Safely convert a value to float."""
    if val is None:
        return default
    try:
        # Remove common non-numeric chars
        if isinstance(val, str):
            val = val.replace(",", "").replace("%", "").replace("+", "").strip()
            if val in ("", "-", "--", "N/A", "E"):
                return default
        return float(val)
    except (ValueError, TypeError):
        return default


# ============================================================
# FEATURE: WEATHER (Open-Meteo — free, no key)
# ============================================================

COURSE_COORDS = {
    "augusta": (33.503, -82.022),
    "tpc_sawgrass": (30.198, -81.394),
    "pebble": (36.568, -121.950),
    "torrey_south": (32.896, -117.252),
    "riviera": (34.049, -118.502),
    "valhalla": (38.253, -85.498),
    "pinehurst_2": (35.191, -79.470),
    "royal_troon": (55.543, -4.851),
    "quail_hollow": (35.103, -80.848),
    "east_lake": (33.740, -84.335),
    "bay_hill": (28.460, -81.506),
    "harbour_town": (32.137, -80.818),
    "colonial": (32.730, -97.392),
    "memorial": (40.089, -83.177),
    "tpc_scottsdale": (33.639, -111.906),
    # 2026 Major venues
    "aronimink": (39.971, -75.394),       # Newtown Square, PA — 2026 PGA Championship
    "shinnecock": (40.890, -72.428),      # Southampton, NY — 2026 U.S. Open
    "royal_birkdale": (53.633, -3.027),   # Southport, England — 2026 The Open
}

# Reverse lookup: match ESPN venue names to course keys
VENUE_ALIASES = {
    "augusta national": "augusta", "masters": "augusta",
    "tpc sawgrass": "tpc_sawgrass", "players": "tpc_sawgrass",
    "pebble beach": "pebble",
    "torrey pines": "torrey_south",
    "riviera": "riviera", "genesis": "riviera",
    "valhalla": "valhalla",
    "pinehurst": "pinehurst_2",
    "royal troon": "royal_troon", "troon": "royal_troon",
    "quail hollow": "quail_hollow",
    "east lake": "east_lake",
    "bay hill": "bay_hill", "arnold palmer": "bay_hill",
    "harbour town": "harbour_town", "harbor town": "harbour_town",
    "colonial": "colonial",
    "muirfield village": "memorial", "memorial": "memorial",
    "tpc scottsdale": "tpc_scottsdale", "phoenix open": "tpc_scottsdale",

    # --- 2026 PGA Tour schedule coverage (added) ---
    # 3M Open — TPC Twin Cities, Blaine, MN
    "tpc twin cities": "tpc_twin_cities", "3m open": "tpc_twin_cities",
    # American Express — PGA West / La Quinta CC, La Quinta, CA
    "pga west": "pga_west", "american express": "pga_west", "la quinta": "pga_west",
    # Bank of Utah Championship — Black Desert Resort, Ivins, UT
    "black desert": "black_desert", "bank of utah": "black_desert",
    # Baycurrent Classic — Yokohama Country Club, Japan
    "yokohama country club": "yokohama", "baycurrent": "yokohama",
    # Biltmore Championship — The Cliffs at Walnut Cove, Asheville, NC
    "walnut cove": "walnut_cove", "biltmore championship": "walnut_cove",
    # Butterfield Bermuda Championship — Port Royal Golf Course, Southampton
    "port royal": "port_royal", "bermuda championship": "port_royal",
    # Cadillac Championship — Trump National Doral (Blue Monster), Miami, FL
    "doral": "doral", "blue monster": "doral", "cadillac championship": "doral",
    # CJ Cup Byron Nelson — TPC Craig Ranch, McKinney, TX
    "tpc craig ranch": "tpc_craig_ranch", "byron nelson": "tpc_craig_ranch",
    # Cognizant Classic — PGA National (Champion), Palm Beach Gardens, FL
    "pga national": "pga_national", "cognizant classic": "pga_national",
    # Corales Puntacana Championship — Corales GC, Punta Cana, DR
    "corales": "corales", "puntacana": "corales",
    # FedEx St. Jude Championship — TPC Southwind, Memphis, TN
    "tpc southwind": "tpc_southwind", "st. jude": "tpc_southwind", "st jude": "tpc_southwind",
    # Genesis Scottish Open — The Renaissance Club, North Berwick, Scotland
    "renaissance club": "renaissance", "scottish open": "renaissance",
    # Good Good Championship — Omni Barton Creek, Austin, TX
    "barton creek": "barton_creek", "good good": "barton_creek",
    # Houston Open — Memorial Park GC, Houston, TX
    "memorial park": "memorial_park", "houston open": "memorial_park",
    # ISCO Championship — Keene Trace GC, Nicholasville, KY
    "keene trace": "keene_trace", "isco championship": "keene_trace",
    # John Deere Classic — TPC Deere Run, Silvis, IL
    "tpc deere run": "tpc_deere_run", "deere run": "tpc_deere_run", "john deere": "tpc_deere_run",
    # Myrtle Beach Classic — Dunes Golf & Beach Club (rotates; alias "myrtle beach")
    "myrtle beach": "myrtle_beach", "dunes golf": "myrtle_beach",
    # Open Championship — Royal Birkdale, Southport, England (2026)
    "royal birkdale": "royal_birkdale", "birkdale": "royal_birkdale",
    "the open championship": "royal_birkdale", "open championship": "royal_birkdale",
    # PGA Championship — Aronimink GC, Newtown Square, PA (2026)
    "aronimink": "aronimink", "pga championship": "aronimink",
    # Puerto Rico Open — Grand Reserve GC, Rio Grande, PR
    "grand reserve": "grand_reserve", "puerto rico open": "grand_reserve",
    # RBC Canadian Open — TPC Toronto at Osprey Valley, Caledon, ON (2026)
    "osprey valley": "osprey_valley", "tpc toronto": "osprey_valley", "canadian open": "osprey_valley",
    # Rocket Classic — Detroit Golf Club, Detroit, MI
    "detroit golf": "detroit_gc", "rocket classic": "detroit_gc", "rocket mortgage": "detroit_gc",
    # RSM Classic — Sea Island Resort (Seaside/Plantation), St. Simons, GA
    "sea island": "sea_island", "rsm classic": "sea_island",
    # Sony Open — Waialae Country Club, Honolulu, HI
    "waialae": "waialae", "sony open": "waialae",
    # Travelers Championship — TPC River Highlands, Cromwell, CT
    "tpc river highlands": "tpc_river_highlands", "river highlands": "tpc_river_highlands",
    "travelers championship": "tpc_river_highlands",
    # Truist Championship — Quail Hollow uses existing "quail hollow" alias above
    "truist championship": "quail_hollow",
    # U.S. Open — Shinnecock Hills, Southampton, NY (2026)
    "shinnecock": "shinnecock", "u.s. open": "shinnecock", "us open": "shinnecock",
    # Valero Texas Open — TPC San Antonio (Oaks), San Antonio, TX
    "tpc san antonio": "tpc_san_antonio", "valero texas": "tpc_san_antonio",
    # Valspar Championship — Innisbrook (Copperhead), Palm Harbor, FL
    "innisbrook": "innisbrook", "copperhead": "innisbrook", "valspar": "innisbrook",
    # VidantaWorld Mexico Open — VidantaWorld, Nuevo Nayarit, MX
    "vidantaworld": "vidanta", "vidanta": "vidanta", "mexico open": "vidanta",
    # World Wide Technology Championship — El Cardonal at Diamante, Cabo San Lucas
    "el cardonal": "el_cardonal", "diamante": "el_cardonal",
    "world wide technology": "el_cardonal",
    # Wyndham Championship — Sedgefield Country Club, Greensboro, NC
    "sedgefield": "sedgefield", "wyndham championship": "sedgefield",
    # Zurich Classic — TPC Louisiana, Avondale, LA
    "tpc louisiana": "tpc_louisiana", "zurich classic": "tpc_louisiana",
    # BMW Championship 2026 — Bellerive CC, St. Louis, MO (rotates)
    "bellerive": "bellerive", "bmw championship": "bellerive",
}


def match_venue_to_course(venue_name, event_name=""):
    """Fuzzy match an ESPN venue/event name to our course key."""
    combined = (venue_name + " " + event_name).lower()
    for alias, key in VENUE_ALIASES.items():
        if alias in combined:
            return key
    return None


# Major-tournament venues across recent + upcoming cycles
_MAJOR_VENUES = {
    "augusta", "augusta national",                          # Masters
    "valhalla", "quail hollow", "oak hill", "southern hills", "aronimink",  # PGA Championship (2026: Aronimink)
    "pinehurst", "pinehurst no. 2", "oakmont", "los angeles country club", "shinnecock", "shinnecock hills",  # US Open (2026: Shinnecock)
    "royal troon", "royal portrush", "royal liverpool", "hoylake", "st andrews", "st. andrews", "royal st georges", "royal birkdale",  # Open Championship (2026: Royal Birkdale)
}

_MAJOR_NAME_KEYWORDS = (
    "masters",
    "pga championship",
    "u.s. open", "us open",
    "the open championship", "british open", "open championship",
)


def load_model_params(base_dir=None):
    """Load tuned model parameters from model_params.json (written by backtest).
    Falls back to defaults if the file is missing — keeps scraper working on
    fresh clones or before the first backtest run.

    May also contain a ``calibration`` block written by scripts/calibrate.py:
    isotonic lookup table mapping raw confScore → calibrated make-cut prob.
    """
    base_dir = base_dir or os.path.dirname(os.path.abspath(__file__))
    defaults = {
        "baseStd": 2.85,
        "roundShockStd": 1.0,
        "sgBlendSeason": 0.7,
        "sgBlendLive": 0.3,
        "fitBoostScale": 25.0,
        "parBaseline": 71.0,
        "hotFormBoost": 0.25,
        "coldFormPenalty": -0.25,
        "windPenaltySlope": 0.08,
        "windStdSlope": 0.05,
        "windThresholdMph": 12.0,
        # Copula correlation parameters for per-hole sim.
        # rhoGlobal = share of variance attributable to a single round-wide
        # momentum factor (replaces the old momentum-std parameter).
        # rhoLocal = AR(1) correlation in the local factor (adjacent-hole
        # correlation beyond what global momentum already creates).
        # Calibrated empirically so total round-score std lands ~3.2 on a
        # par-72 course — matching the PGA-tour round-to-round empirical
        # range (2.9-3.5). Higher values blow up round variance; lower
        # values collapse to independent-holes baseline.
        "rhoGlobal": 0.01,
        "rhoLocal":  0.06,
        "lastTrainedAt": None,
        "lastTrainedBrier": None,
        "calibration": None,
    }
    path = os.path.join(base_dir, "model_params.json")
    if not os.path.isfile(path):
        return defaults
    try:
        with open(path, encoding="utf-8") as f:
            params = json.load(f)
        for k, v in defaults.items():
            params.setdefault(k, v)
        return params
    except (OSError, json.JSONDecodeError):
        return defaults


def lookup_calibrated_prob(score, calibration_table):
    """Piecewise-linear interpolation against a calibration table.
    Table format: [{"score": int, "prob": float}, ...] sorted by score.
    Mirrors scripts/calibrate.py:lookup_calibrated_prob.
    """
    if not calibration_table or score is None:
        return None
    if score <= calibration_table[0]["score"]:
        return calibration_table[0]["prob"]
    if score >= calibration_table[-1]["score"]:
        return calibration_table[-1]["prob"]
    for i in range(len(calibration_table) - 1):
        a, b = calibration_table[i], calibration_table[i + 1]
        if a["score"] <= score <= b["score"]:
            span = b["score"] - a["score"]
            if span == 0:
                return a["prob"]
            return a["prob"] + (b["prob"] - a["prob"]) * (score - a["score"]) / span
    return calibration_table[-1]["prob"]


def apply_confscore_calibration(players, model_params):
    """Attach ``confScoreCalibratedMakeCutProb`` to each player.

    Uses the calibration table from model_params (written by
    scripts/calibrate.py). Does nothing if no calibration is loaded — the
    raw confScore continues to work as a relative ranking signal even
    without calibration.
    """
    cal = (model_params or {}).get("calibration") or {}
    table = cal.get("table")
    if not table:
        return 0
    n_set = 0
    for p in players:
        cs = p.get("confScore")
        if not isinstance(cs, (int, float)):
            continue
        prob = lookup_calibrated_prob(cs, table)
        if prob is not None:
            p["confScoreCalibratedMakeCutProb"] = round(prob, 4)
            n_set += 1
    return n_set


def build_dynamic_majors_schedule(bdl_tournaments_data):
    """Build the next-major tab schedule from BDL's tournaments endpoint —
    eliminates the hardcoded MAJORS_SCHEDULE array in the HTML."""
    if not bdl_tournaments_data:
        return []
    tournaments = (bdl_tournaments_data.get("data")
                   if isinstance(bdl_tournaments_data, dict)
                   else bdl_tournaments_data)
    if not isinstance(tournaments, list):
        return []

    SHORT = {
        "masters": "Masters",
        "pga championship": "PGA Champ.",
        "u.s. open": "US Open", "us open": "US Open",
        "the open championship": "The Open", "open championship": "The Open",
        "british open": "The Open",
    }
    out = []
    for t in tournaments:
        name = (t.get("name") or "").strip()
        name_lc = name.lower()
        matched_short = next((s for kw, s in SHORT.items() if kw in name_lc), None)
        if not matched_short:
            continue

        # BDL returns end_date as a DISPLAY STRING like "Apr 9 - 12" — not parseable
        # as an ISO date in the browser. That was breaking getNextMajor() in the HTML
        # (new Date("Apr 9 - 12") === NaN → loop never matches → falls through to
        # schedule[0] = Masters → banner stuck on "next: Masters" after Masters ends).
        # Derive a clean ISO end date = start + 4 days (standard tournament length).
        start_raw = t.get("start_date") or ""
        start_iso = start_raw[:10] if start_raw else ""
        end_iso = ""
        try:
            start_dt = datetime.fromisoformat(start_iso)
            end_iso = (start_dt + timedelta(days=4)).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            # Fall back to whatever BDL gave if we can't parse the start
            end_iso = t.get("end_date") or ""

        out.append({
            "name": name,
            "short": matched_short,
            "venue": t.get("course_name") or "",
            "city": t.get("city") or "",
            "state": t.get("state") or "",
            "startDate": start_iso,
            "endDate": end_iso,
            "status": t.get("status") or "",
            "par": t.get("par") or 72,
            "yards": t.get("yardage") or None,
            "course_key": match_venue_to_course(t.get("course_name", ""), name) or None,
        })
    out.sort(key=lambda m: m["startDate"] or "")
    return out


def _is_major_event(current_event):
    """Return True if the current tournament is one of the 4 majors.

    NAME is the primary signal — venue alone is NOT sufficient. Quail Hollow
    hosts both PGA Championship (major) AND Truist / Wells Fargo (regular
    Signature event); Pinehurst hosts US Women's Open in non-major years; etc.
    Augusta is the only single-tenant major venue (only ever hosts the
    Masters), so it can auto-flag by venue alone.

    Without this discipline we were wrongly adding LIV players to Truist's
    field whenever Truist landed at Quail Hollow.
    """
    if not isinstance(current_event, dict):
        return False
    name = (current_event.get("name") or "").lower()
    course = (current_event.get("course") or "").lower()
    # Primary: name match
    for kw in _MAJOR_NAME_KEYWORDS:
        if kw in name:
            return True
    # Secondary: Augusta is single-tenant — venue alone confirms Masters
    if "augusta" in course:
        return True
    return False


def scrape_course_weather(course_key):
    """Fetch 3-day weather forecast for a course from Open-Meteo (free, no key)."""
    coords = COURSE_COORDS.get(course_key)
    if not coords:
        return None

    lat, lon = coords
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        f"&hourly=temperature_2m,wind_speed_10m,wind_direction_10m,precipitation_probability,rain"
        f"&temperature_unit=fahrenheit&wind_speed_unit=mph"
        f"&timezone=auto&forecast_days=3"
    )
    print(f"  Fetching weather for {course_key} ({lat}, {lon})...")
    data = fetch_json(url)
    if not data or "hourly" not in data:
        print(f"  Could not fetch weather for {course_key}")
        return None

    hourly = data["hourly"]
    temps = hourly.get("temperature_2m", [])
    winds = hourly.get("wind_speed_10m", [])
    wind_dirs = hourly.get("wind_direction_10m", [])
    rain_pcts = hourly.get("precipitation_probability", [])
    times = hourly.get("time", [])

    # Summarize into daily buckets (tournament hours: 7am-7pm)
    days = {}
    for i, t in enumerate(times):
        date = t[:10]
        hour = int(t[11:13]) if len(t) > 12 else 0
        if hour < 7 or hour > 19:
            continue
        if date not in days:
            days[date] = {"temps": [], "winds": [], "wind_dirs": [], "rain_pcts": []}
        if i < len(temps): days[date]["temps"].append(temps[i])
        if i < len(winds): days[date]["winds"].append(winds[i])
        if i < len(wind_dirs): days[date]["wind_dirs"].append(wind_dirs[i])
        if i < len(rain_pcts): days[date]["rain_pcts"].append(rain_pcts[i])

    forecast = []
    for date, d in sorted(days.items()):
        forecast.append({
            "date": date,
            "tempHigh": round(max(d["temps"]), 1) if d["temps"] else None,
            "tempLow": round(min(d["temps"]), 1) if d["temps"] else None,
            "windAvg": round(sum(d["winds"]) / len(d["winds"]), 1) if d["winds"] else None,
            "windMax": round(max(d["winds"]), 1) if d["winds"] else None,
            "windDir": round(sum(d["wind_dirs"]) / len(d["wind_dirs"])) if d["wind_dirs"] else None,
            "rainPct": round(max(d["rain_pcts"]), 0) if d["rain_pcts"] else None,
        })

    print(f"  Got {len(forecast)}-day forecast for {course_key}")
    return forecast[:3]


# ============================================================
# FEATURE: COURSE-PLAYER FIT ALGORITHM
# ============================================================

COURSE_TRAITS = {
    # Each course has 9 trait dimensions:
    # power/accuracy/scramble/putting = skill weighting (existing)
    # fairway_width   = 0-1 (0=very narrow/tree-lined, 1=very wide/links)
    # gir_difficulty  = 0-1 (0=easy greens to hit, 1=very hard due to slope/approach angle)
    # birdie_rate     = avg birdies/round for the field (historical)
    # bogey_rate      = avg bogeys/round for the field
    # wind_exposure   = 0-1 (0=sheltered, 1=fully exposed links wind)
    # morning_adv     = scoring adv morning tee (strokes vs afternoon average)
    # par5_count = reachable par-5s in regulation (drives par-5 scoring bonus)
    "augusta": {
        "power": 0.8, "accuracy": 0.7, "scramble": 0.9, "putting": 0.6,
        "fairway_width": 0.45, "gir_difficulty": 0.85, "birdie_rate": 3.8,
        "bogey_rate": 2.9, "wind_exposure": 0.25, "morning_adv": 0.4, "par5_count": 4,
    },
    "tpc_sawgrass": {
        "power": 0.5, "accuracy": 0.8, "scramble": 0.7, "putting": 0.8,
        "fairway_width": 0.50, "gir_difficulty": 0.70, "birdie_rate": 4.1,
        "bogey_rate": 3.1, "wind_exposure": 0.60, "morning_adv": 0.3, "par5_count": 2,
    },
    "pebble": {
        "power": 0.4, "accuracy": 0.8, "scramble": 0.8, "putting": 0.7,
        "fairway_width": 0.45, "gir_difficulty": 0.75, "birdie_rate": 3.4,
        "bogey_rate": 3.2, "wind_exposure": 0.80, "morning_adv": 0.5, "par5_count": 2,
    },
    "torrey_south": {
        "power": 0.8, "accuracy": 0.6, "scramble": 0.6, "putting": 0.5,
        "fairway_width": 0.60, "gir_difficulty": 0.65, "birdie_rate": 4.3,
        "bogey_rate": 2.7, "wind_exposure": 0.50, "morning_adv": 0.2, "par5_count": 3,
    },
    "riviera": {
        "power": 0.6, "accuracy": 0.8, "scramble": 0.7, "putting": 0.7,
        "fairway_width": 0.50, "gir_difficulty": 0.75, "birdie_rate": 3.9,
        "bogey_rate": 2.8, "wind_exposure": 0.40, "morning_adv": 0.3, "par5_count": 2,
    },
    "valhalla": {
        "power": 0.9, "accuracy": 0.5, "scramble": 0.5, "putting": 0.5,
        "fairway_width": 0.65, "gir_difficulty": 0.60, "birdie_rate": 4.4,
        "bogey_rate": 2.6, "wind_exposure": 0.35, "morning_adv": 0.2, "par5_count": 3,
    },
    "pinehurst_2": {
        "power": 0.5, "accuracy": 0.9, "scramble": 0.9, "putting": 0.7,
        "fairway_width": 0.65, "gir_difficulty": 0.90, "birdie_rate": 3.2,
        "bogey_rate": 3.5, "wind_exposure": 0.55, "morning_adv": 0.4, "par5_count": 1,
    },
    "royal_troon": {
        "power": 0.6, "accuracy": 0.8, "scramble": 0.8, "putting": 0.6,
        "fairway_width": 0.70, "gir_difficulty": 0.80, "birdie_rate": 3.3,
        "bogey_rate": 3.4, "wind_exposure": 0.90, "morning_adv": 0.6, "par5_count": 1,
    },
    "royal_portrush": {
        "power": 0.6, "accuracy": 0.8, "scramble": 0.8, "putting": 0.6,
        "fairway_width": 0.65, "gir_difficulty": 0.78, "birdie_rate": 3.4,
        "bogey_rate": 3.3, "wind_exposure": 0.95, "morning_adv": 0.6, "par5_count": 1,
    },
    "oakmont": {
        "power": 0.5, "accuracy": 0.9, "scramble": 0.9, "putting": 0.9,
        "fairway_width": 0.30, "gir_difficulty": 0.95, "birdie_rate": 2.8,
        "bogey_rate": 4.0, "wind_exposure": 0.45, "morning_adv": 0.5, "par5_count": 1,
    },
    "quail_hollow": {
        "power": 0.8, "accuracy": 0.6, "scramble": 0.6, "putting": 0.6,
        "fairway_width": 0.55, "gir_difficulty": 0.65, "birdie_rate": 4.2,
        "bogey_rate": 2.8, "wind_exposure": 0.30, "morning_adv": 0.2, "par5_count": 3,
    },
    "east_lake": {
        "power": 0.6, "accuracy": 0.7, "scramble": 0.7, "putting": 0.7,
        "fairway_width": 0.55, "gir_difficulty": 0.70, "birdie_rate": 3.9,
        "bogey_rate": 2.8, "wind_exposure": 0.35, "morning_adv": 0.2, "par5_count": 2,
    },
    "bay_hill": {
        "power": 0.7, "accuracy": 0.7, "scramble": 0.6, "putting": 0.6,
        "fairway_width": 0.50, "gir_difficulty": 0.70, "birdie_rate": 4.0,
        "bogey_rate": 2.9, "wind_exposure": 0.45, "morning_adv": 0.3, "par5_count": 2,
    },
    "harbour_town": {
        "power": 0.2, "accuracy": 0.9, "scramble": 0.7, "putting": 0.8,
        "fairway_width": 0.30, "gir_difficulty": 0.70, "birdie_rate": 3.7,
        "bogey_rate": 2.6, "wind_exposure": 0.50, "morning_adv": 0.3, "par5_count": 2,
    },
    "colonial": {
        "power": 0.3, "accuracy": 0.9, "scramble": 0.7, "putting": 0.8,
        "fairway_width": 0.35, "gir_difficulty": 0.72, "birdie_rate": 3.8,
        "bogey_rate": 2.7, "wind_exposure": 0.40, "morning_adv": 0.3, "par5_count": 2,
    },
    "memorial": {
        "power": 0.7, "accuracy": 0.8, "scramble": 0.7, "putting": 0.6,
        "fairway_width": 0.50, "gir_difficulty": 0.75, "birdie_rate": 3.9,
        "bogey_rate": 2.8, "wind_exposure": 0.30, "morning_adv": 0.2, "par5_count": 2,
    },
    "tpc_scottsdale": {
        "power": 0.6, "accuracy": 0.6, "scramble": 0.5, "putting": 0.7,
        "fairway_width": 0.70, "gir_difficulty": 0.50, "birdie_rate": 5.2,
        "bogey_rate": 2.3, "wind_exposure": 0.55, "morning_adv": 0.3, "par5_count": 2,
    },
}


def compute_player_similarity(players, k=5, min_features=3):
    """For each player, find the ``k`` most similar peers by SG profile.

    Feature vector: [sgOtt, sgApp, sgArg, sgPutt, drivingDistance, scoringAvg].
    Each dimension is z-scored against the field so Euclidean distance is
    meaningful across features with different units (SG in strokes, driving
    distance in yards, etc.).

    Useful for two things:
      1. Rookies / first-timers at a course — find similar players who HAVE
         played the course and infer expected fit.
      2. UX: "players similar to X" gives users a navigation aid plus
         intuition about who the model considers comparable.

    Attaches to each player as ``similarPlayers: [{name, distance}, ...]``
    (k entries, nearest first). Players with fewer than ``min_features``
    non-null stats are skipped to avoid bad neighbors from sparse profiles.
    """
    if not players or len(players) < k + 1:
        return 0

    feature_keys = ["sgOtt", "sgApp", "sgArg", "sgPutt", "drivingDistance", "scoringAvg"]
    # Gather column values for z-score normalization
    col_vals = {k: [] for k in feature_keys}
    for p in players:
        for fk in feature_keys:
            v = p.get(fk)
            if isinstance(v, (int, float)):
                col_vals[fk].append(float(v))
    # Compute mean + std per column (need >=3 values to be meaningful)
    col_mean = {}
    col_std = {}
    for fk, vs in col_vals.items():
        if len(vs) < 3:
            continue
        mean = sum(vs) / len(vs)
        var = sum((x - mean) ** 2 for x in vs) / len(vs)
        std = var ** 0.5 if var > 0 else 1.0
        col_mean[fk] = mean
        col_std[fk] = std

    # Build per-player feature vector (None for missing → handled in distance)
    pvecs = []
    for p in players:
        vec = {}
        n_present = 0
        for fk in feature_keys:
            v = p.get(fk)
            if isinstance(v, (int, float)) and fk in col_std:
                vec[fk] = (float(v) - col_mean[fk]) / col_std[fk]
                n_present += 1
        pvecs.append({"player": p, "vec": vec, "n_features": n_present})

    n_blended = 0
    for i, source in enumerate(pvecs):
        if source["n_features"] < min_features:
            continue
        dists = []
        for j, target in enumerate(pvecs):
            if i == j or target["n_features"] < min_features:
                continue
            # Euclidean over the intersection of features (penalize missing)
            shared = set(source["vec"].keys()) & set(target["vec"].keys())
            if len(shared) < min_features:
                continue
            sq = sum((source["vec"][k] - target["vec"][k]) ** 2 for k in shared)
            # Normalize by the number of features used so missing-data players
            # aren't unfairly close (or far)
            dist = (sq / len(shared)) ** 0.5
            dists.append((target["player"].get("name", "?"), round(dist, 3)))
        dists.sort(key=lambda x: x[1])
        source["player"]["similarPlayers"] = [
            {"name": nm, "distance": d} for nm, d in dists[:k]
        ]
        n_blended += 1
    return n_blended


def effective_sg(p, course_weights=None):
    """Player's best skill estimate: Bayesian-updated SG if available
    (computed mid-tournament from observed round SG), else the season prior.

    When ``course_weights`` is supplied (a dict from
    ``compute_course_sg_weights`` for the active course), we additionally
    apply per-category weighting on the player's SG breakdown — augusta
    rewards SG: Approach disproportionately, Pebble rewards Putting, etc.
    The weighted version is added on top of the base sgTotal so a player
    with strong SG: Approach gets an extra boost at an approach-rewarding
    course beyond what their overall skill predicts.

    Formula when course_weights given:
        base = sgTotalUpdated or sgTotal
        # Per-category over/under-weight relative to equal (-1) weighting.
        # The course coefficients are negative (lower-is-better), so the
        # delta below is positive when the player has more skill than
        # average in the categories this course rewards.
        delta = sum((w_i + 1.0) * sg_i for category i)
                    where w_i comes from regression (typically -1.5 to -0.5)
        return base + delta * adjustment_scale

    Without course_weights, returns the simple max(updated, season) value
    as before.
    """
    if p is None:
        return 0.0
    upd = p.get("sgTotalUpdated")
    base = float(upd) if isinstance(upd, (int, float)) else (
        float(p.get("sgTotal", 0.0)) if isinstance(p.get("sgTotal"), (int, float)) else 0.0
    )
    if not course_weights:
        return base
    # Apply per-category overlay. Coefficients are strokes-per-tournament,
    # divide by ~4 rounds for per-round contribution. The "equal-weight"
    # null hypothesis is w_i = -1 (every SG stroke directly subtracts from
    # finish-par-rel). Deviations from -1 = course-specific weighting.
    sg_ott  = p.get("sgOtt",  0.0) or 0.0
    sg_app  = p.get("sgApp",  0.0) or 0.0
    sg_arg  = p.get("sgArg",  0.0) or 0.0
    sg_putt = p.get("sgPutt", 0.0) or 0.0
    delta_per_tournament = (
        (course_weights.get("ott",  -1.0) + 1.0) * sg_ott  +
        (course_weights.get("app",  -1.0) + 1.0) * sg_app  +
        (course_weights.get("arg",  -1.0) + 1.0) * sg_arg  +
        (course_weights.get("putt", -1.0) + 1.0) * sg_putt
    )
    # Sign flip: lower par-rel = better outcome = higher effective skill
    # The delta is in strokes per tournament; divide by 4 for per-round
    overlay = -delta_per_tournament / 4.0
    # Cap the overlay to avoid runaway effects from noisy small-sample fits
    overlay = max(-0.5, min(0.5, overlay))
    return base + overlay


def american_to_implied_prob(american_odds):
    """American odds → raw implied probability in [0, 1]. None on parse error.

    This is the *raw* (vigged) implied probability — i.e. the probability you'd
    need to break even at these odds. Sum across an outright field is typically
    1.15–1.30 (the overround / vig). Use ``devig_implied_prob`` to strip the
    hold before comparing to model probability.
    """
    if american_odds is None or american_odds == "":
        return None
    try:
        odds = float(american_odds)
    except (TypeError, ValueError):
        return None
    if odds == 0:
        return None
    return 100.0 / (odds + 100.0) if odds > 0 else abs(odds) / (abs(odds) + 100.0)


def compute_market_overround(odds_list):
    """Sum of raw implied probabilities across a market. >1 = book hold.

    For a fair market this returns 1.0. Typical outright winner markets on
    DK/FD return 1.20–1.30 (20–30% hold on top of ~155 players). Two-way
    over/under markets typically return ~1.045 (4.5% juice).
    """
    total = 0.0
    n = 0
    for o in odds_list:
        p = american_to_implied_prob(o)
        if p is not None:
            total += p
            n += 1
    return total if n > 0 else None


def devig_implied_prob(american_odds, overround=None, opposite_american=None):
    """De-vig book odds into a fair implied probability.

    Three modes, picked by which kwarg you pass:
      * ``overround`` (float, e.g. 1.27) — normalize against a pre-computed
        full-field sum. Use for outright markets where you have every player.
      * ``opposite_american`` (number) — pair de-vig against the opposite side
        of a two-way market (over/under). Returns this side's fair share of
        the implied probability mass.
      * neither — apply a conservative ~3.5% single-side haircut. Use only for
        milestone markets (make_cut single line) where no companion exists.

    Returns probability in [0, 1] or ``None`` if odds can't be parsed.
    """
    raw = american_to_implied_prob(american_odds)
    if raw is None:
        return None
    if overround is not None and overround > 0:
        return raw / overround
    if opposite_american is not None:
        opp = american_to_implied_prob(opposite_american)
        if opp is not None and (raw + opp) > 0:
            return raw / (raw + opp)
    # Milestone fallback: assume ~3.5% single-side hold. Conservative.
    return raw / 1.035


def calculate_ev_score(book_odds_american, model_probability_pct,
                       overround=None, opposite_american=None):
    """
    Calculate EV: positive = value bet, negative = fade.

    Critical: ``book_odds_american`` is converted to a *de-vigged* fair
    implied probability via ``devig_implied_prob`` before comparing to
    ``model_probability_pct``. Pass ``overround`` (preferred) for outright
    markets where you've summed the full field, or ``opposite_american`` for
    two-way markets. With neither, falls back to the single-side haircut.
    """
    if not book_odds_american or model_probability_pct is None:
        return None
    fair_prob = devig_implied_prob(
        book_odds_american,
        overround=overround,
        opposite_american=opposite_american,
    )
    if fair_prob is None or fair_prob <= 0:
        return None
    try:
        model_prob = float(model_probability_pct) / 100.0
    except (TypeError, ValueError):
        return None
    return round((model_prob - fair_prob) / fair_prob * 100, 1)


def build_market_overrounds(players, props_by_type=None):
    """Compute per-market overrounds from the current field of odds.

    Looks at every player's stored odds (DK preferred, FD fallback) for each
    outright market we model (winner / top5 / top10 / top20 / makeCut). Returns
    a dict the frontend can use to de-vig in real time:

        {
          "winner":  {"overround": 1.27, "bookCount": 152, "bestVendor": "dk"},
          "top5":    {"overround": 1.18, "bookCount": 145, "bestVendor": "dk"},
          ...
        }

    Markets without enough book lines (< 30 players) are omitted so the
    frontend falls back to the single-side milestone haircut.
    """
    if not players:
        return {}

    # Each player.odds typically has dk/fd/mgm at the top level (winner) and
    # nested dicts under odds.top5, odds.top10, odds.top20, odds.makeCut.
    # We collect the *best* available book per player per market.
    market_keys = [
        ("winner", None),       # winner odds live at p.odds.{dk,fd,...}
        ("top5", "top5"),
        ("top10", "top10"),
        ("top20", "top20"),
        ("makeCut", "makeCut"),
    ]
    out = {}
    for mkt, nested_key in market_keys:
        odds_list = []
        for p in players:
            odds_obj = p.get("odds") or {}
            if nested_key:
                src = odds_obj.get(nested_key) or {}
            else:
                src = odds_obj
            if not isinstance(src, dict):
                continue
            best = None
            for vendor in ("dk", "fd", "mgm", "br", "bovada"):
                v = src.get(vendor)
                if v in (None, ""):
                    continue
                try:
                    n = int(v)
                except (TypeError, ValueError):
                    continue
                if best is None or n > best:
                    best = n
            if best is not None:
                odds_list.append(best)
        if len(odds_list) < 30:
            continue  # Not enough field coverage for a reliable overround
        ov = compute_market_overround(odds_list)
        if ov and ov > 1.0:
            out[mkt] = {
                "overround": round(ov, 4),
                "bookCount": len(odds_list),
                "vigPct": round((ov - 1.0) * 100, 2),
            }
    return out


def calculate_course_fit(player, course_key, wind_avg=None):
    """Calculate a 0-100 course fit score based on player SG profile and course traits."""
    traits = COURSE_TRAITS.get(course_key)
    if not traits:
        return 65  # neutral default

    # Base score from overall skill
    score = 50 + player.get("sgTotal", 0) * 10

    # Weight SG categories by course demands
    score += player.get("sgOtt", 0) * traits["power"] * 8
    score += player.get("sgApp", 0) * traits["accuracy"] * 10
    score += player.get("sgArg", 0) * traits["scramble"] * 12
    score += player.get("sgPutt", 0) * traits["putting"] * 10

    # Wind adjustment: low ball flights benefit in wind
    if wind_avg and wind_avg > 12:
        flight = player.get("flight", "neutral")
        if flight.startswith("low_"):
            score += 4
        elif flight.startswith("high_"):
            score -= 3

    return max(20, min(99, round(score)))


def compute_all_course_fits(players, weather=None):
    """Compute course fit for all players at all courses, blending 70% algo + 30% curated."""
    wind_avg = None
    if weather and len(weather) > 0:
        wind_avg = weather[0].get("windAvg")

    for player in players:
        curated = player.get("courseFit", {})
        computed = {}
        for course_key in COURSE_TRAITS:
            algo_score = calculate_course_fit(player, course_key, wind_avg)
            curated_score = curated.get(course_key, algo_score)
            # Blend: 70% algo, 30% curated (curated captures local knowledge)
            computed[course_key] = round(algo_score * 0.7 + curated_score * 0.3)
        player["courseFit"] = computed


def _solve_linear_system(A, b):
    """Solve Ax = b for a small square matrix via Gaussian elimination with
    partial pivoting. Pure Python (no numpy in the deployment env).

    Returns the solution vector x as a list, or ``None`` if the system is
    singular / ill-conditioned. Used by ``compute_course_sg_weights`` to fit
    a 4-feature least-squares regression per course.
    """
    n = len(b)
    # Augmented matrix
    M = [list(row) + [b[i]] for i, row in enumerate(A)]
    for i in range(n):
        # Partial pivot: swap with row having max |M[r][i]| for numerical stability
        max_row = max(range(i, n), key=lambda r: abs(M[r][i]))
        if max_row != i:
            M[i], M[max_row] = M[max_row], M[i]
        if abs(M[i][i]) < 1e-10:
            return None  # singular
        for j in range(i + 1, n):
            factor = M[j][i] / M[i][i]
            for k in range(i, n + 1):
                M[j][k] -= factor * M[i][k]
    # Back substitution
    x = [0.0] * n
    for i in range(n - 1, -1, -1):
        x[i] = (M[i][n] - sum(M[i][j] * x[j] for j in range(i + 1, n))) / M[i][i]
    return x


def compute_course_sg_weights(course_id, current_field, years_back=5, max_events=5,
                              ridge_lambda=0.5):
    """Learn per-category SG weights at this course via least-squares regression.

    For each past tournament at ``course_id`` in the lookback window, pull
    (sg_off_tee, sg_approach, sg_around_green, sg_putting, par_relative_score)
    per player and stack into a single design matrix X (n × 4) and target
    vector y (n × 1, lower-is-better). Solve ridge-regularized least squares
    for the weights w that best predict finish:

        y ≈ b0 + w_ott·sg_ott + w_app·sg_app + w_arg·sg_arg + w_putt·sg_putt

    Augusta has historically loaded heavily on SG: Approach; Riviera also
    on Approach; Pebble on Putting; Whistling Straits on Driving. These
    differential weights are the substance of "course fit" — much more
    informative than a single composite sgTotal.

    Returns ``{ott, app, arg, putt, intercept, n_players, n_events, r2}``
    or ``None`` if there's not enough historical sample to be reliable
    (fewer than 30 player-events).

    Ridge regularization (``ridge_lambda``) shrinks all weights toward 1.0
    (the "naive equal-weight" prior, since each SG component is one stroke
    if the player gains a stroke in that category). Prevents over-fitting
    to small samples and protects against multicollinearity.
    """
    if not course_id:
        return None
    print(f"[COURSE WEIGHTS] Regressing SG categories at course_id={course_id}...")
    try:
        past_raw = bdl_fetch_all(
            "tournaments",
            {"course_ids[]": str(course_id), "status": "COMPLETED"},
            max_pages=3,
        )
    except Exception as e:
        print(f"  [WARN] Could not fetch past tournaments: {e}")
        return None
    if not past_raw:
        return None

    from datetime import datetime as _dt
    cutoff_year = _dt.utcnow().year - years_back
    past = sorted(past_raw, key=lambda t: t.get("start_date") or "", reverse=True)
    valid_past = []
    for t in past:
        sd = t.get("start_date") or ""
        try:
            yr = int(sd[:4]) if sd else 0
        except ValueError:
            yr = 0
        if yr >= cutoff_year and yr < _dt.utcnow().year:
            valid_past.append((t.get("id"), yr))
        if len(valid_past) >= max_events:
            break
    if not valid_past:
        print(f"  No past tournaments within last {years_back} years.")
        return None

    # Collect (sg_ott, sg_app, sg_arg, sg_putt, par_relative_score) per player-event
    X_rows = []
    y_vec = []
    n_events_used = 0
    for tid, yr in valid_past:
        try:
            sg_rows = bdl_fetch_all(
                "player_round_stats",
                {"tournament_ids[]": str(tid), "round_number": "-1"},
                max_pages=4,
            )
            res_rows = bdl_fetch_all(
                "tournament_results",
                {"tournament_ids[]": str(tid), "per_page": "100"},
                max_pages=4,
            )
        except Exception as e:
            print(f"  [WARN] Failed to fetch SG/results for tid={tid}: {e}")
            continue
        if not sg_rows or not res_rows:
            continue
        # Build par-relative lookup per player from results
        par_rel_by_name = {}
        for r in res_rows:
            player = r.get("player") or {}
            pname = player.get("display_name") or (
                f"{player.get('first_name','')} {player.get('last_name','')}".strip()
            )
            pr = r.get("par_relative_score")
            if pname and isinstance(pr, (int, float)):
                par_rel_by_name[normalize_name(pname)] = float(pr)
        # Join with SG rows
        added_this_event = 0
        for sr in sg_rows:
            player = sr.get("player") or {}
            pname = player.get("display_name") or (
                f"{player.get('first_name','')} {player.get('last_name','')}".strip()
            )
            if not pname:
                continue
            key = normalize_name(pname)
            par_rel = par_rel_by_name.get(key)
            if par_rel is None:
                continue
            sg_ott  = sr.get("sg_off_tee")
            sg_app  = sr.get("sg_approach")
            sg_arg  = sr.get("sg_around_green")
            sg_putt = sr.get("sg_putting")
            if not all(isinstance(v, (int, float)) for v in [sg_ott, sg_app, sg_arg, sg_putt]):
                continue
            X_rows.append([float(sg_ott), float(sg_app), float(sg_arg), float(sg_putt)])
            y_vec.append(par_rel)
            added_this_event += 1
        if added_this_event:
            n_events_used += 1

    n = len(y_vec)
    if n < 30:
        print(f"  Insufficient sample ({n} player-events, need 30+) — skipping course weights.")
        return None
    print(f"  Sample: {n} player-events across {n_events_used} historical events")

    # Build normal equations with intercept column. Design matrix:
    # X' = [1, sg_ott, sg_app, sg_arg, sg_putt], 5 columns, n rows.
    # Solve (X'^T X' + lambda*I) w = X'^T y  for w = [b0, w_ott, w_app, w_arg, w_putt]
    cols = 5
    XtX = [[0.0] * cols for _ in range(cols)]
    Xty = [0.0] * cols
    for i in range(n):
        row = [1.0] + X_rows[i]  # prepend intercept
        for a in range(cols):
            Xty[a] += row[a] * y_vec[i]
            for c in range(cols):
                XtX[a][c] += row[a] * row[c]
    # Ridge: add lambda to diagonal except intercept
    for d in range(1, cols):
        XtX[d][d] += ridge_lambda

    sol = _solve_linear_system(XtX, Xty)
    if sol is None:
        print("  Regression matrix singular — skipping.")
        return None
    b0, w_ott, w_app, w_arg, w_putt = sol

    # Compute R² for sanity / surfacing
    y_mean = sum(y_vec) / n
    ss_tot = sum((y - y_mean) ** 2 for y in y_vec)
    ss_res = 0.0
    for i in range(n):
        x = X_rows[i]
        pred = b0 + w_ott * x[0] + w_app * x[1] + w_arg * x[2] + w_putt * x[3]
        ss_res += (y_vec[i] - pred) ** 2
    r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

    result = {
        "intercept": round(b0, 3),
        # Coefficients are NEGATIVE when better SG predicts lower (better) score —
        # which is what we expect. Magnitude = strokes of finish-position
        # delta per 1 SG in that category.
        "ott":  round(w_ott, 3),
        "app":  round(w_app, 3),
        "arg":  round(w_arg, 3),
        "putt": round(w_putt, 3),
        "nPlayers": n,
        "nEvents": n_events_used,
        "r2": round(r2, 3),
        "ridgeLambda": ridge_lambda,
    }
    # Pretty-print which category dominates
    cats = [("OTT", w_ott), ("APP", w_app), ("ARG", w_arg), ("PUTT", w_putt)]
    cats.sort(key=lambda x: x[1])  # most-negative = most-predictive of better finish
    print(f"  Coefficients (neg = lower-finish): " + ", ".join(f"{c}={w:+.2f}" for c, w in cats))
    print(f"  R²={r2:.3f} · top driver at this course: {cats[0][0]}")
    return result


def compute_field_strength(field_owgrs, baseline_avg_owgr=70.0, cap=(0.55, 1.3)):
    """Convert a field's average OWGR into a strength multiplier.

    Strong fields (low avg OWGR) → multiplier > 1.0; weak fields → < 1.0. The
    baseline (1.0) corresponds to a typical PGA Tour field whose top-20-by-
    OWGR-rank average is roughly 50. Majors typically run ~25; opposite-field
    events ~120.

    Formula: ``strength = baseline / max(actual_avg, 5)``, then clipped to
    ``cap`` so a single outlier (or missing OWGR) can't blow up the factor.

    Returns ``(strength, observed_avg)`` or ``(1.0, None)`` when there's no
    OWGR data to work from.
    """
    vals = [o for o in field_owgrs if isinstance(o, (int, float)) and 0 < o < 1500]
    if len(vals) < 10:
        return 1.0, None
    # Use the top-quartile OWGR average to characterize the field's
    # competitive core — a few amateurs/qualifiers at OWGR 800 shouldn't
    # drag a major's strength reading.
    vals.sort()
    quartile = max(20, len(vals) // 4)
    top_quartile = vals[:quartile]
    avg = sum(top_quartile) / len(top_quartile)
    strength = baseline_avg_owgr / max(avg, 5.0)
    lo, hi = cap
    strength = max(lo, min(hi, strength))
    return round(strength, 3), round(avg, 1)


def bdl_get_field_owgrs(tournament_id, max_pages=4):
    """Pull the OWGR distribution for a specific tournament's field.

    Used by ``compute_learned_course_fit`` to weight each historical event's
    residuals by that event's strength. A +0.5 SG residual at a major-strength
    field is more impressive than the same residual at a weak opposite-field
    event; this gives us the multiplier to encode that.
    """
    try:
        rows = bdl_fetch_all(
            "tournament_field",
            {"tournament_id": str(tournament_id)},
            max_pages=max_pages,
        )
    except Exception as e:
        print(f"  [WARN] Field OWGRs fetch failed for tid={tournament_id}: {e}")
        return []
    owgrs = []
    for r in (rows or []):
        # OWGR can live at row.owgr or row.player.owgr depending on response shape
        o = r.get("owgr")
        if not isinstance(o, (int, float)):
            p = r.get("player") or {}
            o = p.get("owgr")
        if isinstance(o, (int, float)):
            owgrs.append(o)
    return owgrs


def compute_learned_course_fit(course_id, current_field, years_back=5, max_events=5):
    """Per-player learned course fit derived from historical SG residuals.

    For each past tournament at ``course_id`` in the last ``years_back`` years
    (capped at ``max_events`` events), pull every player's tournament-total SG
    via BDL ``player_round_stats`` with ``round_number=-1``. Each appearance
    produces a residual = course_SG - field_avg_SG_that_week. Averaging the
    residual across a player's appearances gives a course-specific skill
    adjustment in strokes-per-round.

    Returns ``{ normalized_name: {residual: float, n: int, events: [{tid, year, sg}]} }``.

    Players with N >= 2 appearances get a real signal; everyone else falls
    back to the trait-based prior. This is the core "DataGolf-style" course
    fit move — anchoring fit to observed outcomes rather than guessed traits.
    """
    if not course_id:
        return {}
    print(f"[LEARNED FIT] Fetching past tournaments at course_id={course_id}...")
    try:
        past_raw = bdl_fetch_all(
            "tournaments",
            {"course_ids[]": str(course_id), "status": "COMPLETED"},
            max_pages=3,
        )
    except Exception as e:
        print(f"  [WARN] Could not fetch past tournaments: {e}")
        return {}
    if not past_raw:
        print(f"  No past tournaments found at course_id={course_id}")
        return {}

    from datetime import datetime as _dt
    cutoff_year = _dt.utcnow().year - years_back
    # Sort newest-first by start_date and cap to max_events
    past = sorted(
        past_raw,
        key=lambda t: t.get("start_date") or "",
        reverse=True,
    )
    valid_past = []
    for t in past:
        sd = t.get("start_date") or ""
        try:
            yr = int(sd[:4]) if sd else 0
        except ValueError:
            yr = 0
        if yr >= cutoff_year and yr < _dt.utcnow().year:
            valid_past.append((t.get("id"), yr))
        if len(valid_past) >= max_events:
            break
    if not valid_past:
        print(f"  No past tournaments within last {years_back} years.")
        return {}
    print(f"  Pulling tournament SG totals from {len(valid_past)} past events: {valid_past}")

    # Aggregate: per player, collect (event_year, sg_total, field_avg, tid,
    # strength_factor). The strength factor scales each event's residual: a
    # +0.5 SG at a major-strength field (multiplier ~1.2) becomes effectively
    # +0.6 SG when averaging across events; the same +0.5 at an opposite-
    # field event (multiplier ~0.85) becomes +0.425. This corrects for the
    # fact that beating a weak field by X strokes is easier than beating a
    # strong one by the same margin.
    aggregate = {}  # normalized_name -> [(year, sg, field_avg, tid, strength)]
    event_strength = {}  # tid -> (strength_multiplier, observed_avg_owgr)
    for tid, yr in valid_past:
        try:
            rows = bdl_fetch_all(
                "player_round_stats",
                {"tournament_ids[]": str(tid), "round_number": "-1"},
                max_pages=4,
            )
        except Exception as e:
            print(f"  [WARN] Failed to fetch SG for tournament {tid}: {e}")
            continue
        if not rows:
            continue
        sg_values = [float(r.get("sg_total")) for r in rows
                     if isinstance(r.get("sg_total"), (int, float))]
        if not sg_values:
            continue
        field_avg = sum(sg_values) / len(sg_values)
        # Field strength for this past event (skipped on small/no OWGR data → 1.0)
        owgrs = bdl_get_field_owgrs(tid)
        strength, avg_owgr = compute_field_strength(owgrs)
        event_strength[tid] = (strength, avg_owgr)
        # Per-player rows
        for r in rows:
            sg = r.get("sg_total")
            if not isinstance(sg, (int, float)):
                continue
            player = r.get("player") or {}
            pname = player.get("display_name") or (
                f"{player.get('first_name','')} {player.get('last_name','')}".strip()
            )
            if not pname:
                continue
            key = normalize_name(pname)
            aggregate.setdefault(key, []).append(
                (yr, float(sg), field_avg, tid, strength)
            )

    # Build output: average strength-weighted residual per player in current field
    current_names = {normalize_name(p.get("name", "")) for p in current_field if p.get("name")}
    out = {}
    for key, appearances in aggregate.items():
        if key not in current_names:
            continue
        residuals = [(sg - field_avg) * strength
                     for (_, sg, field_avg, _, strength) in appearances]
        if not residuals:
            continue
        avg_residual = sum(residuals) / len(residuals)
        out[key] = {
            "residual": round(avg_residual, 3),
            "n": len(residuals),
            "events": [
                {"tid": tid, "year": yr, "sg": round(sg, 2),
                 "strength": event_strength.get(tid, (1.0, None))[0]}
                for (yr, sg, _, tid, _) in appearances
            ],
        }
    # Annotate event-level strength so methodology / debugging can see it
    if event_strength:
        sample = list(event_strength.items())[:5]
        print(f"  Field-strength factors (sample): " +
              ", ".join(f"tid={t} mult={m} avgOWGR={ao}" for t, (m, ao) in sample))
    print(f"  Learned course fit: {len(out)} players in current field with prior data")
    return out


def apply_learned_course_fit(players, learned, course_key, fit_scale=12.0,
                             trait_weight_when_no_learned=1.0):
    """Blend learned course fit residuals into each player's courseFit[course_key].

    The residual is in strokes-per-round (typically -0.5 to +0.5). Convert to
    a fit-score delta by multiplying by ``fit_scale``, then blend with the
    existing trait-based score using sample-size shrinkage:

        weight_learned = n / (n + 2)   (Bayesian shrinkage to prior, k=2)
        new_fit = weight_learned * (trait + delta) + (1 - weight_learned) * trait

    A player with n=2 events gets 50% weight on the learned signal; n=5 gets
    71%; n=1 gets 33%. This anchors course fit in outcomes while protecting
    against tiny samples.
    """
    if not learned or not course_key:
        return 0
    n_changed = 0
    for player in players:
        key = normalize_name(player.get("name", ""))
        entry = learned.get(key)
        if not entry:
            continue
        cf = player.get("courseFit") or {}
        prior = cf.get(course_key, 70)
        n = entry["n"]
        weight = n / (n + 2.0)  # k=2 shrinkage
        delta = entry["residual"] * fit_scale
        new_score = round(weight * (prior + delta) + (1 - weight) * prior)
        new_score = max(20, min(99, new_score))
        cf[course_key] = new_score
        player["courseFit"] = cf
        # Annotate so frontend / methodology can show learned vs prior
        lf = player.setdefault("learnedCourseFit", {})
        lf[course_key] = {
            "residual": entry["residual"],
            "n": n,
            "prior": prior,
            "learned": new_score,
            "weight": round(weight, 2),
        }
        n_changed += 1
    return n_changed


# ============================================================
# FEATURE: HISTORICAL DATA ARCHIVING
# ============================================================

SNAPSHOT_VERSION = 1  # bump when the archived schema changes in a breaking way


def compute_clv_proxy(players, base_dir, lookback_hours=24, edge_threshold=2.0):
    """Closing-line value PROXY using only odds_history.json (no API calls).

    For each player with a meaningful model edge ``|edgeScore| >= edge_threshold``,
    look back ``lookback_hours`` in the existing odds history. If the implied
    probability moved in the direction the model predicted (favorites shortened
    for model picks, lengthened for fades), that's a CLV-positive observation.
    Sharp models target >55% CLV-positive on positive-edge bets.

    Computed entirely from data we already snapshot. Adds:
      - ``output["clvSummary"]`` aggregate stats
      - ``player["clvLineMoveBp"]`` per-player line move in basis points
      - ``player["clvPositive"]`` boolean (only for players with edge threshold)

    A sharp's read on the model: if ``clvPositivePct`` consistently > 55%, the
    picks beat the close — the strongest single signal that the model has
    predictive skill (stronger than win/loss because line movement is
    incrementally re-priced by sharp money, not by random outcomes).
    """
    path = os.path.join(base_dir, "odds_history.json")
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            history = json.load(f) or {}
    except (OSError, json.JSONDecodeError):
        return None

    from datetime import datetime as _dt, timezone as _tz, timedelta as _td

    now = _dt.now(_tz.utc)
    cutoff = now - _td(hours=lookback_hours)

    def _parse_ts(ts):
        if not ts:
            return None
        try:
            # ISO with optional timezone — coerce to UTC
            if ts.endswith("Z"):
                ts = ts.replace("Z", "+00:00")
            d = _dt.fromisoformat(ts)
            if d.tzinfo is None:
                d = d.replace(tzinfo=_tz.utc)
            return d.astimezone(_tz.utc)
        except (ValueError, TypeError):
            return None

    def _implied_from_row(row):
        """Best (lowest implied = highest payout) book in the row."""
        best_implied = None
        for book in ("dk", "fd", "mgm", "br", "bovada"):
            v = row.get(book)
            if not isinstance(v, (int, float)):
                continue
            ip = american_to_implied_prob(v)
            if ip is None:
                continue
            if best_implied is None or ip < best_implied:
                best_implied = ip
        return best_implied

    n_eligible = 0
    n_positive = 0
    sum_move_bp = 0.0
    move_count = 0
    for p in players:
        edge = p.get("edgeScore")
        if not isinstance(edge, (int, float)) or abs(edge) < edge_threshold:
            continue
        name = (p.get("name") or "").strip().lower()
        if not name:
            continue
        series = (history.get(name) or {}).get("win") or []
        if len(series) < 2:
            continue
        current = series[-1]
        # Find baseline = oldest entry within lookback window (or oldest available)
        baseline = None
        for row in series[:-1]:
            t = _parse_ts(row.get("t"))
            if t is None:
                continue
            if t >= cutoff:
                baseline = row
                break
        if baseline is None:
            # Fall back to the entry just outside lookback (closer to now)
            for row in reversed(series[:-1]):
                t = _parse_ts(row.get("t"))
                if t is None or t < cutoff:
                    baseline = row
                    break
        if baseline is None:
            continue
        ip_then = _implied_from_row(baseline)
        ip_now = _implied_from_row(current)
        if ip_then is None or ip_now is None:
            continue
        move_bp = round((ip_now - ip_then) * 10000, 1)  # basis points
        # CLV positive: line moved IN the model's direction
        # - Positive edge (model thinks player wins more than market): want shortening (ip_now > ip_then)
        # - Negative edge (model thinks player wins less): want lengthening (ip_now < ip_then)
        if edge > 0:
            clv_pos = move_bp > 0
        else:
            clv_pos = move_bp < 0
        p["clvLineMoveBp"] = move_bp
        p["clvPositive"] = bool(clv_pos)
        n_eligible += 1
        sum_move_bp += abs(move_bp)
        move_count += 1
        if clv_pos:
            n_positive += 1

    if n_eligible == 0:
        return None
    clv_pct = round(n_positive / n_eligible * 100, 1)
    summary = {
        "pickCount": n_eligible,
        "clvPositiveCount": n_positive,
        "clvPositivePct": clv_pct,
        "avgAbsMoveBp": round(sum_move_bp / max(move_count, 1), 1),
        "lookbackHours": lookback_hours,
        "edgeThreshold": edge_threshold,
        "interpretation": (
            "Strong sharp signal — picks beating the close" if clv_pct >= 60
            else "Positive sharp signal" if clv_pct >= 55
            else "No signal — picks moving with noise" if clv_pct >= 45
            else "Negative signal — close moving against picks"
        ),
    }
    print(f"[CLV] {n_eligible} picks (|edge| >= {edge_threshold}) over last {lookback_hours}h: "
          f"{n_positive} CLV-positive ({clv_pct}%) · avg move {summary['avgAbsMoveBp']}bp")
    return summary


def persist_odds_history(output, base_dir):
    """Append current outright odds to odds_history.json for line-movement charts.

    Dedup: only append a row when (player, market, book) odds have changed
    since the last snapshot. Keeps file size bounded.
    Rotation: cap each (player, market) series to the last 100 entries.

    File lives at repo root; workflow copies it to docs/ for public fetch.
    """
    players = output.get("players") or []
    if not players:
        return 0

    path = os.path.join(base_dir, "odds_history.json")
    history = {}
    if os.path.isfile(path):
        try:
            with open(path, encoding="utf-8") as f:
                history = json.load(f) or {}
        except (OSError, json.JSONDecodeError):
            history = {}

    ts = output.get("generatedAt") or datetime.now().isoformat()
    appended = 0
    for p in players:
        name = (p.get("name") or "").strip().lower()
        if not name:
            continue
        odds = p.get("odds") or {}
        if not isinstance(odds, dict) or not odds:
            continue
        player_hist = history.setdefault(name, {})
        market_series = player_hist.setdefault("win", [])
        # Build current row — strip the "+" prefix, store as int
        current = {}
        for book, val in odds.items():
            try:
                v = int(str(val).replace("+", ""))
                current[book] = v
            except (ValueError, TypeError):
                continue
        if not current:
            continue
        # Dedup against last entry: only append if any book's odds differ
        last = market_series[-1] if market_series else None
        if last and all(last.get(b) == v for b, v in current.items()):
            continue
        row = {"t": ts, **current}
        market_series.append(row)
        # Rotate to last 100
        if len(market_series) > 100:
            del market_series[:-100]
        appended += 1

    if appended > 0:
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(history, f, separators=(",", ":"), ensure_ascii=False)
            print(f"  Odds history: appended {appended} new rows (file: {os.path.getsize(path) // 1024} KB)")
        except OSError as e:
            print(f"  [WARN] Could not write odds_history.json: {e}")
    return appended


def archive_data(output, base_dir):
    """Save a timestamped copy to history/ for trend analysis.

    The archive is a byte-for-byte snapshot of the same dict that gets
    written to golf-data.json, plus a top-level `snapshotVersion` key so
    downstream consumers (backtest, trend UI) can detect schema drift.

    Guards:
      - If today's run produced a suspiciously thin payload (no players
        AND no currentEvent), we keep the prior day's archive instead of
        overwriting with garbage — prevents backtest joins from breaking
        on a single bad scraper run.
      - Compact JSON (separators=(",",":"), no indent). Do not add indent
        here — it balloons history/ size by ~5x and hurts git blame.
    """
    history_dir = os.path.join(base_dir, "history")
    os.makedirs(history_dir, exist_ok=True)

    date_str = datetime.now().strftime("%Y-%m-%d")
    archive_path = os.path.join(history_dir, f"{date_str}.json")

    # Tag every archive with a schema version so old snapshots can be
    # detected and gracefully skipped forever.
    payload = dict(output)
    payload["snapshotVersion"] = SNAPSHOT_VERSION

    # Backfill-from-prior-day safeguard: if today's payload looks empty
    # or partial, copy forward the most recent good archive instead of
    # overwriting with a bad one. Guards weekday cron hiccups.
    looks_partial = (
        not payload.get("players")
        or not payload.get("currentEvent")
    )
    if looks_partial:
        prior = _most_recent_archive(history_dir, before=date_str)
        if prior is not None:
            prior_path, prior_data = prior
            prior_data["snapshotVersion"] = SNAPSHOT_VERSION
            prior_data["backfilledFrom"] = os.path.basename(prior_path)
            with open(archive_path, "w", encoding="utf-8") as f:
                json.dump(prior_data, f, separators=(",", ":"), ensure_ascii=False)
            print(f"  Archived (BACKFILLED from {os.path.basename(prior_path)}) "
                  f"to {archive_path} — today's payload was partial")
        else:
            # No prior archive to lean on — write what we have but flag it.
            payload["partial"] = True
            with open(archive_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, separators=(",", ":"), ensure_ascii=False)
            print(f"  Archived (FLAGGED partial, no prior to backfill) to {archive_path}")
    else:
        with open(archive_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, separators=(",", ":"), ensure_ascii=False)  # compact
        print(f"  Archived to {archive_path}")

    # Cleanup: remove files older than 365 days
    cutoff = datetime.now() - timedelta(days=365)
    for fname in os.listdir(history_dir):
        if not fname.endswith(".json"):
            continue
        try:
            fdate = datetime.strptime(fname[:10], "%Y-%m-%d")
            if fdate < cutoff:
                os.remove(os.path.join(history_dir, fname))
                print(f"  Cleaned up old archive: {fname}")
        except ValueError:
            pass


def _most_recent_archive(history_dir, before):
    """Return (path, loaded_dict) for the newest YYYY-MM-DD.json strictly
    older than `before`, or None. Skips unparseable files."""
    try:
        candidates = sorted(
            f for f in os.listdir(history_dir)
            if f.endswith(".json") and f[:10] < before
        )
    except OSError:
        return None
    for fname in reversed(candidates):
        path = os.path.join(history_dir, fname)
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and data.get("players"):
                return path, data
        except (OSError, json.JSONDecodeError):
            continue
    return None


# ============================================================
# FEATURE: BETTING ODDS (The Odds API — free tier, 500 credits/mo)
# ============================================================

def scrape_betting_odds():
    """
    Fetch PGA Tour outright winner odds from The Odds API.
    Used as a SUPPLEMENT to BDL — fills in books that BDL doesn't cover.
    Pulls ALL available US-region bookmakers (not just DK/FD).
    """
    if not ODDS_API_KEY:
        print("[5/7] Skipping Odds API — no ODDS_API_KEY set")
        return None

    # Throttle: refresh outright odds at key betting windows.
    #   Mon/Tue 1AM ET (UTC 5)  — off-week morning
    #   Wed 11AM ET (UTC 15)    — opening lines + tee sheet
    #   Wed 7PM ET (UTC 23)     — evening sharp action
    #   Thu 7AM ET (UTC 11)     — first tournament run
    # BDL still fires every hour during tournament week; Odds API is throttled to conserve credits.
    now = datetime.utcnow()
    now_utc_hour = now.hour
    weekday = now.weekday()  # Mon=0 ... Sun=6
    allowed = {5, 11}  # off-week morning + Thu 7AM ET
    if weekday == 2:  # Wednesday
        allowed.update({15, 23})
    if now_utc_hour not in allowed:
        print(f"[5/7] Skipping Odds API — throttled to save credits "
              f"(UTC hour {now_utc_hour}, weekday {weekday}, allowed: {sorted(allowed)})")
        return None

    print("[5/7] Fetching ALL sportsbook odds from The Odds API...")

    # Try multiple golf sport keys — active key depends on which tournament is upcoming
    GOLF_SPORT_KEYS = [
        "golf_masters_tournament_winner",
        "golf_pga_championship_winner",
        "golf_us_open_winner",
        "golf_the_open_championship_winner",
        "golf_pga_tour_winner",
    ]

    data = None
    for sport_key in GOLF_SPORT_KEYS:
        url = (
            f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
            f"?apiKey={ODDS_API_KEY}&regions=us&markets=outrights&oddsFormat=american"
        )
        result = fetch_json(url)
        if result:
            print(f"  Got data from sport key: {sport_key}")
            data = result
            break
        else:
            print(f"  No data for {sport_key}, trying next...")

    if not data:
        print("  Could not fetch odds from any golf sport key")
        return None

    # Map Odds API bookmaker keys to short display names
    BOOK_MAP = {
        "draftkings":       "dk",
        "fanduel":          "fd",
        "betmgm":           "mgm",
        "caesars":           "czr",
        "pointsbetus":      "pb",
        "bet365":            "365",
        "bovada":            "bov",
        "betonlineag":       "bol",
        "betrivers":         "riv",
        "unibet_us":         "uni",
        "wynnbet":           "wyn",
        "superbook":         "sup",
        "twinspires":        "twn",
        "betus":             "bus",
        "lowvig":            "low",
        "mybookieag":        "myb",
        "williamhill_us":    "czr",   # William Hill = Caesars rebrand
        "espnbet":           "espn",
        "fliff":             "flf",
        "hardrockbet":       "hrb",
        "fanatics":          "fan",
    }

    odds_map = {}
    books_seen = set()
    for event in data if isinstance(data, list) else [data]:
        for bookmaker in event.get("bookmakers", []):
            bk_key = bookmaker.get("key", "")
            short = BOOK_MAP.get(bk_key, bk_key[:3])
            books_seen.add(f"{bk_key}={short}")
            for market in bookmaker.get("markets", []):
                for outcome in market.get("outcomes", []):
                    name = outcome.get("name", "")
                    price = outcome.get("price", 0)
                    if not name:
                        continue
                    # Strip phantom "market closed" placeholders (books park
                    # eliminated players at +100000 / +500000 etc.)
                    try:
                        if abs(int(price)) >= 50000:
                            continue
                    except (ValueError, TypeError):
                        continue
                    if name not in odds_map:
                        odds_map[name] = {}
                    odds_map[name][short] = f"{'+' if price > 0 else ''}{price}"

    print(f"  Odds API: {len(odds_map)} players from {len(books_seen)} books: {', '.join(sorted(books_seen))}")
    return odds_map


# ============================================================
# FEATURE: 3-BALL ODDS + PREDICTIVE MODEL
# ============================================================
# A 3-ball is a bet on which of the 3 players in a tee-time group shoots the
# lowest round. US books settle ties via dead-heat rules (win / n_tied).
# Market = `3_balls` on The Odds API (also seen as `h2h_3_way` on some keys).
# ============================================================

THREEBALL_BOOK_MAP = {
    "draftkings": "dk", "fanduel": "fd", "betmgm": "mgm", "caesars": "czr",
    "pointsbetus": "pb", "bet365": "365", "bovada": "bov", "betonlineag": "bol",
    "betrivers": "riv", "unibet_us": "uni", "wynnbet": "wyn", "superbook": "sup",
    "twinspires": "twn", "betus": "bus", "lowvig": "low", "mybookieag": "myb",
    "williamhill_us": "czr", "espnbet": "espn", "fliff": "flf", "hardrockbet": "hrb",
    "fanatics": "fan",
}


def bdl_get_matchup_odds(tournament_id):
    """Try BDL's matchup-odds endpoint. BDL's public docs list
    `odds/matchups`; if that 404s we try a couple of alternate spellings.
    Returns list of {eventId, commence, round, players:[{name, odds:[...]}]}.
    """
    for endpoint in ("odds/matchups", "odds/3_balls", "matchup_odds"):
        raw = bdl_fetch(endpoint, {"tournament_id": str(tournament_id)})
        if not raw:
            continue
        rows = raw.get("data") if isinstance(raw, dict) else raw
        if not rows:
            continue
        print(f"[3BALL] BDL endpoint '{endpoint}' returned {len(rows)} rows")
        groups = {}
        for r in rows:
            gid = r.get("matchup_id") or r.get("group_id") or r.get("id")
            if not gid:
                continue
            player = r.get("player") or {}
            pname = (
                player.get("display_name")
                or f"{player.get('first_name','')} {player.get('last_name','')}".strip()
                if isinstance(player, dict) else str(player)
            )
            if not pname:
                continue
            price = r.get("odds") or r.get("american") or r.get("price")
            if price is None:
                continue
            book = r.get("book") or r.get("sportsbook") or "bdl"
            g = groups.setdefault(gid, {
                "eventId": str(gid),
                "commence": r.get("tee_time") or r.get("commence_time") or "",
                "round": r.get("round") or r.get("round_number"),
                "players": {},
            })
            g["players"].setdefault(pname, {"name": pname, "odds": []})
            g["players"][pname]["odds"].append({"book": str(book)[:5].lower(),
                                                "american": int(price)})
        out = []
        for g in groups.values():
            players_list = list(g["players"].values())
            if len(players_list) == 2:
                g["type"] = "2ball"
                g["players"] = players_list
                out.append(g)
            elif len(players_list) >= 3:
                g["type"] = "3ball"
                g["players"] = players_list[:3]
                out.append(g)
        if out:
            return out
    return []


def synthesize_matchups_from_tee_times(tee_times, current_round=None):
    """Build synthetic 2-ball or 3-ball groups from tee-time data.

    Signature events (RBC Heritage, Genesis, Memorial, etc.) have 72-player
    no-cut fields and play in 2-ball pairings. Regular events play 3-balls
    Thu/Fri then 2-balls Sat/Sun after the cut.

    Returns groups with type="2ball" or "3ball" and synthetic=True."""
    if not tee_times:
        return []
    from collections import defaultdict
    buckets = defaultdict(list)
    for t in tee_times:
        tt = t.get("teeTime")
        rnd = t.get("round")
        hole = t.get("startHole", 1)
        name = t.get("player")
        if not name or not tt:
            continue
        buckets[(rnd, tt, hole)].append(name)

    groups = []
    for (rnd, tt, hole), names in buckets.items():
        if len(names) == 2:
            group_type = "2ball"
        elif len(names) == 3:
            group_type = "3ball"
        else:
            continue
        groups.append({
            "eventId": f"synth-r{rnd}-{tt}-{hole}",
            "commence": tt,
            "round": rnd,
            "type": group_type,
            "players": [{"name": n, "odds": []} for n in names],
            "synthetic": True,
        })
    return groups


def _fetch_odds_api_once(url):
    """Single-shot fetch for The Odds API — no retries on 4xx (422 = market
    not supported for this sport; retrying just burns credits & log noise)."""
    try:
        req = Request(url, headers={"User-Agent": USER_AGENT})
        with urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw), resp.status
    except HTTPError as e:
        return None, e.code
    except (URLError, TimeoutError, json.JSONDecodeError, OSError):
        return None, None


def scrape_matchup_odds():
    """Fetch 2-ball + 3-ball matchup odds from The Odds API.

    Markets:
      * 3_balls / h2h_3_way  = 3-player groups (regular PGA events Thu/Fri)
      * h2h                  = 2-player groups (signature events + Sat/Sun after cut)

    Strategy: discover active golf keys dynamically, try each market type,
    skip 422 (market not supported).
    """
    if not ODDS_API_KEY:
        print("[MATCHUP] Skipping — no ODDS_API_KEY")
        return []

    print("[MATCHUP] Discovering active golf sport keys from The Odds API...")
    sports_url = f"https://api.the-odds-api.com/v4/sports?apiKey={ODDS_API_KEY}&all=false"
    sports_data, _ = _fetch_odds_api_once(sports_url)
    if not sports_data:
        print("  Could not fetch sports list")
        return []

    golf_keys = [s.get("key") for s in sports_data
                 if isinstance(s, dict) and str(s.get("group", "")).lower() == "golf"
                 and s.get("active")]
    # Winner-only keys only support 'outrights' — exclude them for matchup lookup
    golf_keys = [k for k in golf_keys if k and not k.endswith("_winner")]
    if not golf_keys:
        print("  No non-winner golf keys active (matchups only post during event weeks)")
        return []
    print(f"  Candidate golf keys: {golf_keys}")

    MATCHUP_MARKETS = ("3_balls", "h2h_3_way", "h2h")  # last one = 2-ball
    data = None
    sport_key_used = None
    for sport_key in golf_keys:
        url = (
            f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
            f"?apiKey={ODDS_API_KEY}&regions=us&markets={','.join(MATCHUP_MARKETS)}&oddsFormat=american"
        )
        result, status = _fetch_odds_api_once(url)
        if status == 422:
            # Try each market individually to see which are supported
            supported = []
            for m in MATCHUP_MARKETS:
                single_url = url.split("&markets=")[0] + f"&markets={m}" + "&oddsFormat=american"
                single_url = (
                    f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
                    f"?apiKey={ODDS_API_KEY}&regions=us&markets={m}&oddsFormat=american"
                )
                r, s = _fetch_odds_api_once(single_url)
                if s != 422 and isinstance(r, list):
                    supported.append((m, r))
            if not supported:
                print(f"  {sport_key}: no matchup markets supported")
                continue
            # Combine events across supported markets
            merged = {}
            for _m, evs in supported:
                for ev in evs:
                    merged[ev.get("id")] = ev
            result = list(merged.values())
        if isinstance(result, list) and result:
            has_matchup = any(
                any(m.get("key") in MATCHUP_MARKETS
                    for bk in ev.get("bookmakers", []) for m in bk.get("markets", []))
                for ev in result
            )
            if has_matchup:
                data = result
                sport_key_used = sport_key
                break
            else:
                print(f"  {sport_key}: endpoint OK but no matchup markets posted yet")

    if not data:
        print("  No matchup markets posted right now (books typically open lines Wed evening)")
        return []
    print(f"  Matchup market live on sport key: {sport_key_used} ({len(data)} events)")

    groups = []
    for event in data:
        # Collect odds per player across all books, and track matchup type
        players_map = {}
        matchup_type = None
        for bk in event.get("bookmakers", []):
            book_short = THREEBALL_BOOK_MAP.get(bk.get("key", ""), bk.get("key", "")[:3])
            for market in bk.get("markets", []):
                mkey = market.get("key")
                if mkey not in MATCHUP_MARKETS:
                    continue
                if mkey == "h2h":
                    matchup_type = "2ball"
                elif mkey in ("3_balls", "h2h_3_way"):
                    matchup_type = "3ball"
                for outcome in market.get("outcomes", []):
                    name = outcome.get("name", "").strip()
                    price = outcome.get("price")
                    if not name or price is None:
                        continue
                    if name.lower() in ("tie", "the field", "draw"):
                        continue
                    key = normalize_name(name)
                    if key not in players_map:
                        players_map[key] = {"name": name, "odds": []}
                    players_map[key]["odds"].append({"book": book_short, "american": int(price)})

        # Determine valid group: 2-ball or 3-ball
        pcount = len(players_map)
        if pcount == 2:
            final_type = matchup_type or "2ball"
        elif pcount >= 3:
            final_type = matchup_type or "3ball"
        else:
            continue  # 1-player "matchup" is malformed

        commence = event.get("commence_time", "")
        round_num = _infer_round_from_commence(commence)
        cap = 2 if final_type == "2ball" else 3

        group = {
            "eventId": event.get("id", ""),
            "commence": commence,
            "round": round_num,
            "type": final_type,
            "players": list(players_map.values())[:cap],
        }
        groups.append(group)

    type_counts = {}
    for g in groups:
        type_counts[g["type"]] = type_counts.get(g["type"], 0) + 1
    print(f"  Parsed {len(groups)} matchup groups: {type_counts}")
    return groups


def _infer_round_from_commence(commence_iso):
    """Map an ISO commence time to a round number. Thu=1, Fri=2, Sat=3, Sun=4."""
    if not commence_iso:
        return None
    try:
        # strip trailing Z
        ts = commence_iso.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        # weekday: Mon=0 ... Sun=6
        wd = dt.weekday()
        return {3: 1, 4: 2, 5: 3, 6: 4}.get(wd)
    except (ValueError, TypeError):
        return None


def _american_to_implied_prob(american):
    """Convert American odds to implied probability (0..1)."""
    if american is None:
        return None
    if american >= 0:
        return 100.0 / (american + 100.0)
    return -american / (-american + 100.0)


def _prob_to_american(p):
    """Convert probability to American odds. Returns None if p not in (0,1)."""
    if p is None or p <= 0 or p >= 1:
        return None
    if p >= 0.5:
        return int(round(-100 * p / (1 - p)))
    return int(round(100 * (1 - p) / p))


def predict_matchups(matchups, players, course_key=None, weather=None,
                     tournament_sg=None, sims=10000, course_sg_weights=None):
    """Monte Carlo model for matchup win probability, handles 2-ball AND 3-ball.

    Tie semantics differ by market:
      * 2-ball H2H: tie = PUSH (stake returned, EV neutral on ties)
      * 3-ball: dead-heat rules (winnings divided by n_tied)

    Model assumptions:
      mean_score_i = par - (0.7 * sgTotal + 0.3 * liveTourneySG) * strokes_per_sg
                         - courseFit_boost - form_boost + weather_penalty
      score_i = round(mean_score_i + round_shock + individual_noise)

    round_shock is shared across all 3 players in the group (course played
    hard/easy that morning). Integer rounding produces realistic tie rates.
    """
    import random
    random.seed(42)

    # Build lookup of season SG by name
    by_name = {normalize_name(p.get("name", "")): p for p in players}

    # Build lookup of live tournament SG by name
    live_sg = {}
    for row in (tournament_sg or []):
        live_sg[normalize_name(row.get("name", ""))] = row.get("sgTotal", 0) or 0

    # Weather penalty — use average wind over round windows as a stroke penalty
    wx_penalty = 0.0
    wx_std_bump = 0.0
    if isinstance(weather, dict):
        wind_values = []
        for day in (weather.get("forecast") or []):
            w = day.get("windMph") or day.get("wind_mph") or day.get("wind")
            if isinstance(w, (int, float)):
                wind_values.append(w)
        if wind_values:
            avg_wind = sum(wind_values) / len(wind_values)
            # Linear above 12 mph: each extra mph ≈ +0.08 strokes, +0.05 std dev
            wx_penalty = max(0.0, (avg_wind - 12) * 0.08)
            wx_std_bump = max(0.0, (avg_wind - 12) * 0.05)

    PAR = 71  # PGA field-average round baseline (tour avg ~71.0 for par-70/71/72 mix)
    BASE_STD = 2.85
    ROUND_SHOCK_STD = 1.0  # course difficulty shift shared across group

    for group in matchups:
        # Build per-player params
        pp_list = []
        for p in group["players"]:
            pdata = by_name.get(normalize_name(p["name"]), {}) or {}
            sg_season = effective_sg(pdata, course_weights=course_sg_weights)
            sg_live = live_sg.get(normalize_name(p["name"]), 0.0) or 0.0
            sg_blended = 0.7 * sg_season + 0.3 * sg_live if sg_live else sg_season

            fit_boost = 0.0
            if course_key:
                # Fallback to DEFAULT_COURSE_FIT (75 = neutral, 0 stroke boost)
                # so newly-added 2026 venues without curated per-player fits
                # don't drop through as NaN/0.
                fit = (pdata.get("courseFit") or {}).get(course_key, DEFAULT_COURSE_FIT)
                if isinstance(fit, (int, float)):
                    # 75 = neutral, range 60-100 → ±1 stroke
                    fit_boost = (fit - 75) / 25.0

            form_boost = 0.0
            form = pdata.get("recentForm") or {}
            if form.get("trend") == "hot":
                form_boost = 0.25
            elif form.get("trend") == "cold":
                form_boost = -0.25

            mean_score = PAR - sg_blended - fit_boost - form_boost + wx_penalty

            # Per-player variance: prefer empirical stddev from history
            # (attached to player as scoreStd) over the global BASE_STD.
            # Falls back to BASE_STD for players without sufficient history.
            empirical_std = pdata.get("scoreStd")
            if isinstance(empirical_std, (int, float)) and 1.5 <= empirical_std <= 5.0:
                base = float(empirical_std)
            else:
                base = BASE_STD
            # Volatility adjustment: hot/cold trends add tail weight
            volatility = 0.0
            if form.get("trend") in ("hot", "cold"):
                volatility += 0.15
            std_i = base + wx_std_bump + volatility

            pp_list.append({
                "player": p,
                "mean": mean_score,
                "std": std_i,
                "sgBlended": round(sg_blended, 2),
                "fitBoost": round(fit_boost, 2),
                # Decomposition for matchup-explanation UI. Each field is the
                # contribution (in strokes vs PAR baseline) that this factor
                # makes to the player's projected mean score. The frontend
                # subtracts opponent contributions to show "Player A's
                # advantage comes from +0.6 SG and +0.2 course fit."
                "contrib": {
                    "sg":      round(sg_blended, 3),
                    "fit":     round(fit_boost, 3),
                    "form":    round(form_boost, 3),
                    "weather": round(-wx_penalty, 3),  # weather hurts, so advantage = -penalty
                },
            })

        # Monte Carlo — handle 2-ball and 3-ball
        n = len(pp_list)
        is_2ball = (n == 2)
        clear_wins = [0] * n
        tie2_wins = [0] * n   # i in 2-way tie for lowest (3-ball only)
        tie_push = [0] * n    # 2-ball push — both scored lowest equally
        tie3 = 0              # all 3 tied (3-ball only)

        for _ in range(sims):
            shock = random.gauss(0.0, ROUND_SHOCK_STD)
            scores = []
            for pp in pp_list:
                raw = pp["mean"] + shock + random.gauss(0.0, pp["std"])
                scores.append(round(raw))  # integer strokes — enables realistic ties
            lo = min(scores)
            winners = [i for i, s in enumerate(scores) if s == lo]
            if is_2ball:
                if len(winners) == 1:
                    clear_wins[winners[0]] += 1
                else:
                    # 2-ball tie = push for both
                    for i in winners:
                        tie_push[i] += 1
            else:
                if len(winners) == 1:
                    clear_wins[winners[0]] += 1
                elif len(winners) == 2:
                    for i in winners:
                        tie2_wins[i] += 1
                else:
                    tie3 += 1

        # Attach results to each player
        for i, pp in enumerate(pp_list):
            p_clear = clear_wins[i] / sims
            if is_2ball:
                p_push = tie_push[i] / sims
                p_lose = 1 - p_clear - p_push
                # For a 2-ball H2H, tie is a PUSH — doesn't add to win value
                win_value = p_clear
                p_t2 = p_t3 = None
            else:
                p_t2 = tie2_wins[i] / sims
                p_t3 = tie3 / sims
                p_push = None
                p_lose = 1 - p_clear - p_t2 - p_t3
                # Dead-heat-weighted (3-ball): clear + 0.5*t2 + 0.333*t3
                win_value = p_clear + 0.5 * p_t2 + (1.0 / 3.0) * p_t3

            odds_list = pp["player"].get("odds", [])
            best = max(odds_list, key=lambda o: o["american"]) if odds_list else None
            implied_best = _american_to_implied_prob(best["american"]) if best else None

            ev = None
            if best and win_value > 0:
                am = best["american"]
                payout_profit = (am / 100.0) if am > 0 else (100.0 / -am)
                if is_2ball:
                    # 2-ball: clear win pays full, push = stake back (0 EV), lose = -1
                    ep = p_clear * payout_profit + p_push * 0 - p_lose * 1
                else:
                    # 3-ball dead-heat: clear full, 2-tie half, 3-tie third, else -1
                    ep = (
                        p_clear * payout_profit
                        + p_t2 * (payout_profit * 0.5) - p_t2 * 0.5
                        + p_t3 * (payout_profit / 3.0) - p_t3 * (2.0 / 3.0)
                        - p_lose * 1
                    )
                ev = round(ep * 100, 2)  # %EV per $1 staked

            result = {
                "modelMean": round(pp["mean"], 2),
                "modelStd": round(pp["std"], 2),
                "sgBlended": pp["sgBlended"],
                "fitBoost": pp["fitBoost"],
                "contrib": pp["contrib"],
                "pClearWin": round(p_clear, 4),
                "deadHeatWinValue": round(win_value, 4),
                "fairOdds": _prob_to_american(win_value),
                "bestBook": best,
                "impliedProbBest": round(implied_best, 4) if implied_best else None,
                "ev": ev,
            }
            if is_2ball:
                result["pPush"] = round(p_push, 4)
            else:
                result["pTie2"] = round(p_t2, 4)
                result["pTie3"] = round(p_t3, 4)
            pp["player"].update(result)

        # Group-level metadata
        group.setdefault("type", "2ball" if is_2ball else "3ball")
        group["dhRulesApplied"] = not is_2ball  # 2-balls use push, not DH
        group["bookSample"] = sorted({o["book"] for p in group["players"] for o in p.get("odds", [])})
        group["simCount"] = sims

    # Sort groups by best available edge (descending)
    def group_edge(g):
        evs = [p.get("ev") for p in g["players"] if isinstance(p.get("ev"), (int, float))]
        return max(evs) if evs else -999
    matchups.sort(key=group_edge, reverse=True)
    return matchups


# ============================================================
# FEATURE: RECENT FORM (L5/L10 from ESPN)
# ============================================================

def scrape_recent_form(espn_event_data):
    """Build recent form data from ESPN leaderboard history.
    Uses the current event leaderboard + any cached historical data.
    Returns dict of {player_name: form_data}.
    """
    print("[6/7] Building recent form profiles...")
    form_map = {}

    # Use current leaderboard as most recent data point — but ONLY when the
    # event has actually been played. Pre-tournament snapshots wrote stale
    # "lastResult" rows that looked like a finish position to users
    # ("#144 at PGA Championship" before tee-off). The leaderboard merge
    # logic later in run_pipeline already token-matches ESPN against BDL;
    # here we additionally require the event to be COMPLETED, so a stale
    # ESPN Truist record can never seep into recent-form even if names
    # happened to overlap.
    espn_status = ""
    if espn_event_data:
        espn_status = str(espn_event_data.get("status") or "").upper()
    is_completed = espn_status in ("COMPLETED", "STATUS_FINAL", "FINAL", "POSTEVENT")

    if is_completed and espn_event_data and espn_event_data.get("leaderboard"):
        lb = espn_event_data["leaderboard"]
        has_scores = any((r.get("totalStrokes") or 0) > 0 for r in lb)
        if has_scores:
            event_name = espn_event_data.get("name", "Unknown")
            for i, entry in enumerate(lb):
                name = entry.get("name", "")
                if not name:
                    continue
                # Prefer the leaderboard's own position string ("T15") over
                # the loop index — index lies whenever leaders are tied.
                pos_raw = entry.get("position") or f"#{i + 1}"
                pos_display = pos_raw if str(pos_raw).startswith(("T", "#")) else f"#{pos_raw}"
                form_map[name] = {
                    "lastResult": f"{pos_display} at {event_name}",
                    "lastPosition": i + 1,
                }
    elif espn_event_data:
        # Pre-tournament or in-progress: don't write a misleading lastResult.
        # The L5/L10 averages below still compute from completed history.
        print(f"  Skipping lastResult — ESPN event status is "
              f"'{espn_status or 'unknown'}', not COMPLETED")

    # Load historical archive files for L5/L10 calculation
    base_dir = os.path.dirname(os.path.abspath(__file__))
    history_dir = os.path.join(base_dir, "history")
    if os.path.isdir(history_dir):
        history_files = sorted(
            [f for f in os.listdir(history_dir) if f.endswith(".json")],
            reverse=True
        )[:10]  # Last 10 weeks

        player_finishes = {}  # {name: [list of positions]}
        player_rounds = {}    # {name: {1: [scores], 2: [scores], 3: [], 4: []}}
        for hfile in history_files:
            try:
                with open(os.path.join(history_dir, hfile)) as f:
                    hdata = json.load(f)
                evt = hdata.get("currentEvent", {})
                if not evt or not evt.get("leaderboard"):
                    continue
                for i, entry in enumerate(evt["leaderboard"]):
                    name = entry.get("name", "")
                    if name:
                        if name not in player_finishes:
                            player_finishes[name] = []
                        player_finishes[name].append(i + 1)

                        # Collect round-by-round scores for R1-R4 split analysis
                        if name not in player_rounds:
                            player_rounds[name] = {1: [], 2: [], 3: [], 4: []}
                        for rnd in [1, 2, 3, 4]:
                            key = f"round{rnd}"
                            val = entry.get(key, 0)
                            if val and val > 50:  # Sanity check — valid golf score
                                player_rounds[name][rnd].append(val)
            except (json.JSONDecodeError, IOError):
                continue

        for name, finishes in player_finishes.items():
            l5 = finishes[:5]
            l10 = finishes[:10]
            l5_avg = sum(l5) / len(l5) if l5 else None
            l10_avg = sum(l10) / len(l10) if l10 else None

            # Determine trend
            trend = "steady"
            if l5_avg and l10_avg:
                if l5_avg < l10_avg - 5:
                    trend = "hot"
                elif l5_avg > l10_avg + 5:
                    trend = "cold"

            # Round-by-round averages (for future R1 vs R4 split analysis)
            round_avgs = {}
            rounds = player_rounds.get(name, {})
            for rnd in [1, 2, 3, 4]:
                scores = rounds.get(rnd, [])
                if len(scores) >= 2:  # Need at least 2 data points
                    round_avgs[f"r{rnd}Avg"] = round(sum(scores) / len(scores), 1)
            # Compute closing strength: R3+R4 avg vs R1+R2 avg
            # Negative = closes better (good); Positive = fades on weekends
            if round_avgs.get("r1Avg") and round_avgs.get("r3Avg"):
                early = (round_avgs.get("r1Avg", 72) + round_avgs.get("r2Avg", 72)) / 2
                late  = (round_avgs.get("r3Avg", 72) + round_avgs.get("r4Avg", 72)) / 2
                round_avgs["closingDelta"] = round(late - early, 1)

            if name not in form_map:
                form_map[name] = {}
            form_map[name].update({
                "l5AvgFinish": round(l5_avg, 1) if l5_avg else None,
                "l10AvgFinish": round(l10_avg, 1) if l10_avg else None,
                "l5McPct": round(sum(1 for f in l5 if f <= 65) / len(l5) * 100) if l5 else None,
                "trend": trend,
                "events": len(l10),
                "roundAvgs": round_avgs if round_avgs else None,
            })

    print(f"  Built form data for {len(form_map)} players")
    return form_map


# ============================================================
# FEATURE: DISCORD ALERTS
# ============================================================

def send_discord_alerts(output):
    """Send Discord webhook alerts for high-edge prop opportunities.
    Throttled to 3 sends/day: UTC hours 11, 17, 23 (7AM, 1PM, 7PM ET).
    """
    if not DISCORD_WEBHOOK_URL:
        print("  Skipping Discord alerts — no webhook URL set")
        return

    # Throttle to 3x/day — only fire at 11, 17, 23 UTC (7AM, 1PM, 7PM ET)
    now_utc_hour = datetime.utcnow().hour
    if now_utc_hour not in (11, 17, 23):
        print(f"[7/7] Skipping Discord alerts — throttled to 3x/day (current UTC hour: {now_utc_hour}, allowed: 11, 17, 23)")
        return

    print("[7/7] Checking for high-edge prop alerts...")

    alerts = []
    event_name = ""
    if output.get("currentEvent"):
        event_name = output["currentEvent"].get("name", "")

    # CRITICAL: only alert on props where a real sportsbook is actually
    # offering a line. Previously we hardcoded "OVER 3.5 Birdies" / "UNDER
    # 2.5 Bogeys" and sent alerts using season averages — but if no book
    # was offering 3.5 Birdies that week (most non-tournament days), the
    # alert was pointing at a market that didn't exist. User reported this.
    prop_lines = output.get("propLines") or {}

    # Helper: is there a real book line for (player, market)?
    def _real_line(player_name, market):
        pack = (prop_lines.get(player_name) or {}).get(market) or {}
        line_val = pack.get("line")
        if not isinstance(line_val, (int, float)):
            return None
        return {
            "line": float(line_val),
            "over": pack.get("overOdds"),
            "under": pack.get("underOdds"),
            "book": (pack.get("book") or "").strip(),
        }

    skipped_no_line = 0
    for player in output.get("players", []):
        name = player.get("name", "")
        rank = player.get("rank", "?")

        # ---- Birdies OVER: only if book is posting a line ----
        birdie_avg = player.get("birdieAvg")
        if birdie_avg is not None:
            book_birdie = _real_line(name, "birdies")
            if book_birdie is None:
                skipped_no_line += 1
            else:
                # Compare season avg vs ACTUAL book line. If book is 4.5 birdies
                # over 4 rounds, compare the 4-round projection, not per-round.
                # Heuristic: lines >= 8 are tournament totals (4-rd), lines < 8
                # are per-round.
                line = book_birdie["line"]
                is_tourney = line >= 8
                projection = birdie_avg * 4 if is_tourney else birdie_avg
                edge = projection - line
                if edge >= 0.8:
                    conf = min(95, round(65 + edge * 15))
                    if conf >= 75:
                        over_odds = book_birdie["over"]
                        odds_str = ""
                        if isinstance(over_odds, (int, float)):
                            odds_str = f" {'+' if over_odds > 0 else ''}{int(over_odds)}"
                        book_str = f" @ {book_birdie['book'].upper()}" if book_birdie["book"] else ""
                        alerts.append({
                            "player": name,
                            "prop": f"OVER {line} Birdies{book_str}{odds_str}",
                            "model": f"{projection:.1f} proj",
                            "edge": f"+{edge:.1f}",
                            "conf": conf,
                            "rank": rank,
                        })

        # ---- Bogeys UNDER: only if book is posting a line ----
        bogey_avg = player.get("bogeyAvg")
        if bogey_avg is not None:
            book_bogey = _real_line(name, "bogeys")
            if book_bogey is None:
                skipped_no_line += 1
            else:
                line = book_bogey["line"]
                is_tourney = line >= 6
                projection = bogey_avg * 4 if is_tourney else bogey_avg
                edge = line - projection
                if edge >= 0.4:
                    conf = min(95, round(60 + edge * 20))
                    if conf >= 75:
                        under_odds = book_bogey["under"]
                        odds_str = ""
                        if isinstance(under_odds, (int, float)):
                            odds_str = f" {'+' if under_odds > 0 else ''}{int(under_odds)}"
                        book_str = f" @ {book_bogey['book'].upper()}" if book_bogey["book"] else ""
                        alerts.append({
                            "player": name,
                            "prop": f"UNDER {line} Bogeys{book_str}{odds_str}",
                            "model": f"{projection:.1f} proj",
                            "edge": f"+{edge:.1f}",
                            "conf": conf,
                            "rank": rank,
                        })

    # ---- Outright winner + placement (Top 5/10/20) + make cut alerts ----
    # Only alert on real book odds. Per-market EV thresholds get TIGHTER
    # for markets where our model is less calibrated (winner is hardest,
    # make-cut is easiest). All tagged BETA until backtest validates.
    #
    # Model-probability calibration is heuristic — propScores[market]/100
    # scaled to each market's peak-player rate. Refined automatically once
    # backtest-report.json has 30+ scored events.
    # Per-market sanity bands. Book odds OUTSIDE these ranges are either
    # flat-default longshots (books don't actually care about that edge) or
    # overwhelming favorites where no real edge is findable.
    # min_american / max_american: acceptable book-price window
    # max_ratio:  reject if model_prob / book_implied exceeds this (prevents
    #            "+50000 at 3% model = fake 1400% EV" bug)
    MARKET_CAL = {
        "winner":  {"peak": 0.15, "ev_threshold": 15.0, "label": "Outright",
                    "min_american": -400, "max_american":  3000, "max_ratio": 2.5},
        "top5":    {"peak": 0.25, "ev_threshold": 12.0, "label": "Top 5",
                    "min_american": -250, "max_american":  2000, "max_ratio": 2.2},
        "top10":   {"peak": 0.35, "ev_threshold": 10.0, "label": "Top 10",
                    "min_american": -200, "max_american":  1500, "max_ratio": 2.0},
        "top20":   {"peak": 0.55, "ev_threshold":  8.0, "label": "Top 20",
                    "min_american": -300, "max_american":  1000, "max_ratio": 1.8},
        "makeCut": {"peak": 0.85, "ev_threshold":  6.0, "label": "Make Cut",
                    "min_american": -500, "max_american":   500, "max_ratio": 1.6},
        "r1Leader": {"peak": 0.06, "ev_threshold": 12.0, "label": "R1 Leader",
                     "min_american": -200, "max_american":  5000, "max_ratio": 2.8},
    }

    def _parse_american(raw):
        if raw is None:
            return None
        try:
            return int(str(raw).replace("+", "").strip())
        except (ValueError, TypeError):
            return None

    def _ev_on_stake(model_prob, american):
        if american is None or not (0 < model_prob < 1):
            return None
        payout = american / 100.0 if american > 0 else 100.0 / -american
        return (model_prob * payout - (1 - model_prob)) * 100  # %

    def _best_odds(odds_dict):
        """Best American odds (highest positive / closest to zero negative)."""
        if not isinstance(odds_dict, dict):
            return None, None
        best_book, best_val = None, None
        for book, raw in odds_dict.items():
            v = _parse_american(raw)
            if v is None:
                continue
            if best_val is None or v > best_val:
                best_val = v
                best_book = book
        return best_book, best_val

    # Keep the top 3 highest-confidence picks per placement market across the
    # whole field. The pre-best-per-market behavior emitted every player ×
    # market pair that passed the EV threshold — on a 156-player field that
    # produced dozens of placement alerts before the global cap kicked in.
    # Now: 3 decisive picks per market (Outright / Top 5 / Top 10 / Top 20 /
    # Make Cut / R1 Leader). Birdie / bogey / matchup alerts keep their own
    # per-pick logic below.
    PICKS_PER_MARKET = 3
    props_by_type = output.get("propsByType") or {}
    candidates_per_market = {}  # market_key -> list of alert dicts
    for player in output.get("players", []):
        name = player.get("name", "")
        rank = player.get("rank", "?")
        conf = player.get("confScore")
        prop_scores = player.get("propScores") or {}

        for market, cal in MARKET_CAL.items():
            # ---- Source of real book odds for this market ----
            if market == "winner":
                book_odds = player.get("odds") or {}
            else:
                # propsByType[market] is keyed by player name, value is odds string
                market_prices = props_by_type.get(market) or {}
                raw = market_prices.get(name)
                if raw is None:
                    continue  # no real book line — skip (don't fabricate)
                # Normalize to the {book: odds} shape
                if isinstance(raw, dict):
                    book_odds = raw
                else:
                    book_odds = {"book": raw}

            best_book, best_val = _best_odds(book_odds)
            if best_val is None:
                continue  # no parseable odds

            # ---- GUARD 1: book-odds sanity band ----
            # Flat-default longshots (+50000 type) aren't real priced markets;
            # overwhelming favorites have no findable edge. Skip both.
            # Use .get() with safe defaults so a missing key in MARKET_CAL
            # never crashes the whole pipeline (Discord alerts are not
            # critical-path; deploy must still ship).
            min_am = cal.get("min_american", -10000)
            max_am = cal.get("max_american", 50000)
            if best_val < min_am or best_val > max_am:
                continue

            # ---- Book implied probability ----
            book_implied = _american_to_implied_prob(best_val)
            if book_implied is None or book_implied <= 0:
                continue

            # ---- Model probability for this market ----
            if market == "winner":
                if conf is None:
                    continue
                model_prob = (conf / 100.0) * cal["peak"]
            else:
                score = prop_scores.get(market)
                if score is None:
                    # Fall back to confScore as rough proxy
                    score = conf
                if score is None:
                    continue
                model_prob = (score / 100.0) * cal["peak"]

            # ---- GUARD 2: model/book ratio sanity ----
            # If our model says 3x+ what the book says, that's overconfidence,
            # not edge. Books generally price longshot fields correctly — any
            # "massive" edge vs a +2000 line is almost always our noise.
            ratio = model_prob / book_implied if book_implied > 0 else 0
            if ratio > cal.get("max_ratio", 5.0):
                continue

            ev = _ev_on_stake(model_prob, best_val)
            if ev is None or ev < cal["ev_threshold"]:
                continue

            book_label = str(best_book).upper() if best_book and best_book != "book" else ""
            odds_str = f"{'+' if best_val > 0 else ''}{best_val}"
            prop_label = (
                f"{cal['label']} — {name}"
                + (f" @ {book_label}" if book_label else "")
                + f" {odds_str}"
            )
            candidate_conf = min(95, round(55 + ev * 1.5))
            candidates_per_market.setdefault(market, []).append({
                "player": name,
                "prop": prop_label,
                "model": f"{model_prob*100:.1f}% model prob",
                "edge": f"+{ev:.1f}% EV",
                "conf": candidate_conf,
                "rank": rank,
            })

    # Take the top PICKS_PER_MARKET highest-conf candidates per market and
    # promote them into the global alerts list. EV is the dominant term in
    # `conf`, so this is effectively "highest EV" with the same monotonic
    # mapping the rest of the alerter already uses.
    for market_picks in candidates_per_market.values():
        market_picks.sort(key=lambda x: x["conf"], reverse=True)
        alerts.extend(market_picks[:PICKS_PER_MARKET])

    # ---- 2-ball / 3-ball matchup alerts ----
    # Only fire when real book odds exist (source != SyntheticTeeTimes)
    # and EV >= threshold. Dead-heat math is already baked into ev/fairOdds
    # at scrape time, so we just read and filter here.
    MATCHUP_EV_THRESHOLD = 5.0  # %
    MATCHUP_EV_STRONG = 10.0    # escalate label for 10%+ edges
    MATCHUP_MIN_AMERICAN = -250  # skip matchups priced heavier than -250
    MATCHUP_MAX_AMERICAN =  400  # skip lopsided "not really a matchup" lines
    MATCHUP_MAX_RATIO = 1.8      # model/book ratio cap (overconfidence guard)
    matchup_source = output.get("threeBallsSource")
    if matchup_source and matchup_source != "SyntheticTeeTimes":
        for group in output.get("threeBalls") or []:
            group_type = group.get("type") or "2ball"
            round_num = group.get("round")
            players = group.get("players") or []
            for p in players:
                ev = p.get("ev")
                if not isinstance(ev, (int, float)) or ev < MATCHUP_EV_THRESHOLD:
                    continue
                best = p.get("bestBook") or {}
                odds_val = best.get("american")
                book_label = (best.get("book") or "").upper()
                if not isinstance(odds_val, (int, float)):
                    continue
                # Sanity band — skip lopsided or flat-pricing lines
                if odds_val < MATCHUP_MIN_AMERICAN or odds_val > MATCHUP_MAX_AMERICAN:
                    continue
                # Overconfidence guard
                book_implied_p = _american_to_implied_prob(odds_val) or 0
                model_prob_p = p.get("deadHeatWinValue") or 0
                if book_implied_p > 0 and model_prob_p / book_implied_p > MATCHUP_MAX_RATIO:
                    continue
                # Build opponent summary so the Discord reader sees the
                # matchup in context (who we're backing vs. who)
                opponents = [op.get("name") for op in players if op is not p]
                opp_str = " vs ".join(o for o in opponents if o)
                win_pct = (p.get("deadHeatWinValue") or 0) * 100
                conf = min(95, round(60 + ev * 2.5))  # rough mapping
                tier = "STRONG" if ev >= MATCHUP_EV_STRONG else "EDGE"
                prop_label = (
                    f"{group_type.upper()} R{round_num} — {p.get('name')} vs "
                    f"{opp_str} @ {book_label} "
                    f"{'+' if odds_val > 0 else ''}{int(odds_val)}"
                )
                alerts.append({
                    "player": p.get("name", ""),
                    "prop": prop_label,
                    "model": f"{win_pct:.1f}% win",
                    "edge": f"{tier} +{ev:.1f}% EV",
                    "conf": conf,
                    "rank": "MU",
                })

    if not alerts:
        print(
            f"  No high-edge alerts this cycle "
            f"({skipped_no_line} player/market pairs skipped — no book line posted)"
        )
        return

    # Sort by confidence
    alerts.sort(key=lambda x: x["conf"], reverse=True)
    # Discord embed field limit is 25. With 3 placement picks × 6 markets =
    # 18 max placements, plus a few birdies/bogeys/matchups, we stay well
    # under the limit.
    top_alerts = alerts[:22]

    # Build Discord embed
    fields = []
    for a in top_alerts:
        fields.append({
            "name": f"#{a['rank']} {a['player']}",
            "value": f"**{a['prop']}** | Model: {a['model']} | Edge: {a['edge']} | Conf: {a['conf']}%",
            "inline": False,
        })

    payload = {
        "embeds": [{
            "title": "🔬 PropsBot Golf — High-Edge Prop + Matchup Alerts [BETA]",
            "description": f"**{event_name}** | {len(alerts)} signals found, showing top {len(top_alerts)}\n*Beta signals — model confidence thresholds are experimental.*",
            "color": 1441730,  # #15ffc2
            "fields": fields,
            "footer": {"text": "PropsBot Golf Intelligence [BETA] · Educational Tool Only · Not financial advice"},
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }]
    }

    try:
        import urllib.request
        req = urllib.request.Request(
            DISCORD_WEBHOOK_URL,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
            method="POST"
        )
        resp = urllib.request.urlopen(req, timeout=10)
        print(f"  Sent {len(top_alerts)} alerts to Discord (HTTP {resp.status})")
    except Exception as e:
        print(f"  Discord alert failed: {e}")


# ============================================================
# MASTERS / AUGUSTA INTEL
# ============================================================

AUGUSTA_HOLE_NAMES = {
    1: "Tea Olive", 2: "Pink Dogwood", 3: "Flowering Peach",
    4: "Flowering Crab Apple", 5: "Magnolia", 6: "Juniper",
    7: "Pampas", 8: "Yellow Jasmine", 9: "Carolina Cherry",
    10: "Camellia", 11: "White Dogwood", 12: "Golden Bell",
    13: "Azalea", 14: "Chinese Fir", 15: "Firethorn",
    16: "Redbud", 17: "Nandina", 18: "Holly"
}

# Known BDL course_ids — Augusta confirmed; others discovered dynamically
BDL_COURSE_ID_MAP = {
    "augusta": 37,
}


def bdl_find_course_id(course_name):
    """Search BDL courses endpoint for a matching course_id by name."""
    if not course_name:
        return None
    courses = bdl_fetch_all("courses", {})
    if not courses:
        return None
    cn = course_name.lower()
    # Exact or strong partial match
    for c in courses:
        name = (c.get("name") or c.get("course_name") or "").lower()
        if not name:
            continue
        if name == cn or name in cn or cn in name:
            return c.get("id") or c.get("course_id")
    # Fallback: word-overlap match (requires 2+ meaningful words)
    cn_words = [w for w in cn.split() if len(w) > 3]
    for c in courses:
        name = (c.get("name") or c.get("course_name") or "").lower()
        if sum(1 for w in cn_words if w in name) >= 2:
            return c.get("id") or c.get("course_id")
    return None


def bdl_parse_hole_stats(stats_raw, course_id=None):
    """
    Parse raw BDL tournament_course_stats into a dict keyed by hole number.
    Returns {hole_num: {avg, eagle, birdie, parPct, bogey, dbl, difficultyRank}}
    """
    by_hole = {}
    for s in (stats_raw or []):
        num = s.get("hole_number") or s.get("hole")
        if not num:
            continue
        num = int(num)
        total = (
            (s.get("eagles") or 0) + (s.get("birdies") or 0) +
            (s.get("pars") or 0) + (s.get("bogeys") or 0) +
            (s.get("double_bogeys") or 0)
        )
        if total == 0:
            continue
        eagle_pct  = round((s.get("eagles") or 0) / total * 100, 1)
        birdie_pct = round((s.get("birdies") or 0) / total * 100, 1)
        par_pct    = round((s.get("pars") or 0) / total * 100, 1)
        bogey_pct  = round(((s.get("bogeys") or 0) + (s.get("double_bogeys") or 0)) / total * 100, 1)
        dbl_pct    = round((s.get("double_bogeys") or 0) / total * 100, 1)
        by_hole[num] = {
            "avg": s.get("scoring_average"),
            "eagle": eagle_pct,
            "birdie": birdie_pct,
            "parPct": par_pct,
            "bogey": bogey_pct,
            "dbl": dbl_pct,
            "difficultyRank": s.get("difficulty_rank"),
            "scoringDiff": s.get("scoring_diff"),
        }
    return by_hole


def bdl_build_course_intel(course_key, current_tid, course_name="", event_name="", par=72, yards=7000, course_id=None):
    """
    Build hole-by-hole intelligence for any PGA Tour venue.
    Fetches par/yardage + historical/live scoring from BDL.
    Returns a COURSE_DATA-compatible dict ready for output["courses"][course_key].
    """
    print(f"\n[COURSE INTEL] Building hole data for {course_key} (event={event_name})...")

    # 1. Resolve course_id
    if not course_id:
        course_id = BDL_COURSE_ID_MAP.get(course_key)
    if not course_id:
        course_id = bdl_find_course_id(course_name)
    if not course_id:
        print(f"  Could not find BDL course_id for '{course_name}' — skipping hole data")
        return None

    # Cache for next time
    if course_key not in BDL_COURSE_ID_MAP:
        BDL_COURSE_ID_MAP[course_key] = course_id
    print(f"  course_id={course_id}")

    # 2. Hole par + yardage
    holes_raw = bdl_fetch_all("course_holes", {"course_ids[]": str(course_id)})
    holes_by_num = {}
    for h in (holes_raw or []):
        num = h.get("hole_number") or h.get("number")
        if num:
            holes_by_num[int(num)] = {
                "par": h.get("par"),
                "yards": h.get("yardage") or h.get("yards"),
            }
    print(f"  Hole par/yardage: {len(holes_by_num)} holes")

    # 3. Try current-year (live) stats first
    live_raw = bdl_fetch_all("tournament_course_stats", {
        "tournament_ids[]": str(current_tid),
        "course_id": str(course_id),
    })
    stat_by_hole = bdl_parse_hole_stats(live_raw)
    is_live = bool(stat_by_hole)
    if is_live:
        print(f"  Live scoring data: {len(stat_by_hole)} holes")
    else:
        # 4. Fall back to most recent completed tournament at this course
        print("  No live data — searching for previous tournament at same course...")
        past = bdl_fetch_all("tournaments", {"course_ids[]": str(course_id), "status": "COMPLETED"})
        if past:
            past.sort(key=lambda t: t.get("start_date", ""), reverse=True)
            prev_tid = past[0].get("id")
            if prev_tid and prev_tid != current_tid:
                print(f"  Using prev tournament id={prev_tid} ({past[0].get('name','')})")
                hist_raw = bdl_fetch_all("tournament_course_stats", {
                    "tournament_ids[]": str(prev_tid),
                    "course_id": str(course_id),
                })
                stat_by_hole = bdl_parse_hole_stats(hist_raw)
                if stat_by_hole:
                    print(f"  Historical scoring: {len(stat_by_hole)} holes")

    # 5. Per-round averages (try round_number filter; gracefully skip if BDL doesn't support it)
    round_avgs = {1: {}, 2: {}, 3: {}, 4: {}}
    for rnd in [1, 2, 3, 4]:
        rnd_raw = bdl_fetch_all("tournament_course_stats", {
            "tournament_ids[]": str(current_tid),
            "course_id": str(course_id),
            "round_number": str(rnd),
        })
        if rnd_raw:
            for s in rnd_raw:
                num = s.get("hole_number") or s.get("hole")
                avg = s.get("scoring_average")
                if num and avg:
                    round_avgs[rnd][int(num)] = avg

    # 6. Build holes list
    holes_list = []
    for n in range(1, 19):
        info = holes_by_num.get(n, {})
        stat = stat_by_hole.get(n, {})
        avg  = stat.get("avg") or info.get("par", 4)
        holes_list.append({
            "hole":   n,
            "par":    info.get("par"),
            "yards":  info.get("yards"),
            "r1":     round_avgs[1].get(n, avg),
            "r2":     round_avgs[2].get(n, avg),
            "r3":     round_avgs[3].get(n, avg),
            "r4":     round_avgs[4].get(n, avg),
            "eagle":  stat.get("eagle", 0),
            "birdie": stat.get("birdie", 0),
            "parPct": stat.get("parPct", 0),
            "bogey":  stat.get("bogey", 0),
            "dbl":    stat.get("dbl", 0),
            "difficultyRank":  stat.get("difficultyRank"),
            "scoringDiff":     stat.get("scoringDiff"),
        })

    holes_with_par = sum(1 for h in holes_list if h["par"])
    print(f"  [COURSE INTEL] Done — {holes_with_par}/18 holes with par data, {len(stat_by_hole)}/18 with scoring")

    # Fetch course meta (architect / grasses / established) from /courses.
    # These fields are static per course and add real broadcast-style context
    # (e.g. "Donald Ross design · bentgrass greens"). Cheap call — one course id.
    course_meta = {}
    try:
        cmeta_rows = bdl_fetch_all("courses", {"course_ids[]": str(course_id)}, max_pages=1)
        if cmeta_rows:
            c = cmeta_rows[0]
            course_meta = {
                "architect":    c.get("architect"),
                "established":  c.get("established"),
                "fairwayGrass": c.get("fairway_grass"),
                "roughGrass":   c.get("rough_grass"),
                "greenGrass":   c.get("green_grass"),
            }
    except Exception as _e:
        print(f"  [WARN] Course meta fetch failed: {_e}")

    out = {
        "name":    course_name or course_key.replace("_", " ").title(),
        "event":   event_name,
        "par":     par,
        "yards":   yards,
        "holes":   holes_list,
        "isLive":  is_live,
        "source":  "BallDontLie PGA API",
    }
    out.update({k: v for k, v in course_meta.items() if v})
    return out


def bdl_build_masters_intel(tournament_id=20, course_id=37):
    """
    Build comprehensive Masters / Augusta National intelligence package.

    Pulls course holes, historical hole-by-hole scoring, live scoring (when
    available), past results, and player SG breakdowns. Returns a merged dict
    ready for the PropsBot frontend.

    Args:
        tournament_id: 2026 Masters = 20 (default). 2025 Masters = 60.
        course_id: Augusta National = 37 (default).

    Returns:
        dict with keys: augusta_holes, historical_results, augusta_hole_names
    """
    print("\n[MASTERS INTEL] Building Augusta intelligence package...")

    # ------------------------------------------------------------------
    # 1. Course holes — par & yardage per hole
    # ------------------------------------------------------------------
    print("  Fetching Augusta course holes (course_id=37)...")
    holes_raw = bdl_fetch_all("course_holes", {"course_ids[]": str(course_id)})
    holes_by_num = {}
    for h in (holes_raw or []):
        num = h.get("hole_number") or h.get("number")
        if num:
            holes_by_num[int(num)] = {
                "par": h.get("par"),
                "yards": h.get("yardage") or h.get("yards"),
            }
    print(f"    Got {len(holes_by_num)} holes")

    # ------------------------------------------------------------------
    # 2. Historical hole-by-hole scoring (2025 Masters, tournament_id=60)
    # ------------------------------------------------------------------
    print("  Fetching 2025 Masters hole-by-hole stats (tournament_id=60)...")
    hist_stats_raw = bdl_fetch_all(
        "tournament_course_stats",
        {"tournament_ids[]": "60", "course_id": str(course_id)}
    )
    hist_by_hole = {}
    for s in (hist_stats_raw or []):
        num = s.get("hole_number") or s.get("hole")
        if not num:
            continue
        num = int(num)
        total_played = (
            (s.get("eagles") or 0) + (s.get("birdies") or 0) +
            (s.get("pars") or 0) + (s.get("bogeys") or 0) +
            (s.get("double_bogeys") or 0)
        )
        birdie_pct = round((s.get("birdies") or 0) / total_played * 100, 1) if total_played else 0
        bogey_pct = round(
            ((s.get("bogeys") or 0) + (s.get("double_bogeys") or 0)) / total_played * 100, 1
        ) if total_played else 0

        hist_by_hole[num] = {
            "scoringAvg": s.get("scoring_average"),
            "scoringDiff": s.get("scoring_diff"),
            "difficultyRank": s.get("difficulty_rank"),
            "eagles": s.get("eagles") or 0,
            "birdies": s.get("birdies") or 0,
            "pars": s.get("pars") or 0,
            "bogeys": s.get("bogeys") or 0,
            "doublePlus": s.get("double_bogeys") or 0,
            "birdiePct": birdie_pct,
            "bogeyPct": bogey_pct,
        }
    print(f"    Got historical stats for {len(hist_by_hole)} holes")

    # ------------------------------------------------------------------
    # 3. Live hole-by-hole scoring (2026 Masters) — null before tournament
    # ------------------------------------------------------------------
    print(f"  Fetching 2026 Masters live stats (tournament_id={tournament_id})...")
    live_stats_raw = bdl_fetch_all(
        "tournament_course_stats",
        {"tournament_ids[]": str(tournament_id), "course_id": str(course_id)}
    )
    live_by_hole = {}
    for s in (live_stats_raw or []):
        num = s.get("hole_number") or s.get("hole")
        if not num:
            continue
        num = int(num)
        total_played = (
            (s.get("eagles") or 0) + (s.get("birdies") or 0) +
            (s.get("pars") or 0) + (s.get("bogeys") or 0) +
            (s.get("double_bogeys") or 0)
        )
        if total_played == 0:
            continue  # No data yet — tournament hasn't started
        birdie_pct = round((s.get("birdies") or 0) / total_played * 100, 1)
        bogey_pct = round(
            ((s.get("bogeys") or 0) + (s.get("double_bogeys") or 0)) / total_played * 100, 1
        )
        live_by_hole[num] = {
            "scoringAvg": s.get("scoring_average"),
            "scoringDiff": s.get("scoring_diff"),
            "difficultyRank": s.get("difficulty_rank"),
            "eagles": s.get("eagles") or 0,
            "birdies": s.get("birdies") or 0,
            "pars": s.get("pars") or 0,
            "bogeys": s.get("bogeys") or 0,
            "doublePlus": s.get("double_bogeys") or 0,
            "birdiePct": birdie_pct,
            "bogeyPct": bogey_pct,
        }
    if live_by_hole:
        print(f"    Got LIVE stats for {len(live_by_hole)} holes")
    else:
        print("    No live data yet (tournament not started or no rounds completed)")

    # ------------------------------------------------------------------
    # 4. Merge into augusta_holes list (1-18)
    # ------------------------------------------------------------------
    augusta_holes = []
    for n in range(1, 19):
        hole_info = holes_by_num.get(n, {})
        augusta_holes.append({
            "hole": n,
            "par": hole_info.get("par"),
            "yards": hole_info.get("yards"),
            "name": AUGUSTA_HOLE_NAMES.get(n, f"Hole {n}"),
            "historical": hist_by_hole.get(n),
            "live": live_by_hole.get(n) if live_by_hole else None,
        })

    # ------------------------------------------------------------------
    # 5. Historical results — 2025 Masters (tournament_id=60), top 20
    # ------------------------------------------------------------------
    print("  Fetching 2025 Masters results (tournament_id=60)...")
    results_raw = bdl_fetch_all("tournament_results", {"tournament_ids[]": "60"})
    historical_results = []
    for r in sorted(results_raw or [], key=lambda x: x.get("position") or 999):
        pos = r.get("position")
        try:
            pos = int(pos) if pos else 999
        except (ValueError, TypeError):
            pos = 999
        if pos <= 20:
            player = r.get("player", {})
            name = (
                f"{player.get('first_name', '')} {player.get('last_name', '')}".strip()
                if isinstance(player, dict) else str(r.get("player_name", "Unknown"))
            )
            historical_results.append({
                "position": pos,
                "name": name,
                "score": r.get("score") or r.get("total_score"),
                "earnings": r.get("earnings") or r.get("money"),
            })
    print(f"    Got {len(historical_results)} results (top 20)")

    # ------------------------------------------------------------------
    # 6. Player SG breakdowns — 2025 Masters player_round_stats
    # ------------------------------------------------------------------
    print("  Fetching 2025 Masters player round stats for SG data...")
    sg_raw = bdl_fetch_all("player_round_stats", {"tournament_id": "60"}, max_pages=5)
    player_sg = {}
    for ps in (sg_raw or []):
        player = ps.get("player", {})
        pid = player.get("id") or ps.get("player_id")
        if not pid:
            continue
        name = (
            f"{player.get('first_name', '')} {player.get('last_name', '')}".strip()
            if isinstance(player, dict) else "Unknown"
        )
        if pid not in player_sg:
            player_sg[pid] = {
                "name": name,
                "rounds": 0,
                "sg_total": 0, "sg_ott": 0, "sg_app": 0,
                "sg_atg": 0, "sg_putt": 0,
            }
        entry = player_sg[pid]
        entry["rounds"] += 1
        entry["sg_total"] += ps.get("sg_total") or ps.get("strokes_gained_total") or 0
        entry["sg_ott"] += ps.get("sg_off_the_tee") or ps.get("sg_ott") or 0
        entry["sg_app"] += ps.get("sg_approach") or ps.get("sg_app") or 0
        entry["sg_atg"] += ps.get("sg_around_the_green") or ps.get("sg_atg") or 0
        entry["sg_putt"] += ps.get("sg_putting") or ps.get("sg_putt") or 0

    # Average SG per round
    sg_leaders = []
    for pid, sg in player_sg.items():
        rd = sg["rounds"]
        if rd > 0:
            sg_leaders.append({
                "name": sg["name"],
                "rounds": rd,
                "sgTotal": round(sg["sg_total"] / rd, 2),
                "sgOTT": round(sg["sg_ott"] / rd, 2),
                "sgAPP": round(sg["sg_app"] / rd, 2),
                "sgATG": round(sg["sg_atg"] / rd, 2),
                "sgPutt": round(sg["sg_putt"] / rd, 2),
            })
    sg_leaders.sort(key=lambda x: x["sgTotal"], reverse=True)
    print(f"    Got SG data for {len(sg_leaders)} players")

    # ------------------------------------------------------------------
    # FINAL OUTPUT
    # ------------------------------------------------------------------
    intel = {
        "augusta_holes": augusta_holes,
        "historical_results": historical_results,
        "sg_leaders_2025": sg_leaders[:30],
        "augusta_hole_names": AUGUSTA_HOLE_NAMES,
        "meta": {
            "source": "BallDontLie PGA API",
            "historical_tournament_id": 60,
            "live_tournament_id": tournament_id,
            "course_id": course_id,
            "built_at": datetime.utcnow().isoformat() + "Z",
            "has_live_data": bool(live_by_hole),
        }
    }
    print(f"  [MASTERS INTEL] Complete — {len(augusta_holes)} holes, "
          f"{len(historical_results)} results, {len(sg_leaders)} SG players, "
          f"live={'YES' if live_by_hole else 'NO'}")
    return intel


# ============================================================
# TEE TIMES — ESPN FREE API
# ============================================================

def fetch_tee_times(bdl_field=None, expected_event_name=None):
    """Fetch tee times, preferring BallDontLie field data (paid, structured).
    Falls back to ESPN scoreboard. BDL exposes tee_time on tournament_field rows.

    expected_event_name (optional): if provided, ESPN fallback only emits tee
    times when ESPN's current event token-overlaps this name. Prevents
    "PGA Championship tee times" being filled with last week's Truist data
    during the Mon-Wed gap between events.
    """
    tee_times = []

    # Preferred: BDL tournament_field (we pay for it — use it)
    if bdl_field:
        print(f"[TEE TIMES] Using BDL tournament field ({len(bdl_field)} entries)")
        for row in bdl_field:
            player = row.get("player") or {}
            if isinstance(player, dict):
                name = (player.get("display_name")
                        or f"{player.get('first_name','')} {player.get('last_name','')}".strip())
            else:
                name = str(player)
            tee_time = row.get("tee_time") or row.get("teeTime") or ""
            start_hole = row.get("start_hole") or row.get("starting_hole") or 1
            round_num = row.get("round") or row.get("round_number") or 1
            if name:
                tee_times.append({
                    "player": name,
                    "teeTime": tee_time,
                    "round": round_num,
                    "startHole": start_hole,
                })
        with_times = sum(1 for t in tee_times if t["teeTime"])
        if with_times > 0:
            print(f"  {with_times}/{len(tee_times)} entries have tee times")
            return tee_times
        print("  BDL field had no tee_time values — falling back to ESPN")
        tee_times = []

    # Fallback: ESPN scoreboard. ESPN buries per-round tee times inside
    # competitor.linescores[round-1].statistics.categories[0].stats[-1].displayValue
    # (a human-readable datetime like "Thu Apr 16 13:50:00 PDT 2026").
    print("[TEE TIMES] Fetching tee times from ESPN...")
    url = "https://site.api.espn.com/apis/site/v2/sports/golf/pga/scoreboard"
    data = fetch_json(url)
    if not data:
        return tee_times

    try:
        events = data.get("events", [])
        if not events:
            return tee_times
        event = events[0]
        # Gate ESPN tee times by event match — same logic as the leaderboard fix.
        if expected_event_name:
            espn_name = (event.get("name") or "").strip().lower()
            expected = expected_event_name.strip().lower()
            STOPWORDS = {"the","championship","tournament","open","classic","of","and","&"}
            etoks = {w for w in espn_name.split() if w not in STOPWORDS and len(w) > 2}
            xtoks = {w for w in expected.split() if w not in STOPWORDS and len(w) > 2}
            if (etoks and xtoks) and not (etoks & xtoks):
                print(f"  ESPN event '{event.get('name')}' does not match expected "
                      f"'{expected_event_name}' — skipping ESPN tee times.")
                return tee_times
        for comp in event.get("competitions", []):
            for competitor in comp.get("competitors", []):
                athlete = competitor.get("athlete", {}) or {}
                name = athlete.get("displayName", "")
                if not name:
                    continue
                # Emit one entry per round from linescores
                for ls in (competitor.get("linescores") or []):
                    period = ls.get("period")
                    if not period or not (1 <= period <= 4):
                        continue
                    tee_time = ""
                    try:
                        stats = ls.get("statistics", {}).get("categories", [{}])[0].get("stats", [])
                        # The last stat entry carries displayValue = tee-time datetime
                        for s in stats:
                            dv = s.get("displayValue", "")
                            if isinstance(dv, str) and any(day in dv for day in
                                    ("Mon ", "Tue ", "Wed ", "Thu ", "Fri ", "Sat ", "Sun ")):
                                tee_time = dv
                                break
                    except (IndexError, AttributeError, TypeError):
                        pass
                    tee_times.append({
                        "player": name,
                        "teeTime": tee_time,
                        "round": period,
                        "startHole": 1,  # ESPN scoreboard doesn't expose start hole
                    })
        with_time = sum(1 for t in tee_times if t["teeTime"])
        print(f"  Got {len(tee_times)} tee-time entries from ESPN ({with_time} with times)")
    except Exception as e:
        print(f"  Error parsing tee times: {e}")
    return tee_times


# ============================================================
# ODDS MOVEMENT TRACKER
# ============================================================

def compute_odds_movement(current_players, archive_dir):
    """
    Compare current odds to the most recent archive snapshot.
    Returns dict: {player_name: {current, previous, change, direction}}
    """
    print("[ODDS MOVEMENT] Computing line movement vs previous snapshot...")

    # Find most recent archive file (not today's)
    today = datetime.now().strftime("%Y-%m-%d")
    archive_path = Path(archive_dir) / "history"

    if not archive_path.exists():
        print("  No archive found. Skipping movement.")
        return {}

    archives = sorted(archive_path.glob("*.json"), reverse=True)
    prev_file = None
    for f in archives:
        if f.stem != today:
            prev_file = f
            break

    if not prev_file:
        print("  No previous snapshot found.")
        return {}

    try:
        with open(prev_file) as f:
            prev_data = json.load(f)
        prev_players = {p["name"]: p for p in prev_data.get("players", [])}
    except Exception as e:
        print(f"  Error reading archive: {e}")
        return {}

    movement = {}
    for player in current_players:
        name = player.get("name", "")
        curr_odds = player.get("odds", {})
        curr_dk = curr_odds.get("dk") or curr_odds.get("fd") or curr_odds.get("mgm")

        prev_player = prev_players.get(name, {})
        prev_odds = prev_player.get("odds", {})
        prev_dk = prev_odds.get("dk") or prev_odds.get("fd") or prev_odds.get("mgm")

        if curr_dk and prev_dk and curr_dk != prev_dk:
            try:
                change = int(curr_dk) - int(prev_dk)
                movement[name] = {
                    "current": curr_dk,
                    "previous": prev_dk,
                    "change": change,
                    "direction": "shorter" if change < 0 else "longer",
                    "significant": abs(change) >= 500
                }
            except (ValueError, TypeError):
                pass

    print(f"  Found {len(movement)} players with odds movement")
    return movement


# ============================================================
# CUT LINE PREDICTOR
# ============================================================

def predict_player_position_probs(players, course_key=None, weather=None,
                                  tournament_sg=None, course_par=None,
                                  live_leaderboard=None, model_params_override=None,
                                  sims=4000, cut_size=65, course_sg_weights=None):
    """Full 4-round tournament Monte Carlo. Returns per-player probabilities
    for win / top-5 / top-10 / top-20 / make-cut.

    Reuses the same per-player score distribution as ``predict_matchups`` and
    ``predict_cut_line`` (mean = par - SG - fit - form - weather, per-player
    std, shared round shock). When live round scores are present they are
    held fixed and only remaining rounds are simulated — tightens predictions
    dramatically as the week progresses.

    Output: ``{ normalized_name: {win, top5, top10, top20, makeCut, expectedTotal} }``.

    This is the substrate the frontend uses to compute honest edge against
    real book lines (top-5 and make-cut from BDL ``/futures``). Top-10 and
    Top-20 markets are estimated by BDL but aren't natively offered, so we
    use the model probabilities directly without a comparison line.
    """
    import random
    random.seed(11)
    print("[PROPS] Simulating full-tournament position probabilities...")

    if not players:
        return {}

    par_round = int(course_par) if course_par else 71
    mp = model_params_override or load_model_params()
    par_baseline = mp.get("parBaseline", par_round)
    base_std = mp.get("baseStd", 2.85)
    shock_std = mp.get("roundShockStd", 1.0)
    sg_blend_season = mp.get("sgBlendSeason", 0.7)
    sg_blend_live = mp.get("sgBlendLive", 0.3)
    fit_scale = mp.get("fitBoostScale", 25.0)
    hot_boost = mp.get("hotFormBoost", 0.25)
    cold_penalty = mp.get("coldFormPenalty", -0.25)
    wind_slope = mp.get("windPenaltySlope", 0.08)
    wind_std_slope = mp.get("windStdSlope", 0.05)
    wind_threshold = mp.get("windThresholdMph", 12.0)

    # Weather
    wx_penalty = 0.0
    wx_std_bump = 0.0
    if isinstance(weather, dict):
        winds = []
        for day in (weather.get("forecast") or []):
            w = day.get("windMph") or day.get("wind_mph") or day.get("wind")
            if isinstance(w, (int, float)):
                winds.append(w)
        if winds:
            avg_wind = sum(winds) / len(winds)
            wx_penalty = max(0.0, (avg_wind - wind_threshold) * wind_slope)
            wx_std_bump = max(0.0, (avg_wind - wind_threshold) * wind_std_slope)

    live_sg = {}
    for row in (tournament_sg or []):
        live_sg[normalize_name(row.get("name", ""))] = row.get("sgTotal", 0) or 0

    # Live round scores (R1..R4 strokes per player when played)
    fixed_rounds = {}
    if live_leaderboard:
        for row in live_leaderboard:
            name = normalize_name(row.get("name", ""))
            rs = {}
            for ri in (1, 2, 3, 4):
                v = row.get(f"round{ri}")
                if isinstance(v, (int, float)) and v > 0:
                    rs[ri] = float(v)
            if rs:
                fixed_rounds[name] = rs

    pp_list = []
    for p in players:
        sg_season = effective_sg(p, course_weights=course_sg_weights)
        name_key = normalize_name(p.get("name", ""))
        sg_live = live_sg.get(name_key, 0.0)
        sg_blended = sg_blend_season * sg_season + sg_blend_live * sg_live if sg_live else sg_season
        fit_boost = 0.0
        if course_key:
            fit = (p.get("courseFit") or {}).get(course_key, 75)
            if isinstance(fit, (int, float)):
                fit_boost = (fit - 75) / fit_scale
        form = p.get("recentForm") or {}
        form_boost = (hot_boost if form.get("trend") == "hot"
                      else cold_penalty if form.get("trend") == "cold" else 0.0)
        empirical_std = p.get("scoreStd")
        if isinstance(empirical_std, (int, float)) and 1.5 <= empirical_std <= 5.0:
            std_i = float(empirical_std) + wx_std_bump
        else:
            std_i = base_std + wx_std_bump
        if form.get("trend") in ("hot", "cold"):
            std_i += 0.15
        mean_round = par_baseline - sg_blended - fit_boost - form_boost + wx_penalty
        pp_list.append({
            "name": p.get("name"),
            "key": name_key,
            "mean_round": mean_round,
            "std": std_i,
            "fixed": fixed_rounds.get(name_key, {}),
        })

    n = len(pp_list)
    wins = [0] * n
    top5 = [0] * n
    top10 = [0] * n
    top20 = [0] * n
    cuts = [0] * n
    sum_totals = [0.0] * n
    par_36 = par_round * 2

    for s in range(sims):
        # Shared round shocks
        shocks = [random.gauss(0, shock_std) for _ in range(4)]
        totals_36 = []  # for cut determination
        totals_72 = []  # for finish position
        for pp in pp_list:
            total = 0
            fr = pp["fixed"]
            for ri in (1, 2, 3, 4):
                if ri in fr:
                    total += fr[ri]
                else:
                    r = round(random.gauss(pp["mean_round"], pp["std"]) + shocks[ri - 1])
                    total += r
                if ri == 2:
                    totals_36.append(total)
            totals_72.append(total)
        # Cut: top cut_size+ties after 36 holes
        cut_thresh_sorted = sorted(totals_36)
        cut_idx = min(cut_size - 1, n - 1)
        cut_thresh = cut_thresh_sorted[cut_idx]
        # Finishing position over 72 holes
        sorted_72 = sorted(enumerate(totals_72), key=lambda t: t[1])
        for rank_idx, (orig_i, t) in enumerate(sorted_72):
            if rank_idx == 0:
                wins[orig_i] += 1
            if rank_idx < 5:
                top5[orig_i] += 1
            if rank_idx < 10:
                top10[orig_i] += 1
            if rank_idx < 20:
                top20[orig_i] += 1
        for i, t36 in enumerate(totals_36):
            if t36 <= cut_thresh:
                cuts[i] += 1
            sum_totals[i] += totals_72[i]

    out = {}
    for i, pp in enumerate(pp_list):
        out[pp["key"]] = {
            "name": pp["name"],
            "win": round(wins[i] / sims, 4),
            "top5": round(top5[i] / sims, 4),
            "top10": round(top10[i] / sims, 4),
            "top20": round(top20[i] / sims, 4),
            "makeCut": round(cuts[i] / sims, 4),
            "expectedTotal": round(sum_totals[i] / sims, 1),
            "expectedParRel": round(sum_totals[i] / sims - par_round * 4, 1),
        }
    # Quick sanity: top-3 win-prob players
    top_win = sorted(out.values(), key=lambda x: -x["win"])[:3]
    print(f"  Top win probs: " + ", ".join(
        f"{x['name']} {x['win']*100:.1f}%" for x in top_win))
    return out


# Generic per-hole outcome distribution used when course-specific scoring
# stats aren't available (early tournament week, new venues). Tour-typical
# averages: ~2% eagles, ~22% birdies, ~57% pars, ~16% bogeys, ~3% double+.
# Tuned to roughly match field scoring of par + 1 stroke per round.
GENERIC_HOLE_DIST = {
    "eagle": 0.02, "birdie": 0.22, "par": 0.57, "bogey": 0.16, "double": 0.03,
}

# Per-hole skill multipliers for a +1.0 SG (per round) player. Negative-SG
# players get the inverse. Tuned so a +1 SG player gains ~0.5 strokes per
# round vs field (matches DataGolf empirical relationship).
HOLE_SKILL_MULT = {
    "eagle": 1.40, "birdie": 1.25, "par": 1.00, "bogey": 0.80, "double": 0.65,
}


def _build_hole_dists(course_data):
    """Extract per-hole base outcome distributions from course data.

    Returns list of 18 dicts {par, dist:{eagle, birdie, par, bogey, double}}.
    Falls back to GENERIC_HOLE_DIST when a hole has no historical scoring
    stats yet.
    """
    holes = (course_data or {}).get("holes") or []
    out = []
    for h in holes[:18]:
        par = h.get("par") or 4
        eagles = h.get("eagles") or 0
        birdies = h.get("birdies") or 0
        pars = h.get("pars") or 0
        bogeys = h.get("bogeys") or 0
        doubles = h.get("doubles") or h.get("doubleBogeys") or 0
        total = eagles + birdies + pars + bogeys + doubles
        if total >= 10:  # need a real sample size
            dist = {
                "eagle":  eagles / total,
                "birdie": birdies / total,
                "par":    pars / total,
                "bogey":  bogeys / total,
                "double": doubles / total,
            }
        else:
            dist = dict(GENERIC_HOLE_DIST)
        out.append({"par": int(par), "dist": dist})
    # Pad with generic par-4 holes if course data has fewer than 18 entries
    while len(out) < 18:
        out.append({"par": 4, "dist": dict(GENERIC_HOLE_DIST)})
    return out


def _skill_adjusted_dist(base_dist, sg_per_round):
    """Apply skill multipliers and renormalize. Clipped to non-negative."""
    skill = max(-2.5, min(2.5, sg_per_round))  # cap extreme outliers
    adj = {}
    for k, base in base_dist.items():
        mult = HOLE_SKILL_MULT[k]
        # Lerp the multiplier toward 1.0 by |skill|/1.0
        effective_mult = 1.0 + (mult - 1.0) * skill
        effective_mult = max(0.0, effective_mult)
        adj[k] = max(0.0, base * effective_mult)
    s = sum(adj.values())
    if s <= 0:
        return dict(base_dist)
    return {k: v / s for k, v in adj.items()}


def _sample_hole_outcome(dist, rng):
    """Sample one hole outcome from a 5-way categorical distribution.
    Returns string in {eagle, birdie, par, bogey, double}.
    """
    r = rng.random()
    cum = 0.0
    for k in ("eagle", "birdie", "par", "bogey", "double"):
        cum += dist[k]
        if r <= cum:
            return k
    return "par"  # numerical safety


def _outcome_from_uniform(q, dist):
    """Same categorical sample as _sample_hole_outcome but driven by a
    pre-supplied uniform [0,1] value (from the copula). This is the inverse
    CDF mapping: the cumulative distribution F(eagle), F(birdie), ... is
    walked and we return the first category whose threshold q falls below.
    """
    cum = 0.0
    for k in ("eagle", "birdie", "par", "bogey", "double"):
        cum += dist[k]
        if q <= cum:
            return k
    return "par"


# Standard normal CDF Φ(x) — used to convert correlated standard normals
# into uniforms for the copula inverse-CDF transform.
import math as _math
def _norm_cdf(x):
    return 0.5 * (1.0 + _math.erf(x / _math.sqrt(2.0)))


def _sample_copula_uniforms(n_holes, rho_global, rho_local, rng):
    """Draw n_holes correlated uniforms via a Gaussian copula.

    Two-factor structure:
      * Global momentum ``M ~ N(0, 1)`` shared across every hole — captures
        round-wide hot/cold streaks. Loaded by ``√ρ_global``.
      * Local AR(1) chain ``L_i = ρ_local · L_{i-1} + √(1-ρ²_local) · z_i``
        with z_i iid N(0, 1) — captures adjacent-hole correlation that
        decays with hole distance.
      * Latent value ``u_i = √ρ_global · M + √(1-ρ_global) · L_i``
      * Uniform value ``q_i = Φ(u_i)``

    Returns a list of 18 uniforms in [0,1]. The caller maps each to a hole
    outcome via the per-hole categorical inverse CDF.

    Why this matters: pure-independent holes underestimate round-score
    variance and miss the tail thickness needed to price multi-hole props
    (front-9, back-9, hole-N) honestly. AR(1) + shared factor is the
    minimum credible structure — same shape DataGolf uses internally
    (per their public methodology notes).
    """
    M = rng.gauss(0.0, 1.0)
    sqrt_rho_g = rho_global ** 0.5
    sqrt_one_minus_g = (1.0 - rho_global) ** 0.5
    sqrt_one_minus_l2 = (1.0 - rho_local * rho_local) ** 0.5
    uniforms = []
    L_prev = rng.gauss(0.0, 1.0)
    for i in range(n_holes):
        if i == 0:
            L_i = L_prev
        else:
            L_i = rho_local * L_prev + sqrt_one_minus_l2 * rng.gauss(0.0, 1.0)
        u = sqrt_rho_g * M + sqrt_one_minus_g * L_i
        uniforms.append(_norm_cdf(u))
        L_prev = L_i
    return uniforms


# Strokes delta per outcome relative to hole par. Eagles include albatross
# (treated as -2 for simplicity; true albatross is -3 but vanishingly rare).
OUTCOME_TO_DELTA = {"eagle": -2, "birdie": -1, "par": 0, "bogey": 1, "double": 2}


def predict_per_hole_props(players, course_data, course_par=None,
                           model_params_override=None, sims=2000,
                           course_sg_weights=None):
    """Per-hole Monte Carlo for one-round prop distributions.

    For each player, simulates ``sims`` rounds of 18 holes drawing each hole's
    outcome from a course-specific 5-way categorical distribution (eagle /
    birdie / par / bogey / double+). The per-hole base distribution comes from
    BDL ``tournament_course_stats`` aggregated counts; the player's skill
    (sgTotal per round) multiplicatively adjusts the outcome probabilities.

    Aggregates over the sim runs to produce per-player per-round distributions
    for: round score, birdies, bogeys, eagles, double+'s. These power BDL
    ``/odds/player_props`` market pricing — the only path to honest edge on
    round-score and birdie/bogey over/unders.

    Returns ``{ normalized_name: { roundScore: {mean, std, p_dist}, birdies: {...},
    bogeys: {...}, eagles: {...}, doublePlus: {...} } }``.

    The ``p_dist`` for each stat is a dict ``{value: cumulative_p_geq}`` so the
    frontend can compute P(stat > line) for any line by a fast lookup.
    """
    import random
    rng = random.Random(13)
    print("[PER-HOLE] Simulating per-hole outcomes for prop pricing...")

    hole_defs = _build_hole_dists(course_data or {})
    hole_pars = [h["par"] for h in hole_defs]
    par_round_total = sum(hole_pars)

    if not players:
        return {}

    mp = model_params_override or load_model_params()
    sg_blend_season = mp.get("sgBlendSeason", 0.7)

    # Gaussian-copula correlation parameters. Pulled from model_params so a
    # later backtest job can tune them against observed round-score variance.
    rho_global = mp.get("rhoGlobal", 0.10)
    rho_local  = mp.get("rhoLocal",  0.15)

    n_holes = len(hole_defs)
    front_par = sum(hole_pars[:9])
    back_par  = sum(hole_pars[9:18]) if n_holes >= 18 else 0

    out = {}
    for p in players:
        sg = effective_sg(p, course_weights=course_sg_weights)
        # Each hole's skill-adjusted categorical (still computed once per
        # player). The copula adds correlated uniforms drawn per sim; we
        # don't shift the dist itself per sim anymore — that's what the
        # global-momentum factor in the copula handles.
        adj_dists = [_skill_adjusted_dist(h["dist"], sg) for h in hole_defs]

        # Aggregate counters across sims
        round_scores = [0] * sims
        front_scores = [0] * sims
        back_scores  = [0] * sims
        birdies      = [0] * sims
        bogeys       = [0] * sims
        eagles_cnt   = [0] * sims
        doubles_cnt  = [0] * sims
        # Per-hole score deltas across sims for hole-level distributions
        per_hole_deltas = [[0] * sims for _ in range(n_holes)]

        for s in range(sims):
            # Draw 18 correlated uniforms via the Gaussian copula
            qs = _sample_copula_uniforms(n_holes, rho_global, rho_local, rng)
            total = 0
            front_total = 0
            back_total = 0
            for hi in range(n_holes):
                outcome = _outcome_from_uniform(qs[hi], adj_dists[hi])
                delta = OUTCOME_TO_DELTA[outcome]
                hole_score = hole_pars[hi] + delta
                per_hole_deltas[hi][s] = hole_score
                total += hole_score
                if hi < 9:
                    front_total += hole_score
                elif hi < 18:
                    back_total += hole_score
                if outcome == "birdie":
                    birdies[s] += 1
                elif outcome == "bogey":
                    bogeys[s] += 1
                elif outcome == "eagle":
                    eagles_cnt[s] += 1
                elif outcome == "double":
                    doubles_cnt[s] += 1
            round_scores[s] = total
            front_scores[s] = front_total
            back_scores[s]  = back_total

        def _stat_summary(arr, max_val):
            n = len(arr)
            mean = sum(arr) / n
            var = sum((x - mean) ** 2 for x in arr) / n
            cum = {}
            for k in range(max_val + 1):
                cum[str(k)] = round(sum(1 for x in arr if x >= k) / n, 4)
            return {"mean": round(mean, 2), "std": round(var ** 0.5, 2), "pGte": cum}

        def _score_dist(arr, par_total, half_width=5):
            """Mean/std + cumulative pLte[score] table around par_total."""
            n = len(arr)
            mean = sum(arr) / n
            var = sum((x - mean) ** 2 for x in arr) / n
            cum = {}
            for thr in range(par_total - half_width, par_total + half_width + 1):
                cum[str(thr)] = round(sum(1 for x in arr if x <= thr) / n, 4)
            return {"mean": round(mean, 2), "std": round(var ** 0.5, 2), "pLte": cum}

        def _per_hole_stat(arr, par_hole):
            """Per-hole score distribution: mean + pLte at par-2..par+3."""
            n = len(arr)
            mean = sum(arr) / n
            var = sum((x - mean) ** 2 for x in arr) / n
            cum = {}
            for thr in range(max(1, par_hole - 2), par_hole + 4):
                cum[str(thr)] = round(sum(1 for x in arr if x <= thr) / n, 4)
            return {"mean": round(mean, 2), "std": round(var ** 0.5, 2), "pLte": cum}

        out[normalize_name(p.get("name", ""))] = {
            "name": p.get("name"),
            "sims": sims,
            "roundScore":  _score_dist(round_scores, par_round_total),
            "frontNine":   _score_dist(front_scores, front_par),
            "backNine":    _score_dist(back_scores,  back_par) if n_holes >= 18 else None,
            "birdies":     _stat_summary(birdies,     12),
            "bogeys":      _stat_summary(bogeys,      10),
            "eagles":      _stat_summary(eagles_cnt,   3),
            "doublePlus":  _stat_summary(doubles_cnt,  5),
            # Per-hole distributions — keyed by hole number (1-18)
            "holes": {
                str(hi + 1): _per_hole_stat(per_hole_deltas[hi], hole_pars[hi])
                for hi in range(n_holes)
            },
        }

    # Sanity check
    sample = list(out.values())[:3]
    print(f"  Per-hole sims complete for {len(out)} players (copula: rho_global={rho_global}, rho_local={rho_local})")
    for s in sample:
        print(f"    {s['name']}: round avg={s['roundScore']['mean']} (std {s['roundScore']['std']}) | front {s['frontNine']['mean']} | back {s['backNine']['mean'] if s['backNine'] else '-'}")
    return out


def price_player_props(prop_lines, per_hole_props, overround_makecut_default=1.05):
    """For each BDL /odds/player_props market, compute model fair probability.

    BDL ships over/under and milestone markets. We match by player + market
    type (birdies / bogeys / scoring_total / round_X_score) and look up our
    Monte Carlo distribution to derive the fair model probability of the
    over.

    Returns dict keyed by ``(player_norm, prop_type, line)`` →
    ``{modelProbOver, modelProbUnder, fairImpliedOver, edgeOverPct, edgeUnderPct}``.

    With no matching market in prop_lines (empty pre-Wed), returns ``{}``.
    """
    if not prop_lines or not per_hole_props:
        return {}
    out = {}
    for player_key, markets in (prop_lines or {}).items():
        norm = normalize_name(player_key)
        php = per_hole_props.get(norm)
        if not php:
            continue
        for m in (markets if isinstance(markets, list) else []):
            prop_type = (m.get("prop_type") or "").lower()
            line_raw = m.get("line_value")
            try:
                line = float(line_raw)
            except (TypeError, ValueError):
                continue
            # Map prop_type to one of our distribution keys. With the
            # Gaussian copula in place we can now price front-9, back-9, and
            # single-hole markets — all unlocked because the copula sampler
            # gives us a joint distribution over per-hole scores, not just
            # the round total.
            dist_key = None
            hole_match = None  # for single-hole props like "hole_5_score"
            if "front" in prop_type and "9" in prop_type:
                dist_key = "frontNine"
            elif "back" in prop_type and "9" in prop_type:
                dist_key = "backNine"
            elif "hole_" in prop_type or "hole-" in prop_type:
                # Extract hole number from prop_type like "hole_5_score" / "hole-12-par"
                import re as _re
                _m = _re.search(r"hole[_-](\d{1,2})", prop_type)
                if _m:
                    hole_match = _m.group(1)
                    dist_key = "holes"
            elif "birdie" in prop_type:
                dist_key = "birdies"
            elif "bogey" in prop_type:
                dist_key = "bogeys"
            elif "eagle" in prop_type:
                dist_key = "eagles"
            elif "scoring_total" in prop_type or "round" in prop_type or "score" in prop_type:
                dist_key = "roundScore"
            if not dist_key:
                continue
            if dist_key in ("roundScore", "frontNine", "backNine"):
                stat = php.get(dist_key)
                if not stat or not stat.get("pLte"):
                    continue
                p_lte = stat["pLte"]
                int_line = int(round(line))
                # P(score > line). pLte may not include this exact key — use
                # closest available threshold.
                key = str(int_line)
                if key not in p_lte:
                    keys = sorted(int(k) for k in p_lte.keys())
                    # Find nearest available
                    nearest = min(keys, key=lambda k: abs(k - int_line))
                    key = str(nearest)
                p_over = 1.0 - p_lte.get(key, 0.5)
            elif dist_key == "holes":
                hole_stat = (php.get("holes") or {}).get(hole_match)
                if not hole_stat or not hole_stat.get("pLte"):
                    continue
                p_lte = hole_stat["pLte"]
                int_line = int(round(line))
                key = str(int_line)
                if key not in p_lte:
                    keys = sorted(int(k) for k in p_lte.keys())
                    nearest = min(keys, key=lambda k: abs(k - int_line))
                    key = str(nearest)
                p_over = 1.0 - p_lte.get(key, 0.5)
            else:
                p_gte = php[dist_key]["pGte"]
                int_line = int(line + 0.5)
                p_over = p_gte.get(str(int_line), 0.0)
            p_under = 1.0 - p_over
            # De-vig the book pair if both sides exist
            mkt = m.get("market") or {}
            book_over_odds = mkt.get("over_odds") or m.get("over_odds")
            book_under_odds = mkt.get("under_odds") or m.get("under_odds")
            fair_over = None
            edge_over = None
            edge_under = None
            if book_over_odds is not None and book_under_odds is not None:
                fair_over = devig_implied_prob(
                    book_over_odds, opposite_american=book_under_odds
                )
                if fair_over is not None and fair_over > 0:
                    edge_over = round((p_over - fair_over) / fair_over * 100, 1)
                    edge_under = round(((1 - p_over) - (1 - fair_over)) / (1 - fair_over) * 100, 1)
            out[f"{norm}|{prop_type}|{line}"] = {
                "player": player_key,
                "propType": prop_type,
                "line": line,
                "modelProbOver": round(p_over, 4),
                "modelProbUnder": round(p_under, 4),
                "fairImpliedOver": round(fair_over, 4) if fair_over else None,
                "edgeOverPct": edge_over,
                "edgeUnderPct": edge_under,
            }
    return out


def predict_cut_line(players, course_key="augusta", weather=None, tournament_sg=None,
                     course_par=None, cut_size=65, sims=4000, live_leaderboard=None,
                     model_params_override=None, course_sg_weights=None):
    """Monte Carlo cut-line predictor.

    Simulates every player's 36-hole total using the same per-player score
    distribution model as ``predict_matchups`` (mean = par - SG - fit - form,
    std from history or default), then finds the score at which the cumulative
    make-cut count crosses the field's cut threshold (PGA standard: top 65 +
    ties after R2 unless the event overrides).

    When R1 scores are already in (live_leaderboard provides round1 strokes),
    those are held fixed and only R2 is simulated — much sharper prediction
    Friday morning than Wednesday morning.

    Returns the same dict shape the frontend already consumes plus
    ``cumulativeMakeCut`` (probability each player makes the cut), which the
    Cut Bubble filter can use for true cut-bubble selection.
    """
    import random
    random.seed(7)
    print("[CUT PREDICTOR] Simulating cut line...")

    if not players:
        return {
            "predictedCut": None, "predictedScore": None, "fieldStrength": 0,
            "likelyMakers": 0, "confidence": "Low",
            "note": "No field data available.", "model": "monte_carlo",
        }

    par_round = int(course_par) if course_par else 71
    par_36 = par_round * 2

    mp = model_params_override or load_model_params()
    par_baseline = mp.get("parBaseline", par_round)
    base_std = mp.get("baseStd", 2.85)
    shock_std = mp.get("roundShockStd", 1.0)
    sg_blend_season = mp.get("sgBlendSeason", 0.7)
    sg_blend_live = mp.get("sgBlendLive", 0.3)
    fit_scale = mp.get("fitBoostScale", 25.0)
    hot_boost = mp.get("hotFormBoost", 0.25)
    cold_penalty = mp.get("coldFormPenalty", -0.25)
    wind_slope = mp.get("windPenaltySlope", 0.08)
    wind_std_slope = mp.get("windStdSlope", 0.05)
    wind_threshold = mp.get("windThresholdMph", 12.0)

    # Weather penalty
    wx_penalty = 0.0
    wx_std_bump = 0.0
    if isinstance(weather, dict):
        winds = []
        for day in (weather.get("forecast") or []):
            w = day.get("windMph") or day.get("wind_mph") or day.get("wind")
            if isinstance(w, (int, float)):
                winds.append(w)
        if winds:
            avg_wind = sum(winds) / len(winds)
            wx_penalty = max(0.0, (avg_wind - wind_threshold) * wind_slope)
            wx_std_bump = max(0.0, (avg_wind - wind_threshold) * wind_std_slope)

    # Live tournament SG lookup (in-progress events)
    live_sg = {}
    for row in (tournament_sg or []):
        live_sg[normalize_name(row.get("name", ""))] = row.get("sgTotal", 0) or 0

    # Live R1 scores lookup (if available)
    fixed_r1 = {}
    if live_leaderboard:
        for row in live_leaderboard:
            name = normalize_name(row.get("name", ""))
            r1 = row.get("round1")
            if isinstance(r1, (int, float)) and r1 > 0:
                fixed_r1[name] = float(r1)

    # Build per-player score distribution params
    pp_list = []
    for p in players:
        # Skip players with no SG signal at all (auto-added field entries)
        # to avoid populating the cut field with random noise — they were
        # already replaced with field averages so they'd just be a centered
        # blob anyway, but better to mark them explicitly.
        sg_season = effective_sg(p, course_weights=course_sg_weights)
        name_key = normalize_name(p.get("name", ""))
        sg_live = live_sg.get(name_key, 0.0)
        sg_blended = sg_blend_season * sg_season + sg_blend_live * sg_live if sg_live else sg_season

        fit_boost = 0.0
        if course_key:
            fit = (p.get("courseFit") or {}).get(course_key, 75)
            if isinstance(fit, (int, float)):
                fit_boost = (fit - 75) / fit_scale

        form = p.get("recentForm") or {}
        form_boost = (hot_boost if form.get("trend") == "hot"
                      else cold_penalty if form.get("trend") == "cold"
                      else 0.0)

        empirical_std = p.get("scoreStd")
        if isinstance(empirical_std, (int, float)) and 1.5 <= empirical_std <= 5.0:
            std_i = float(empirical_std) + wx_std_bump
        else:
            std_i = base_std + wx_std_bump
        if form.get("trend") in ("hot", "cold"):
            std_i += 0.15

        mean_round = par_baseline - sg_blended - fit_boost - form_boost + wx_penalty
        pp_list.append({
            "name": p.get("name"),
            "key": name_key,
            "mean_round": mean_round,
            "std": std_i,
            "fixed_r1": fixed_r1.get(name_key),
        })

    # Monte Carlo: simulate 36-hole totals
    n = len(pp_list)
    cut_makes = [0] * n
    finish_totals = [[] for _ in range(n)]
    # Always generate two shocks per sim — mixed field (some R1 done, some
    # not) is the common Friday-morning case, and players without R1 need
    # both shocks while those with R1 only consume the R2 shock.
    for s in range(sims):
        shock_r1 = random.gauss(0, shock_std)
        shock_r2 = random.gauss(0, shock_std)
        totals = []
        for pp in pp_list:
            if pp["fixed_r1"] is not None:
                r2 = round(random.gauss(pp["mean_round"], pp["std"]) + shock_r2)
                total = pp["fixed_r1"] + r2
            else:
                r1 = round(random.gauss(pp["mean_round"], pp["std"]) + shock_r1)
                r2 = round(random.gauss(pp["mean_round"], pp["std"]) + shock_r2)
                total = r1 + r2
            totals.append(total)
        # Determine cut threshold for this sim: top cut_size + ties
        sorted_totals = sorted(totals)
        cut_idx = min(cut_size - 1, n - 1)
        cut_thresh = sorted_totals[cut_idx]
        for i, t in enumerate(totals):
            if t <= cut_thresh:
                cut_makes[i] += 1
            finish_totals[i].append(t)

    # Aggregate cut probability per player + expected cut score
    cumulative = []
    for i, pp in enumerate(pp_list):
        make_pct = cut_makes[i] / sims
        cumulative.append({
            "name": pp["name"],
            "key": pp["key"],
            "makeCutProb": round(make_pct, 4),
            "expectedTotal": round(sum(finish_totals[i]) / sims, 1),
        })

    # Predicted cut line = average across sims of the (cut_size-th best total)
    # rounded to integer strokes vs par.
    cut_thresh_per_sim = []
    for s in range(sims):
        sim_totals = sorted(finish_totals[i][s] for i in range(n))
        cut_idx = min(cut_size - 1, n - 1)
        cut_thresh_per_sim.append(sim_totals[cut_idx])
    predicted_score = round(sum(cut_thresh_per_sim) / sims)
    predicted_par_rel = predicted_score - par_36

    # Field strength = average SG of top 30
    sg_vals = sorted([p.get("sgTotal", 0) for p in players if p.get("sgTotal")], reverse=True)
    field_strength = round(sum(sg_vals[:30]) / max(len(sg_vals[:30]), 1), 2) if sg_vals else 0

    likely_makers = sum(1 for c in cumulative if c["makeCutProb"] >= 0.5)

    # Confidence: how tight is the cut distribution? std < 1 stroke = High,
    # < 2 = Medium, else Low. Live R1 sharply tightens this.
    import statistics as _stats
    cut_std = _stats.stdev(cut_thresh_per_sim) if len(cut_thresh_per_sim) > 1 else 0
    confidence = "High" if cut_std < 1.0 else ("Medium" if cut_std < 2.0 else "Low")
    note = ("Live R1 in — only R2 simulated." if fixed_r1
            else f"Pre-tournament forecast over both rounds. Field SG top-30 avg {field_strength:+.2f}.")

    # 90% confidence interval on the cut score (5th, 50th, 95th percentiles).
    # Surfaces real uncertainty: a tight CI (±1 stroke) reads "+3 (very
    # confident)"; a wide one (±4) reads "+3 (could be +1 to +7)".
    sorted_thresh = sorted(cut_thresh_per_sim)
    p05 = sorted_thresh[int(0.05 * len(sorted_thresh))]
    p50 = sorted_thresh[int(0.50 * len(sorted_thresh))]
    p95 = sorted_thresh[int(0.95 * len(sorted_thresh))]
    ci_low_par_rel = int(p05 - par_36)
    ci_high_par_rel = int(p95 - par_36)

    result = {
        "predictedCut": predicted_par_rel,
        "predictedScore": predicted_score,
        "predictedCutCI90": {
            "low":  ci_low_par_rel,
            "median": int(p50 - par_36),
            "high": ci_high_par_rel,
            "lowScore":  int(p05),
            "highScore": int(p95),
        },
        "fieldStrength": field_strength,
        "likelyMakers": likely_makers,
        "confidence": confidence,
        "note": note,
        "model": "monte_carlo",
        "courseKey": course_key,
        "coursePar": par_round,
        "cutSize": cut_size,
        "sims": sims,
        "cutStd": round(cut_std, 2),
        # Top 20 closest-to-cut players (sorted by |make% - 50%|) — frontend
        # can use this as a true "cut bubble" cohort.
        "bubble": sorted(
            cumulative, key=lambda c: abs(c["makeCutProb"] - 0.5)
        )[:20],
        # Per-player make-cut probabilities (full field), keyed by name for
        # frontend lookup.
        "playerMakeCutProb": {c["key"]: c["makeCutProb"] for c in cumulative},
    }
    print(f"  Predicted cut: {predicted_par_rel:+d} ({predicted_score}) · sim std {cut_std:.2f} · {likely_makers} likely makers · {confidence} confidence")
    return result


# ============================================================
# PLAYER NEWS FEED — ESPN
# ============================================================

def fetch_player_news():
    """Fetch recent PGA Tour / Masters news from ESPN's news API."""
    print("[NEWS] Fetching golf news from ESPN...")
    url = "https://site.api.espn.com/apis/site/v2/sports/golf/pga/news?limit=20"
    data = fetch_json(url)
    if not data:
        return []

    news_items = []
    try:
        articles = data.get("articles", [])
        for article in articles[:15]:
            item = {
                "headline": article.get("headline", ""),
                "description": article.get("description", ""),
                "published": article.get("published", ""),
                "link": article.get("links", {}).get("web", {}).get("href", ""),
                "player": "",
                "image": article.get("images", [{}])[0].get("url", "") if article.get("images") else ""
            }
            # Try to extract player name from categories
            for cat in article.get("categories", []):
                if cat.get("type") == "athlete":
                    item["player"] = cat.get("description", "")
                    break
            if item["headline"]:
                news_items.append(item)
    except Exception as e:
        print(f"  Error parsing news: {e}")

    print(f"  Got {len(news_items)} news items")
    return news_items


def extract_stat_prop_lines(bdl_props_raw):
    """Extract actual book lines for birdie/bogey/scoring stat props.

    Returns dict: {player_name: {market: {line, overOdds, underOdds, book}}}
    where market ∈ {birdies, bogeys, scoring, eagles}.

    bdl_props_raw is the list returned by bdl_get_player_props — each row has
    {type, line, over_odds, under_odds, vendor, player}. We aggregate across
    vendors and pick the book with the best over-odds for display.
    """
    if not bdl_props_raw:
        return {}

    market_map = [
        (("birdie",), "birdies"),
        (("bogey",), "bogeys"),
        (("scoring", "score"), "scoring"),
        (("eagle",), "eagles"),
    ]

    out = {}
    for player_name, props in bdl_props_raw.items():
        if not isinstance(props, list):
            continue
        player_out = {}
        for p in props:
            ptype = str(p.get("type", "")).lower()
            line = p.get("line")
            if line is None:
                continue
            matched = None
            for keywords, market in market_map:
                if any(k in ptype for k in keywords):
                    matched = market
                    break
            if not matched:
                continue
            vendor = p.get("vendor", "")
            over = p.get("over_odds")
            under = p.get("under_odds")
            current = player_out.get(matched)
            # Prefer the book with the highest over-odds (best price for over)
            is_better = (
                current is None or
                (isinstance(over, (int, float)) and
                 (not isinstance(current.get("overOdds"), (int, float)) or over > current["overOdds"]))
            )
            if is_better:
                try:
                    line_f = float(line)
                except (TypeError, ValueError):
                    continue
                player_out[matched] = {
                    "line": line_f,
                    "overOdds": over,
                    "underOdds": under,
                    "book": vendor,
                }
        if player_out:
            out[player_name] = player_out
    return out


# ============================================================
# ML ENGINE: BAYESIAN ENSEMBLE + ANOMALY DETECTION
# ============================================================
# Lightweight ML that runs in GitHub Actions (no GPU, no sklearn).
# Uses historical archive data for training signal.
#
# 1. Bayesian Ensemble: combines prior (season SG) with likelihood
#    (recent course-type performance) to compute posterior score.
# 2. Anomaly Detection: Z-score + IQR on odds vs model to find
#    mispriced props in low-frequency markets.
# ============================================================

def _ols_normal_equations(X, y):
    """Solve ordinary least squares beta = (X'X)^-1 X'y by hand.

    Pure-Python 2x2 up to NxN Gauss-Jordan. Designed for small feature
    counts (we run with 5 SG components + intercept = 6 coefficients).
    Returns the coefficient vector, or None if the system is singular.
    """
    n_rows = len(X)
    if n_rows == 0:
        return None
    k = len(X[0])
    # Build augmented normal matrix [X'X | X'y]
    xtx = [[0.0] * k for _ in range(k)]
    xty = [0.0] * k
    for i in range(n_rows):
        xi = X[i]
        yi = y[i]
        for a in range(k):
            xty[a] += xi[a] * yi
            for b in range(k):
                xtx[a][b] += xi[a] * xi[b]
    # Ridge-style stabilizer — tiny diagonal to avoid singularity on
    # near-collinear SG columns in small samples.
    for a in range(k):
        xtx[a][a] += 1e-6
    # Gauss-Jordan elimination on [xtx | xty]
    aug = [row + [xty[i]] for i, row in enumerate(xtx)]
    for c in range(k):
        # pivot: largest absolute value in column c at or below row c
        pivot = c
        for r in range(c + 1, k):
            if abs(aug[r][c]) > abs(aug[pivot][c]):
                pivot = r
        if abs(aug[pivot][c]) < 1e-12:
            return None
        aug[c], aug[pivot] = aug[pivot], aug[c]
        # normalize pivot row
        pv = aug[c][c]
        for j in range(c, k + 1):
            aug[c][j] /= pv
        # eliminate other rows
        for r in range(k):
            if r == c:
                continue
            factor = aug[r][c]
            if factor == 0:
                continue
            for j in range(c, k + 1):
                aug[r][j] -= factor * aug[c][j]
    return [aug[i][k] for i in range(k)]


def compute_course_fit_v2(players, history_dir, course_key, min_events=5, max_weeks=60):
    """Data-driven course-fit score from regression on historical finishes.

    Walks history snapshots for the given course_key, gathers the
    leaderboard finish position + SG components for players at each
    event, regresses inverse-finish on (SG_OTT, SG_APP, SG_ARG, SG_PUTT,
    SG_TOTAL) via OLS normal equations, and predicts each current
    player's fit on a 0-100 scale.

    Returns a dict {normalized_name: fit_score_0_to_100} — empty if
    insufficient history (<min_events distinct completed events at this
    course). Does not touch the hardcoded v1 courseFit.
    """
    if not course_key or not os.path.isdir(history_dir):
        return {}, 0
    files = sorted(
        [f for f in os.listdir(history_dir) if f.endswith(".json")],
        reverse=True,
    )[:max_weeks]

    # Collect one record per (event_date, player) — use the LATEST snapshot
    # per event since it has the most finish info.
    # event_key = (event_name, start_date) to dedupe mid-event snapshots.
    per_event_best = {}  # event_key -> (snapshot_date, snap dict)
    for fname in files:
        try:
            with open(os.path.join(history_dir, fname), encoding="utf-8") as f:
                snap = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
        ce = snap.get("currentEvent") or {}
        ename = ce.get("name") or ""
        course = ce.get("course") or ""
        ck = match_venue_to_course(course, ename)
        if ck != course_key:
            continue
        lb = ce.get("leaderboard") or []
        # Require a completed-ish event: at least 5 players with round3>0
        finished = sum(1 for e in lb if (e.get("round3") or 0) > 0)
        if finished < 5:
            continue
        evt_key = (ename, ce.get("startDate") or "")
        prior = per_event_best.get(evt_key)
        if (prior is None) or (fname > prior[0]):
            per_event_best[evt_key] = (fname, snap)

    events = list(per_event_best.values())
    if len(events) < min_events:
        return {}, len(events)

    # Build training rows: features = [1, sgOtt, sgApp, sgArg, sgPutt, sgTotal]
    # target = -finish_position (higher = better, so positive regression coefs
    # on "good" SG make sense). Normalize position within event to [−1, 0]
    # to account for varying field sizes.
    X, y = [], []
    for _, snap in events:
        lb = snap.get("currentEvent", {}).get("leaderboard") or []
        snap_players = snap.get("players") or []
        sg_by_name = {
            normalize_name(p.get("name", "")): p for p in snap_players
        }
        # Sort leaderboard by totalStrokes asc for finish position.
        ranked = [e for e in lb if (e.get("totalStrokes") or 0) > 0]
        ranked.sort(key=lambda e: e.get("totalStrokes") or 999)
        field = max(len(ranked), 1)
        for pos, entry in enumerate(ranked, start=1):
            name_lc = normalize_name(entry.get("name", ""))
            pd = sg_by_name.get(name_lc)
            if not pd:
                continue
            sg_ott = pd.get("sgOtt")
            sg_app = pd.get("sgApp")
            sg_arg = pd.get("sgArg")
            sg_putt = pd.get("sgPutt")
            sg_tot = pd.get("sgTotal")
            feats = [sg_ott, sg_app, sg_arg, sg_putt, sg_tot]
            if not all(isinstance(v, (int, float)) for v in feats):
                continue
            # Normalized "goodness" target in [-1, 0]: 1st place -> 0, last -> -1.
            target = -(pos - 1) / field
            X.append([1.0] + [float(v) for v in feats])
            y.append(target)

    if len(X) < 20:
        # Not enough rows for a stable fit even if event-count passes.
        return {}, len(events)

    beta = _ols_normal_equations(X, y)
    if beta is None:
        return {}, len(events)

    # Predict on current players, then rescale predicted range to 0-100.
    preds = {}
    for p in players:
        feats = [
            p.get("sgOtt"),
            p.get("sgApp"),
            p.get("sgArg"),
            p.get("sgPutt"),
            p.get("sgTotal"),
        ]
        if not all(isinstance(v, (int, float)) for v in feats):
            continue
        x = [1.0] + [float(v) for v in feats]
        yhat = sum(b * xi for b, xi in zip(beta, x))
        preds[normalize_name(p.get("name", ""))] = yhat

    if not preds:
        return {}, len(events)

    lo = min(preds.values())
    hi = max(preds.values())
    span = hi - lo if hi > lo else 1.0
    scaled = {}
    for name_lc, yhat in preds.items():
        # Map worst predicted to 50, best to 95 (keep on the courseFit scale,
        # which v1 uses 60-100). Intentionally compressed since the model
        # has thin history early on.
        score = 50.0 + 45.0 * (yhat - lo) / span
        scaled[name_lc] = round(score, 1)
    return scaled, len(events)


def _compute_player_variance_from_bdl(seasons=None, min_rounds=8, max_pages_per_season=30):
    """Per-player par-relative-score stddev from BDL ``/player_round_results``.

    Walks 1-2 seasons of per-round results to build a much richer empirical
    variance estimate than what local history snapshots provide. A player
    with 30 rounds in their data set gets a high-confidence std; a player
    with 8 rounds gets a usable one; fewer than 8 → skipped, downstream
    falls back to global BASE_STD or to the local snapshot estimate.

    Returns ``{normalized_name: std_strokes}`` where std is round-score
    standard deviation in strokes (typical range 2.2 - 3.8).

    API cost: ~30 pages × 100 rows × 2 seasons = ~6,000 round records per
    run. Well within BDL rate limits. Cached results would be the next
    optimization (this currently re-fetches every cron) but for a once-
    daily-ish refresh it's fine.
    """
    if not BDL_API_KEY:
        return {}
    from datetime import datetime as _dt
    if seasons is None:
        yr = _dt.utcnow().year
        seasons = [yr, yr - 1]

    rounds_by_player = {}
    for season in seasons:
        try:
            rows = bdl_fetch_all(
                "player_round_results",
                {"season": str(season), "per_page": "100"},
                max_pages=max_pages_per_season,
            )
        except Exception as e:
            print(f"  [WARN] BDL variance fetch failed for {season}: {e}")
            continue
        if not rows:
            continue
        n_added = 0
        for r in rows:
            player = r.get("player") or {}
            pname = player.get("display_name") or (
                f"{player.get('first_name','')} {player.get('last_name','')}".strip()
            )
            if not pname:
                continue
            par_rel = r.get("par_relative_score")
            if not isinstance(par_rel, (int, float)):
                continue
            # Skip absurd outliers (data artifacts)
            v = float(par_rel)
            if v < -15 or v > 20:
                continue
            rounds_by_player.setdefault(normalize_name(pname), []).append(v)
            n_added += 1
        print(f"  BDL variance: {season} → {n_added} rounds, {len(rounds_by_player)} players seen so far")

    out = {}
    out_meta = {}
    for name, scores in rounds_by_player.items():
        if len(scores) < min_rounds:
            continue
        mean = sum(scores) / len(scores)
        var = sum((s - mean) ** 2 for s in scores) / (len(scores) - 1)
        std = var ** 0.5
        if 1.8 <= std <= 4.5:
            out[name] = round(std, 2)
            out_meta[name] = len(scores)
    if out:
        sample_sizes = sorted(out_meta.values())
        median_n = sample_sizes[len(sample_sizes) // 2]
        print(f"  BDL variance: {len(out)} players with stable std (median sample {median_n} rounds)")
    return out


def _compute_player_variance(history_dir, max_weeks=20, min_rounds=5):
    """Compute per-player round-score stddev from history.

    Walks the last N snapshots, collects each player's round1/2/3/4 scores
    across all events, returns {normalized_name: stddev}. Players with
    <min_rounds data points fall back to the global BASE_STD at model time.
    """
    if not os.path.isdir(history_dir):
        return {}
    files = sorted(
        [f for f in os.listdir(history_dir) if f.endswith(".json")],
        reverse=True,
    )[:max_weeks]
    rounds_by_player = {}
    for fname in files:
        try:
            with open(os.path.join(history_dir, fname), encoding="utf-8") as f:
                snap = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
        lb = ((snap.get("currentEvent") or {}).get("leaderboard")) or []
        for entry in lb:
            name = (entry.get("name") or "").lower()
            if not name:
                continue
            for rnd in (1, 2, 3, 4):
                v = entry.get(f"round{rnd}")
                if isinstance(v, (int, float)) and 55 < v < 95:
                    rounds_by_player.setdefault(name, []).append(float(v))

    out = {}
    for name, scores in rounds_by_player.items():
        if len(scores) < min_rounds:
            continue
        mean = sum(scores) / len(scores)
        var = sum((s - mean) ** 2 for s in scores) / (len(scores) - 1)
        std = var ** 0.5
        # Clamp to a reasonable range — anyone below 1.5 or above 5.0 is
        # almost certainly a data artifact (tiny sample or scraped garbage)
        if 1.5 <= std <= 5.0:
            out[name] = round(std, 2)
    return out


def _load_historical_performances(history_dir, max_weeks=20):
    """Load player performance data from history archives for ML training."""
    if not os.path.isdir(history_dir):
        return {}

    history_files = sorted(
        [f for f in os.listdir(history_dir) if f.endswith(".json")],
        reverse=True
    )[:max_weeks]

    # {player_name: [{event, position, sg, courseFit, confScore, date}, ...]}
    player_history = {}
    for hfile in history_files:
        try:
            with open(os.path.join(history_dir, hfile)) as f:
                hdata = json.load(f)
            date = hfile[:10]
            evt = hdata.get("currentEvent", {})
            event_name = evt.get("name", "")
            lb = evt.get("leaderboard", [])
            players = hdata.get("players", [])

            # Build player lookup from this snapshot
            player_map = {p.get("name", "").lower(): p for p in players}

            for i, entry in enumerate(lb):
                name = entry.get("name", "")
                if not name:
                    continue
                name_lc = name.lower()
                pdata = player_map.get(name_lc, {})

                if name not in player_history:
                    player_history[name] = []
                player_history[name].append({
                    "date": date,
                    "event": event_name,
                    "position": i + 1,
                    "fieldSize": len(lb),
                    "sgTotal": pdata.get("sgTotal", 0),
                    "confScore": pdata.get("confScore", 50),
                    "courseFit": 0,  # Will be filled if we match course
                })
        except (json.JSONDecodeError, IOError):
            continue

    return player_history


def bayesian_player_score(player, player_history, course_key="augusta"):
    """
    Bayesian Ensemble Score (0-100).

    Prior: season-long SG profile (what we expect from this player overall)
    Likelihood: recent performance weighted by course similarity
    Posterior: updated prediction for this specific tournament

    This is mathematically equivalent to a Gaussian process with
    conjugate prior updating — the gold standard for small-sample
    prediction in sports analytics.
    """
    name = player.get("name", "")
    history = player_history.get(name, [])

    if len(history) < 2:
        return None  # Not enough data for Bayesian update

    # PRIOR: season-long expected performance from SG
    sg_total = player.get("sgTotal", 0.0)
    # Convert SG to expected percentile (SG +2.0 ≈ top 5%, SG 0 ≈ 50th)
    prior_mu = max(5, min(95, 50 + sg_total * 20))
    prior_sigma = 18.0  # High uncertainty = let data talk

    # LIKELIHOOD: recent results with recency weighting
    # More recent results get exponentially more weight (half-life = 4 events)
    weighted_scores = []
    weights = []
    for i, h in enumerate(history[:12]):
        # Position percentile (1st in 50 = 98th percentile, 25th = 50th)
        field = max(h.get("fieldSize", 50), 20)
        pctile = max(5, min(95, (1 - (h["position"] - 1) / field) * 100))

        # Recency weight: exponential decay, half-life = 4 events
        weight = math.exp(-0.693 * i / 4.0)  # ln(2) ≈ 0.693
        weighted_scores.append(pctile * weight)
        weights.append(weight)

    if not weights:
        return None

    likelihood_mu = sum(weighted_scores) / sum(weights)
    # Variance of recent results (lower = more consistent → tighter posterior)
    if len(history) >= 3:
        mean_score = likelihood_mu
        variance = sum(w * (s/w - mean_score)**2 for s, w in zip(weighted_scores, weights)) / sum(weights)
        likelihood_sigma = max(5.0, min(25.0, math.sqrt(variance)))
    else:
        likelihood_sigma = 20.0

    # POSTERIOR: Bayesian conjugate update (Gaussian)
    # posterior_mu = (prior_mu/prior_sigma^2 + likelihood_mu/likelihood_sigma^2) /
    #               (1/prior_sigma^2 + 1/likelihood_sigma^2)
    prior_precision = 1.0 / (prior_sigma ** 2)
    likelihood_precision = 1.0 / (likelihood_sigma ** 2)
    posterior_precision = prior_precision + likelihood_precision
    posterior_mu = (prior_mu * prior_precision + likelihood_mu * likelihood_precision) / posterior_precision
    posterior_sigma = math.sqrt(1.0 / posterior_precision)

    # Convert posterior to 0-100 score
    bayes_score = max(1, min(100, round(posterior_mu)))

    return {
        "score": bayes_score,
        "prior": round(prior_mu, 1),
        "likelihood": round(likelihood_mu, 1),
        "posterior_sigma": round(posterior_sigma, 1),
        "data_points": len(history),
    }


def detect_odds_anomalies(players, course_key="augusta"):
    """
    Anomaly Detection for Low-Frequency Markets.

    Identifies mispriced props by comparing model expectations vs market odds
    using Z-score and IQR methods. Focuses on markets where books are less
    efficient: Top 20, Make Cut, Round 1 Leader.

    Returns: {player_name: {market: {anomaly_type, z_score, edge_pct, direction}}}
    """
    anomalies = {}

    # Collect all players with both model scores and odds
    scored_players = []
    for p in players:
        conf = p.get("confScore", 0)
        odds = p.get("odds", {})
        best_odds = None
        for book in ["dk", "fd", "mgm", "czr", "pb", "365"]:
            v = odds.get(book)
            if v:
                try:
                    american = int(v)
                    if best_odds is None or american < best_odds:
                        best_odds = american
                except (ValueError, TypeError):
                    pass

        if conf > 0 and best_odds is not None:
            # Convert odds to implied probability
            if best_odds > 0:
                implied_pct = 100.0 / (best_odds + 100.0) * 100.0
            else:
                implied_pct = abs(best_odds) / (abs(best_odds) + 100.0) * 100.0

            # Model expected win% from confScore
            model_pct = (conf / 100.0) * 15.0

            scored_players.append({
                "name": p.get("name", ""),
                "confScore": conf,
                "implied_pct": implied_pct,
                "model_pct": model_pct,
                "edge": model_pct - implied_pct,
                "best_odds": best_odds,
            })

    if len(scored_players) < 10:
        return anomalies

    # Calculate edge distribution statistics
    edges = [sp["edge"] for sp in scored_players]
    mean_edge = sum(edges) / len(edges)
    variance = sum((e - mean_edge) ** 2 for e in edges) / len(edges)
    std_edge = math.sqrt(variance) if variance > 0 else 1.0

    # IQR for robust outlier detection
    sorted_edges = sorted(edges)
    n = len(sorted_edges)
    q1 = sorted_edges[n // 4]
    q3 = sorted_edges[3 * n // 4]
    iqr = q3 - q1
    upper_fence = q3 + 1.5 * iqr
    lower_fence = q1 - 1.5 * iqr

    # Detect anomalies
    for sp in scored_players:
        z_score = (sp["edge"] - mean_edge) / std_edge if std_edge > 0 else 0
        player_anomalies = []

        # Z-score anomaly: |z| > 1.8 (more aggressive than typical 2.0 for betting)
        if abs(z_score) > 1.8:
            direction = "UNDERPRICED" if z_score > 0 else "OVERPRICED"
            player_anomalies.append({
                "type": "z_score",
                "z": round(z_score, 2),
                "direction": direction,
                "edge_pct": round(sp["edge"], 2),
                "market": "outright",
            })

        # IQR anomaly: outside 1.5×IQR fences
        if sp["edge"] > upper_fence or sp["edge"] < lower_fence:
            direction = "UNDERPRICED" if sp["edge"] > upper_fence else "OVERPRICED"
            player_anomalies.append({
                "type": "iqr_outlier",
                "direction": direction,
                "edge_pct": round(sp["edge"], 2),
                "fence": round(upper_fence if sp["edge"] > 0 else lower_fence, 2),
                "market": "outright",
            })

        # Low-frequency market anomalies (Top 20, Make Cut)
        # Players with high confScore but long odds = potential value in placement markets
        if sp["confScore"] >= 60 and sp["best_odds"] > 5000:
            player_anomalies.append({
                "type": "low_freq_value",
                "direction": "UNDERPRICED",
                "market": "top20_makecut",
                "note": f"confScore {sp['confScore']} but odds {sp['best_odds']:+d} — likely value in placement markets",
            })

        # Reverse: low confScore but short odds = overpriced by market
        if sp["confScore"] < 35 and sp["best_odds"] < 3000:
            player_anomalies.append({
                "type": "low_freq_fade",
                "direction": "OVERPRICED",
                "market": "outright_fade",
                "note": f"confScore {sp['confScore']} but odds {sp['best_odds']:+d} — market may be overvaluing name",
            })

        if player_anomalies:
            anomalies[sp["name"]] = player_anomalies

    return anomalies


# ============================================================
# CONFIDENCE SCORE MODEL
# ============================================================

def calculate_player_confidence_score(player, all_players, course_key="augusta", player_history=None):
    """
    PropsBot Confidence Score v3 — 0 to 100.
    12-factor composite model for tournament performance prediction.
    Industry-leading accuracy through multi-signal fusion with
    cross-correlation amplification.

    Core Factors:
      20%  Strokes Gained            (field-normalized, course-component weighted)
      16%  Course Fit                (algorithmic match to course trait profile)
      14%  Recent Form & Trend       (L5 recency-weighted, momentum slope, injury flag)
      10%  Tournament / Course History (finish, top-10s, cut%, avg score)
      10%  GIR & Approach Quality    (GIR% × course difficulty, proximity, scramble)
       7%  Driving Profile           (accuracy + distance matched to course needs)
       7%  Birdie / Bogey Profile    (rate vs course avg, net scoring tendency)
       5%  Cut Consistency           (career + recent made-cut %)
       4%  Closing Ability           (R3+R4 vs R1+R2 weekend performance)
       3%  Market Consensus          (book implied probability as outside signal)
       2%  Field Strength Premium    (bonus for success in elite fields)
       2%  Consistency / Variance    (low-variance = reliable, high-variance = risky)

    Post-Processing:
       ±4  Par-5 Scoring Bonus
       ±4  Weather / Wind Adaptation
       ±2  Tee Time Morning Advantage
       ±3  Cross-Signal Amplifier    (when 3+ factors agree on weakness/strength)
       ±5  Bayesian ML Ensemble      (learns from historical outcomes, self-corrects)
    """

    ct = COURSE_TRAITS.get(course_key, COURSE_TRAITS.get("augusta", {}))
    score = 0.0
    score_breakdown = {}   # store component scores for frontend display

    # =========================================================
    # 1. STROKES GAINED  (22%)
    # Normalize sgTotal vs full player pool, then blend with
    # course-weighted component breakdown.
    # =========================================================
    sg_vals = [p.get("sgTotal", 0) for p in all_players if p.get("sgTotal") is not None]
    if sg_vals:
        sg_min, sg_max = min(sg_vals), max(sg_vals)
        sg_range = sg_max - sg_min if sg_max != sg_min else 1
        sg_norm = (player.get("sgTotal", 0) - sg_min) / sg_range
    else:
        sg_norm = 0.5

    # Course-weighted SG blend: Augusta values App > Putt > OTT > ARG
    # Pull weights from course traits if available
    acc_need  = ct.get("accuracy", 0.7)
    pwr_need  = ct.get("power",    0.6)
    scr_need  = ct.get("scramble", 0.7)
    putt_need = ct.get("putting",  0.6)
    total_need = acc_need + pwr_need * 0.5 + scr_need * 0.5 + putt_need
    sg_weighted = (
        player.get("sgApp",  0) * (acc_need / total_need) +
        player.get("sgPutt", 0) * (putt_need / total_need) +
        player.get("sgOtt",  0) * (pwr_need * 0.5 / total_need) +
        player.get("sgArg",  0) * (scr_need * 0.5 / total_need)
    )
    sg_w_vals = [
        p.get("sgApp",0)*(acc_need/total_need) + p.get("sgPutt",0)*(putt_need/total_need) +
        p.get("sgOtt",0)*(pwr_need*0.5/total_need) + p.get("sgArg",0)*(scr_need*0.5/total_need)
        for p in all_players
    ]
    sw_min, sw_max = (min(sg_w_vals) if sg_w_vals else 0), (max(sg_w_vals) if sg_w_vals else 1)
    sw_range = sw_max - sw_min if sw_max != sw_min else 1
    sg_w_norm = (sg_weighted - sw_min) / sw_range if sw_range else 0.5

    sg_component = 0.5 * sg_norm + 0.5 * sg_w_norm
    score += sg_component * 20.0
    score_breakdown["sg"] = round(sg_component * 20.0, 1)

    # =========================================================
    # 2. COURSE FIT  (18%)
    # Pulls from curated courseFit dict; bonus for elite fit.
    # =========================================================
    course_fit = DEFAULT_COURSE_FIT
    cf = player.get("courseFit")
    if isinstance(cf, dict):
        # Fallback chain: exact course key → augusta (legacy default) →
        # DEFAULT_COURSE_FIT so new 2026 venues with no curated fit still
        # score neutral instead of zeroing out the 16-point fit bucket.
        course_fit = cf.get(course_key, cf.get("augusta", DEFAULT_COURSE_FIT))
    elif isinstance(cf, (int, float)):
        course_fit = cf

    fit_norm = max(0, min(100, course_fit)) / 100.0
    if course_fit >= 88:
        fit_norm = min(1.0, fit_norm * 1.10)
    elif course_fit < 55:
        fit_norm *= 0.80

    score += fit_norm * 16.0
    score_breakdown["fit"] = round(fit_norm * 16.0, 1)

    # =========================================================
    # 3. TOURNAMENT / COURSE HISTORY  (12%)
    # Best finish + top-10 count + appearances + scoring avg.
    # Made-cut rate is also computed here and stored separately.
    # =========================================================
    history = player.get("augustaHistory", {})
    hist_score = 0.35  # neutral default for debutants

    if history:
        appearances = max(1, min(history.get("appearances", 1), 35))
        best_finish = history.get("bestFinish", 70)
        top10s      = history.get("top10", 0)
        avg_score   = history.get("avgScore", 74.0)
        cuts_made   = history.get("cuts", 0)

        # Best finish points: 1→100, 5→82, 10→65, 20→40, 30+→10
        finish_pts = max(0, 100 - (best_finish - 1) * 3.3)
        # Top-10 rate rewards consistent excellence, not just one lucky week
        top10_rate_pts = min(70, (top10s / appearances) * 140)
        # Experience value (capped — appearance 20+ is mastery tier)
        exp_pts = min(25, appearances * 1.4)
        # Scoring average (relative to par 72)
        avg_pts = max(0, 40 - (avg_score - 72.0) * 8) if avg_score else 0
        # Career cut % at this course
        career_cut_pct = cuts_made / appearances
        cut_pts = career_cut_pct * 30  # 100% cut rate = 30pts

        raw_hist = (finish_pts * 0.35 + top10_rate_pts * 0.25 +
                    exp_pts * 0.15 + avg_pts * 0.15 + cut_pts * 0.10)
        hist_score = min(100, max(0, raw_hist)) / 100.0
        # Store for cut consistency factor later
        player["_career_cut_pct"] = career_cut_pct

    score += hist_score * 10.0
    score_breakdown["history"] = round(hist_score * 10.0, 1)

    # =========================================================
    # 4. RECENT FORM & TREND  (12%)
    # Recency-weighted L5 results + hot/cold/injured penalty.
    # =========================================================
    recent_form = player.get("recentForm", {})
    form_score = 0.5

    if recent_form and isinstance(recent_form, dict):
        results = recent_form.get("results", recent_form.get("finishes", []))
        if results:
            form_pts = []
            for r in results[:5]:
                pos = r.get("position", r.get("pos", 50))
                try:
                    pos_int = int(str(pos).replace("T","").replace("MC","82").replace("WD","95"))
                except:
                    pos_int = 50
                if   pos_int == 1:  pts = 100
                elif pos_int <= 3:  pts = 92
                elif pos_int <= 5:  pts = 84
                elif pos_int <= 10: pts = 70
                elif pos_int <= 20: pts = 56
                elif pos_int <= 30: pts = 42
                elif pos_int <= 50: pts = 28
                elif pos_int <= 70: pts = 15
                else:               pts = 4   # MC or WD
                form_pts.append(pts)
            if form_pts:
                weights = [2.0, 1.5, 1.0, 0.7, 0.5][:len(form_pts)]
                form_score = sum(p*w for p,w in zip(form_pts, weights)) / sum(weights) / 100.0

        trend = recent_form.get("trend", "neutral")
        if trend == "hot":
            form_score = min(1.0, form_score * 1.18)
        elif trend == "cold":
            form_score *= 0.82
        elif trend == "injured" or trend == "struggling":
            form_score *= 0.70  # significant penalty for injury/form crisis

        # Store recent cut % for cut-consistency factor
        r_results = recent_form.get("results", recent_form.get("finishes", []))
        if r_results:
            recent_made = sum(1 for r in r_results[:10]
                              if str(r.get("position", r.get("pos","MC"))).replace("T","").isdigit()
                              and int(str(r.get("position", r.get("pos","99"))).replace("T","")) < 80)
            player["_recent_cut_pct"] = recent_made / min(len(r_results), 10)
        else:
            player["_recent_cut_pct"] = 0.60

    # Momentum signal: compare most recent 2 results vs next 3
    # Positive momentum (improving) → boost; negative → drag
    if recent_form and isinstance(recent_form, dict):
        results_for_momentum = recent_form.get("results", recent_form.get("finishes", []))
        if len(results_for_momentum) >= 4:
            try:
                recent_2 = [int(str(r.get("position", r.get("pos", 50))).replace("T","").replace("MC","82").replace("WD","95")) for r in results_for_momentum[:2]]
                older_3  = [int(str(r.get("position", r.get("pos", 50))).replace("T","").replace("MC","82").replace("WD","95")) for r in results_for_momentum[2:5]]
                avg_recent = sum(recent_2) / len(recent_2)
                avg_older  = sum(older_3) / len(older_3)
                # Positive momentum = finishing better recently (lower number)
                momentum = (avg_older - avg_recent) / 30.0  # normalize: 30 pos improvement = max
                momentum = max(-0.15, min(0.15, momentum))
                form_score = max(0, min(1.0, form_score + momentum))
            except (ValueError, TypeError):
                pass

    score += form_score * 14.0
    score_breakdown["form"] = round(form_score * 14.0, 1)

    # =========================================================
    # 5. GIR & APPROACH QUALITY  (10%)
    # GIR% adjusted by course difficulty.  Proximity + scramble
    # bonus at courses that punish missed greens.
    # =========================================================
    gir         = player.get("gir", 65.0)
    prox_avg    = player.get("proxAvg", 36.0)    # feet from hole
    scramble    = player.get("scramble", 58.0)    # % recovery from off-green
    sg_app      = player.get("sgApp", 0.0)
    gir_diff    = ct.get("gir_difficulty", 0.70)
    scr_need    = ct.get("scramble", 0.70)

    # GIR norm: 52% poor → 0, 74%+ elite → 1.0
    gir_norm    = max(0.0, min(1.0, (gir - 52.0) / 22.0))
    # Proximity norm: 40ft avg poor → 0, 25ft excellent → 1.0
    prox_norm   = max(0.0, min(1.0, (40.0 - prox_avg) / 15.0))
    # Scramble norm: 45% poor → 0, 70% great → 1.0 (weighted by course need)
    scramble_norm = max(0.0, min(1.0, (scramble - 45.0) / 25.0)) * scr_need
    # SG:App norm: -1.5 poor → 0, +1.5 elite → 1.0
    sgapp_norm  = max(0.0, min(1.0, (sg_app + 1.5) / 3.0))

    # At harder GIR courses, weight GIR% and approach more heavily
    gir_component = (
        gir_norm    * (0.30 + gir_diff * 0.20) +
        sgapp_norm  * 0.35 +
        prox_norm   * 0.15 +
        scramble_norm * 0.10
    )
    gir_component = max(0.0, min(1.0, gir_component / (0.30 + gir_diff * 0.20 + 0.60)))

    score += gir_component * 10.0
    score_breakdown["gir"] = round(gir_component * 10.0, 1)

    # =========================================================
    # 6. DRIVING PROFILE  (8%)
    # Fairway accuracy vs distance matched to course requirements.
    # Narrow fairways amplify the accuracy premium.
    # =========================================================
    fairways     = player.get("fairways", 62.0)
    sg_ott       = player.get("sgOtt", 0.0)
    fw_width     = ct.get("fairway_width", 0.50)   # 0 = very narrow, 1 = very wide
    acc_need_raw = ct.get("accuracy", 0.70)
    pwr_need_raw = ct.get("power", 0.60)

    # Fairway % norm: 50% poor → 0, 78% elite → 1.0
    fw_norm  = max(0.0, min(1.0, (fairways - 50.0) / 28.0))
    # OTT norm: -1.0 poor → 0, +1.5 elite → 1.0
    ott_norm = max(0.0, min(1.0, (sg_ott + 1.0) / 2.5))

    # Narrow fairways increase accuracy premium (tight tree-lined tracks)
    acc_weight = acc_need_raw + max(0, 0.5 - fw_width) * 0.4
    pwr_weight = pwr_need_raw
    total_drv  = acc_weight + pwr_weight
    drv_component = (fw_norm * acc_weight + ott_norm * pwr_weight) / (total_drv if total_drv else 1)

    score += drv_component * 7.0
    score_breakdown["driving"] = round(drv_component * 7.0, 1)

    # =========================================================
    # 7. BIRDIE / BOGEY PROFILE  (8%)
    # Birdie rate vs course avg, bogey avoidance, net tendency.
    # Pin position context is approximated by course bogey_rate.
    # =========================================================
    birdie_avg       = player.get("birdieAvg", 3.5)
    bogey_avg        = player.get("bogeyAvg",  2.5)
    course_birdie_r  = ct.get("birdie_rate",   3.8)
    course_bogey_r   = ct.get("bogey_rate",    2.9)

    # Excess birdies vs course average: -1.5 → 0, +1.5 → 1.0
    birdie_excess = birdie_avg - course_birdie_r
    birdie_norm   = max(0.0, min(1.0, (birdie_excess + 1.5) / 3.0))

    # Bogey avoidance: at high-bogey courses, avoiding bogeys is premium
    # bogey_excess negative = better than course avg
    bogey_excess  = bogey_avg - course_bogey_r
    bogey_norm    = max(0.0, min(1.0, (1.5 - bogey_excess) / 3.0))

    # Net scoring tendency: birdie - bogey delta vs course norms
    net_tendency  = (birdie_avg - bogey_avg) - (course_birdie_r - course_bogey_r)
    net_norm      = max(0.0, min(1.0, (net_tendency + 1.5) / 3.0))

    bb_component = birdie_norm * 0.35 + bogey_norm * 0.40 + net_norm * 0.25
    score += bb_component * 7.0
    score_breakdown["birdie_bogey"] = round(bb_component * 7.0, 1)

    # =========================================================
    # 8. CUT CONSISTENCY  (6%)
    # Career cut % at this course + recent L10 cut rate.
    # Players who can't make cuts don't matter for most props.
    # =========================================================
    career_cut = player.get("_career_cut_pct", 0.60)
    recent_cut = player.get("_recent_cut_pct", 0.60)
    # Weight recent cuts 55%, career 45% (recent form more predictive)
    cut_score  = career_cut * 0.45 + recent_cut * 0.55

    # Store for frontend display
    player["makeCutPct"]       = round(career_cut * 100, 1)
    player["recentMakeCutPct"] = round(recent_cut * 100, 1)

    score += cut_score * 5.0
    score_breakdown["cut"] = round(cut_score * 5.0, 1)

    # =========================================================
    # 9. MARKET CONSENSUS  (3%)
    # Sharp-money signal from book odds.  Intentionally low weight
    # so our model leads, not follows.  BDL book list only.
    # =========================================================
    odds = player.get("odds", {})
    best_odds = None
    for book in ["dk", "fd", "mgm", "czr", "pb", "365"]:
        v = odds.get(book)
        if v:
            try:
                american = int(v)
                if best_odds is None or american < best_odds:
                    best_odds = american
            except:
                pass

    if best_odds is not None:
        if best_odds > 0:
            impl_prob = 100.0 / (best_odds + 100.0)
        else:
            impl_prob = abs(best_odds) / (abs(best_odds) + 100.0)
        market_norm = min(1.0, max(0.0, impl_prob * 8.0))
    else:
        rank = player.get("rank", 100)
        market_norm = max(0.0, min(1.0, (101 - min(rank, 100)) / 100.0))

    score += market_norm * 3.0
    score_breakdown["market"] = round(market_norm * 3.0, 1)

    # =========================================================
    # 10. CLOSING ABILITY  (4%)
    # Weekend performance: R3+R4 vs R1+R2 from historical archives.
    # Negative closingDelta = player scores BETTER on weekends (closer).
    # Positive = fades under Sunday pressure.
    # At majors and elite events, closing ability is a significant edge.
    # =========================================================
    closing_score = 0.5  # neutral default
    round_avgs = {}
    if recent_form and isinstance(recent_form, dict):
        round_avgs = recent_form.get("roundAvgs", {}) or {}
    closing_delta = round_avgs.get("closingDelta")
    if closing_delta is not None:
        # closingDelta: -2.0 (elite closer) → 1.0, +2.0 (fader) → 0.0
        closing_score = max(0.0, min(1.0, (2.0 - closing_delta) / 4.0))
        # Store for frontend display
        player["closingDelta"] = closing_delta

    score += closing_score * 4.0
    score_breakdown["closing"] = round(closing_score * 4.0, 1)

    # =========================================================
    # 11. FIELD STRENGTH PREMIUM  (2%)
    # Players who perform well against elite fields (low rank =
    # consistently beating strong competition) deserve a premium.
    # This rewards players who thrive under pressure vs those
    # who pad stats at weak-field events.
    # =========================================================
    p_rank = player.get("rank", 100)
    # Top-10 world rank = elite field competitor; rank 80+ = weak-field stat padder
    if p_rank <= 10:
        field_premium = 1.0
    elif p_rank <= 25:
        field_premium = 0.75
    elif p_rank <= 50:
        field_premium = 0.50
    elif p_rank <= 80:
        field_premium = 0.25
    else:
        field_premium = 0.0

    score += field_premium * 2.0
    score_breakdown["field_strength"] = round(field_premium * 2.0, 1)

    # =========================================================
    # 12. CONSISTENCY / VARIANCE  (2%)
    # Low-variance players are more reliable for props — their
    # outcomes cluster near expectations.  High-variance players
    # can win or miss the cut.
    # Measured via scoring avg distance from SG expectation,
    # and bogey rate spread.
    # =========================================================
    scoring_avg = player.get("scoringAvg", 71.0)
    # Expected scoring avg from SG:Total (par 72 baseline)
    expected_scoring = 72.0 - player.get("sgTotal", 0.0) * 1.1
    # Small deviation = consistent; large deviation = volatile
    scoring_dev = abs(scoring_avg - expected_scoring)
    consistency_score = max(0.0, min(1.0, 1.0 - scoring_dev / 3.0))

    # Bogey avoidance adds consistency signal
    bogey_rate_raw = player.get("bogeyAvg", 2.5)
    # Low bogey rate = more consistent round-to-round
    bogey_consistency = max(0.0, min(1.0, (4.0 - bogey_rate_raw) / 2.5))
    consistency_final = consistency_score * 0.6 + bogey_consistency * 0.4

    score += consistency_final * 2.0
    score_breakdown["consistency"] = round(consistency_final * 2.0, 1)

    # =========================================================
    # COMPETITIVENESS GATE
    # Historical greatness cannot override current inability to
    # compete.  Uses two independent signals (rank + SG) so that
    # retired players AND recently injured players are caught.
    # Past champions invited to majors (e.g. Weir, Langer, Couples)
    # must be caught even when scraper provides default stats.
    # =========================================================
    PAST_CHAMPS_CEREMONIAL = {
        'mike weir', 'fred couples', 'bernhard langer', 'fuzzy zoeller',
        'sandy lyle', 'ian woosnam', 'larry mize', 'jack nicklaus',
        'gary player', 'angel cabrera', 'trevor immelman', 'charl schwartzel',
        'jose maria olazabal', 'danny willett', 'zach johnson', 'bubba watson',
        'ben crenshaw', 'mark oʼmeara', 'craig stadler', 'tom watson',
        'raymond floyd', 'charles coody',
    }
    rank     = player.get("rank",    100)
    sg_total = player.get("sgTotal", 0.0)
    name_lc  = player.get("name", "").lower()

    is_ceremonial     = (rank >= 500) or (sg_total < -2.0) or (name_lc in PAST_CHAMPS_CEREMONIAL)
    is_non_competitive= (rank > 300)  or (sg_total < -1.2)
    is_declining      = (rank > 150)  or (sg_total < -0.3)

    final_score = round(min(100, max(1, score)))

    # =========================================================
    # POST-PROCESSING ADJUSTMENT A: PAR-5 SCORING BONUS
    # Courses with many reachable par-5s reward power + approach
    # together — Bryson/DJ benefit significantly at Augusta.
    # Effect: ±4 pts at Augusta (4 par-5s), ±2 pts at 2-par-5 courses.
    # =========================================================
    par5_count = ct.get("par5_count", 2)
    if par5_count >= 3:
        # par-5 scoring proxy: blended OTT (reach in 2) + App (layup conversion)
        par5_skill = player.get("sgOtt", 0.0) * 0.65 + player.get("sgApp", 0.0) * 0.35
        # Scale: +1.5 elite → +4pts, -1.0 poor → -3pts
        par5_adj = max(-3.0, min(4.0, par5_skill * 2.5 * (par5_count / 3.0)))
        final_score = max(1, min(100, final_score + round(par5_adj, 1)))
        score_breakdown["par5"] = round(par5_adj, 1)

    # =========================================================
    # POST-PROCESSING ADJUSTMENT B: WEATHER / WIND ADAPTATION
    # Only applied when forecast data is available via Open-Meteo.
    # Low ball flight players gain on exposed/windy courses;
    # high ball flight players are penalised. Max ±4 pts.
    # =========================================================
    weather_data = player.get("_weather", {})
    wind_avg = weather_data.get("wind_avg", 0)
    wind_exp = ct.get("wind_exposure", 0.30)
    if wind_avg > 10 and wind_exp >= 0.40:
        flight = player.get("flight", "neutral")
        wind_factor = min(1.0, (wind_avg - 10) / 15.0) * wind_exp
        if "low" in flight:
            weather_adj = round(wind_factor * 4.0, 1)   # low flight = advantage
        elif "high" in flight:
            weather_adj = round(-wind_factor * 3.5, 1)  # high flight = penalised
        else:
            weather_adj = 0
        if weather_adj != 0:
            final_score = max(1, min(100, final_score + weather_adj))
            score_breakdown["weather"] = weather_adj

    # =========================================================
    # POST-PROCESSING ADJUSTMENT C: TEE TIME / MORNING ADVANTAGE
    # Some courses (links, exposed layouts) show a significant
    # morning scoring edge due to calmer winds, softer greens.
    # AM tee time + high morning_adv → boost; PM → slight penalty.
    # Max ±2 pts — meaningful but not dominant.
    # =========================================================
    tee_str = player.get("_teeTime", "")
    morning_adv = ct.get("morning_adv", 0.0)
    if tee_str and morning_adv > 0.15:
        try:
            # Parse ISO tee time to extract hour (UTC)
            # Format: "2026-04-10T11:30:00Z" or "2026-04-10T11:30Z"
            hour_part = tee_str.split("T")[1][:2]
            tee_hour_utc = int(hour_part)
            # US ET courses: AM tees typically before 17 UTC (1pm ET)
            is_am = tee_hour_utc < 17
            if is_am:
                # Boost: morning_adv 0.6 → +1.2 pts, 0.3 → +0.6 pts
                tee_adj = round(morning_adv * 2.0, 1)
            else:
                # Slight penalty for afternoon (harder conditions)
                tee_adj = round(-morning_adv * 1.0, 1)
            if tee_adj != 0:
                final_score = max(1, min(100, final_score + tee_adj))
                score_breakdown["tee_time"] = tee_adj
        except (ValueError, IndexError):
            pass  # Unparseable tee time — skip silently

    # =========================================================
    # POST-PROCESSING ADJUSTMENT D: CROSS-SIGNAL AMPLIFIER
    # When multiple independent signals agree that a player is
    # strong or weak, the combined effect should be MORE than
    # the sum of parts (avoid "average of averages" problem).
    # Count how many factors scored in the top or bottom quartile.
    # 3+ top-quartile → bonus up to +3; 3+ bottom → penalty -3.
    # This is the key differentiator vs simple weighted averages.
    # =========================================================
    top_signals = 0    # factors scoring > 75th percentile
    bottom_signals = 0 # factors scoring < 25th percentile
    factor_values = {
        "sg": sg_component, "fit": fit_norm, "form": form_score,
        "gir": gir_component, "driving": drv_component,
        "bb": bb_component, "closing": closing_score,
        "consistency": consistency_final,
    }
    for fv in factor_values.values():
        if fv >= 0.75:
            top_signals += 1
        elif fv <= 0.25:
            bottom_signals += 1

    cross_adj = 0
    if top_signals >= 4:
        cross_adj = min(3.0, (top_signals - 3) * 1.5 + 1.5)
    elif top_signals >= 3:
        cross_adj = 1.5
    if bottom_signals >= 4:
        cross_adj = max(-3.0, -(bottom_signals - 3) * 1.5 - 1.5)
    elif bottom_signals >= 3:
        cross_adj = min(cross_adj, -1.5)

    if cross_adj != 0:
        final_score = max(1, min(100, final_score + round(cross_adj, 1)))
        score_breakdown["cross_signal"] = round(cross_adj, 1)

    # =========================================================
    # POST-PROCESSING ADJUSTMENT E: BAYESIAN ML ENSEMBLE
    # When we have enough historical data (≥3 events), use
    # Bayesian conjugate updating to blend our model prior with
    # observed tournament results. This is the ML component —
    # it learns from actual outcomes and self-corrects.
    # Max ±5 pts — significant but doesn't override fundamentals.
    # =========================================================
    if player_history:
        bayes = bayesian_player_score(player, player_history, course_key)
        if bayes and bayes["data_points"] >= 3:
            # Compare Bayesian posterior to our current score
            bayes_delta = bayes["score"] - final_score
            # Scale: cap at ±5 pts, weighted by data confidence
            data_confidence = min(1.0, bayes["data_points"] / 8.0)
            ml_adj = round(max(-5.0, min(5.0, bayes_delta * 0.3 * data_confidence)), 1)
            if abs(ml_adj) >= 0.5:
                final_score = max(1, min(100, final_score + ml_adj))
                score_breakdown["ml_bayes"] = ml_adj
            # Store Bayesian data for frontend display
            player["bayesianScore"] = bayes

    if is_ceremonial:
        final_score = min(final_score, 6)
        player["competitiveness"] = "ceremonial"
    elif is_non_competitive:
        final_score = min(final_score, 18)
        player["competitiveness"] = "non_competitive"
    elif is_declining:
        final_score = min(final_score, 35)
        player["competitiveness"] = "declining"
    else:
        player["competitiveness"] = "active"

    # Store component breakdown for frontend confidence score visualization
    player["confBreakdown"] = score_breakdown

    # =========================================================
    # EDGE SCORE  (model win% minus market implied%)
    # Positive = our model rates them higher than books do.
    #
    # NOTE: this is a PRELIMINARY edge_score using the legacy
    # `(confScore/100) * 15` heuristic. It is overwritten in the
    # main pipeline AFTER softmax-normalization assigns each player
    # a calibrated `modelWinProb` that integrates to ~1.0 across
    # the field. Downstream consumers should rely on the
    # post-softmax recomputation for any betting decisions.
    # =========================================================
    edge_score = None
    # Ceremonial / non-competitive players should NEVER show positive edge
    if player.get("competitiveness") in ("ceremonial", "non_competitive"):
        edge_score = 0.0
    elif best_odds is not None:
        if best_odds > 0:
            market_implied_pct = 100.0 / (best_odds + 100.0) * 100.0
        else:
            market_implied_pct = abs(best_odds) / (abs(best_odds) + 100.0) * 100.0
        model_win_pct = (final_score / 100.0) * 15.0
        edge_score = round(model_win_pct - market_implied_pct, 2)

    # =========================================================
    # PROP-ADJUSTED SCORES
    # Different prop types reward different player profiles.
    # Reweight the component breakdown for each prop type.
    # Win = SG/fit/closing dominant; MakeCut = consistency/cut dominant.
    # =========================================================
    PROP_WEIGHTS = {
        "win":     {"sg":25,"fit":18,"form":14,"history":10,"gir":8,"driving":5,"birdie_bogey":5,"cut":2,"market":3,"closing":4,"field_strength":4,"consistency":2},
        "top5":    {"sg":20,"fit":15,"form":14,"history":10,"gir":9,"driving":6,"birdie_bogey":6,"cut":4,"market":3,"closing":5,"field_strength":3,"consistency":5},
        "top10":   {"sg":16,"fit":12,"form":14,"history":10,"gir":9,"driving":6,"birdie_bogey":7,"cut":6,"market":3,"closing":4,"field_strength":2,"consistency":11},
        "top20":   {"sg":10,"fit":8,"form":14,"history":10,"gir":8,"driving":6,"birdie_bogey":8,"cut":10,"market":2,"closing":2,"field_strength":1,"consistency":21},
        "makeCut": {"sg":6,"fit":5,"form":12,"history":10,"gir":6,"driving":5,"birdie_bogey":6,"cut":22,"market":2,"closing":0,"field_strength":0,"consistency":26},
    }

    prop_scores = {}
    for prop_type, weights in PROP_WEIGHTS.items():
        # Reconstruct score from stored component breakdown using prop-specific weights
        ps = 0.0
        for factor, weight in weights.items():
            # Component values were stored as raw points (factor_norm * original_weight)
            # We need the normalized 0-1 value, so divide by original weight
            raw = score_breakdown.get(factor, 0)
            # Original weights from v3 model
            orig_w = {"sg":20,"fit":16,"form":14,"history":10,"gir":10,"driving":7,
                      "birdie_bogey":7,"cut":5,"market":3,"closing":4,
                      "field_strength":2,"consistency":2}.get(factor, 1)
            if orig_w > 0:
                norm_val = raw / orig_w  # back to 0-1 range
                ps += norm_val * weight
        # Apply same post-processing adjustments (par5, weather, tee, cross, ml)
        for adj_key in ["par5", "weather", "tee_time", "cross_signal", "ml_bayes"]:
            adj = score_breakdown.get(adj_key, 0)
            ps += adj
        ps = max(1, min(100, round(ps)))
        # Apply competitiveness gate
        if is_ceremonial:
            ps = min(ps, 6)
        elif is_non_competitive:
            ps = min(ps, 18)
        elif is_declining:
            ps = min(ps, 35)
        prop_scores[prop_type] = ps

    player["propScores"] = prop_scores

    # Clean up temp keys
    player.pop("_career_cut_pct", None)
    player.pop("_recent_cut_pct", None)
    player.pop("_teeTime", None)

    return final_score, edge_score


# ============================================================
# MAIN PIPELINE
# ============================================================

def run_pipeline():
    """Run the full data collection pipeline and output golf-data.json."""
    print("=" * 60)
    print("PropsBot Golf Data Pipeline")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    output = {
        "version": 3,
        "generatedAt": datetime.now().isoformat(),
        "generatedBy": "PropsBot Golf Scraper v2.0",
        "sources": [
            "BallDontLie PGA API (GOAT tier — players, field, odds, props, stats)",
            "DataGolf.com (rankings, SG breakdowns)",
            "ESPN API (live leaderboard + tee times)",
            "The Odds API (outrights + 3-ball matchups)",
            "Open-Meteo (weather forecasts)",
            "Manual curation (course fit, betting notes)"
        ],
        "players": [],
        "currentEvent": None,
        "courses": {},
        "weather": None,
    }

    # ============================================================
    # PRIMARY: BallDontLie PGA API (GOAT tier)
    # ============================================================
    bdl_tournament = None
    bdl_futures = {k: {} for k in EMPTY_FUTURES}
    bdl_odds = {}
    bdl_props = {}
    bdl_field = []

    if BDL_API_KEY:
        bdl_tournament = bdl_get_current_tournament()

        if bdl_tournament:
            tid = bdl_tournament["id"]

            # Get tournament field
            bdl_field = bdl_get_tournament_field(tid)

            # Get futures odds (winner + top5/10/20/makeCut/r1Leader).
            # Pass status so phantom-odds threshold tightens while play is live.
            bdl_futures = bdl_get_futures_odds(tid, bdl_tournament.get("status"))
            bdl_odds = bdl_futures.get("winner", {})

            # Get player props (if tournament is upcoming/in-progress)
            if bdl_tournament.get("status") in ("NOT_STARTED", "IN_PROGRESS"):
                bdl_props = bdl_get_player_props(tid)

            # Get results/leaderboard if in progress or completed
            if bdl_tournament.get("status") in ("IN_PROGRESS", "COMPLETED"):
                bdl_results = bdl_get_tournament_results(tid)
            else:
                bdl_results = []

            # Build currentEvent from BDL. Captures more fields than before —
            # purse, defending champion, country, endDate — that BDL already
            # ships on /tournaments but we were discarding. Defending champ
            # comes from looking at the latest prior tournament with the same
            # course; falls back to the BDL `champion` field on the current
            # tournament (which is sometimes pre-populated for re-runs).
            _champ_obj = bdl_tournament.get("champion") or {}
            _champ_name = (
                _champ_obj.get("display_name")
                or (f"{_champ_obj.get('first_name','')} {_champ_obj.get('last_name','')}".strip()
                    if isinstance(_champ_obj, dict) else "")
            ).strip() or None
            output["currentEvent"] = {
                "name": bdl_tournament.get("name", ""),
                "course": bdl_tournament.get("course_name", ""),
                "startDate": bdl_tournament.get("start_date", ""),
                "endDate": bdl_tournament.get("end_date", ""),
                "status": bdl_tournament.get("status", ""),
                "city": bdl_tournament.get("city", ""),
                "state": bdl_tournament.get("state", ""),
                "country": bdl_tournament.get("country", ""),
                "purse": bdl_tournament.get("purse"),
                "defendingChampion": _champ_name,
                "bdlId": tid,
                "leaderboard": [],
            }

            # Build leaderboard from results
            for r in bdl_results[:50]:
                player = r.get("player", {})
                output["currentEvent"]["leaderboard"].append({
                    "name": player.get("display_name", ""),
                    "position": r.get("position", ""),
                    "score": r.get("total_to_par", ""),
                    "totalStrokes": safe_float(r.get("total_strokes", 0)),
                })

            # ============================================================
            # COURSE HOLE DATA — build for current tournament's venue
            # ============================================================
            cname = bdl_tournament.get("course_name", "")
            ename = bdl_tournament.get("name", "")
            course_key = match_venue_to_course(cname, ename)
            if course_key:
                course_intel = bdl_build_course_intel(
                    course_key=course_key,
                    current_tid=tid,
                    course_name=cname,
                    event_name=ename,
                    par=bdl_tournament.get("par", 72),
                    yards=bdl_tournament.get("yardage", 7000),
                )
                if course_intel:
                    output["courses"][course_key] = course_intel
                    print(f"[PIPELINE] Course hole data written → courses['{course_key}'] ({len(course_intel['holes'])} holes)")
            else:
                print(f"[PIPELINE] No course key matched for '{cname}' — hole data skipped")

            # ============================================================
            # TOURNAMENT SG LEADERBOARD — live strokes-gained during event
            # ============================================================
            if bdl_tournament.get("status") in ("IN_PROGRESS", "COMPLETED"):
                print("[PIPELINE] Fetching tournament SG leaderboard (player_round_stats)...")
                sg_raw = bdl_get_player_round_stats(tid)
                player_sg = {}
                for ps in (sg_raw or []):
                    player = ps.get("player", {})
                    pid = player.get("id") or ps.get("player_id")
                    if not pid:
                        continue
                    name = (
                        f"{player.get('first_name', '')} {player.get('last_name', '')}".strip()
                        if isinstance(player, dict) else str(player)
                    ) or ps.get("player_name", "")
                    if not name:
                        continue
                    rnd = ps.get("round_number") or ps.get("round") or 0
                    if pid not in player_sg:
                        player_sg[pid] = {"name": name, "rounds": 0,
                                          "sg_total": 0, "sg_ott": 0,
                                          "sg_app": 0, "sg_atg": 0, "sg_putt": 0}
                    e = player_sg[pid]
                    e["rounds"] += 1
                    e["sg_total"] += ps.get("sg_total") or ps.get("strokes_gained_total") or 0
                    e["sg_ott"]   += ps.get("sg_off_the_tee") or ps.get("sg_ott") or 0
                    e["sg_app"]   += ps.get("sg_approach") or ps.get("sg_app") or 0
                    e["sg_atg"]   += ps.get("sg_around_the_green") or ps.get("sg_atg") or 0
                    e["sg_putt"]  += ps.get("sg_putting") or ps.get("sg_putt") or 0

                sg_leaders = []
                for pid, sg in player_sg.items():
                    rd = sg["rounds"]
                    if rd > 0:
                        sg_leaders.append({
                            "name":    sg["name"],
                            "rounds":  rd,
                            "sgTotal": round(sg["sg_total"] / rd, 2),
                            "sgOTT":   round(sg["sg_ott"]   / rd, 2),
                            "sgAPP":   round(sg["sg_app"]   / rd, 2),
                            "sgATG":   round(sg["sg_atg"]   / rd, 2),
                            "sgPutt":  round(sg["sg_putt"]  / rd, 2),
                        })
                sg_leaders.sort(key=lambda x: x["sgTotal"], reverse=True)
                if sg_leaders:
                    output["tournamentSG"] = sg_leaders[:40]
                    print(f"[PIPELINE] Tournament SG written for {len(sg_leaders)} players")

                    # Bayesian skill update: blend season SG (prior) with
                    # this week's observed SG. After 4 rounds the posterior
                    # weighs the observation ~50% if the player has been
                    # notably different from prior, much less for a single
                    # round. Posterior std also exposed for downstream
                    # Sharpe-style edge sorting.
                    PRIOR_STD = 0.5      # uncertainty in true skill (SG/round)
                    OBS_STD = 1.5        # within-round volatility (SG/round)
                    n_updated = 0
                    for entry in sg_leaders:
                        nm_key = normalize_name(entry.get("name", ""))
                        rd = entry.get("rounds", 0) or 0
                        obs_mean = entry.get("sgTotal")
                        if rd <= 0 or not isinstance(obs_mean, (int, float)):
                            continue
                        # Match to player object
                        target = None
                        for p in output["players"]:
                            if normalize_name(p.get("name", "")) == nm_key:
                                target = p
                                break
                        if target is None:
                            continue
                        prior_mean = target.get("sgTotal") or 0.0
                        prior_var = PRIOR_STD ** 2
                        eff_obs_var = (OBS_STD ** 2) / rd
                        w_prior = 1.0 / prior_var
                        w_obs = 1.0 / eff_obs_var
                        post_mean = (prior_mean * w_prior + obs_mean * w_obs) / (w_prior + w_obs)
                        post_var = 1.0 / (w_prior + w_obs)
                        target["sgTotalUpdated"] = round(post_mean, 3)
                        target["sgTotalUpdatedStd"] = round(post_var ** 0.5, 3)
                        target["sgUpdateRounds"] = rd
                        n_updated += 1
                    if n_updated:
                        print(f"[BAYES] Updated skill estimates for {n_updated} players (prior N({PRIOR_STD}), obs/round N({OBS_STD}))")

            # ============================================================
            # PER-ROUND PAR-RELATIVE SCORES (Track C)
            # ============================================================
            if bdl_tournament.get("status") in ("IN_PROGRESS", "COMPLETED"):
                rr_keep = set()
                lb_now = (output.get("currentEvent") or {}).get("leaderboard") or []
                for row in lb_now[:80]:
                    nm = row.get("name", "")
                    if nm:
                        rr_keep.add(normalize_name(nm))
                try:
                    rr_raw = bdl_get_player_round_results(tid)
                    rr_nested = bdl_build_player_round_results(rr_raw, keep_names=rr_keep or None)
                    if rr_nested:
                        output["playerRoundResults"] = rr_nested
                        print(f"[PIPELINE] Round results: {len(rr_nested)} players")
                except Exception as e:
                    print(f"  [WARN] Player round results fetch failed: {e}")

            # ============================================================
            # SEASON RANKS (Track C)
            # ============================================================
            try:
                from datetime import datetime as _dt
                season_yr = _dt.utcnow().year
                ss_raw = bdl_get_player_season_stats(season_yr)
                ss_lookup = bdl_build_season_ranks(ss_raw, keep_names=None)
                if not ss_lookup and (_dt.utcnow().month <= 2):
                    ss_raw = bdl_get_player_season_stats(season_yr - 1)
                    ss_lookup = bdl_build_season_ranks(ss_raw, keep_names=None)
                if ss_lookup:
                    output["playerSeasonRanks"] = ss_lookup
                    print(f"[PIPELINE] Season ranks: {len(ss_lookup)} players, " +
                          f"{sum(len(v) for v in ss_lookup.values())} stat-rank pairs")
            except Exception as e:
                print(f"  [WARN] Player season stats fetch failed: {e}")

            # ============================================================
            # PER-PLAYER PER-HOLE SCORECARDS (Track B: 18-cell strip)
            # Powers the hole-by-hole heatmap. Limited to top 50 by leaderboard
            # position to keep JSON size manageable (~250KB at full coverage).
            # ============================================================
            if bdl_tournament.get("status") in ("IN_PROGRESS", "COMPLETED"):
                print("[PIPELINE] Fetching player scorecards (hole-by-hole)...")
                sc_keep = set()
                lb_now = (output.get("currentEvent") or {}).get("leaderboard") or []
                for row in lb_now[:50]:
                    nm = row.get("name", "")
                    if nm:
                        sc_keep.add(normalize_name(nm))
                if not sc_keep and bdl_field:
                    sorted_field = sorted(bdl_field, key=lambda e: (e.get("owgr") or 9999))
                    for e in sorted_field[:50]:
                        bp = e.get("player") or {}
                        nm = bp.get("display_name", "")
                        if nm:
                            sc_keep.add(normalize_name(nm))
                try:
                    sc_raw = bdl_get_player_scorecards(tid)
                    sc_nested = bdl_build_player_scorecards(sc_raw, keep_names=sc_keep or None)
                    if sc_nested:
                        output["playerScorecards"] = sc_nested
                        total_holes = sum(
                            len(rounds) for p in sc_nested.values() for rounds in p.values()
                        )
                        print(f"[PIPELINE] Player scorecards: {len(sc_nested)} players, {total_holes} hole entries")
                except Exception as e:
                    print(f"  [WARN] Player scorecards fetch failed: {e}")

    # ============================================================
    # FALLBACK: Free scrapers (DataGolf, ESPN, PGA Tour)
    # ============================================================
    print("\n--- Free data sources (fallback + enrichment) ---")

    # Step 1: DataGolf rankings
    dg_players = scrape_datagolf_rankings()

    # Step 2: DataGolf event holes
    dg_event = scrape_datagolf_event_holes()

    # Step 3: PGA Tour stats
    pga_stats = scrape_pgatour_stats()

    # Step 4: ESPN leaderboard (fallback if BDL has no results yet)
    espn_event = scrape_espn_leaderboard()

    # Use ESPN event if BDL didn't provide one
    if not output["currentEvent"] and espn_event:
        output["currentEvent"] = espn_event

    # Merge ESPN live scores into BDL event when BDL shell has empty leaderboard.
    # BDL's tournament_results often lags live play, so we prefer live ESPN scores
    # whenever the BDL leaderboard is missing totalStrokes for all entries.
    ce = output.get("currentEvent") or {}
    if espn_event and ce:
        # CRITICAL: ESPN's scoreboard endpoint shows the most-recently-played PGA
        # Tour event regardless of BDL's "current event" flag. During the gap
        # between events (Sun night → Wed), ESPN shows the COMPLETED tournament
        # (Truist) while BDL has already flipped to the NEXT one (PGA Champ).
        # If we naively merge, we end up showing Truist scores under the PGA
        # Championship banner. Match by NAME and reject mismatched ESPN data.
        bdl_name = (ce.get("name") or "").strip().lower()
        espn_name = (espn_event.get("name") or "").strip().lower()
        bdl_status = str(ce.get("status", "")).upper()
        # Token-overlap match: any non-trivial word in common counts as same event.
        # (Handles "RBC Heritage" vs "Heritage Tournament" etc.)
        STOPWORDS = {"the", "championship", "tournament", "open", "classic", "of", "and", "&"}
        bdl_tokens = {w for w in bdl_name.split() if w not in STOPWORDS and len(w) > 2}
        espn_tokens = {w for w in espn_name.split() if w not in STOPWORDS and len(w) > 2}
        events_match = bool(bdl_tokens & espn_tokens) if (bdl_tokens and espn_tokens) else (bdl_name == espn_name)

        bdl_lb = ce.get("leaderboard") or []
        bdl_has_scores = any((p.get("totalStrokes") or 0) > 0 for p in bdl_lb)
        espn_lb = espn_event.get("leaderboard") or []
        espn_has_scores = any((p.get("totalStrokes") or 0) > 0 for p in espn_lb)

        if not events_match:
            print(f"[PIPELINE] ESPN event '{espn_event.get('name')}' does NOT match BDL event "
                  f"'{ce.get('name')}' — ignoring ESPN leaderboard/tee-times for this run "
                  f"(common during Mon-Wed transition).")
            ce["leaderboard"] = []
            ce["leaderboardSource"] = "none-pretournament"
        else:
            if espn_has_scores and (not bdl_has_scores or len(bdl_lb) == 0):
                print(f"[PIPELINE] Replacing empty BDL leaderboard with live ESPN data ({len(espn_lb)} entries)")
                ce["leaderboard"] = espn_lb[:80]
                ce["leaderboardSource"] = "ESPN"
            elif bdl_has_scores:
                ce["leaderboardSource"] = "BallDontLie"
            # Fill missing city/state/course from ESPN if BDL omitted them
            for k in ("city", "state", "course"):
                if not ce.get(k) and espn_event.get(k):
                    ce[k] = espn_event[k]
            status_live = bdl_status in ("IN_PROGRESS", "IN PROGRESS", "COMPLETED", "STATUS_IN_PROGRESS")
            if status_live and not (ce.get("leaderboard") or []):
                print("[PIPELINE] WARNING: tournament is live but leaderboard is empty from all sources")

    # ============================================================
    # MERGE PLAYER DATA
    # ============================================================
    fallback = get_fallback_players()

    if dg_players and len(dg_players) > 5:
        merged = []
        # Pre-compute is_major once — used to filter LIV players out of non-majors
        # even when DataGolf's world rankings include them
        is_major = _is_major_event(output.get("currentEvent") or {})
        liv_filtered = 0
        for dg in dg_players:
            fb = next((f for f in fallback if f["name"].lower() == dg["name"].lower()), None)
            if fb:
                # Skip LIV players during non-major weeks — they don't tee it up at
                # regular PGA Tour events regardless of OWGR ranking
                if fb.get("liv") and not is_major:
                    liv_filtered += 1
                    continue
                player = dict(fb)
                player["sgTotal"] = dg["sgTotal"] if dg["sgTotal"] else fb["sgTotal"]
                player["sgOtt"] = dg["sgOtt"] if dg["sgOtt"] else fb["sgOtt"]
                player["sgApp"] = dg["sgApp"] if dg["sgApp"] else fb["sgApp"]
                player["sgArg"] = dg["sgArg"] if dg["sgArg"] else fb["sgArg"]
                player["sgPutt"] = dg["sgPutt"] if dg["sgPutt"] else fb["sgPutt"]
                player["rank"] = dg["rank"]
                merged.append(player)
            else:
                # Realistic PGA Tour defaults (2026 season averages, per Tour stats)
                # Tour-average birdie: ~3.55/round, bogey: ~2.85/round, score: ~71.3
                dg["birdieAvg"] = 3.5
                dg["bogeyAvg"] = 2.9
                dg["scoringAvg"] = 71.3
                dg["gir"] = 66.0
                dg["fairways"] = 62.0
                dg["scramble"] = 58.0
                dg["proxAvg"] = 34.0
                dg["missDir"] = "neutral"
                dg["flight"] = "neutral"
                dg["courseFit"] = {}
                dg["notes"] = "Auto-scraped player."
                merged.append(dg)
        # LIV players only play majors (Masters, PGA, US Open, Open Championship).
        # Force-include them ONLY during a major week — otherwise they shouldn't
        # appear in a regular PGA Tour event's field at all.
        merged_names = {p["name"].lower() for p in merged}
        liv_added = 0
        if is_major:
            for fb in fallback:
                if fb.get("liv") and fb["name"].lower() not in merged_names:
                    merged.append(dict(fb))
                    liv_added += 1
        output["players"] = merged
        if is_major:
            suffix = f", incl. {liv_added} LIV (major week)"
        else:
            suffix = f", {liv_filtered} LIV filtered out (non-major)"
        print(f"\n  Merged {len(merged)} players (scraped + curated{suffix})")
    else:
        # Fallback-only path (DataGolf 404 or other). Still filter LIV players
        # out unless the current event is a major.
        is_major = _is_major_event(output.get("currentEvent") or {})
        if is_major:
            output["players"] = list(fallback)
            liv_note = ", LIV included (major week)"
        else:
            filtered = [p for p in fallback if not p.get("liv")]
            liv_note = f", {len(fallback) - len(filtered)} LIV filtered (non-major)"
            output["players"] = filtered
        print(f"\n  Using fallback data for {len(output['players'])} players{liv_note}")

    # ============================================================
    # BDL FIELD IS THE SOURCE OF TRUTH — filter players to who's actually teeing off
    # When BDL provides a tournament field, drop any non-field player (unless
    # they still show on ESPN leaderboard). This is the strongest defense
    # against stale fallback data leaking players who aren't playing.
    #
    # DURING MAJORS: also whitelist our fallback LIV list, because BDL can be
    # slow to list LIV players in the tournament_field endpoint even after
    # invitations are public. Without this whitelist, LIV players visibly
    # vanish on Masters/PGA/US Open/Open Championship weeks.
    # ============================================================
    if bdl_field:
        field_names = set()
        for entry in bdl_field:
            p = entry.get("player", {}) or {}
            name = p.get("display_name", "") or f"{p.get('first_name','')} {p.get('last_name','')}".strip()
            if name:
                field_names.add(normalize_name(name))
        # Also include anyone currently on the live leaderboard (covers mid-event)
        lb = (output.get("currentEvent") or {}).get("leaderboard") or []
        for lbe in lb:
            n = normalize_name(lbe.get("name", "") or "")
            if n:
                field_names.add(n)
        # Major-week safeguard: keep LIV roster regardless of BDL field lag
        is_major_check = _is_major_event(output.get("currentEvent") or {})
        liv_whitelist = set()
        if is_major_check:
            for fb in fallback:
                if fb.get("liv"):
                    liv_whitelist.add(normalize_name(fb["name"]))
        if field_names:
            before = len(output["players"])
            output["players"] = [
                p for p in output["players"]
                if normalize_name(p["name"]) in field_names
                or normalize_name(p["name"]) in liv_whitelist
            ]
            dropped = before - len(output["players"])
            liv_kept = sum(1 for p in output["players"]
                           if normalize_name(p["name"]) in liv_whitelist)
            if dropped > 0:
                suffix = f", {liv_kept} LIV kept via major-week whitelist" if liv_kept else ""
                print(f"  Filtered to actual field: kept {len(output['players'])}, dropped {dropped} (not in BDL field/leaderboard){suffix}")

    # ============================================================
    # ADD MISSING FIELD PLAYERS FROM BDL FIELD + ESPN LEADERBOARD
    # Players on the actual course but not in our data get auto-added
    # with minimal stats so they can receive odds and appear on leaderboard.
    # ============================================================
    existing_names = {normalize_name(p["name"]) for p in output["players"]}
    field_additions = 0

    # From BDL field list
    if bdl_field:
        for entry in bdl_field:
            p = entry.get("player", {})
            name = p.get("display_name", "")
            if name and normalize_name(name) not in existing_names:
                res_city = p.get("residence_city")
                res_state = p.get("residence_state")
                residence = ", ".join(filter(None, [res_city, res_state])) or None
                output["players"].append({
                    "id": 9000 + field_additions,
                    "name": name,
                    "rank": p.get("owgr", 200),
                    "sgTotal": 0.0, "sgOtt": 0.0, "sgApp": 0.0, "sgArg": 0.0, "sgPutt": 0.0,
                    "birdieAvg": 3.5, "bogeyAvg": 2.5, "scoringAvg": 72.0,
                    "gir": 65.0, "fairways": 60.0, "scramble": 57.0, "proxAvg": 35.0,
                    "missDir": "neutral", "flight": "neutral", "courseFit": {},
                    "notes": "Auto-added from tournament field.",
                    # Bio fields from BDL tournament_field player schema
                    "owgr": p.get("owgr"),
                    "country": p.get("country"),
                    "countryCode": p.get("country_code"),
                    "dob": p.get("birth_date"),
                    "school": p.get("school"),
                    "residence": residence,
                    "isAmateur": bool(entry.get("is_amateur")),
                })
                existing_names.add(normalize_name(name))
                field_additions += 1

    # From ESPN leaderboard (catches amateurs, past champions, qualifiers)
    espn_lb = output.get("currentEvent", {}).get("leaderboard", [])
    for entry in espn_lb:
        name = entry.get("name", "")
        if name and normalize_name(name) not in existing_names:
            output["players"].append({
                "id": 9500 + field_additions,
                "name": name,
                "rank": 200,
                "sgTotal": 0.0, "sgOtt": 0.0, "sgApp": 0.0, "sgArg": 0.0, "sgPutt": 0.0,
                "birdieAvg": 3.5, "bogeyAvg": 2.5, "scoringAvg": 72.0,
                "gir": 65.0, "fairways": 60.0, "scramble": 57.0, "proxAvg": 35.0,
                "missDir": "neutral", "flight": "neutral", "courseFit": {},
                "notes": "Auto-added from ESPN leaderboard.",
            })
            existing_names.add(normalize_name(name))
            field_additions += 1

    if field_additions:
        print(f"  Added {field_additions} missing field players from BDL/ESPN")

    # ============================================================
    # ENRICH EVERY PLAYER WITH BIO DATA FROM BDL FIELD
    # Walk the BDL tournament_field response once and merge bio fields
    # (country, country_code, owgr, dob, school, residence) onto every
    # matching player in output["players"]. Covers our static top-50
    # players (no bios) as well as auto-added field entries.
    # ============================================================
    if bdl_field:
        bdl_by_name = {}
        for entry in bdl_field:
            bp = entry.get("player") or {}
            bname = bp.get("display_name", "")
            if bname:
                bdl_by_name[normalize_name(bname)] = (bp, entry)
        enriched_bio = 0
        for player in output["players"]:
            key = normalize_name(player.get("name", ""))
            if key not in bdl_by_name:
                continue
            bp, entry = bdl_by_name[key]
            changed = False
            def _set_if_missing(field, value):
                nonlocal changed
                if value is not None and value != "" and not player.get(field):
                    player[field] = value
                    changed = True
            _set_if_missing("owgr", entry.get("owgr") or bp.get("owgr"))
            _set_if_missing("country", bp.get("country"))
            _set_if_missing("countryCode", bp.get("country_code"))
            _set_if_missing("dob", bp.get("birth_date"))
            _set_if_missing("school", bp.get("school"))
            res_city = bp.get("residence_city")
            res_state = bp.get("residence_state")
            residence = ", ".join(filter(None, [res_city, res_state]))
            if residence:
                _set_if_missing("residence", residence)
            if entry.get("is_amateur") and not player.get("isAmateur"):
                player["isAmateur"] = True
                changed = True
            if changed:
                enriched_bio += 1
        if enriched_bio:
            print(f"  Enriched {enriched_bio} players with BDL bio data (country, OWGR, dob, school)")

    # Merge PGA Tour stats
    if pga_stats:
        for stat_key, entries in pga_stats.items():
            for entry in entries:
                player = next((p for p in output["players"] if entry["name"].lower() in p["name"].lower()), None)
                if player:
                    if stat_key == "scoring_avg": player["scoringAvg"] = entry["value"]
                    elif stat_key == "birdie_avg": player["birdieAvg"] = entry["value"]
                    elif stat_key == "gir_pct": player["gir"] = entry["value"]

    # Add course data
    output["courses"] = get_course_data()

    # ============================================================
    # ENRICH: BDL Odds + Props + Field info
    # ============================================================
    if bdl_odds:
        # Build normalized name lookup for odds
        odds_norm = {normalize_name(k): v for k, v in bdl_odds.items()}
        odds_norm_keys = list(odds_norm.keys())
        matched_odds = 0
        for player in output["players"]:
            pn = normalize_name(player["name"])
            # Exact normalized match first
            odds = odds_norm.get(pn)
            if not odds:
                # Fuzzy: check substring both ways
                for oname in odds_norm_keys:
                    if pn in oname or oname in pn:
                        odds = odds_norm[oname]
                        break
                # Last resort: last name match
                if not odds:
                    p_last = pn.split()[-1] if pn else ''
                    if len(p_last) > 3:
                        for oname in odds_norm_keys:
                            o_last = oname.split()[-1] if oname else ''
                            o_first = oname.split()[0] if oname else ''
                            p_first = pn.split()[0] if pn else ''
                            if p_last == o_last and p_first[0] == o_first[0]:
                                odds = odds_norm[oname]
                                break
            if odds:
                player["odds"] = odds
                matched_odds += 1
        print(f"  BDL odds matched: {matched_odds}/{len(output['players'])} players")

    if bdl_props:
        for player in output["players"]:
            props = bdl_props.get(player["name"])
            if props:
                player["props"] = props

    # Add field entry status (OWGR, qualifier info)
    if bdl_field:
        field_map = {}
        for entry in bdl_field:
            p = entry.get("player", {})
            name = p.get("display_name", "")
            if name:
                field_map[name] = {
                    "owgr": p.get("owgr"),
                    "entryStatus": entry.get("entry_status", ""),
                    "inField": True,
                }
        for player in output["players"]:
            finfo = field_map.get(player["name"])
            if finfo:
                player["fieldInfo"] = finfo
                if finfo["owgr"]:
                    player["owgr"] = finfo["owgr"]

    # ============================================================
    # WEATHER
    # ============================================================
    weather_data = None
    event_for_weather = output.get("currentEvent")
    if event_for_weather:
        course_key = match_venue_to_course(
            event_for_weather.get("course", ""),
            event_for_weather.get("name", "")
        )
        if course_key:
            weather_data = scrape_course_weather(course_key)
            if weather_data:
                output["weather"] = {"course": course_key, "forecast": weather_data}
                event_for_weather["weather"] = weather_data

    # ============================================================
    # COURSE FIT ALGORITHM
    # ============================================================
    print("\n  Computing course fit scores...")
    compute_all_course_fits(output["players"], weather_data)

    # ============================================================
    # ODDS API (SUPPLEMENT — merges books BDL doesn't cover)
    # Always runs on scrape days. BDL odds take priority for any
    # book it already covers; Odds API fills in the rest.
    # ============================================================
    odds_api_data = scrape_betting_odds()
    if odds_api_data:
        merged_count = 0
        new_books = set()
        api_norm = {normalize_name(k): v for k, v in odds_api_data.items()}
        api_norm_keys = list(api_norm.keys())
        for player in output["players"]:
            pn = normalize_name(player["name"])
            api_odds = api_norm.get(pn)
            if not api_odds:
                for oname in api_norm_keys:
                    if pn in oname or oname in pn:
                        api_odds = api_norm[oname]
                        break
            if api_odds:
                existing = player.get("odds", {})
                added = 0
                for book, price in api_odds.items():
                    if book not in existing:
                        existing[book] = price
                        new_books.add(book)
                        added += 1
                if added > 0:
                    merged_count += 1
                player["odds"] = existing
        print(f"  Odds API supplement: merged {merged_count} players, new books: {', '.join(sorted(new_books)) if new_books else 'none'}")

    # ============================================================
    # RECENT FORM
    # ============================================================
    form_data = scrape_recent_form(espn_event)
    if form_data:
        for player in output["players"]:
            form = form_data.get(player["name"])
            if form:
                player["recentForm"] = form

    # ============================================================
    # TEE TIMES (fetched early so confidence score can use morning_adv)
    # ============================================================
    tee_times = fetch_tee_times(
        bdl_field=bdl_field,
        expected_event_name=((output.get("currentEvent") or {}).get("name") or None),
    )
    output["teeTimes"] = tee_times
    # Stash tee time on each player for confidence score morning_adv
    if tee_times:
        tt_map = {}
        for tt in tee_times:
            tt_map[tt["player"]] = tt.get("teeTime", "")
        for player in output["players"]:
            tt_str = tt_map.get(player["name"], "")
            if tt_str:
                player["_teeTime"] = tt_str

    # ============================================================
    # ML: LOAD HISTORICAL DATA FOR BAYESIAN ENSEMBLE
    # ============================================================
    base_dir = os.path.dirname(os.path.abspath(__file__))
    history_dir = os.path.join(base_dir, "history")
    ml_player_history = _load_historical_performances(history_dir, max_weeks=20)
    ml_count = sum(1 for v in ml_player_history.values() if len(v) >= 3)
    print(f"\n  ML Engine: loaded {len(ml_player_history)} player histories ({ml_count} with 3+ data points)")

    # ============================================================
    # LEARNED COURSE FIT — historical SG residuals at this venue
    # ------------------------------------------------------------
    # Pulls last 5 years of tournament_results at the same course_id and
    # blends each player's average SG residual into courseFit[course_key].
    # Sample-size shrinkage (k=2) ensures small-N players still lean on
    # the trait-based prior. Skipped on early failures so we never block
    # the rest of the pipeline.
    # ============================================================
    print("  Applying learned course fit (historical SG residuals)...")
    _resolved_for_fit = match_venue_to_course(
        output.get("currentEvent", {}).get("course", ""),
        output.get("currentEvent", {}).get("name", "")
    )
    _bdl_course_id = None
    try:
        _bdl_course_id = (BDL_COURSE_ID_MAP.get(_resolved_for_fit)
                          if _resolved_for_fit else None)
        if not _bdl_course_id:
            _bdl_course_id = bdl_find_course_id(
                output.get("currentEvent", {}).get("course", "")
            )
    except Exception as _e:
        print(f"  [WARN] Could not resolve BDL course_id: {_e}")
    if _bdl_course_id and _resolved_for_fit:
        try:
            _learned = compute_learned_course_fit(_bdl_course_id, output["players"])
            n_blended = apply_learned_course_fit(
                output["players"], _learned, course_key=_resolved_for_fit
            )
            output["learnedFitSummary"] = {
                "courseId": _bdl_course_id,
                "courseKey": _resolved_for_fit,
                "playersBlended": n_blended,
            }
            print(f"  Learned fit: blended {n_blended} players (n>=1 prior appearances)")
        except Exception as _e:
            print(f"  [WARN] Learned course fit failed: {_e}")

    # ---- PER-COURSE SG CATEGORY WEIGHTS (regression-learned) ----
    # Learn which SG category most predicts finish position at this venue.
    # Augusta loads onto SG: Approach; Pebble onto SG: Putting. Coefficients
    # are persisted on output and threaded through every downstream
    # predictor so the model can favor approach-strong players at Augusta,
    # putting-strong players at Pebble, etc.
    if _bdl_course_id and _resolved_for_fit:
        try:
            _sg_weights = compute_course_sg_weights(_bdl_course_id, output["players"])
            if _sg_weights:
                output.setdefault("courseSgWeights", {})[_resolved_for_fit] = _sg_weights
                print(f"  Course SG weights persisted for {_resolved_for_fit}")
        except Exception as _e:
            print(f"  [WARN] Course SG weight learning failed: {_e}")

    # ---- CURRENT EVENT FIELD STRENGTH ----
    # Computed from OWGRs already attached to player objects (via Track A
    # bio enrichment). Surfaces to JSON as a transparency artifact and to
    # downstream modeling as a hint that current SG signals from a stronger
    # field have less noise.
    try:
        _curr_owgrs = [p.get("owgr") for p in output["players"]
                       if isinstance(p.get("owgr"), (int, float))]
        _curr_strength, _curr_avg_owgr = compute_field_strength(_curr_owgrs)
        output["fieldStrength"] = {
            "multiplier": _curr_strength,
            "avgOwgrTopQuartile": _curr_avg_owgr,
            "fieldSize": len(_curr_owgrs),
            "interpretation": (
                "Strong field (1.0+ = major-strength)" if _curr_strength >= 1.1
                else "Weak field" if _curr_strength <= 0.9
                else "Standard PGA field"
            ),
        }
        print(f"  Field strength: {_curr_strength}x (avg top-quartile OWGR {_curr_avg_owgr}, n={len(_curr_owgrs)})")
    except Exception as _e:
        print(f"  [WARN] Field strength compute failed: {_e}")

    # ============================================================
    # CONFIDENCE SCORE MODEL (v3 + Bayesian ML)
    # ============================================================
    print("  Calculating PropsBot Confidence Scores...")
    resolved_course = match_venue_to_course(
        output.get("currentEvent", {}).get("course", ""),
        output.get("currentEvent", {}).get("name", "")
    ) or "tpc_sawgrass"  # generic default — not tournament-specific
    for player in output["players"]:
        try:
            conf, edge = calculate_player_confidence_score(
                player,
                output["players"],
                course_key=resolved_course,
                player_history=ml_player_history,
            )
            player["confScore"] = conf
            if edge is not None:
                player["edgeScore"] = edge
        except Exception as e:
            player["confScore"] = 50
            player["edgeScore"] = 0
            print(f"    confScore error for {player.get('name','?')}: {e}")

    # Deduplicate players by name (keep highest confScore)
    seen = {}
    for p in output["players"]:
        key = p.get("name", "").lower().strip()
        if key not in seen or p.get("confScore", 0) > seen[key].get("confScore", 0):
            seen[key] = p
    if len(seen) < len(output["players"]):
        print(f"  Dedup: removed {len(output['players']) - len(seen)} duplicate players")
    output["players"] = list(seen.values())

    # Sort players by confScore descending
    output["players"].sort(key=lambda p: p.get("confScore", 0), reverse=True)

    # Assign sequential PropsBot ranks, preserve original OWGR rank
    for i, player in enumerate(output["players"], 1):
        player["owgrRank"] = player.get("rank", 999)
        player["rank"] = i

    # ============================================================
    # SOFTMAX-NORMALIZED WIN PROBABILITY
    # ------------------------------------------------------------
    # The raw confScore-based heuristic `(confScore/100) * 15`
    # produces "win%" values that sum to ~500-600% across a 70+
    # player field — so edge/EV against true book prices are
    # meaningless. Softmax-normalize confScore into a calibrated
    # win-probability distribution that integrates to ~1.0.
    #
    # Temperature is tuned so the field favorite lands in the
    # 12-18% range, matching realistic outright odds (e.g. Scheffler
    # at ~+700 implies ~12.5% true win equity).
    # ============================================================
    SOFTMAX_TEMPERATURE = 12.0
    competitive_players = [
        p for p in output["players"]
        if p.get("competitiveness") not in ("ceremonial", "non_competitive")
    ]
    if competitive_players:
        logits = [
            float(p.get("confScore") or 0) / SOFTMAX_TEMPERATURE
            for p in competitive_players
        ]
        max_logit = max(logits)
        exps = [math.exp(l - max_logit) for l in logits]
        total = sum(exps) or 1.0
        for cp, e in zip(competitive_players, exps):
            cp["modelWinProb"] = e / total
    # Ceremonial / non-competitive players get a tiny floor probability
    # so they don't break order-statistics math but contribute nothing.
    for p in output["players"]:
        if "modelWinProb" not in p:
            p["modelWinProb"] = 1e-6

    prob_sum = sum(float(p.get("modelWinProb") or 0) for p in output["players"])
    top_prob = max((float(p.get("modelWinProb") or 0) for p in output["players"]), default=0.0)
    print(f"  Softmax win-prob: T={SOFTMAX_TEMPERATURE}, sum={prob_sum:.4f}, "
          f"top1={top_prob * 100:.2f}%")

    # ============================================================
    # ORDER-STATISTIC MONTE CARLO — top5 / top10 / top20 PROBS
    # ------------------------------------------------------------
    # Simulate the field's 4-round tournament scores using the same
    # scoring distribution that powers predict_matchups: mean from
    # confScore + course fit, std from per-player scoreStd (or
    # BASE_STD fallback). Count finish-position frequencies to
    # derive calibrated top-N place probabilities. Capped at top 50
    # by confScore to keep runtime bounded.
    # ============================================================
    import random as _mc_random
    _mc_random.seed(42)
    MC_BASE_STD = 2.85
    SIM_ROUNDS = 5000
    SIM_FIELD_CAP = 50

    sim_field = sorted(
        output["players"],
        key=lambda p: -(p.get("confScore") or 0),
    )[:SIM_FIELD_CAP]

    if sim_field:
        # Mean strokes-under-par proxy: anchor confScore=50 at 0,
        # rescale by ~0.04 strokes per confScore point so the
        # favorite sits ~1.2 strokes ahead of mid-field per round
        # — same order of magnitude as the SG-derived mean used in
        # predict_matchups.
        sim_means = []
        sim_stds = []
        for p in sim_field:
            conf = float(p.get("confScore") or 50.0)
            fit = (p.get("courseFit") or {}).get(resolved_course)
            fit_boost = 0.0
            if isinstance(fit, (int, float)):
                fit_boost = (fit - 75) / 25.0
            mean = -((conf - 50.0) * 0.04) - fit_boost
            std = p.get("scoreStd")
            if not (isinstance(std, (int, float)) and 1.5 <= std <= 5.0):
                std = MC_BASE_STD
            sim_means.append(mean)
            sim_stds.append(float(std))

        n_field = len(sim_field)
        top5_counts = [0] * n_field
        top10_counts = [0] * n_field
        top20_counts = [0] * n_field
        gauss = _mc_random.gauss

        for _ in range(SIM_ROUNDS):
            # 4-round total: per-round noise has std ~ stds[i],
            # so 4-round-total std ≈ stds[i] * 2 (sqrt(4)).
            scores = [
                4.0 * sim_means[i] + gauss(0.0, sim_stds[i] * 2.0)
                for i in range(n_field)
            ]
            order = sorted(range(n_field), key=lambda i: scores[i])
            for finish, idx in enumerate(order):
                if finish < 5:
                    top5_counts[idx] += 1
                if finish < 10:
                    top10_counts[idx] += 1
                if finish < 20:
                    top20_counts[idx] += 1

        for i, p in enumerate(sim_field):
            p["modelTop5Prob"] = round(top5_counts[i] / SIM_ROUNDS, 4)
            p["modelTop10Prob"] = round(top10_counts[i] / SIM_ROUNDS, 4)
            p["modelTop20Prob"] = round(top20_counts[i] / SIM_ROUNDS, 4)

        # Players outside the simulated field get tiny floor values.
        for p in output["players"]:
            if "modelTop5Prob" not in p:
                p["modelTop5Prob"] = 0.0
                p["modelTop10Prob"] = 0.0
                p["modelTop20Prob"] = 0.001

        print(f"  Order-stat MC: simulated {n_field} players x {SIM_ROUNDS} rounds")

    # ============================================================
    # ML: ANOMALY DETECTION (Low-Frequency Market Mispricings)
    # ============================================================
    anomalies = detect_odds_anomalies(output["players"], course_key=resolved_course)
    if anomalies:
        output["anomalies"] = anomalies
        # Also tag individual players with their anomalies
        for player in output["players"]:
            pa = anomalies.get(player.get("name"))
            if pa:
                player["anomalies"] = pa
        print(f"  Anomaly Detection: found {len(anomalies)} mispriced players")
    else:
        print("  Anomaly Detection: no significant anomalies found (need more odds data)")
    print(f"  Confidence scores calculated for {len(output['players'])} players")

    # ============================================================
    # EV SCORING + EDGE RECOMPUTE (post-softmax, vig-corrected)
    # ------------------------------------------------------------
    # Both edgeScore and evScore now consume the calibrated
    # modelWinProb (softmax over confScore) instead of the legacy
    # `(confScore/100) * 15` heuristic, AND de-vig the book line
    # against the field overround before comparing. Outright winner
    # markets typically carry 20-30% hold; without de-vigging, every
    # "+5% EV" reported to users was really break-even or worse.
    # ============================================================
    output["bookVigInfo"] = build_market_overrounds(output["players"])
    winner_overround = (output["bookVigInfo"].get("winner") or {}).get("overround")
    if winner_overround:
        print(f"[VIG] Winner market overround: {winner_overround} ({(winner_overround-1)*100:.1f}% hold across {output['bookVigInfo']['winner']['bookCount']} players)")
    for player in output["players"]:
        win_prob = float(player.get("modelWinProb") or 0.0)
        model_win_pct = win_prob * 100.0  # convert to percent for downstream funcs

        best_odds = player.get("odds", {})
        if best_odds:
            dk_odds = best_odds.get("dk") or best_odds.get("fd") or best_odds.get("mgm")
            if dk_odds and win_prob > 0:
                try:
                    dk_odds_num = float(dk_odds)
                except (TypeError, ValueError):
                    continue
                # Fair (de-vigged) implied prob for the edge calc
                fair_prob = devig_implied_prob(dk_odds_num, overround=winner_overround)
                fair_implied_pct = (fair_prob or 0.0) * 100.0
                # Ceremonial / non-competitive players keep zero edge
                if player.get("competitiveness") in ("ceremonial", "non_competitive"):
                    player["edgeScore"] = 0.0
                else:
                    player["edgeScore"] = round(model_win_pct - fair_implied_pct, 2)
                player["evScore"] = calculate_ev_score(
                    dk_odds, model_win_pct, overround=winner_overround
                )
                player["fairImpliedWinPct"] = round(fair_implied_pct, 2)

    # ============================================================
    # MASTERS INTELLIGENCE (only during Masters week — early April)
    # ============================================================
    now = datetime.now()
    is_masters_week = now.month == 4 and 1 <= now.day <= 14
    if is_masters_week:
        masters_intel = bdl_build_masters_intel()
        if masters_intel:
            output["mastersIntel"] = masters_intel
        print(f"  Masters Intel: {'loaded' if masters_intel else 'skipped (no data)'}")
    else:
        print(f"  Masters Intel: skipped (not Masters week — {now.strftime('%b %d')})")

    # (Tee times already fetched above before confidence score loop)

    # ============================================================
    # ODDS MOVEMENT
    # ============================================================
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output["oddsMovement"] = compute_odds_movement(output["players"], base_dir)

    # ============================================================
    # CUT LINE PREDICTION (Monte Carlo, course-aware)
    # ============================================================
    # Pull course par + live R1 leaderboard so the cut model uses the actual
    # venue (not a hardcoded Augusta history) and tightens up Friday when R1
    # scores are in. Cut size defaults to 65+ties (PGA standard); majors that
    # use top 50 + ties can override here later if needed.
    _course_par = None
    if isinstance(output.get("courses"), dict):
        _cdata = output["courses"].get(resolved_course) if resolved_course else None
        if isinstance(_cdata, dict):
            _course_par = _cdata.get("par")
    if not _course_par and isinstance(output.get("currentEvent"), dict):
        _course_par = (output["currentEvent"].get("course_par")
                       or output["currentEvent"].get("par"))
    _live_lb = ((output.get("currentEvent") or {}).get("leaderboard")) or []
    _course_sg_weights = (output.get("courseSgWeights") or {}).get(resolved_course)
    output["cutPrediction"] = predict_cut_line(
        output["players"],
        course_key=resolved_course,
        weather=output.get("weather"),
        tournament_sg=output.get("tournamentSG"),
        course_par=_course_par,
        live_leaderboard=_live_lb,
        course_sg_weights=_course_sg_weights,
    )

    # ============================================================
    # FULL-TOURNAMENT POSITION PROBABILITIES
    # ------------------------------------------------------------
    # Per-player P(win), P(top-5), P(top-10), P(top-20), P(make-cut)
    # from a 4-round Monte Carlo. Honest model probabilities for the
    # markets where BDL ships book lines (top-5, make-cut), and the
    # substrate for de-vigged edge calc on those markets.
    # ============================================================
    try:
        position_probs = predict_player_position_probs(
            output["players"],
            course_key=resolved_course,
            weather=output.get("weather"),
            tournament_sg=output.get("tournamentSG"),
            course_par=_course_par,
            live_leaderboard=_live_lb,
            course_sg_weights=_course_sg_weights,
        )
        if position_probs:
            output["playerPositionProbs"] = position_probs
            # Attach to player objects for direct frontend lookup
            for p in output["players"]:
                k = normalize_name(p.get("name", ""))
                if k in position_probs:
                    pp = position_probs[k]
                    p["modelTop5Prob"] = pp["top5"]
                    p["modelTop10Prob"] = pp["top10"]
                    p["modelTop20Prob"] = pp["top20"]
                    p["modelMakeCutProb"] = pp["makeCut"]
            print(f"[PROPS] Position probs attached to {len(position_probs)} players")
    except Exception as _e:
        print(f"  [WARN] Position prob simulation failed: {_e}")

    # ============================================================
    # PLAYER SIMILARITY (kNN on SG profiles)
    # Attaches per-player ``similarPlayers: [{name, distance}, ...]`` so the
    # frontend can show comps and rookies inherit course intuition from
    # similar veterans.
    # ============================================================
    try:
        n_sim = compute_player_similarity(output["players"], k=5)
        if n_sim:
            print(f"[SIMILARITY] Computed top-5 comps for {n_sim} players")
    except Exception as _e:
        print(f"  [WARN] Player similarity compute failed: {_e}")

    # ============================================================
    # PER-HOLE PROP PRICING (round-score / birdie / bogey / eagle props)
    # ------------------------------------------------------------
    # Runs a per-hole Monte Carlo using the course's historical hole-by-hole
    # scoring distribution adjusted by each player's skill. Produces full
    # round-score and event-count distributions per player. Pairs with any
    # /odds/player_props lines BDL ships to compute de-vigged edge on
    # birdies-o/u, bogeys-o/u, round-score-o/u, eagle-yes/no.
    # ============================================================
    try:
        _course_for_holes = None
        if isinstance(output.get("courses"), dict):
            _course_for_holes = output["courses"].get(resolved_course)
        if _course_for_holes:
            per_hole = predict_per_hole_props(
                output["players"], _course_for_holes,
                course_par=_course_par, sims=1500,
                course_sg_weights=_course_sg_weights,
            )
            if per_hole:
                output["perHoleProps"] = per_hole
                # Attach summary fields to player objects (per-round means)
                for p in output["players"]:
                    k = normalize_name(p.get("name", ""))
                    ph = per_hole.get(k)
                    if ph:
                        p["modelExpectedRoundScore"] = ph["roundScore"]["mean"]
                        p["modelExpectedBirdies"] = ph["birdies"]["mean"]
                        p["modelExpectedBogeys"] = ph["bogeys"]["mean"]
                        p["modelExpectedEagles"] = ph["eagles"]["mean"]
                print(f"[PROPS] Per-hole sims complete for {len(per_hole)} players")
                # Price any prop_lines we have against the new distributions
                if output.get("propLines"):
                    priced = price_player_props(output["propLines"], per_hole)
                    if priced:
                        output["pricedPlayerProps"] = priced
                        with_edge = sum(1 for v in priced.values() if v.get("edgeOverPct") is not None)
                        print(f"[PROPS] Priced {len(priced)} props ({with_edge} with edge calc)")
    except Exception as _e:
        print(f"  [WARN] Per-hole prop pricing failed: {_e}")

    # ============================================================
    # PLAYER NEWS
    # ============================================================
    output["news"] = fetch_player_news()

    # ============================================================
    # CATEGORIZED PROPS
    # ============================================================
    # Placement markets (top 5/10/20 + make cut + R1 leader) come from the
    # same BDL `futures` endpoint as the outright winner — they are NOT in
    # `player_props` (which is birdie / bogey / scoring totals only). The
    # previous version of this block fed `bdl_props` into a parser that
    # never produced any data, so placement Discord alerts never fired.
    # Build a normalized lookup of our actual field — used to filter out any
    # cross-sport prop entries that BDL might return (e.g. LPGA player names
    # leaking into PGA Championship's top5 market). Without this filter the
    # user-facing Props tab showed "Nelly Korda" / "Jeeno Thitikul" during
    # PGA Championship week.
    _field_norm = {normalize_name(p.get("name", "")) for p in output.get("players", [])}

    def _filter_to_field(market_dict):
        """Drop any entry whose normalized name isn't in our actual field.
        Logs the leak count for monitoring."""
        if not market_dict:
            return market_dict, 0
        kept = {}
        leaked = 0
        for name, val in market_dict.items():
            if normalize_name(name) in _field_norm:
                kept[name] = val
            else:
                leaked += 1
        return kept, leaked

    raw_props = {
        "top5":     bdl_futures.get("top5", {}),
        "top10":    bdl_futures.get("top10", {}),
        "top20":    bdl_futures.get("top20", {}),
        "makeCut":  bdl_futures.get("makeCut", {}),
        "r1Leader": bdl_futures.get("r1Leader", {}),
    }
    output["propsByType"] = {}
    total_leaked = 0
    for market_key, market_dict in raw_props.items():
        filtered, leaked = _filter_to_field(market_dict)
        output["propsByType"][market_key] = filtered
        total_leaked += leaked
    if total_leaked > 0:
        print(f"  [CROSS-SPORT FILTER] Dropped {total_leaked} prop entries for players not in our field")

    if bdl_props:
        # Real book lines for stat props (birdies/bogeys/scoring/eagles)
        raw_lines = extract_stat_prop_lines(bdl_props)
        # Same field filter
        output["propLines"] = {n: v for n, v in raw_lines.items() if normalize_name(n) in _field_norm}
        line_leaked = len(raw_lines) - len(output["propLines"])
        suffix = f" ({line_leaked} cross-sport entries dropped)" if line_leaked else ""
        print(f"  Extracted book lines for {len(output['propLines'])} players{suffix}")
    else:
        output["propLines"] = {}

    # ============================================================
    # 3-BALL MATCHUPS + PREDICTIVE MODEL
    # Priority: BDL matchup odds → Odds API 3_balls → synthesize from tee times
    # ============================================================
    matchups_raw = []
    source = None
    if bdl_tournament:
        matchups_raw = bdl_get_matchup_odds(bdl_tournament["id"])
        if matchups_raw:
            source = "BallDontLie"
    if not matchups_raw:
        matchups_raw = scrape_matchup_odds()
        if matchups_raw:
            source = "TheOddsAPI"
    if not matchups_raw and tee_times:
        matchups_raw = synthesize_matchups_from_tee_times(tee_times)
        if matchups_raw:
            source = "SyntheticTeeTimes"
            type_counts = {}
            for g in matchups_raw:
                type_counts[g["type"]] = type_counts.get(g["type"], 0) + 1
            print(f"[MATCHUP] Synthesized from tee times: {type_counts} (no book odds yet)")

    if matchups_raw:
        matchups_scored = predict_matchups(
            matchups_raw,
            players=output["players"],
            course_key=resolved_course,
            weather=output.get("weather"),
            tournament_sg=output.get("tournamentSG") or [],
            sims=10000,
            course_sg_weights=_course_sg_weights,
        )
        output["threeBalls"] = matchups_scored  # keep the key name — UI already uses it
        output["threeBallsSource"] = source
        edges = sum(1 for g in matchups_scored
                    for p in g["players"]
                    if isinstance(p.get("ev"), (int, float)) and p["ev"] > 5)
        print(f"[MATCHUP] {len(matchups_scored)} groups modeled from {source}, {edges} picks with >5% EV")
    else:
        output["threeBalls"] = []
        output["threeBallsSource"] = None

    # ---- DYNAMIC MAJORS SCHEDULE ----
    # Emit from BDL tournaments instead of hardcoding in HTML — keeps the
    # "next major" tab accurate every year without touching code.
    schedule = build_dynamic_majors_schedule(_LAST_TOURNAMENTS_RAW)
    if schedule:
        output["majorsSchedule"] = schedule
        print(f"[MAJORS] Emitted {len(schedule)} majors from BDL tournaments")
    else:
        output["majorsSchedule"] = []

    # ---- CANONICALIZE PLAYER NAMES ----
    # Unicode display forms come from BDL/ESPN (e.g. "Ludvig Åberg", "Sami Välimäki")
    # but our fallback dict uses ASCII ("Ludvig Aberg"). HTML joins between
    # players[] and leaderboard[] compare literally — mismatched forms caused
    # ~2 players to show blank recent-form cards. Canonicalize to the live
    # event's display form (usually BDL/ESPN unicode).
    lb = ((output.get("currentEvent") or {}).get("leaderboard")) or []
    lb_name_map = {normalize_name(e.get("name", "")): e.get("name", "")
                   for e in lb if e.get("name")}
    if lb_name_map:
        renamed = 0
        for p in output["players"]:
            canonical = lb_name_map.get(normalize_name(p.get("name", "")))
            if canonical and canonical != p.get("name"):
                p["name"] = canonical
                renamed += 1
        if renamed:
            print(f"  Canonicalized {renamed} player names to leaderboard display form")

    # ---- FIX cutPrediction COURSE CONTEXT ----
    # predict_cut_line() defaults its historical-avg note to Augusta even
    # when we're playing Harbour Town. Patch the note to reference the
    # actual current course if we have one.
    cp = output.get("cutPrediction") or {}
    cur_course_name = ((output.get("currentEvent") or {}).get("course") or "").strip()
    if cp and cur_course_name:
        note = cp.get("note", "")
        # If the note hardcodes "Augusta" but we're not at Augusta, rewrite it
        if "augusta" in note.lower() and "augusta" not in cur_course_name.lower():
            cp["note"] = note.lower().replace("augusta", cur_course_name).replace("Augusta", cur_course_name).capitalize()
            # Simple replace often mangles casing; just prefix a clarifier instead
            cp["note"] = f"{cur_course_name}: {cp.get('courseAvg', '—')} strokes historical cut avg"
            output["cutPrediction"] = cp

    # ---- MODEL PARAMS (tuned by backtest, if available) ----
    output["modelParams"] = load_model_params(base_dir)

    # ---- CONFSCORE CALIBRATION ----
    # If scripts/calibrate.py has been run, attach calibrated make-cut
    # probabilities to each player. Frontend can display these instead of
    # raw 0-100 scores when the user wants a real probability.
    n_calibrated = apply_confscore_calibration(output["players"], output["modelParams"])
    if n_calibrated:
        print(f"[CALIBRATION] Applied to {n_calibrated} players (trained {output['modelParams'].get('calibration', {}).get('trainedAt', 'unknown')})")

    # ---- UPDATE CADENCE (exposed to UI for freshness badges) ----
    output["updateCadence"] = {
        "offWeek": "Mon + Tue 1AM ET",
        "wednesday": "5x (5AM / 11AM / 2PM / 7PM / 10PM ET)",
        "tournament": "every 30 min, 7AM-7PM ET Thu-Sun (~100 runs/week)",
        "backtest": "Mon 2AM ET — weekly model retune if calibration drifts",
    }

    # ---- SELF-HEAL FALLBACK ----
    # Write current dynamic stats back to fallback_dynamic.json so future
    # runs have fresh values even if DataGolf + PGA Tour are both down.
    # Only save when we have >=50 real players (don't overwrite with
    # degraded runs).
    if len(output["players"]) >= 50:
        overrides = _load_fallback_overrides()
        saved = 0
        for p in output["players"]:
            name_key = p.get("name", "").lower()
            if not name_key:
                continue
            # Require at least one real SG value — skip auto-added shells
            has_real_sg = any(p.get(f) for f in ("sgTotal", "sgOtt", "sgApp"))
            if not has_real_sg:
                continue
            entry = overrides.get(name_key, {})
            for field in _FALLBACK_DYNAMIC_FIELDS:
                if p.get(field) is not None:
                    entry[field] = p[field]
            entry["_updatedAt"] = datetime.now().strftime("%Y-%m-%d")
            overrides[name_key] = entry
            saved += 1
        _save_fallback_overrides(overrides)
        print(f"  Self-heal: refreshed fallback_dynamic.json with {saved} player updates")

    # ---- PER-PLAYER ROUND VARIANCE ----
    # Two-tier: prefer BDL (1-2 seasons of /player_round_results, ~30+
    # rounds per player) over the local-history snapshot version (limited
    # to whatever weeks we've archived locally). BDL gives a much richer
    # sample; local is a graceful fallback when BDL is unavailable.
    bdl_variance = _compute_player_variance_from_bdl()
    local_variance = _compute_player_variance(history_dir) if not bdl_variance else {}
    # Merge — BDL takes precedence, local fills gaps
    variance_map = dict(local_variance)
    variance_map.update(bdl_variance)
    if variance_map:
        attached = 0
        bdl_count = 0
        for p in output["players"]:
            nm = normalize_name(p.get("name", ""))
            std = variance_map.get(nm)
            if std is not None:
                p["scoreStd"] = std
                attached += 1
                if nm in bdl_variance:
                    bdl_count += 1
        if attached:
            src = f"{bdl_count} from BDL, {attached - bdl_count} from local history"
            print(f"  Per-player variance: attached stddev to {attached} players ({src})")

    # ---- COURSE FIT v2 (regression scaffolding, inactive in predict) ----
    # Build data-driven fit scores from historical SG -> finish regression.
    # If the course has <5 completed events in archive we skip gracefully.
    v2_coverage = 0.0
    v2_attached = 0
    if resolved_course:
        v2_map, v2_events = compute_course_fit_v2(
            output["players"], history_dir, resolved_course
        )
        if v2_map:
            for p in output["players"]:
                score = v2_map.get(normalize_name(p.get("name", "")))
                if score is None:
                    continue
                fit_v2 = p.get("courseFitV2")
                if not isinstance(fit_v2, dict):
                    fit_v2 = {}
                fit_v2[resolved_course] = score
                p["courseFitV2"] = fit_v2
                if score > 0:
                    v2_attached += 1
            v2_coverage = round(v2_attached / max(len(output["players"]), 1), 3)
            print(
                f"  [COURSE FIT v2] Computed for {v2_attached} players at "
                f"{resolved_course} from {v2_events} weeks of history"
            )
        else:
            print(
                f"  [COURSE FIT v2] Skipped {resolved_course}: "
                f"only {v2_events} completed events in archive (need 5+)"
            )
    else:
        print("  [COURSE FIT v2] Skipped: no resolved course_key for current event")

    # ---- DATA QUALITY METADATA ----
    ce = output.get("currentEvent") or {}
    lb = ce.get("leaderboard") or []
    players_with_odds = sum(1 for p in output["players"] if p.get("odds"))
    players_with_form = sum(1 for p in output["players"] if p.get("recentForm"))
    output["dataQuality"] = {
        "leaderboardEntries": len(lb),
        "leaderboardSource": ce.get("leaderboardSource", "none"),
        "leaderboardHasScores": any((p.get("totalStrokes") or 0) > 0 for p in lb),
        "playersTotal": len(output["players"]),
        "playersWithOdds": players_with_odds,
        "playersWithForm": players_with_form,
        "oddsCoverage": round(players_with_odds / max(len(output["players"]), 1), 3),
        "tournamentStatus": ce.get("status", ""),
        "teeTimesTotal": len(output.get("teeTimes") or []),
        "teeTimesWithValues": sum(1 for t in (output.get("teeTimes") or []) if t.get("teeTime")),
        "threeBallGroups": len(output.get("threeBalls") or []),
        "threeBallEdges5pct": sum(
            1 for g in (output.get("threeBalls") or [])
            for p in g.get("players", [])
            if isinstance(p.get("ev"), (int, float)) and p["ev"] > 5
        ),
        "courseFitV2Coverage": v2_coverage,
        # Sanity-check: softmax-normalized win probabilities must sum
        # to ~1.0 across the field. Any future regression of the
        # calibration step will surface here as a number much larger
        # (legacy heuristic ~5.0+) or much smaller than 1.0.
        "modelProbSum": round(
            sum(float(p.get("modelWinProb") or 0) for p in output["players"]),
            4,
        ),
        # Coverage of each placement market (real book lines, post-filter).
        # If any drops to 0 unexpectedly mid-tournament-week, an alert fires.
        "propMarketCoverage": {
            m: len(output["propsByType"].get(m, {}) or {})
            for m in ("top5", "top10", "top20", "makeCut", "r1Leader")
        },
    }

    # ---- WRITE OUTPUT ----
    output_path = os.path.join(base_dir, OUTPUT_FILE)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    # ---- ARCHIVE ----
    archive_data(output, base_dir)

    # ---- ODDS HISTORY (line-movement snapshots) ----
    persist_odds_history(output, base_dir)

    # ---- CLV PROXY (closing-line value from existing history) ----
    # Zero new API calls — reads odds_history.json we just persisted to
    # compare current vs lookback odds for every player with a meaningful
    # model edge. The sharp's metric: % of model picks that beat the close.
    clv = compute_clv_proxy(output["players"], base_dir, lookback_hours=24)
    if clv:
        output["clvSummary"] = clv

    # ---- DISCORD ALERTS ----
    # Wrapped: alert path is non-critical. A bug here must NEVER block the
    # deploy — the data files have already been written and the website
    # update is the priority. Yesterday (May 7) we lost 5 deploys to a
    # KeyError in the alerter for exactly this reason.
    try:
        send_discord_alerts(output)
    except Exception as _alert_err:
        print(f"  [WARN] Discord alerter raised: {type(_alert_err).__name__}: {_alert_err}")
        import traceback as _tb
        _tb.print_exc()
        print(f"  [WARN] Continuing — data files already written.")

    file_size = os.path.getsize(output_path)
    print(f"\n{'=' * 60}")
    print(f"Pipeline complete!")
    print(f"Output: {output_path} ({file_size / 1024:.1f} KB)")
    print(f"Players: {len(output['players'])}")
    print(f"Current Event: {output['currentEvent']['name'] if output.get('currentEvent') else 'None'}")
    print(f"Weather: {'Yes' if weather_data else 'No'}")
    print(f"Odds: {'Yes (BDL)' if bdl_odds else 'No'}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}")

    return output


if __name__ == "__main__":
    run_pipeline()
