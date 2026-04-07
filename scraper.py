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
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

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

# Request timeout in seconds
TIMEOUT = 15

# Delay between requests to be respectful
REQUEST_DELAY = 2.0

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

        # Get leaderboard
        leaderboard = []
        for comp in competitions:
            for competitor in comp.get("competitors", []):
                athlete = competitor.get("athlete", {})
                stats = {}
                for s in competitor.get("statistics", []):
                    stats[s.get("name", "")] = s.get("displayValue", s.get("value", ""))

                entry = {
                    "name": athlete.get("displayName", ""),
                    "position": competitor.get("status", {}).get("position", {}).get("displayName", ""),
                    "score": competitor.get("score", ""),
                    "totalStrokes": safe_float(stats.get("totalStrokes", 0)),
                    "round1": safe_float(stats.get("round1", 0)),
                    "round2": safe_float(stats.get("round2", 0)),
                    "round3": safe_float(stats.get("round3", 0)),
                    "round4": safe_float(stats.get("round4", 0)),
                    "thru": competitor.get("status", {}).get("thru", ""),
                }
                leaderboard.append(entry)

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
    data = bdl_fetch("tournaments", {"season": "2026", "per_page": "50"})
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


def bdl_get_futures_odds(tournament_id):
    """Get outright winner futures odds from BDL (replaces The Odds API)."""
    print(f"[BDL] Fetching futures odds (tournament_id={tournament_id})...")
    odds = bdl_fetch_all("futures", {"tournament_ids[]": str(tournament_id)})
    if not odds:
        return {}

    # Group by player: {player_name: {vendor: american_odds}}
    odds_map = {}
    for o in odds:
        if o.get("market_type") != "tournament_winner":
            continue
        player = o.get("player", {})
        name = player.get("display_name", "")
        vendor = o.get("vendor", "")
        american = o.get("american_odds", 0)
        if not name:
            continue

        # Shorten vendor names
        short = {"fanduel": "fd", "draftkings": "dk", "betmgm": "mgm", "caesars": "czr",
                 "pointsbet": "pb", "bet365": "365"}.get(vendor, vendor[:3])

        if name not in odds_map:
            odds_map[name] = {}
        odds_map[name][short] = f"+{american}" if american > 0 else str(american)

    print(f"  Got odds for {len(odds_map)} players")
    return odds_map


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


# ============================================================
# FALLBACK PLAYER DATA
# ============================================================
# If scraping fails, we use this curated dataset based on real stats.
# Updated manually as a safety net.

def get_fallback_players():
    """Return hardcoded player data based on real 2024-2025 Tour stats.
    50 players with full curated data including course fit, tendencies, and betting notes.
    """
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
        {"id":12,"name":"Max Homa","rank":12,"sgTotal":1.05,"sgOtt":0.42,"sgApp":0.38,"sgArg":0.15,"sgPutt":0.10,"birdieAvg":4.2,"bogeyAvg":2.3,"scoringAvg":70.3,"gir":67.0,"fairways":64.5,"scramble":61.0,"proxAvg":34.5,"missDir":"left","flight":"high_draw","courseFit":{"augusta":65,"tpc_sawgrass":70,"pebble":78,"torrey_south":82,"riviera":88,"valhalla":68,"pinehurst_2":65,"royal_troon":62,"quail_hollow":72,"east_lake":70,"bay_hill":68,"harbour_town":72,"colonial":75,"memorial":72,"tpc_scottsdale":72},"notes":"Solid all-around. Riviera and Torrey specialist. Reliable at home courses."},
        {"id":13,"name":"Shane Lowry","rank":13,"sgTotal":1.02,"sgOtt":0.30,"sgApp":0.42,"sgArg":0.18,"sgPutt":0.12,"birdieAvg":4.1,"bogeyAvg":2.1,"scoringAvg":70.2,"gir":67.5,"fairways":65.5,"scramble":63.0,"proxAvg":34.0,"missDir":"left","flight":"low_draw","courseFit":{"augusta":68,"tpc_sawgrass":78,"pebble":82,"torrey_south":72,"riviera":75,"valhalla":82,"pinehurst_2":80,"royal_troon":95,"quail_hollow":72,"east_lake":72,"bay_hill":72,"harbour_town":82,"colonial":78,"memorial":72,"tpc_scottsdale":68},"notes":"Links specialist. Open champion. Low ball flight dominates in wind. Best in coastal/windy conditions."},
        {"id":14,"name":"Sungjae Im","rank":14,"sgTotal":0.95,"sgOtt":0.28,"sgApp":0.40,"sgArg":0.15,"sgPutt":0.12,"birdieAvg":4.0,"bogeyAvg":2.0,"scoringAvg":70.4,"gir":68.5,"fairways":69.0,"scramble":61.5,"proxAvg":35.0,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":72,"tpc_sawgrass":75,"pebble":70,"torrey_south":70,"riviera":72,"valhalla":72,"pinehurst_2":70,"royal_troon":68,"quail_hollow":75,"east_lake":72,"bay_hill":72,"harbour_town":75,"colonial":78,"memorial":75,"tpc_scottsdale":75},"notes":"Iron man — plays every week. Rarely misses cuts. Great for MC and top 20 props."},
        {"id":15,"name":"Sam Burns","rank":15,"sgTotal":0.92,"sgOtt":0.45,"sgApp":0.35,"sgArg":0.08,"sgPutt":0.04,"birdieAvg":4.5,"bogeyAvg":2.4,"scoringAvg":70.3,"gir":67.0,"fairways":60.0,"scramble":58.5,"proxAvg":34.5,"missDir":"right","flight":"high_fade","courseFit":{"augusta":72,"tpc_sawgrass":78,"pebble":70,"torrey_south":75,"riviera":72,"valhalla":78,"pinehurst_2":72,"royal_troon":65,"quail_hollow":78,"east_lake":78,"bay_hill":75,"harbour_town":72,"colonial":75,"memorial":78,"tpc_scottsdale":82},"notes":"Talented ball-striker with streaky putting. When putter is hot, can contend anywhere."},
        {"id":16,"name":"Tony Finau","rank":16,"sgTotal":0.90,"sgOtt":0.55,"sgApp":0.30,"sgArg":0.05,"sgPutt":0.00,"birdieAvg":4.4,"bogeyAvg":2.3,"scoringAvg":70.3,"gir":67.5,"fairways":60.5,"scramble":58.0,"proxAvg":34.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":72,"tpc_sawgrass":70,"pebble":72,"torrey_south":78,"riviera":72,"valhalla":80,"pinehurst_2":72,"royal_troon":68,"quail_hollow":78,"east_lake":75,"bay_hill":75,"harbour_town":65,"colonial":68,"memorial":75,"tpc_scottsdale":82},"notes":"Elite power but inconsistent approach and flat-stick. Best at bombers courses. Fade candidate at short tracks."},
        {"id":17,"name":"Keegan Bradley","rank":17,"sgTotal":0.88,"sgOtt":0.35,"sgApp":0.32,"sgArg":0.12,"sgPutt":0.09,"birdieAvg":4.1,"bogeyAvg":2.2,"scoringAvg":70.4,"gir":67.5,"fairways":65.0,"scramble":60.5,"proxAvg":34.2,"missDir":"left","flight":"high_draw","courseFit":{"augusta":70,"tpc_sawgrass":72,"pebble":68,"torrey_south":72,"riviera":72,"valhalla":75,"pinehurst_2":72,"royal_troon":68,"quail_hollow":75,"east_lake":72,"bay_hill":72,"harbour_town":75,"colonial":78,"memorial":75,"tpc_scottsdale":72},"notes":"Steady veteran. Consistent without being flashy. Good MC and top-20 candidate."},
        {"id":18,"name":"Justin Thomas","rank":18,"sgTotal":0.85,"sgOtt":0.48,"sgApp":0.42,"sgArg":0.05,"sgPutt":-0.10,"birdieAvg":4.6,"bogeyAvg":2.6,"scoringAvg":70.5,"gir":68.0,"fairways":60.0,"scramble":56.0,"proxAvg":33.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":80,"tpc_sawgrass":78,"pebble":72,"torrey_south":75,"riviera":78,"valhalla":88,"pinehurst_2":78,"royal_troon":72,"quail_hollow":85,"east_lake":82,"bay_hill":78,"harbour_town":68,"colonial":72,"memorial":82,"tpc_scottsdale":80},"notes":"Former world #1 in a slump. Ball-striking still elite but putter has gone cold. High ceiling, low floor."},
        {"id":19,"name":"Jason Day","rank":19,"sgTotal":0.82,"sgOtt":0.40,"sgApp":0.28,"sgArg":0.08,"sgPutt":0.06,"birdieAvg":4.2,"bogeyAvg":2.3,"scoringAvg":70.5,"gir":66.5,"fairways":62.0,"scramble":60.0,"proxAvg":34.5,"missDir":"right","flight":"high_fade","courseFit":{"augusta":80,"tpc_sawgrass":82,"pebble":72,"torrey_south":78,"riviera":72,"valhalla":78,"pinehurst_2":72,"royal_troon":68,"quail_hollow":82,"east_lake":78,"bay_hill":85,"harbour_town":72,"colonial":72,"memorial":78,"tpc_scottsdale":78},"notes":"Resurgent veteran. Short game wizard when healthy. Bay Hill specialist. Good at TPC Sawgrass."},
        {"id":20,"name":"Russell Henley","rank":20,"sgTotal":0.80,"sgOtt":0.22,"sgApp":0.38,"sgArg":0.12,"sgPutt":0.08,"birdieAvg":3.9,"bogeyAvg":2.0,"scoringAvg":70.4,"gir":68.0,"fairways":68.5,"scramble":62.0,"proxAvg":34.0,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":68,"tpc_sawgrass":78,"pebble":75,"torrey_south":72,"riviera":75,"valhalla":72,"pinehurst_2":78,"royal_troon":75,"quail_hollow":72,"east_lake":75,"bay_hill":72,"harbour_town":82,"colonial":85,"memorial":75,"tpc_scottsdale":72},"notes":"Extremely accurate. Low bogey rate. Great at precision courses like Harbour Town and Colonial."},
        {"id":21,"name":"Brian Harman","rank":21,"sgTotal":0.78,"sgOtt":0.15,"sgApp":0.30,"sgArg":0.20,"sgPutt":0.13,"birdieAvg":3.8,"bogeyAvg":1.9,"scoringAvg":70.5,"gir":66.5,"fairways":70.5,"scramble":65.0,"proxAvg":35.5,"missDir":"left","flight":"low_draw","courseFit":{"augusta":62,"tpc_sawgrass":75,"pebble":80,"torrey_south":68,"riviera":72,"valhalla":68,"pinehurst_2":78,"royal_troon":90,"quail_hollow":68,"east_lake":72,"bay_hill":68,"harbour_town":88,"colonial":85,"memorial":72,"tpc_scottsdale":68},"notes":"Open champion. Lefty with great short game. Not a power player. Thrives on accuracy courses and links."},
        {"id":22,"name":"Cameron Young","rank":22,"sgTotal":0.75,"sgOtt":0.65,"sgApp":0.18,"sgArg":-0.02,"sgPutt":-0.06,"birdieAvg":4.5,"bogeyAvg":2.6,"scoringAvg":70.6,"gir":66.0,"fairways":56.0,"scramble":55.0,"proxAvg":34.8,"missDir":"left","flight":"high_draw","courseFit":{"augusta":72,"tpc_sawgrass":68,"pebble":65,"torrey_south":75,"riviera":72,"valhalla":78,"pinehurst_2":68,"royal_troon":65,"quail_hollow":78,"east_lake":72,"bay_hill":72,"harbour_town":58,"colonial":62,"memorial":72,"tpc_scottsdale":75},"notes":"Huge power but accuracy issues. Multiple runner-up finishes. Volatile scorer — birdie overs at long courses."},
        {"id":23,"name":"Denny McCarthy","rank":23,"sgTotal":0.72,"sgOtt":0.05,"sgApp":0.15,"sgArg":0.18,"sgPutt":0.34,"birdieAvg":3.6,"bogeyAvg":1.8,"scoringAvg":70.6,"gir":65.0,"fairways":70.0,"scramble":68.0,"proxAvg":36.0,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":58,"tpc_sawgrass":72,"pebble":75,"torrey_south":65,"riviera":72,"valhalla":62,"pinehurst_2":75,"royal_troon":72,"quail_hollow":65,"east_lake":68,"bay_hill":65,"harbour_town":85,"colonial":88,"memorial":70,"tpc_scottsdale":72},"notes":"Best putter on Tour. Short off the tee. Needs accuracy courses where length doesn't matter. Elite bogey under."},
        {"id":24,"name":"Byeong Hun An","rank":24,"sgTotal":0.70,"sgOtt":0.32,"sgApp":0.28,"sgArg":0.06,"sgPutt":0.04,"birdieAvg":4.0,"bogeyAvg":2.2,"scoringAvg":70.6,"gir":67.0,"fairways":64.0,"scramble":59.0,"proxAvg":34.5,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":68,"tpc_sawgrass":72,"pebble":70,"torrey_south":72,"riviera":72,"valhalla":72,"pinehurst_2":70,"royal_troon":68,"quail_hollow":72,"east_lake":72,"bay_hill":72,"harbour_town":72,"colonial":75,"memorial":72,"tpc_scottsdale":72},"notes":"Consistent ball-striker. No elite category but no major weakness. Reliable for top-20 finishes."},
        {"id":25,"name":"Adam Scott","rank":25,"sgTotal":0.68,"sgOtt":0.35,"sgApp":0.25,"sgArg":0.05,"sgPutt":0.03,"birdieAvg":4.0,"bogeyAvg":2.2,"scoringAvg":70.6,"gir":67.5,"fairways":63.0,"scramble":59.5,"proxAvg":34.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":85,"tpc_sawgrass":78,"pebble":72,"torrey_south":75,"riviera":82,"valhalla":75,"pinehurst_2":72,"royal_troon":72,"quail_hollow":75,"east_lake":78,"bay_hill":78,"harbour_town":72,"colonial":72,"memorial":78,"tpc_scottsdale":72},"notes":"Veteran with elite swing. Augusta specialist. Still competitive but ceiling has lowered."},
        {"id":26,"name":"Aaron Rai","rank":26,"sgTotal":0.65,"sgOtt":0.18,"sgApp":0.32,"sgArg":0.10,"sgPutt":0.05,"birdieAvg":3.9,"bogeyAvg":2.1,"scoringAvg":70.7,"gir":68.0,"fairways":67.0,"scramble":61.0,"proxAvg":34.5,"missDir":"right","flight":"low_fade","courseFit":{"augusta":62,"tpc_sawgrass":75,"pebble":78,"torrey_south":70,"riviera":75,"valhalla":68,"pinehurst_2":78,"royal_troon":82,"quail_hollow":68,"east_lake":70,"bay_hill":68,"harbour_town":82,"colonial":82,"memorial":72,"tpc_scottsdale":70},"notes":"Precise iron player. Accuracy over power. Good at courses that demand shotmaking. Steady for props."},
        {"id":27,"name":"Billy Horschel","rank":27,"sgTotal":0.62,"sgOtt":0.28,"sgApp":0.22,"sgArg":0.08,"sgPutt":0.04,"birdieAvg":3.9,"bogeyAvg":2.2,"scoringAvg":70.7,"gir":66.5,"fairways":65.0,"scramble":60.0,"proxAvg":35.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":62,"tpc_sawgrass":82,"pebble":72,"torrey_south":68,"riviera":72,"valhalla":72,"pinehurst_2":70,"royal_troon":68,"quail_hollow":72,"east_lake":88,"bay_hill":78,"harbour_town":72,"colonial":72,"memorial":72,"tpc_scottsdale":72},"notes":"East Lake specialist. TPC Sawgrass has history. Emotional player — performs well when locked in."},
        {"id":28,"name":"Tom Kim","rank":28,"sgTotal":0.60,"sgOtt":0.30,"sgApp":0.22,"sgArg":0.05,"sgPutt":0.03,"birdieAvg":4.2,"bogeyAvg":2.4,"scoringAvg":70.7,"gir":66.0,"fairways":62.0,"scramble":57.5,"proxAvg":34.5,"missDir":"left","flight":"high_draw","courseFit":{"augusta":68,"tpc_sawgrass":72,"pebble":68,"torrey_south":72,"riviera":72,"valhalla":72,"pinehurst_2":68,"royal_troon":65,"quail_hollow":78,"east_lake":75,"bay_hill":72,"harbour_town":68,"colonial":70,"memorial":72,"tpc_scottsdale":78},"notes":"Young talent with flair. Aggressive player with high birdie ceiling. Inconsistent but fun for props."},
        {"id":29,"name":"Corey Conners","rank":29,"sgTotal":0.58,"sgOtt":0.20,"sgApp":0.45,"sgArg":0.02,"sgPutt":-0.09,"birdieAvg":3.8,"bogeyAvg":2.1,"scoringAvg":70.7,"gir":70.0,"fairways":68.0,"scramble":57.0,"proxAvg":33.0,"missDir":"right","flight":"low_fade","courseFit":{"augusta":75,"tpc_sawgrass":78,"pebble":78,"torrey_south":72,"riviera":78,"valhalla":72,"pinehurst_2":80,"royal_troon":78,"quail_hollow":72,"east_lake":72,"bay_hill":72,"harbour_town":80,"colonial":82,"memorial":78,"tpc_scottsdale":72},"notes":"Elite iron player but can't putt. Highest GIR with lowest conversion. Great approach stats, fade putting props."},
        {"id":30,"name":"Sepp Straka","rank":30,"sgTotal":0.55,"sgOtt":0.30,"sgApp":0.18,"sgArg":0.05,"sgPutt":0.02,"birdieAvg":4.0,"bogeyAvg":2.2,"scoringAvg":70.8,"gir":67.0,"fairways":63.0,"scramble":59.0,"proxAvg":34.5,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":68,"tpc_sawgrass":72,"pebble":70,"torrey_south":72,"riviera":72,"valhalla":72,"pinehurst_2":70,"royal_troon":68,"quail_hollow":75,"east_lake":72,"bay_hill":72,"harbour_town":72,"colonial":72,"memorial":72,"tpc_scottsdale":72},"notes":"Steady ball-striker from Austria. No standout category. Reliable for MC and top-20 at mid-strength events."},
        {"id":31,"name":"Chris Kirk","rank":31,"sgTotal":0.52,"sgOtt":0.22,"sgApp":0.18,"sgArg":0.08,"sgPutt":0.04,"birdieAvg":3.8,"bogeyAvg":2.1,"scoringAvg":70.8,"gir":66.5,"fairways":66.0,"scramble":61.0,"proxAvg":35.0,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":65,"tpc_sawgrass":72,"pebble":72,"torrey_south":68,"riviera":72,"valhalla":72,"pinehurst_2":72,"royal_troon":70,"quail_hollow":72,"east_lake":72,"bay_hill":72,"harbour_town":78,"colonial":78,"memorial":72,"tpc_scottsdale":72},"notes":"Comeback story. Steady and consistent. Good at shorter accuracy courses. Low variance player."},
        {"id":32,"name":"Taylor Pendrith","rank":32,"sgTotal":0.50,"sgOtt":0.55,"sgApp":0.10,"sgArg":-0.05,"sgPutt":-0.10,"birdieAvg":4.3,"bogeyAvg":2.5,"scoringAvg":70.9,"gir":65.5,"fairways":57.0,"scramble":55.0,"proxAvg":35.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":68,"tpc_sawgrass":65,"pebble":62,"torrey_south":75,"riviera":68,"valhalla":78,"pinehurst_2":65,"royal_troon":60,"quail_hollow":78,"east_lake":68,"bay_hill":72,"harbour_town":58,"colonial":60,"memorial":68,"tpc_scottsdale":75},"notes":"Bomber who relies on length. Weak short game. Best at wide-open bombers courses. Fade at precision tracks."},
        {"id":33,"name":"Matt Fitzpatrick","rank":33,"sgTotal":0.48,"sgOtt":0.10,"sgApp":0.30,"sgArg":0.05,"sgPutt":0.03,"birdieAvg":3.7,"bogeyAvg":2.1,"scoringAvg":70.9,"gir":67.5,"fairways":69.0,"scramble":60.0,"proxAvg":34.0,"missDir":"left","flight":"low_draw","courseFit":{"augusta":65,"tpc_sawgrass":78,"pebble":78,"torrey_south":68,"riviera":78,"valhalla":68,"pinehurst_2":85,"royal_troon":80,"quail_hollow":68,"east_lake":72,"bay_hill":68,"harbour_town":82,"colonial":85,"memorial":75,"tpc_scottsdale":68},"notes":"US Open champion. Precision player. Not long but very accurate. Thrives on tight, demanding courses."},
        {"id":34,"name":"Robert MacIntyre","rank":34,"sgTotal":0.45,"sgOtt":0.32,"sgApp":0.15,"sgArg":0.02,"sgPutt":-0.04,"birdieAvg":4.0,"bogeyAvg":2.3,"scoringAvg":71.0,"gir":66.0,"fairways":62.0,"scramble":58.0,"proxAvg":35.0,"missDir":"left","flight":"low_draw","courseFit":{"augusta":62,"tpc_sawgrass":68,"pebble":72,"torrey_south":68,"riviera":72,"valhalla":72,"pinehurst_2":72,"royal_troon":85,"quail_hollow":72,"east_lake":68,"bay_hill":68,"harbour_town":75,"colonial":72,"memorial":68,"tpc_scottsdale":68},"notes":"Scottish lefty with links pedigree. Gritty competitor. Good in wind. Putter can go cold."},
        {"id":35,"name":"Akshay Bhatia","rank":35,"sgTotal":0.42,"sgOtt":0.38,"sgApp":0.15,"sgArg":0.02,"sgPutt":-0.13,"birdieAvg":4.3,"bogeyAvg":2.5,"scoringAvg":71.0,"gir":66.0,"fairways":58.0,"scramble":56.0,"proxAvg":35.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":68,"tpc_sawgrass":65,"pebble":62,"torrey_south":72,"riviera":68,"valhalla":72,"pinehurst_2":62,"royal_troon":58,"quail_hollow":72,"east_lake":68,"bay_hill":68,"harbour_town":58,"colonial":62,"memorial":68,"tpc_scottsdale":75},"notes":"Young lefty talent. Aggressive. Putting holds him back. High birdie ceiling with high floor."},
        {"id":36,"name":"Min Woo Lee","rank":36,"sgTotal":0.40,"sgOtt":0.42,"sgApp":0.12,"sgArg":0.00,"sgPutt":-0.14,"birdieAvg":4.2,"bogeyAvg":2.4,"scoringAvg":71.0,"gir":66.0,"fairways":59.0,"scramble":57.0,"proxAvg":35.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":65,"tpc_sawgrass":68,"pebble":68,"torrey_south":72,"riviera":70,"valhalla":72,"pinehurst_2":65,"royal_troon":72,"quail_hollow":72,"east_lake":68,"bay_hill":68,"harbour_town":65,"colonial":68,"memorial":68,"tpc_scottsdale":72},"notes":"Australian talent with power. Sister Minjee is LPGA star. Inconsistent but can go low. Good for T20 at mid-tier."},
        {"id":37,"name":"Nicolai Hojgaard","rank":37,"sgTotal":0.38,"sgOtt":0.35,"sgApp":0.12,"sgArg":0.00,"sgPutt":-0.09,"birdieAvg":4.1,"bogeyAvg":2.3,"scoringAvg":71.1,"gir":66.0,"fairways":61.0,"scramble":57.5,"proxAvg":35.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":65,"tpc_sawgrass":68,"pebble":65,"torrey_south":70,"riviera":68,"valhalla":72,"pinehurst_2":65,"royal_troon":72,"quail_hollow":72,"east_lake":68,"bay_hill":68,"harbour_town":65,"colonial":68,"memorial":68,"tpc_scottsdale":72},"notes":"Danish twin (brother Rasmus also on Tour). Athletic and powerful. Still adapting to PGA Tour courses."},
        {"id":38,"name":"Davis Thompson","rank":38,"sgTotal":0.35,"sgOtt":0.25,"sgApp":0.12,"sgArg":0.02,"sgPutt":-0.04,"birdieAvg":3.9,"bogeyAvg":2.2,"scoringAvg":71.1,"gir":66.5,"fairways":64.0,"scramble":58.5,"proxAvg":35.0,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":62,"tpc_sawgrass":72,"pebble":68,"torrey_south":68,"riviera":68,"valhalla":68,"pinehurst_2":68,"royal_troon":65,"quail_hollow":72,"east_lake":72,"bay_hill":68,"harbour_town":72,"colonial":75,"memorial":72,"tpc_scottsdale":72},"notes":"Young steady player. Won his first Tour event. No standout skill but consistent. Good for MC props."},
        {"id":39,"name":"Austin Eckroat","rank":39,"sgTotal":0.33,"sgOtt":0.30,"sgApp":0.08,"sgArg":0.00,"sgPutt":-0.05,"birdieAvg":4.0,"bogeyAvg":2.3,"scoringAvg":71.2,"gir":66.0,"fairways":62.0,"scramble":57.0,"proxAvg":35.5,"missDir":"right","flight":"high_fade","courseFit":{"augusta":62,"tpc_sawgrass":65,"pebble":65,"torrey_south":72,"riviera":65,"valhalla":72,"pinehurst_2":62,"royal_troon":60,"quail_hollow":72,"east_lake":65,"bay_hill":68,"harbour_town":62,"colonial":65,"memorial":68,"tpc_scottsdale":72},"notes":"Oklahoma product with power. Still developing consistency. Better at longer courses. Thin field value play."},
        {"id":40,"name":"Harris English","rank":40,"sgTotal":0.30,"sgOtt":0.22,"sgApp":0.12,"sgArg":0.02,"sgPutt":-0.06,"birdieAvg":3.8,"bogeyAvg":2.2,"scoringAvg":71.2,"gir":66.0,"fairways":65.0,"scramble":59.0,"proxAvg":35.5,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":65,"tpc_sawgrass":72,"pebble":70,"torrey_south":72,"riviera":68,"valhalla":72,"pinehurst_2":68,"royal_troon":65,"quail_hollow":72,"east_lake":78,"bay_hill":72,"harbour_town":75,"colonial":72,"memorial":72,"tpc_scottsdale":72},"notes":"Steady veteran. Good all-around game without elite skill. East Lake familiarity. Reliable for MC at full fields."},
        {"id":41,"name":"Jake Knapp","rank":41,"sgTotal":0.28,"sgOtt":0.58,"sgApp":0.00,"sgArg":-0.12,"sgPutt":-0.18,"birdieAvg":4.2,"bogeyAvg":2.7,"scoringAvg":71.3,"gir":64.0,"fairways":54.0,"scramble":52.0,"proxAvg":36.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":60,"tpc_sawgrass":58,"pebble":55,"torrey_south":68,"riviera":62,"valhalla":72,"pinehurst_2":55,"royal_troon":50,"quail_hollow":72,"east_lake":60,"bay_hill":65,"harbour_town":48,"colonial":50,"memorial":62,"tpc_scottsdale":72},"notes":"Longest hitter on Tour. Accuracy is a problem. Short game is Tour-worst tier. Only play at bomber-friendly tracks."},
        {"id":42,"name":"Keith Mitchell","rank":42,"sgTotal":0.25,"sgOtt":0.48,"sgApp":0.02,"sgArg":-0.10,"sgPutt":-0.15,"birdieAvg":4.1,"bogeyAvg":2.6,"scoringAvg":71.3,"gir":65.0,"fairways":56.0,"scramble":53.0,"proxAvg":35.5,"missDir":"right","flight":"high_fade","courseFit":{"augusta":62,"tpc_sawgrass":60,"pebble":58,"torrey_south":72,"riviera":62,"valhalla":75,"pinehurst_2":58,"royal_troon":55,"quail_hollow":75,"east_lake":62,"bay_hill":68,"harbour_town":52,"colonial":55,"memorial":62,"tpc_scottsdale":72},"notes":"Power player with accuracy issues. Thrives at wide courses. Short game liability."},
        {"id":43,"name":"Stephan Jaeger","rank":43,"sgTotal":0.22,"sgOtt":0.15,"sgApp":0.10,"sgArg":0.02,"sgPutt":-0.05,"birdieAvg":3.7,"bogeyAvg":2.2,"scoringAvg":71.3,"gir":66.0,"fairways":65.0,"scramble":58.0,"proxAvg":35.5,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":58,"tpc_sawgrass":68,"pebble":68,"torrey_south":65,"riviera":68,"valhalla":65,"pinehurst_2":68,"royal_troon":65,"quail_hollow":68,"east_lake":65,"bay_hill":65,"harbour_town":72,"colonial":72,"memorial":68,"tpc_scottsdale":68},"notes":"German journeyman having a solid stretch. Shot 58 on mini-tour. Reliable for MC at weaker fields."},
        {"id":44,"name":"Eric Cole","rank":44,"sgTotal":0.20,"sgOtt":0.42,"sgApp":0.00,"sgArg":-0.08,"sgPutt":-0.14,"birdieAvg":4.0,"bogeyAvg":2.5,"scoringAvg":71.4,"gir":65.0,"fairways":57.0,"scramble":54.0,"proxAvg":35.5,"missDir":"right","flight":"high_fade","courseFit":{"augusta":62,"tpc_sawgrass":60,"pebble":58,"torrey_south":68,"riviera":62,"valhalla":72,"pinehurst_2":58,"royal_troon":55,"quail_hollow":72,"east_lake":62,"bay_hill":65,"harbour_town":55,"colonial":58,"memorial":62,"tpc_scottsdale":68},"notes":"Big hitter with inconsistent short game. Best at wide-open courses. Avoid at precision tracks."},
        {"id":45,"name":"Christiaan Bezuidenhout","rank":45,"sgTotal":0.18,"sgOtt":0.15,"sgApp":0.12,"sgArg":0.00,"sgPutt":-0.09,"birdieAvg":3.7,"bogeyAvg":2.2,"scoringAvg":71.4,"gir":66.5,"fairways":66.0,"scramble":58.0,"proxAvg":35.0,"missDir":"left","flight":"low_draw","courseFit":{"augusta":62,"tpc_sawgrass":72,"pebble":72,"torrey_south":65,"riviera":72,"valhalla":65,"pinehurst_2":72,"royal_troon":75,"quail_hollow":65,"east_lake":68,"bay_hill":65,"harbour_town":75,"colonial":78,"memorial":68,"tpc_scottsdale":65},"notes":"South African with smooth swing. Accuracy player. Good in wind. Putting holds him back from contending."},
        {"id":46,"name":"Jordan Spieth","rank":46,"sgTotal":0.15,"sgOtt":0.10,"sgApp":0.08,"sgArg":0.05,"sgPutt":-0.08,"birdieAvg":3.8,"bogeyAvg":2.4,"scoringAvg":71.5,"gir":65.0,"fairways":60.0,"scramble":62.0,"proxAvg":35.5,"missDir":"left","flight":"high_draw","courseFit":{"augusta":88,"tpc_sawgrass":72,"pebble":82,"torrey_south":72,"riviera":72,"valhalla":75,"pinehurst_2":72,"royal_troon":82,"quail_hollow":72,"east_lake":82,"bay_hill":72,"harbour_town":72,"colonial":85,"memorial":72,"tpc_scottsdale":78},"notes":"3x major champ in a slump. Course history still matters — elite at Augusta, Colonial, Pebble. Buy low candidate."},
        {"id":47,"name":"Michael Thorbjornsen","rank":47,"sgTotal":0.12,"sgOtt":0.25,"sgApp":0.05,"sgArg":-0.05,"sgPutt":-0.13,"birdieAvg":3.9,"bogeyAvg":2.4,"scoringAvg":71.5,"gir":65.5,"fairways":61.0,"scramble":55.0,"proxAvg":35.5,"missDir":"right","flight":"high_fade","courseFit":{"augusta":58,"tpc_sawgrass":62,"pebble":60,"torrey_south":65,"riviera":62,"valhalla":68,"pinehurst_2":60,"royal_troon":58,"quail_hollow":68,"east_lake":62,"bay_hill":65,"harbour_town":58,"colonial":60,"memorial":65,"tpc_scottsdale":68},"notes":"Promising rookie from Stanford. Athletic and long. Raw but talented. Watch for breakout at long courses."},
        {"id":48,"name":"Nick Dunlap","rank":48,"sgTotal":0.10,"sgOtt":0.28,"sgApp":0.02,"sgArg":-0.08,"sgPutt":-0.12,"birdieAvg":4.0,"bogeyAvg":2.5,"scoringAvg":71.5,"gir":65.0,"fairways":60.0,"scramble":54.0,"proxAvg":36.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":58,"tpc_sawgrass":60,"pebble":58,"torrey_south":65,"riviera":62,"valhalla":68,"pinehurst_2":58,"royal_troon":55,"quail_hollow":68,"east_lake":62,"bay_hill":62,"harbour_town":55,"colonial":58,"memorial":62,"tpc_scottsdale":72},"notes":"Won as amateur on Tour. Young talent still developing. High variance — DFS dart throw at big courses."},
        {"id":49,"name":"Ben Griffin","rank":49,"sgTotal":0.08,"sgOtt":0.18,"sgApp":0.00,"sgArg":-0.02,"sgPutt":-0.08,"birdieAvg":3.7,"bogeyAvg":2.3,"scoringAvg":71.5,"gir":65.5,"fairways":63.0,"scramble":57.0,"proxAvg":35.5,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":55,"tpc_sawgrass":62,"pebble":62,"torrey_south":62,"riviera":62,"valhalla":65,"pinehurst_2":62,"royal_troon":60,"quail_hollow":65,"east_lake":62,"bay_hill":62,"harbour_town":65,"colonial":68,"memorial":62,"tpc_scottsdale":65},"notes":"Steady mid-tier player. No outstanding skill. MC candidate at weaker fields. Fade at marquee events."},
        {"id":50,"name":"Maverick McNealy","rank":50,"sgTotal":0.05,"sgOtt":0.15,"sgApp":0.00,"sgArg":-0.02,"sgPutt":-0.08,"birdieAvg":3.6,"bogeyAvg":2.3,"scoringAvg":71.6,"gir":65.0,"fairways":64.0,"scramble":57.0,"proxAvg":36.0,"missDir":"neutral","flight":"neutral","courseFit":{"augusta":55,"tpc_sawgrass":62,"pebble":68,"torrey_south":68,"riviera":65,"valhalla":62,"pinehurst_2":62,"royal_troon":60,"quail_hollow":62,"east_lake":62,"bay_hill":62,"harbour_town":65,"colonial":68,"memorial":62,"tpc_scottsdale":68},"augustaHistory":{"appearances":1,"bestFinish":40,"cuts":0,"top10":0,"avgScore":75.0},"notes":"Stanford product. West Coast familiarity helps. Pebble and Torrey specialist. Thin field MC candidate."},
        # ---- PAST MASTERS CHAMPIONS (still active/invited) ----
        {"id":51,"name":"Adam Scott","rank":52,"sgTotal":0.55,"sgOtt":0.28,"sgApp":0.22,"sgArg":0.05,"sgPutt":0.00,"birdieAvg":4.1,"bogeyAvg":2.0,"scoringAvg":70.5,"gir":68.0,"fairways":64.0,"scramble":60.0,"proxAvg":33.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":85,"tpc_sawgrass":72,"pebble":80,"torrey_south":72,"riviera":78,"valhalla":75,"pinehurst_2":72,"royal_troon":78,"quail_hollow":75,"east_lake":80,"bay_hill":72,"harbour_town":72,"colonial":72,"memorial":78,"tpc_scottsdale":72},"augustaHistory":{"appearances":23,"bestFinish":1,"cuts":20,"top10":4,"avgScore":71.4},"notes":"2013 champion. Silky iron play and elite putter. Augusta suits his draw. Perennial top-10 threat. Age 45 but still sharp."},
        {"id":52,"name":"Sergio Garcia","rank":75,"sgTotal":0.25,"sgOtt":0.30,"sgApp":0.10,"sgArg":0.05,"sgPutt":-0.20,"birdieAvg":3.9,"bogeyAvg":2.2,"scoringAvg":71.2,"gir":66.0,"fairways":62.0,"scramble":59.0,"proxAvg":34.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":80,"tpc_sawgrass":68,"pebble":72,"torrey_south":68,"riviera":72,"valhalla":72,"pinehurst_2":70,"royal_troon":75,"quail_hollow":68,"east_lake":72,"bay_hill":68,"harbour_town":68,"colonial":68,"memorial":70,"tpc_scottsdale":68},"augustaHistory":{"appearances":24,"bestFinish":1,"cuts":21,"top10":6,"avgScore":71.6},"notes":"2017 champion. One of Augusta's great history makers — 23 prior top-10s. Putter remains the weakness. Respect the track record."},
        {"id":53,"name":"Danny Willett","rank":88,"sgTotal":0.10,"sgOtt":0.15,"sgApp":0.05,"sgArg":0.00,"sgPutt":-0.10,"birdieAvg":3.7,"bogeyAvg":2.2,"scoringAvg":71.6,"gir":65.0,"fairways":63.0,"scramble":57.0,"proxAvg":35.0,"missDir":"left","flight":"mid_draw","courseFit":{"augusta":72,"tpc_sawgrass":65,"pebble":68,"torrey_south":65,"riviera":68,"valhalla":68,"pinehurst_2":65,"royal_troon":72,"quail_hollow":68,"east_lake":65,"bay_hill":65,"harbour_town":65,"colonial":65,"memorial":65,"tpc_scottsdale":65},"augustaHistory":{"appearances":8,"bestFinish":1,"cuts":5,"top10":1,"avgScore":72.1},"notes":"2016 champion in stunning Jordan Spieth collapse. Inconsistent since. Pedigree at Augusta — can't fully fade the champion."},
        {"id":54,"name":"Phil Mickelson","rank":200,"sgTotal":-0.50,"sgOtt":0.10,"sgApp":-0.30,"sgArg":0.20,"sgPutt":-0.50,"birdieAvg":3.5,"bogeyAvg":3.0,"scoringAvg":73.0,"gir":62.0,"fairways":52.0,"scramble":62.0,"proxAvg":37.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":82,"tpc_sawgrass":65,"pebble":80,"torrey_south":68,"riviera":75,"valhalla":70,"pinehurst_2":68,"royal_troon":72,"quail_hollow":65,"east_lake":68,"bay_hill":68,"harbour_town":65,"colonial":68,"memorial":68,"tpc_scottsdale":72},"augustaHistory":{"appearances":30,"bestFinish":1,"cuts":26,"top10":9,"avgScore":71.8},"notes":"3x Masters champion (2004, 2006, 2010). Physically declining but Augusta knowledge is unmatched. Career MC prop only."},
        {"id":55,"name":"Bubba Watson","rank":250,"sgTotal":-0.80,"sgOtt":0.30,"sgApp":-0.60,"sgArg":-0.30,"sgPutt":-0.20,"birdieAvg":3.2,"bogeyAvg":3.2,"scoringAvg":73.5,"gir":60.0,"fairways":55.0,"scramble":55.0,"proxAvg":38.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":78,"tpc_sawgrass":60,"pebble":65,"torrey_south":60,"riviera":65,"valhalla":68,"pinehurst_2":62,"royal_troon":60,"quail_hollow":65,"east_lake":60,"bay_hill":62,"harbour_town":58,"colonial":60,"memorial":62,"tpc_scottsdale":65},"augustaHistory":{"appearances":14,"bestFinish":1,"cuts":11,"top10":3,"avgScore":71.9},"notes":"2x champion (2012, 2014). Retired from PGA Tour. Invited as champion. Big fade — not competitive anymore."},
        {"id":56,"name":"Mike Weir","rank":300,"sgTotal":-1.20,"sgOtt":-0.20,"sgApp":-0.50,"sgArg":-0.30,"sgPutt":-0.20,"birdieAvg":2.8,"bogeyAvg":3.0,"scoringAvg":74.0,"gir":58.0,"fairways":63.0,"scramble":55.0,"proxAvg":38.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":65,"tpc_sawgrass":58,"pebble":65,"torrey_south":60,"riviera":62,"valhalla":62,"pinehurst_2":62,"royal_troon":65,"quail_hollow":58,"east_lake":58,"bay_hill":60,"harbour_town":60,"colonial":62,"memorial":60,"tpc_scottsdale":62},"augustaHistory":{"appearances":18,"bestFinish":1,"cuts":12,"top10":2,"avgScore":72.8},"notes":"2003 champion. Playing on Champions Tour. Augusta invite is honorary at this stage. MC prop only."},
        {"id":57,"name":"Zach Johnson","rank":180,"sgTotal":-0.40,"sgOtt":-0.10,"sgApp":-0.10,"sgArg":-0.05,"sgPutt":-0.15,"birdieAvg":3.3,"bogeyAvg":2.5,"scoringAvg":72.5,"gir":64.0,"fairways":68.0,"scramble":58.0,"proxAvg":35.5,"missDir":"neutral","flight":"low_fade","courseFit":{"augusta":72,"tpc_sawgrass":70,"pebble":72,"torrey_south":65,"riviera":68,"valhalla":68,"pinehurst_2":70,"royal_troon":75,"quail_hollow":65,"east_lake":68,"bay_hill":65,"harbour_town":70,"colonial":72,"memorial":68,"tpc_scottsdale":65},"augustaHistory":{"appearances":20,"bestFinish":1,"cuts":17,"top10":4,"avgScore":71.5},"notes":"2007 champion. Precision iron player who thrives at Augusta. Former Ryder Cup captain. Fade for outright; MC solid."},
        {"id":58,"name":"Tiger Woods","rank":999,"sgTotal":-1.50,"sgOtt":-0.20,"sgApp":-0.80,"sgArg":-0.20,"sgPutt":-0.30,"birdieAvg":2.5,"bogeyAvg":3.5,"scoringAvg":74.5,"gir":58.0,"fairways":58.0,"scramble":55.0,"proxAvg":38.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":38,"tpc_sawgrass":35,"pebble":38,"torrey_south":35,"riviera":35,"valhalla":33,"pinehurst_2":35,"royal_troon":32,"quail_hollow":33,"east_lake":33,"bay_hill":36,"harbour_town":30,"colonial":30,"memorial":35,"tpc_scottsdale":33},"augustaHistory":{"appearances":24,"bestFinish":1,"cuts":22,"top10":14,"avgScore":70.5},"notes":"5x champion (1997, 2001, 2002, 2005, 2019). NOT PLAYING 2026 — ongoing injury recovery. Course fit reflects current physical condition, not peak-era legacy. Do not use for any props or sim."},
        {"id":59,"name":"Fred Couples","rank":500,"sgTotal":-2.0,"sgOtt":-0.30,"sgApp":-1.0,"sgArg":-0.40,"sgPutt":-0.30,"birdieAvg":2.2,"bogeyAvg":3.8,"scoringAvg":76.0,"gir":55.0,"fairways":58.0,"scramble":50.0,"proxAvg":40.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":75,"tpc_sawgrass":60,"pebble":68,"torrey_south":62,"riviera":70,"valhalla":62,"pinehurst_2":62,"royal_troon":65,"quail_hollow":60,"east_lake":62,"bay_hill":65,"harbour_town":62,"colonial":65,"memorial":62,"tpc_scottsdale":62},"augustaHistory":{"appearances":33,"bestFinish":1,"cuts":30,"top10":8,"avgScore":71.8},"notes":"1992 champion. Champions Tour legend. Ball never short of 12th hole (famous). Playing as a past champ — for the love of the game."},
        # ---- ACTIVE PGA TOUR — MASTERS FIELD QUALIFIERS ----
        {"id":60,"name":"Sam Burns","rank":22,"sgTotal":1.05,"sgOtt":0.45,"sgApp":0.35,"sgArg":0.15,"sgPutt":0.10,"birdieAvg":4.4,"bogeyAvg":2.0,"scoringAvg":70.1,"gir":68.0,"fairways":63.0,"scramble":60.0,"proxAvg":32.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":80,"tpc_sawgrass":75,"pebble":72,"torrey_south":75,"riviera":78,"valhalla":80,"pinehurst_2":75,"royal_troon":70,"quail_hollow":82,"east_lake":82,"bay_hill":78,"harbour_town":72,"colonial":78,"memorial":80,"tpc_scottsdale":78},"augustaHistory":{"appearances":4,"bestFinish":8,"cuts":3,"top10":1,"avgScore":71.8},"notes":"Elite ball-striker with draw bias that suits Augusta perfectly. Ryder Cup player. Top-10 machine at demanding courses. Legit contender."},
        {"id":61,"name":"Shane Lowry","rank":26,"sgTotal":0.85,"sgOtt":0.20,"sgApp":0.40,"sgArg":0.10,"sgPutt":0.15,"birdieAvg":4.0,"bogeyAvg":1.9,"scoringAvg":70.4,"gir":68.5,"fairways":64.0,"scramble":61.0,"proxAvg":32.5,"missDir":"left","flight":"mid_draw","courseFit":{"augusta":78,"tpc_sawgrass":72,"pebble":80,"torrey_south":72,"riviera":75,"valhalla":78,"pinehurst_2":75,"royal_troon":85,"quail_hollow":75,"east_lake":78,"bay_hill":72,"harbour_town":75,"colonial":72,"memorial":78,"tpc_scottsdale":68},"augustaHistory":{"appearances":6,"bestFinish":4,"cuts":5,"top10":2,"avgScore":71.2},"notes":"2019 Open champion. Excellent iron player with calm temperament. Top-10 at Augusta 2023. Wind and accuracy player. Legitimate sleeper."},
        {"id":62,"name":"Russell Henley","rank":28,"sgTotal":0.82,"sgOtt":0.25,"sgApp":0.38,"sgArg":0.10,"sgPutt":0.09,"birdieAvg":4.2,"bogeyAvg":1.9,"scoringAvg":70.3,"gir":68.5,"fairways":66.0,"scramble":61.0,"proxAvg":32.0,"missDir":"neutral","flight":"low_fade","courseFit":{"augusta":80,"tpc_sawgrass":75,"pebble":78,"torrey_south":78,"riviera":80,"valhalla":75,"pinehurst_2":78,"royal_troon":72,"quail_hollow":78,"east_lake":82,"bay_hill":80,"harbour_town":78,"colonial":80,"memorial":80,"tpc_scottsdale":75},"augustaHistory":{"appearances":8,"bestFinish":5,"cuts":7,"top10":2,"avgScore":71.0},"notes":"Georgia native who plays Augusta with extra motivation. Elite iron accuracy. Consistent top-20 threat. Major breakout candidate."},
        {"id":63,"name":"Corey Conners","rank":30,"sgTotal":0.78,"sgOtt":0.18,"sgApp":0.42,"sgArg":0.08,"sgPutt":0.10,"birdieAvg":4.0,"bogeyAvg":1.8,"scoringAvg":70.4,"gir":70.0,"fairways":68.0,"scramble":60.0,"proxAvg":31.5,"missDir":"neutral","flight":"low_fade","courseFit":{"augusta":84,"tpc_sawgrass":75,"pebble":78,"torrey_south":75,"riviera":78,"valhalla":75,"pinehurst_2":78,"royal_troon":75,"quail_hollow":75,"east_lake":80,"bay_hill":75,"harbour_town":78,"colonial":78,"memorial":78,"tpc_scottsdale":72},"augustaHistory":{"appearances":6,"bestFinish":4,"cuts":6,"top10":3,"avgScore":70.6},"notes":"Tour's most accurate iron player. Augusta suits him perfectly — GIR machine. Consecutive top-5s at Masters. Perennial value pick."},
        {"id":64,"name":"Sepp Straka","rank":31,"sgTotal":0.76,"sgOtt":0.22,"sgApp":0.32,"sgArg":0.12,"sgPutt":0.10,"birdieAvg":4.0,"bogeyAvg":1.9,"scoringAvg":70.5,"gir":67.5,"fairways":65.0,"scramble":61.0,"proxAvg":32.5,"missDir":"left","flight":"high_draw","courseFit":{"augusta":78,"tpc_sawgrass":72,"pebble":75,"torrey_south":72,"riviera":75,"valhalla":78,"pinehurst_2":75,"royal_troon":70,"quail_hollow":78,"east_lake":78,"bay_hill":72,"harbour_town":72,"colonial":72,"memorial":78,"tpc_scottsdale":70},"augustaHistory":{"appearances":4,"bestFinish":6,"cuts":4,"top10":1,"avgScore":71.4},"notes":"Austrian bomber who has found Augusta suits him. T6 in 2023. Consistent and improving. Draw bias ideal for Augusta's right-to-left holes."},
        {"id":65,"name":"Tom Kim","rank":33,"sgTotal":0.72,"sgOtt":0.40,"sgApp":0.22,"sgArg":0.08,"sgPutt":0.02,"birdieAvg":4.5,"bogeyAvg":2.4,"scoringAvg":70.7,"gir":66.0,"fairways":60.0,"scramble":58.0,"proxAvg":33.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":72,"tpc_sawgrass":72,"pebble":70,"torrey_south":72,"riviera":72,"valhalla":75,"pinehurst_2":70,"royal_troon":68,"quail_hollow":75,"east_lake":72,"bay_hill":72,"harbour_town":68,"colonial":68,"memorial":72,"tpc_scottsdale":78},"augustaHistory":{"appearances":3,"bestFinish":15,"cuts":2,"top10":0,"avgScore":72.5},"notes":"Explosive 22-year-old Korean star. Multiple PGA Tour wins. Short but fierce. Augusta demands patience he sometimes lacks. DFS upside."},
        {"id":66,"name":"Si Woo Kim","rank":38,"sgTotal":0.45,"sgOtt":0.20,"sgApp":0.18,"sgArg":0.05,"sgPutt":0.02,"birdieAvg":4.0,"bogeyAvg":2.2,"scoringAvg":70.9,"gir":66.5,"fairways":63.0,"scramble":59.0,"proxAvg":33.5,"missDir":"left","flight":"mid_draw","courseFit":{"augusta":72,"tpc_sawgrass":70,"pebble":68,"torrey_south":68,"riviera":70,"valhalla":72,"pinehurst_2":70,"royal_troon":68,"quail_hollow":70,"east_lake":70,"bay_hill":68,"harbour_town":68,"colonial":68,"memorial":70,"tpc_scottsdale":68},"augustaHistory":{"appearances":7,"bestFinish":8,"cuts":5,"top10":1,"avgScore":71.9},"notes":"Versatile Korean player with multiple wins. Solid all-around game. Steady at Augusta but ceiling is top-10. Reliable MC prop."},
        {"id":67,"name":"Max Homa","rank":35,"sgTotal":0.65,"sgOtt":0.35,"sgApp":0.20,"sgArg":0.08,"sgPutt":0.02,"birdieAvg":4.2,"bogeyAvg":2.2,"scoringAvg":70.6,"gir":67.0,"fairways":62.0,"scramble":59.0,"proxAvg":33.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":70,"tpc_sawgrass":72,"pebble":78,"torrey_south":80,"riviera":82,"valhalla":72,"pinehurst_2":70,"royal_troon":68,"quail_hollow":72,"east_lake":72,"bay_hill":72,"harbour_town":70,"colonial":70,"memorial":72,"tpc_scottsdale":80},"augustaHistory":{"appearances":4,"bestFinish":12,"cuts":3,"top10":0,"avgScore":72.3},"notes":"California native, multiple wins. Fade suits Riviera/West Coast better than Augusta. Has not broken through at Augusta. Better elsewhere."},
        {"id":68,"name":"Akshay Bhatia","rank":34,"sgTotal":0.70,"sgOtt":0.48,"sgApp":0.15,"sgArg":0.08,"sgPutt":-0.01,"birdieAvg":4.6,"bogeyAvg":2.5,"scoringAvg":70.5,"gir":65.5,"fairways":58.0,"scramble":57.0,"proxAvg":33.5,"missDir":"left","flight":"high_draw","courseFit":{"augusta":72,"tpc_sawgrass":70,"pebble":68,"torrey_south":72,"riviera":70,"valhalla":78,"pinehurst_2":68,"royal_troon":65,"quail_hollow":78,"east_lake":72,"bay_hill":72,"harbour_town":65,"colonial":65,"memorial":72,"tpc_scottsdale":75},"augustaHistory":{"appearances":2,"bestFinish":10,"cuts":2,"top10":1,"avgScore":71.8},"notes":"21-year-old prodigy. Massive off the tee with draw. T10 at 2024 Masters. High variance but explosive upside. Sleeper in top-20 props."},
        {"id":69,"name":"Davis Riley","rank":42,"sgTotal":0.38,"sgOtt":0.28,"sgApp":0.15,"sgArg":0.02,"sgPutt":-0.07,"birdieAvg":3.9,"bogeyAvg":2.2,"scoringAvg":71.0,"gir":66.5,"fairways":63.0,"scramble":57.0,"proxAvg":34.5,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":70,"tpc_sawgrass":68,"pebble":68,"torrey_south":70,"riviera":68,"valhalla":72,"pinehurst_2":68,"royal_troon":65,"quail_hollow":72,"east_lake":70,"bay_hill":70,"harbour_town":68,"colonial":70,"memorial":70,"tpc_scottsdale":70},"augustaHistory":{"appearances":2,"bestFinish":22,"cuts":1,"top10":0,"avgScore":72.8},"notes":"Alabama native still adjusting to major speed. Solid iron player. Mid-tier Augusta floor. Better at more forgiving venues."},
        {"id":70,"name":"Keegan Bradley","rank":44,"sgTotal":0.35,"sgOtt":0.30,"sgApp":0.12,"sgArg":0.02,"sgPutt":-0.09,"birdieAvg":3.9,"bogeyAvg":2.3,"scoringAvg":71.1,"gir":66.0,"fairways":63.0,"scramble":58.0,"proxAvg":34.5,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":68,"tpc_sawgrass":70,"pebble":70,"torrey_south":68,"riviera":70,"valhalla":72,"pinehurst_2":70,"royal_troon":68,"quail_hollow":70,"east_lake":70,"bay_hill":70,"harbour_town":70,"colonial":70,"memorial":70,"tpc_scottsdale":70},"augustaHistory":{"appearances":10,"bestFinish":14,"cuts":7,"top10":0,"avgScore":72.5},"notes":"2011 PGA champion. Ryder Cup captain 2025. Competitive but not Augusta-suited. Reliable MC candidate at weaker fields."},
        {"id":71,"name":"Jason Day","rank":55,"sgTotal":0.40,"sgOtt":0.25,"sgApp":0.18,"sgArg":0.05,"sgPutt":-0.08,"birdieAvg":3.8,"bogeyAvg":2.1,"scoringAvg":71.0,"gir":67.0,"fairways":63.0,"scramble":59.0,"proxAvg":34.0,"missDir":"left","flight":"high_draw","courseFit":{"augusta":78,"tpc_sawgrass":72,"pebble":78,"torrey_south":72,"riviera":75,"valhalla":78,"pinehurst_2":75,"royal_troon":75,"quail_hollow":75,"east_lake":78,"bay_hill":78,"harbour_town":72,"colonial":72,"memorial":78,"tpc_scottsdale":72},"augustaHistory":{"appearances":14,"bestFinish":2,"cuts":12,"top10":5,"avgScore":71.2},"notes":"Former world #1. T2 2011 Masters. Back in form recently. Draw bias suits Augusta. Health has been question mark. Value if healthy."},
        {"id":72,"name":"Taylor Montgomery","rank":53,"sgTotal":0.42,"sgOtt":0.30,"sgApp":0.15,"sgArg":0.05,"sgPutt":-0.08,"birdieAvg":4.0,"bogeyAvg":2.3,"scoringAvg":71.0,"gir":66.0,"fairways":63.0,"scramble":57.0,"proxAvg":34.0,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":68,"tpc_sawgrass":68,"pebble":68,"torrey_south":72,"riviera":68,"valhalla":70,"pinehurst_2":68,"royal_troon":65,"quail_hollow":70,"east_lake":68,"bay_hill":70,"harbour_town":65,"colonial":68,"memorial":68,"tpc_scottsdale":75},"augustaHistory":{"appearances":1,"bestFinish":28,"cuts":1,"top10":0,"avgScore":73.1},"notes":"Consistent mid-tier player. Augusta debut was rough. Power game but needs precision Augusta demands. Watch form going in."},
        {"id":73,"name":"Byeong Hun An","rank":56,"sgTotal":0.38,"sgOtt":0.20,"sgApp":0.18,"sgArg":0.05,"sgPutt":-0.05,"birdieAvg":3.9,"bogeyAvg":2.1,"scoringAvg":71.1,"gir":67.0,"fairways":65.0,"scramble":59.0,"proxAvg":34.0,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":70,"tpc_sawgrass":70,"pebble":70,"torrey_south":68,"riviera":70,"valhalla":70,"pinehurst_2":70,"royal_troon":72,"quail_hollow":68,"east_lake":70,"bay_hill":68,"harbour_town":70,"colonial":70,"memorial":70,"tpc_scottsdale":68},"augustaHistory":{"appearances":5,"bestFinish":12,"cuts":4,"top10":0,"avgScore":72.0},"notes":"Korean veteran with solid all-around game. Augusta veteran now. Consistent mid-field finisher. Good MC prop value."},
        {"id":74,"name":"Chris Kirk","rank":58,"sgTotal":0.32,"sgOtt":0.15,"sgApp":0.15,"sgArg":0.05,"sgPutt":-0.03,"birdieAvg":3.8,"bogeyAvg":2.1,"scoringAvg":71.2,"gir":67.5,"fairways":66.0,"scramble":59.0,"proxAvg":34.5,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":70,"tpc_sawgrass":70,"pebble":70,"torrey_south":68,"riviera":70,"valhalla":70,"pinehurst_2":70,"royal_troon":68,"quail_hollow":68,"east_lake":72,"bay_hill":70,"harbour_town":72,"colonial":72,"memorial":70,"tpc_scottsdale":68},"augustaHistory":{"appearances":4,"bestFinish":18,"cuts":3,"top10":0,"avgScore":72.3},"notes":"Georgia product who won comeback player. Steady, not spectacular. Augusta knowledge helps. Solid but not a contender."},
        {"id":75,"name":"Eric Cole","rank":60,"sgTotal":0.30,"sgOtt":0.40,"sgApp":0.08,"sgArg":-0.05,"sgPutt":-0.13,"birdieAvg":4.1,"bogeyAvg":2.5,"scoringAvg":71.3,"gir":64.5,"fairways":60.0,"scramble":55.0,"proxAvg":34.5,"missDir":"right","flight":"high_fade","courseFit":{"augusta":65,"tpc_sawgrass":62,"pebble":62,"torrey_south":70,"riviera":65,"valhalla":70,"pinehurst_2":62,"royal_troon":58,"quail_hollow":72,"east_lake":65,"bay_hill":68,"harbour_town":58,"colonial":58,"memorial":65,"tpc_scottsdale":72},"augustaHistory":{"appearances":2,"bestFinish":20,"cuts":1,"top10":0,"avgScore":73.2},"notes":"Power hitter who trades accuracy for distance. Augusta punishes the wild miss. Better at bomber-friendly tracks."},
        {"id":76,"name":"Seamus Power","rank":62,"sgTotal":0.28,"sgOtt":0.20,"sgApp":0.12,"sgArg":0.05,"sgPutt":-0.09,"birdieAvg":3.8,"bogeyAvg":2.2,"scoringAvg":71.3,"gir":66.0,"fairways":63.0,"scramble":58.0,"proxAvg":34.5,"missDir":"left","flight":"mid_draw","courseFit":{"augusta":70,"tpc_sawgrass":68,"pebble":70,"torrey_south":68,"riviera":68,"valhalla":70,"pinehurst_2":70,"royal_troon":75,"quail_hollow":68,"east_lake":68,"bay_hill":68,"harbour_town":70,"colonial":70,"memorial":68,"tpc_scottsdale":65},"augustaHistory":{"appearances":3,"bestFinish":16,"cuts":2,"top10":0,"avgScore":72.4},"notes":"Irish power player. Better on links and European-style tracks. Augusta is not his best fit but competitive."},
        {"id":77,"name":"Adam Hadwin","rank":65,"sgTotal":0.25,"sgOtt":0.15,"sgApp":0.12,"sgArg":0.05,"sgPutt":-0.07,"birdieAvg":3.7,"bogeyAvg":2.1,"scoringAvg":71.4,"gir":67.0,"fairways":67.0,"scramble":58.0,"proxAvg":35.0,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":68,"tpc_sawgrass":68,"pebble":70,"torrey_south":72,"riviera":70,"valhalla":68,"pinehurst_2":70,"royal_troon":68,"quail_hollow":65,"east_lake":68,"bay_hill":68,"harbour_town":68,"colonial":68,"memorial":65,"tpc_scottsdale":72},"augustaHistory":{"appearances":4,"bestFinish":24,"cuts":3,"top10":0,"avgScore":72.5},"notes":"Canadian accuracy player. Very consistent but lacks the elite ballstriking Augusta demands at the top. MC value."},
        {"id":78,"name":"Mackenzie Hughes","rank":68,"sgTotal":0.22,"sgOtt":0.10,"sgApp":0.12,"sgArg":0.05,"sgPutt":-0.05,"birdieAvg":3.7,"bogeyAvg":2.1,"scoringAvg":71.5,"gir":67.0,"fairways":67.0,"scramble":59.0,"proxAvg":35.0,"missDir":"neutral","flight":"low_fade","courseFit":{"augusta":68,"tpc_sawgrass":68,"pebble":70,"torrey_south":68,"riviera":68,"valhalla":68,"pinehurst_2":70,"royal_troon":70,"quail_hollow":65,"east_lake":68,"bay_hill":65,"harbour_town":68,"colonial":68,"memorial":65,"tpc_scottsdale":68},"augustaHistory":{"appearances":3,"bestFinish":19,"cuts":2,"top10":0,"avgScore":72.6},"notes":"Canadian precision player. Limited Augusta experience. Consistent MC candidate. No contender ceiling yet."},
        {"id":79,"name":"J.T. Poston","rank":70,"sgTotal":0.20,"sgOtt":0.08,"sgApp":0.10,"sgArg":0.05,"sgPutt":-0.03,"birdieAvg":3.7,"bogeyAvg":2.1,"scoringAvg":71.5,"gir":67.0,"fairways":67.0,"scramble":59.0,"proxAvg":35.0,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":68,"tpc_sawgrass":68,"pebble":68,"torrey_south":65,"riviera":68,"valhalla":68,"pinehurst_2":68,"royal_troon":65,"quail_hollow":68,"east_lake":70,"bay_hill":68,"harbour_town":70,"colonial":70,"memorial":68,"tpc_scottsdale":65},"augustaHistory":{"appearances":3,"bestFinish":21,"cuts":2,"top10":0,"avgScore":72.5},"notes":"Ball-striking specialist. Consistent mid-field at Augusta. No major breakthrough yet. Solid MC prop."},
        {"id":80,"name":"Lucas Glover","rank":72,"sgTotal":0.18,"sgOtt":0.10,"sgApp":0.08,"sgArg":0.02,"sgPutt":-0.02,"birdieAvg":3.6,"bogeyAvg":2.1,"scoringAvg":71.6,"gir":66.5,"fairways":67.0,"scramble":59.0,"proxAvg":35.5,"missDir":"neutral","flight":"low_fade","courseFit":{"augusta":70,"tpc_sawgrass":70,"pebble":68,"torrey_south":65,"riviera":68,"valhalla":70,"pinehurst_2":70,"royal_troon":68,"quail_hollow":68,"east_lake":72,"bay_hill":68,"harbour_town":70,"colonial":70,"memorial":70,"tpc_scottsdale":65},"augustaHistory":{"appearances":12,"bestFinish":8,"cuts":9,"top10":1,"avgScore":71.8},"notes":"US Open champion (2009). Augusta veteran who plays with grinder mentality. Consistent MC maker. T8 here proves he can compete."},
        {"id":81,"name":"Min Woo Lee","rank":73,"sgTotal":0.20,"sgOtt":0.38,"sgApp":0.05,"sgArg":-0.08,"sgPutt":-0.15,"birdieAvg":4.2,"bogeyAvg":2.6,"scoringAvg":71.4,"gir":65.0,"fairways":60.0,"scramble":55.0,"proxAvg":34.0,"missDir":"right","flight":"high_fade","courseFit":{"augusta":65,"tpc_sawgrass":65,"pebble":68,"torrey_south":70,"riviera":68,"valhalla":70,"pinehurst_2":65,"royal_troon":72,"quail_hollow":68,"east_lake":65,"bay_hill":65,"harbour_town":62,"colonial":62,"memorial":65,"tpc_scottsdale":72},"augustaHistory":{"appearances":2,"bestFinish":18,"cuts":2,"top10":0,"avgScore":72.8},"notes":"Australian bomber and crowd favourite. Lexi Thompson's brother. Long and spectacular but inconsistent. Augusta demands patience he lacks."},
        {"id":82,"name":"Rasmus Hojgaard","rank":74,"sgTotal":0.22,"sgOtt":0.18,"sgApp":0.12,"sgArg":0.05,"sgPutt":-0.13,"birdieAvg":3.8,"bogeyAvg":2.3,"scoringAvg":71.3,"gir":66.5,"fairways":63.0,"scramble":57.0,"proxAvg":34.5,"missDir":"right","flight":"mid_fade","courseFit":{"augusta":68,"tpc_sawgrass":65,"pebble":68,"torrey_south":65,"riviera":65,"valhalla":70,"pinehurst_2":68,"royal_troon":75,"quail_hollow":68,"east_lake":65,"bay_hill":65,"harbour_town":65,"colonial":65,"memorial":68,"tpc_scottsdale":65},"augustaHistory":{"appearances":1,"bestFinish":14,"cuts":1,"top10":0,"avgScore":72.2},"notes":"Danish twin with DP World Tour pedigree. Solid debut at Augusta. European-style patience suits the course. Rising stock."},
        {"id":83,"name":"Aaron Rai","rank":76,"sgTotal":0.20,"sgOtt":0.12,"sgApp":0.15,"sgArg":0.05,"sgPutt":-0.12,"birdieAvg":3.7,"bogeyAvg":2.1,"scoringAvg":71.5,"gir":67.5,"fairways":67.0,"scramble":58.0,"proxAvg":34.5,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":68,"tpc_sawgrass":65,"pebble":68,"torrey_south":65,"riviera":65,"valhalla":68,"pinehurst_2":70,"royal_troon":75,"quail_hollow":65,"east_lake":65,"bay_hill":65,"harbour_town":68,"colonial":68,"memorial":65,"tpc_scottsdale":65},"augustaHistory":{"appearances":1,"bestFinish":25,"cuts":0,"top10":0,"avgScore":74.0},"notes":"British accuracy player. PGA Tour rookie experience. Augusta debut was tough. Iron play is his calling card. Still adapting."},
        {"id":84,"name":"Erik van Rooyen","rank":78,"sgTotal":0.18,"sgOtt":0.30,"sgApp":0.08,"sgArg":-0.05,"sgPutt":-0.15,"birdieAvg":3.9,"bogeyAvg":2.4,"scoringAvg":71.5,"gir":65.5,"fairways":61.0,"scramble":56.0,"proxAvg":34.5,"missDir":"left","flight":"high_draw","courseFit":{"augusta":68,"tpc_sawgrass":65,"pebble":65,"torrey_south":68,"riviera":65,"valhalla":70,"pinehurst_2":65,"royal_troon":68,"quail_hollow":70,"east_lake":65,"bay_hill":65,"harbour_town":62,"colonial":62,"memorial":65,"tpc_scottsdale":68},"augustaHistory":{"appearances":3,"bestFinish":22,"cuts":2,"top10":0,"avgScore":72.7},"notes":"South African power player. Long off the tee but short game lets him down. Augusta demands scrambling he doesn't have."},
        {"id":85,"name":"Callum Shinkwin","rank":80,"sgTotal":0.15,"sgOtt":0.22,"sgApp":0.08,"sgArg":-0.05,"sgPutt":-0.10,"birdieAvg":3.7,"bogeyAvg":2.2,"scoringAvg":71.6,"gir":65.5,"fairways":62.0,"scramble":57.0,"proxAvg":35.0,"missDir":"right","flight":"mid_fade","courseFit":{"augusta":65,"tpc_sawgrass":62,"pebble":65,"torrey_south":65,"riviera":65,"valhalla":68,"pinehurst_2":65,"royal_troon":72,"quail_hollow":65,"east_lake":62,"bay_hill":62,"harbour_town":65,"colonial":62,"memorial":62,"tpc_scottsdale":62},"augustaHistory":{"appearances":1,"bestFinish":30,"cuts":0,"top10":0,"avgScore":74.5},"notes":"English DP World Tour player. Augusta debut was rough. Links-style game. Not Augusta-suited."},
        {"id":86,"name":"Thriston Lawrence","rank":82,"sgTotal":0.15,"sgOtt":0.18,"sgApp":0.08,"sgArg":0.00,"sgPutt":-0.11,"birdieAvg":3.7,"bogeyAvg":2.2,"scoringAvg":71.7,"gir":66.0,"fairways":64.0,"scramble":57.0,"proxAvg":35.0,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":65,"tpc_sawgrass":62,"pebble":65,"torrey_south":62,"riviera":65,"valhalla":65,"pinehurst_2":65,"royal_troon":70,"quail_hollow":62,"east_lake":62,"bay_hill":62,"harbour_town":65,"colonial":62,"memorial":62,"tpc_scottsdale":62},"augustaHistory":{"appearances":1,"bestFinish":35,"cuts":0,"top10":0,"avgScore":75.0},"notes":"South African DP World Tour player. Augusta debut. Solid European game but long road to contend here."},
        {"id":87,"name":"Ryan Fox","rank":84,"sgTotal":0.18,"sgOtt":0.28,"sgApp":0.08,"sgArg":-0.05,"sgPutt":-0.13,"birdieAvg":3.8,"bogeyAvg":2.3,"scoringAvg":71.6,"gir":65.5,"fairways":62.0,"scramble":56.0,"proxAvg":34.5,"missDir":"left","flight":"high_draw","courseFit":{"augusta":65,"tpc_sawgrass":62,"pebble":68,"torrey_south":65,"riviera":65,"valhalla":68,"pinehurst_2":65,"royal_troon":72,"quail_hollow":65,"east_lake":62,"bay_hill":62,"harbour_town":65,"colonial":62,"memorial":62,"tpc_scottsdale":62},"augustaHistory":{"appearances":2,"bestFinish":28,"cuts":1,"top10":0,"avgScore":73.5},"notes":"New Zealand bomber. Son of Grant Fox. Huge off the tee but Augusta short game demands more. Better on longer bombers tracks."},
        {"id":88,"name":"Lee Hodges","rank":86,"sgTotal":0.12,"sgOtt":0.15,"sgApp":0.05,"sgArg":0.00,"sgPutt":-0.08,"birdieAvg":3.6,"bogeyAvg":2.2,"scoringAvg":71.8,"gir":66.0,"fairways":65.0,"scramble":58.0,"proxAvg":35.5,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":62,"tpc_sawgrass":62,"pebble":62,"torrey_south":62,"riviera":62,"valhalla":65,"pinehurst_2":62,"royal_troon":60,"quail_hollow":65,"east_lake":65,"bay_hill":62,"harbour_town":65,"colonial":65,"memorial":62,"tpc_scottsdale":65},"augustaHistory":{"appearances":2,"bestFinish":32,"cuts":1,"top10":0,"avgScore":73.8},"notes":"Alabama native making his Masters bones. Good ballstriker still learning major speeds. Development player."},
        {"id":89,"name":"Bernhard Langer","rank":999,"sgTotal":-3.0,"sgOtt":-1.0,"sgApp":-1.2,"sgArg":-0.5,"sgPutt":-0.3,"birdieAvg":2.0,"bogeyAvg":4.0,"scoringAvg":77.0,"gir":52.0,"fairways":60.0,"scramble":50.0,"proxAvg":42.0,"missDir":"neutral","flight":"low_fade","courseFit":{"augusta":72,"tpc_sawgrass":55,"pebble":60,"torrey_south":55,"riviera":58,"valhalla":58,"pinehurst_2":60,"royal_troon":65,"quail_hollow":55,"east_lake":55,"bay_hill":55,"harbour_town":58,"colonial":60,"memorial":58,"tpc_scottsdale":55},"augustaHistory":{"appearances":40,"bestFinish":1,"cuts":36,"top10":10,"avgScore":72.0},"notes":"2x champion (1985, 1993). Augusta legend. Playing as a 68-year-old past champion. Beloved but not a betting proposition. Avoid all props."},
        {"id":90,"name":"Larry Mize","rank":999,"sgTotal":-3.5,"sgOtt":-1.2,"sgApp":-1.5,"sgArg":-0.5,"sgPutt":-0.3,"birdieAvg":1.8,"bogeyAvg":4.5,"scoringAvg":78.0,"gir":50.0,"fairways":58.0,"scramble":48.0,"proxAvg":43.0,"missDir":"neutral","flight":"mid_fade","courseFit":{"augusta":68,"tpc_sawgrass":50,"pebble":55,"torrey_south":50,"riviera":52,"valhalla":52,"pinehurst_2":55,"royal_troon":55,"quail_hollow":50,"east_lake":52,"bay_hill":52,"harbour_town":55,"colonial":55,"memorial":52,"tpc_scottsdale":50},"augustaHistory":{"appearances":38,"bestFinish":1,"cuts":28,"top10":3,"avgScore":73.2},"notes":"1987 champion — famous chip-in on 11 in playoff vs Norman. Augusta resident. Honorary start only at this stage. Beloved local."},
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
}


def match_venue_to_course(venue_name, event_name=""):
    """Fuzzy match an ESPN venue/event name to our course key."""
    combined = (venue_name + " " + event_name).lower()
    for alias, key in VENUE_ALIASES.items():
        if alias in combined:
            return key
    return None


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
    "augusta": {
        "power": 0.8, "accuracy": 0.7, "scramble": 0.9, "putting": 0.6,
        "fairway_width": 0.45, "gir_difficulty": 0.85, "birdie_rate": 3.8,
        "bogey_rate": 2.9, "wind_exposure": 0.25, "morning_adv": 0.4,
    },
    "tpc_sawgrass": {
        "power": 0.5, "accuracy": 0.8, "scramble": 0.7, "putting": 0.8,
        "fairway_width": 0.50, "gir_difficulty": 0.70, "birdie_rate": 4.1,
        "bogey_rate": 3.1, "wind_exposure": 0.60, "morning_adv": 0.3,
    },
    "pebble": {
        "power": 0.4, "accuracy": 0.8, "scramble": 0.8, "putting": 0.7,
        "fairway_width": 0.45, "gir_difficulty": 0.75, "birdie_rate": 3.4,
        "bogey_rate": 3.2, "wind_exposure": 0.80, "morning_adv": 0.5,
    },
    "torrey_south": {
        "power": 0.8, "accuracy": 0.6, "scramble": 0.6, "putting": 0.5,
        "fairway_width": 0.60, "gir_difficulty": 0.65, "birdie_rate": 4.3,
        "bogey_rate": 2.7, "wind_exposure": 0.50, "morning_adv": 0.2,
    },
    "riviera": {
        "power": 0.6, "accuracy": 0.8, "scramble": 0.7, "putting": 0.7,
        "fairway_width": 0.50, "gir_difficulty": 0.75, "birdie_rate": 3.9,
        "bogey_rate": 2.8, "wind_exposure": 0.40, "morning_adv": 0.3,
    },
    "valhalla": {
        "power": 0.9, "accuracy": 0.5, "scramble": 0.5, "putting": 0.5,
        "fairway_width": 0.65, "gir_difficulty": 0.60, "birdie_rate": 4.4,
        "bogey_rate": 2.6, "wind_exposure": 0.35, "morning_adv": 0.2,
    },
    "pinehurst_2": {
        "power": 0.5, "accuracy": 0.9, "scramble": 0.9, "putting": 0.7,
        "fairway_width": 0.65, "gir_difficulty": 0.90, "birdie_rate": 3.2,
        "bogey_rate": 3.5, "wind_exposure": 0.55, "morning_adv": 0.4,
    },
    "royal_troon": {
        "power": 0.6, "accuracy": 0.8, "scramble": 0.8, "putting": 0.6,
        "fairway_width": 0.70, "gir_difficulty": 0.80, "birdie_rate": 3.3,
        "bogey_rate": 3.4, "wind_exposure": 0.90, "morning_adv": 0.6,
    },
    "quail_hollow": {
        "power": 0.8, "accuracy": 0.6, "scramble": 0.6, "putting": 0.6,
        "fairway_width": 0.55, "gir_difficulty": 0.65, "birdie_rate": 4.2,
        "bogey_rate": 2.8, "wind_exposure": 0.30, "morning_adv": 0.2,
    },
    "east_lake": {
        "power": 0.6, "accuracy": 0.7, "scramble": 0.7, "putting": 0.7,
        "fairway_width": 0.55, "gir_difficulty": 0.70, "birdie_rate": 3.9,
        "bogey_rate": 2.8, "wind_exposure": 0.35, "morning_adv": 0.2,
    },
    "bay_hill": {
        "power": 0.7, "accuracy": 0.7, "scramble": 0.6, "putting": 0.6,
        "fairway_width": 0.50, "gir_difficulty": 0.70, "birdie_rate": 4.0,
        "bogey_rate": 2.9, "wind_exposure": 0.45, "morning_adv": 0.3,
    },
    "harbour_town": {
        "power": 0.2, "accuracy": 0.9, "scramble": 0.7, "putting": 0.8,
        "fairway_width": 0.30, "gir_difficulty": 0.70, "birdie_rate": 3.7,
        "bogey_rate": 2.6, "wind_exposure": 0.50, "morning_adv": 0.3,
    },
    "colonial": {
        "power": 0.3, "accuracy": 0.9, "scramble": 0.7, "putting": 0.8,
        "fairway_width": 0.35, "gir_difficulty": 0.72, "birdie_rate": 3.8,
        "bogey_rate": 2.7, "wind_exposure": 0.40, "morning_adv": 0.3,
    },
    "memorial": {
        "power": 0.7, "accuracy": 0.8, "scramble": 0.7, "putting": 0.6,
        "fairway_width": 0.50, "gir_difficulty": 0.75, "birdie_rate": 3.9,
        "bogey_rate": 2.8, "wind_exposure": 0.30, "morning_adv": 0.2,
    },
    "tpc_scottsdale": {
        "power": 0.6, "accuracy": 0.6, "scramble": 0.5, "putting": 0.7,
        "fairway_width": 0.70, "gir_difficulty": 0.50, "birdie_rate": 5.2,
        "bogey_rate": 2.3, "wind_exposure": 0.55, "morning_adv": 0.3,
    },
}


def calculate_ev_score(book_odds_american, model_probability_pct):
    """
    Calculate Expected Value score comparing model probability to book implied odds.
    Returns EV as a percentage: positive = value bet, negative = fade.
    """
    if not book_odds_american or not model_probability_pct:
        return None
    try:
        odds = float(book_odds_american)
        model_prob = float(model_probability_pct) / 100.0
        # Convert American odds to implied probability (with ~5% vig)
        if odds > 0:
            implied_prob = 100.0 / (odds + 100.0)
        else:
            implied_prob = abs(odds) / (abs(odds) + 100.0)
        if implied_prob <= 0:
            return None
        ev = round((model_prob - implied_prob) / implied_prob * 100, 1)
        return ev
    except (ValueError, ZeroDivisionError):
        return None


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


# ============================================================
# FEATURE: HISTORICAL DATA ARCHIVING
# ============================================================

def archive_data(output, base_dir):
    """Save a timestamped copy to history/ for trend analysis."""
    history_dir = os.path.join(base_dir, "history")
    os.makedirs(history_dir, exist_ok=True)

    date_str = datetime.now().strftime("%Y-%m-%d")
    archive_path = os.path.join(history_dir, f"{date_str}.json")
    with open(archive_path, "w") as f:
        json.dump(output, f, separators=(",", ":"))  # compact

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

    # Only fetch Mon/Wed/Thu/Sat to conserve credits (matches workflow schedule)
    today = datetime.now().weekday()  # 0=Mon, 2=Wed, 3=Thu, 5=Sat
    if today not in (0, 2, 3, 5):
        print("[5/7] Skipping Odds API — not a scrape day")
        return None

    print("[5/7] Fetching ALL sportsbook odds from The Odds API...")
    url = (
        f"https://api.the-odds-api.com/v4/sports/golf_pga_tour_winner/odds"
        f"?apiKey={ODDS_API_KEY}&regions=us&markets=outrights&oddsFormat=american"
    )
    data = fetch_json(url)
    if not data:
        print("  Could not fetch odds data")
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
                    if name not in odds_map:
                        odds_map[name] = {}
                    odds_map[name][short] = f"{'+' if price > 0 else ''}{price}"

    print(f"  Odds API: {len(odds_map)} players from {len(books_seen)} books: {', '.join(sorted(books_seen))}")
    return odds_map


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

    # Use current leaderboard as most recent data point
    if espn_event_data and espn_event_data.get("leaderboard"):
        lb = espn_event_data["leaderboard"]
        for i, entry in enumerate(lb):
            name = entry.get("name", "")
            if not name:
                continue
            position = i + 1  # Approximate position from leaderboard order
            form_map[name] = {
                "lastResult": f"#{position} at {espn_event_data.get('name', 'Unknown')}",
                "lastPosition": position,
            }

    # Load historical archive files for L5/L10 calculation
    base_dir = os.path.dirname(os.path.abspath(__file__))
    history_dir = os.path.join(base_dir, "history")
    if os.path.isdir(history_dir):
        history_files = sorted(
            [f for f in os.listdir(history_dir) if f.endswith(".json")],
            reverse=True
        )[:10]  # Last 10 weeks

        player_finishes = {}  # {name: [list of positions]}
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

            if name not in form_map:
                form_map[name] = {}
            form_map[name].update({
                "l5AvgFinish": round(l5_avg, 1) if l5_avg else None,
                "l10AvgFinish": round(l10_avg, 1) if l10_avg else None,
                "l5McPct": round(sum(1 for f in l5 if f <= 65) / len(l5) * 100) if l5 else None,
                "trend": trend,
                "events": len(l10),
            })

    print(f"  Built form data for {len(form_map)} players")
    return form_map


# ============================================================
# FEATURE: DISCORD ALERTS
# ============================================================

def send_discord_alerts(output):
    """Send Discord webhook alerts for high-edge prop opportunities."""
    if not DISCORD_WEBHOOK_URL:
        print("  Skipping Discord alerts — no webhook URL set")
        return

    print("[7/7] Checking for high-edge prop alerts...")

    alerts = []
    event_name = ""
    if output.get("currentEvent"):
        event_name = output["currentEvent"].get("name", "")

    for player in output.get("players", []):
        # Check birdie over potential
        birdie_avg = player.get("birdieAvg", 4.0)
        line = 3.5
        edge = birdie_avg - line
        if edge >= 0.8:  # Strong over signal
            conf = min(95, round(65 + edge * 15))
            if conf >= 75:
                alerts.append({
                    "player": player["name"],
                    "prop": f"OVER {line} Birdies",
                    "model": f"{birdie_avg} avg",
                    "edge": f"+{edge:.1f}",
                    "conf": conf,
                    "rank": player.get("rank", "?"),
                })

        # Check bogey under potential
        bogey_avg = player.get("bogeyAvg", 2.5)
        line = 2.5
        edge = line - bogey_avg
        if edge >= 0.4:
            conf = min(95, round(60 + edge * 20))
            if conf >= 75:
                alerts.append({
                    "player": player["name"],
                    "prop": f"UNDER {line} Bogeys",
                    "model": f"{bogey_avg} avg",
                    "edge": f"+{edge:.1f}",
                    "conf": conf,
                    "rank": player.get("rank", "?"),
                })

    if not alerts:
        print("  No high-edge alerts this cycle")
        return

    # Sort by confidence
    alerts.sort(key=lambda x: x["conf"], reverse=True)
    top_alerts = alerts[:5]  # Max 5 per cycle

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
            "title": "PropsBot Golf — High-Edge Prop Alerts",
            "description": f"**{event_name}** | {len(alerts)} signals found, showing top {len(top_alerts)}",
            "color": 1441730,  # #15ffc2
            "fields": fields,
            "footer": {"text": "PropsBot Golf Intelligence | Educational Tool Only"},
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

def fetch_tee_times():
    """Fetch tee times from ESPN's PGA Tour API for the current event."""
    print("[TEE TIMES] Fetching tee times from ESPN...")
    url = "https://site.api.espn.com/apis/site/v2/sports/golf/pga/scoreboard"
    data = fetch_json(url)
    if not data:
        return []

    tee_times = []
    try:
        events = data.get("events", [])
        if not events:
            return []
        event = events[0]
        for comp in event.get("competitions", []):
            for competitor in comp.get("competitors", []):
                athlete = competitor.get("athlete", {})
                name = athlete.get("displayName", "")
                tee_time = competitor.get("status", {}).get("teeTime", "")
                round_num = comp.get("status", {}).get("period", 1)
                hole = competitor.get("status", {}).get("hole", 1)
                group = competitor.get("linescores", [{}])[0].get("value", "") if competitor.get("linescores") else ""
                if name:
                    tee_times.append({
                        "player": name,
                        "teeTime": tee_time,
                        "round": round_num,
                        "startHole": hole,
                    })
        print(f"  Got {len(tee_times)} tee time entries")
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

def predict_cut_line(players, course_key="augusta"):
    """
    Predict the cut line based on field strength, historical Augusta cuts, and current scoring.
    Augusta historical cuts (last 10 years): +1, +2, +1, +3, +1, +2, +1, +4, +2, +1
    Typical range: +1 to +3 (score of 145-147 for 36 holes)
    """
    print("[CUT PREDICTOR] Calculating predicted cut line...")

    # Augusta historical cut data
    AUGUSTA_CUT_HISTORY = {
        2024: 3, 2023: 2, 2022: 2, 2021: 4, 2020: 1,
        2019: 2, 2018: 2, 2017: 1, 2016: 3, 2015: 2,
        2014: 2, 2013: 3, 2012: 2, 2011: 4, 2010: 2
    }

    historical_avg = round(sum(AUGUSTA_CUT_HISTORY.values()) / len(AUGUSTA_CUT_HISTORY), 1)

    # Measure field strength using average SG total of top 30
    if players:
        sg_vals = sorted([p.get("sgTotal", 0) for p in players if p.get("sgTotal")], reverse=True)
        top30_sg = sg_vals[:30]
        field_strength = round(sum(top30_sg) / len(top30_sg), 2) if top30_sg else 0
    else:
        field_strength = 0

    # Adjust cut prediction based on field strength
    # Strong field = more birdies but also more pressure = similar cut
    base_cut = historical_avg
    if field_strength > 1.5:
        predicted = base_cut - 0.5  # Elite field goes lower
    elif field_strength < 0.8:
        predicted = base_cut + 0.5  # Weaker field plays harder
    else:
        predicted = base_cut

    predicted = round(predicted)

    # Count players likely to make cut (course fit + SG above threshold)
    likely_makers = sum(1 for p in players if (p.get("sgTotal", 0) > 0.2 or
                        (p.get("courseFit", {}).get("augusta", 0) if isinstance(p.get("courseFit"), dict) else 0) > 70))

    result = {
        "predictedCut": predicted,
        "predictedScore": 144 + predicted,  # Par 144 for 36 holes at Augusta (par 72 x 2)
        "historicalAvg": historical_avg,
        "fieldStrength": field_strength,
        "likelyMakers": min(likely_makers, 50),
        "confidence": "High" if predicted == round(historical_avg) else "Medium",
        "note": f"Augusta historical avg cut: +{historical_avg}. {'Strong' if field_strength > 1.5 else 'Average'} field this week.",
        "history": AUGUSTA_CUT_HISTORY
    }

    print(f"  Predicted cut: +{predicted} (historical avg: +{historical_avg})")
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


# ============================================================
# PROPS BY TYPE PARSER
# ============================================================

def parse_props_by_type(bdl_props):
    """
    Parse BDL props dict into categorized prop groups.
    bdl_props is keyed by player name with prop type sub-keys.
    Returns dict with top5, top10, top20, r1Leader, make_cut sections.
    """
    top5 = {}
    top10 = {}
    top20 = {}
    r1_leader = {}
    make_cut = {}

    for player_name, props in bdl_props.items():
        if isinstance(props, dict):
            for prop_key, odds_val in props.items():
                key_lower = prop_key.lower()
                if "top_5" in key_lower or "top5" in key_lower or "top 5" in key_lower:
                    top5[player_name] = odds_val
                elif "top_10" in key_lower or "top10" in key_lower or "top 10" in key_lower:
                    top10[player_name] = odds_val
                elif "top_20" in key_lower or "top20" in key_lower or "top 20" in key_lower:
                    top20[player_name] = odds_val
                elif "first_round" in key_lower or "r1_leader" in key_lower or "round 1 leader" in key_lower:
                    r1_leader[player_name] = odds_val
                elif "make_cut" in key_lower or "cut" in key_lower:
                    make_cut[player_name] = odds_val

    return {
        "top5": top5,
        "top10": top10,
        "top20": top20,
        "r1Leader": r1_leader,
        "makeCut": make_cut
    }


# ============================================================
# CONFIDENCE SCORE MODEL
# ============================================================

def calculate_player_confidence_score(player, all_players, course_key="augusta"):
    """
    PropsBot Confidence Score v2 — 0 to 100.
    9-factor composite model for tournament performance prediction.

    Weights:
      22%  Strokes Gained            (field-normalized, course-component weighted)
      18%  Course Fit                (algorithmic match to course trait profile)
      12%  Tournament / Course History (finish, top-10s, cut%, avg score)
      12%  Recent Form & Trend       (L5 recency-weighted, injury/cold-streak flag)
      10%  GIR & Approach Quality    (GIR% × course difficulty, proximity, scramble)
       8%  Driving Profile           (accuracy + distance matched to course needs)
       8%  Birdie / Bogey Profile    (rate vs course avg, net scoring tendency)
       6%  Cut Consistency           (career + recent made-cut %)
       4%  Market Consensus          (book implied probability as outside signal)
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
    score += sg_component * 22.0
    score_breakdown["sg"] = round(sg_component * 22.0, 1)

    # =========================================================
    # 2. COURSE FIT  (18%)
    # Pulls from curated courseFit dict; bonus for elite fit.
    # =========================================================
    course_fit = 0
    cf = player.get("courseFit")
    if isinstance(cf, dict):
        course_fit = cf.get(course_key, cf.get("augusta", 0))
    elif isinstance(cf, (int, float)):
        course_fit = cf

    fit_norm = max(0, min(100, course_fit)) / 100.0
    if course_fit >= 88:
        fit_norm = min(1.0, fit_norm * 1.10)
    elif course_fit < 55:
        fit_norm *= 0.80

    score += fit_norm * 18.0
    score_breakdown["fit"] = round(fit_norm * 18.0, 1)

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

    score += hist_score * 12.0
    score_breakdown["history"] = round(hist_score * 12.0, 1)

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

    score += form_score * 12.0
    score_breakdown["form"] = round(form_score * 12.0, 1)

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

    score += drv_component * 8.0
    score_breakdown["driving"] = round(drv_component * 8.0, 1)

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
    score += bb_component * 8.0
    score_breakdown["birdie_bogey"] = round(bb_component * 8.0, 1)

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

    score += cut_score * 6.0
    score_breakdown["cut"] = round(cut_score * 6.0, 1)

    # =========================================================
    # 9. MARKET CONSENSUS  (4%)
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

    score += market_norm * 4.0
    score_breakdown["market"] = round(market_norm * 4.0, 1)

    # =========================================================
    # COMPETITIVENESS GATE
    # Historical greatness cannot override current inability to
    # compete.  Uses two independent signals (rank + SG) so that
    # retired players AND recently injured players are caught.
    # =========================================================
    rank     = player.get("rank",    100)
    sg_total = player.get("sgTotal", 0.0)

    is_ceremonial     = (rank >= 500) or (sg_total < -2.0)
    is_non_competitive= (rank > 300)  or (sg_total < -1.2)
    is_declining      = (rank > 150)  or (sg_total < -0.3)

    final_score = round(min(100, max(1, score)))

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
    # =========================================================
    edge_score = None
    if best_odds is not None:
        if best_odds > 0:
            market_implied_pct = 100.0 / (best_odds + 100.0) * 100.0
        else:
            market_implied_pct = abs(best_odds) / (abs(best_odds) + 100.0) * 100.0
        model_win_pct = (final_score / 100.0) * 15.0
        edge_score = round(model_win_pct - market_implied_pct, 2)

    # Clean up temp keys
    player.pop("_career_cut_pct", None)
    player.pop("_recent_cut_pct", None)

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
            "ESPN API (leaderboard fallback)",
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
    bdl_odds = {}
    bdl_props = {}
    bdl_field = []

    if BDL_API_KEY:
        bdl_tournament = bdl_get_current_tournament()

        if bdl_tournament:
            tid = bdl_tournament["id"]

            # Get tournament field
            bdl_field = bdl_get_tournament_field(tid)

            # Get futures odds (outright winner)
            bdl_odds = bdl_get_futures_odds(tid)

            # Get player props (if tournament is upcoming/in-progress)
            if bdl_tournament.get("status") in ("NOT_STARTED", "IN_PROGRESS"):
                bdl_props = bdl_get_player_props(tid)

            # Get results/leaderboard if in progress or completed
            if bdl_tournament.get("status") in ("IN_PROGRESS", "COMPLETED"):
                bdl_results = bdl_get_tournament_results(tid)
            else:
                bdl_results = []

            # Build currentEvent from BDL
            output["currentEvent"] = {
                "name": bdl_tournament.get("name", ""),
                "course": bdl_tournament.get("course_name", ""),
                "startDate": bdl_tournament.get("start_date", ""),
                "status": bdl_tournament.get("status", ""),
                "city": bdl_tournament.get("city", ""),
                "state": bdl_tournament.get("state", ""),
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

    # ============================================================
    # MERGE PLAYER DATA
    # ============================================================
    fallback = get_fallback_players()

    if dg_players and len(dg_players) > 5:
        merged = []
        for dg in dg_players:
            fb = next((f for f in fallback if f["name"].lower() == dg["name"].lower()), None)
            if fb:
                player = dict(fb)
                player["sgTotal"] = dg["sgTotal"] if dg["sgTotal"] else fb["sgTotal"]
                player["sgOtt"] = dg["sgOtt"] if dg["sgOtt"] else fb["sgOtt"]
                player["sgApp"] = dg["sgApp"] if dg["sgApp"] else fb["sgApp"]
                player["sgArg"] = dg["sgArg"] if dg["sgArg"] else fb["sgArg"]
                player["sgPutt"] = dg["sgPutt"] if dg["sgPutt"] else fb["sgPutt"]
                player["rank"] = dg["rank"]
                merged.append(player)
            else:
                dg["birdieAvg"] = 4.0
                dg["bogeyAvg"] = 2.3
                dg["scoringAvg"] = 70.5
                dg["gir"] = 66.0
                dg["fairways"] = 62.0
                dg["scramble"] = 58.0
                dg["proxAvg"] = 34.0
                dg["missDir"] = "neutral"
                dg["flight"] = "neutral"
                dg["courseFit"] = {}
                dg["notes"] = "Auto-scraped player."
                merged.append(dg)
        output["players"] = merged
        print(f"\n  Merged {len(merged)} players (scraped + curated)")
    else:
        output["players"] = fallback
        print(f"\n  Using fallback data for {len(fallback)} players")

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
        for player in output["players"]:
            odds = bdl_odds.get(player["name"])
            if not odds:
                for oname, odata in bdl_odds.items():
                    if player["name"].lower() in oname.lower() or oname.lower() in player["name"].lower():
                        odds = odata
                        break
            if odds:
                player["odds"] = odds

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
        for player in output["players"]:
            api_odds = odds_api_data.get(player["name"])
            if not api_odds:
                # Fuzzy match
                for oname, odata in odds_api_data.items():
                    if player["name"].lower() in oname.lower() or oname.lower() in player["name"].lower():
                        api_odds = odata
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
    # CONFIDENCE SCORE MODEL
    # ============================================================
    print("\n  Calculating PropsBot Confidence Scores...")
    for player in output["players"]:
        try:
            conf, edge = calculate_player_confidence_score(
                player,
                output["players"],
                course_key=match_venue_to_course(
                    output.get("currentEvent", {}).get("course", ""),
                    output.get("currentEvent", {}).get("name", "")
                ) or "augusta"
            )
            player["confScore"] = conf
            if edge is not None:
                player["edgeScore"] = edge
        except Exception as e:
            player["confScore"] = 50
            player["edgeScore"] = 0
            print(f"    confScore error for {player.get('name','?')}: {e}")

    # Sort players by confScore descending
    output["players"].sort(key=lambda p: p.get("confScore", 0), reverse=True)
    print(f"  Confidence scores calculated for {len(output['players'])} players")

    # ============================================================
    # EV SCORING
    # ============================================================
    for player in output["players"]:
        # Calculate EV score if we have odds
        best_odds = player.get("odds", {})
        if best_odds:
            dk_odds = best_odds.get("dk") or best_odds.get("fd") or best_odds.get("mgm")
            if dk_odds and player.get("confScore"):
                # Use confScore as our model probability proxy
                # confScore 100 ≈ 15% win prob, confScore 50 ≈ 7.5%, confScore 10 ≈ 1.5%
                model_win_pct = (player["confScore"] / 100) * 15.0
                player["evScore"] = calculate_ev_score(dk_odds, model_win_pct)

    # ============================================================
    # MASTERS INTELLIGENCE
    # ============================================================
    masters_intel = bdl_build_masters_intel()
    if masters_intel:
        output["mastersIntel"] = masters_intel

    # ============================================================
    # TEE TIMES
    # ============================================================
    output["teeTimes"] = fetch_tee_times()

    # ============================================================
    # ODDS MOVEMENT
    # ============================================================
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output["oddsMovement"] = compute_odds_movement(output["players"], base_dir)

    # ============================================================
    # CUT LINE PREDICTION
    # ============================================================
    output["cutPrediction"] = predict_cut_line(
        output["players"],
        match_venue_to_course(
            output.get("currentEvent", {}).get("course", ""),
            output.get("currentEvent", {}).get("name", "")
        ) or "augusta"
    )

    # ============================================================
    # PLAYER NEWS
    # ============================================================
    output["news"] = fetch_player_news()

    # ============================================================
    # CATEGORIZED PROPS
    # ============================================================
    if bdl_props:
        output["propsByType"] = parse_props_by_type(bdl_props)
    else:
        output["propsByType"] = {"top5": {}, "top10": {}, "top20": {}, "r1Leader": {}, "makeCut": {}}

    # ---- WRITE OUTPUT ----
    output_path = os.path.join(base_dir, OUTPUT_FILE)
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    # ---- ARCHIVE ----
    archive_data(output, base_dir)

    # ---- DISCORD ALERTS ----
    send_discord_alerts(output)

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
