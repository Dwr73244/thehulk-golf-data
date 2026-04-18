"""
Tournament preview page generator.

Reads golf-data.json and emits one SEO-optimized static HTML preview per
tournament slug to docs/previews/<slug>.html, plus an index at
docs/previews/index.html and an updated docs/sitemap.xml.

Intended to be invoked from .github/workflows/weekly-scrape.yml immediately
after the scraper produces golf-data.json and before the "Copy HTML" step.

No external dependencies — stdlib only.
"""

from __future__ import annotations

import html
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "golf-data.json"
DOCS_DIR = ROOT / "docs"
PREVIEWS_DIR = ROOT / "previews"  # built locally; workflow copies into docs/
SITE_URL = "https://golf.propsbot.ai"
OG_IMAGE = f"{SITE_URL}/og-image.png"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def slugify(text: str) -> str:
    t = re.sub(r"[^a-zA-Z0-9]+", "-", (text or "").strip().lower())
    return re.sub(r"-+", "-", t).strip("-") or "event"


def esc(v) -> str:
    return html.escape("" if v is None else str(v), quote=True)


def american_to_implied(odds) -> float | None:
    """Convert American odds string to implied probability percentage."""
    if odds in (None, "", "N/A"):
        return None
    try:
        s = str(odds).replace("+", "")
        n = float(s)
    except (ValueError, TypeError):
        return None
    if n == 0:
        return None
    if n > 0:
        return 100.0 / (n + 100.0) * 100.0
    return (-n) / ((-n) + 100.0) * 100.0


def fmt_date(iso_str: str) -> str:
    if not iso_str:
        return "TBD"
    try:
        # Accept both ISO with Z and plain date
        s = iso_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.strftime("%b %d, %Y")
    except Exception:
        return iso_str[:10]


def date_range(start: str, end: str | None) -> str:
    a = fmt_date(start)
    b = fmt_date(end) if end else ""
    if a and b and a != b:
        return f"{a} – {b}"
    return a


def course_info(course_key: str | None, course_name: str | None, courses: dict) -> dict:
    """Look up par/yards for a course from the courses index."""
    if course_key and course_key in courses:
        return courses[course_key]
    # Fallback: match by name
    if course_name:
        name_low = course_name.lower().strip()
        for k, v in courses.items():
            if v.get("name", "").lower().strip() == name_low:
                return v
    return {}


def confscore_of(player: dict) -> float:
    return float(player.get("confScore") or 0)


def make_cut_score(player: dict) -> float:
    ps = player.get("propScores") or {}
    v = ps.get("makeCut")
    return float(v) if v is not None else 0.0


def edge_of(player: dict) -> float:
    """Positive edge = model thinks player is underpriced vs book."""
    anomalies = player.get("anomalies") or []
    best = None
    for a in anomalies:
        ep = a.get("edge_pct")
        if ep is None:
            continue
        if best is None or ep > best:
            best = ep
    if best is not None:
        return float(best)
    # Fallback: compute model win% implied via confScore vs implied from odds
    odds = ((player.get("odds") or {}).get("dk")) or ((player.get("odds") or {}).get("fd"))
    imp = american_to_implied(odds)
    if imp is None:
        return -999.0
    # Rough proxy: scale confScore to a pseudo-probability
    model_pct = confscore_of(player) / 100.0 * 20.0  # conservative
    return model_pct - imp


# ---------------------------------------------------------------------------
# Reasoning strings
# ---------------------------------------------------------------------------

def reason_top_pick(p: dict, course_key: str | None) -> str:
    bits = []
    sg = p.get("sgTotal")
    if sg is not None:
        bits.append(f"SG {sg:+.2f}")
    fit = (p.get("courseFit") or {}).get(course_key) if course_key else None
    if fit is not None:
        bits.append(f"course fit {fit}")
    rf = p.get("recentForm") or {}
    if rf.get("l5AvgFinish") is not None:
        bits.append(f"L5 avg finish {rf['l5AvgFinish']:.1f}")
    odds = (p.get("odds") or {}).get("dk")
    if odds:
        bits.append(f"DK {odds}")
    return " · ".join(bits) if bits else p.get("notes", "")


def reason_value(p: dict) -> str:
    edge = edge_of(p)
    odds = (p.get("odds") or {}).get("dk", "N/A")
    imp = american_to_implied(odds)
    parts = [f"DK {odds}"]
    if imp is not None:
        parts.append(f"book implied {imp:.1f}%")
    parts.append(f"model edge {edge:+.1f}%")
    conf = p.get("confScore")
    if conf is not None:
        parts.append(f"conf {conf:.1f}")
    return " · ".join(parts)


def reason_make_cut(p: dict) -> str:
    mc = make_cut_score(p)
    rf = p.get("recentForm") or {}
    l5 = rf.get("l5McPct")
    parts = [f"makeCut score {mc:.0f}"]
    if l5 is not None:
        parts.append(f"L5 made-cut {l5:.0f}%")
    pct = p.get("makeCutPct")
    if pct is not None:
        parts.append(f"season {pct:.0f}%")
    return " · ".join(parts)


# ---------------------------------------------------------------------------
# Event collection
# ---------------------------------------------------------------------------

def _extract_forecast(ce: dict, data: dict) -> list:
    """currentEvent.weather may be a list already, or absent; top-level weather is {course, forecast}."""
    w = ce.get("weather")
    if isinstance(w, list):
        return w
    if isinstance(w, dict):
        return w.get("forecast") or []
    top = data.get("weather") or {}
    if isinstance(top, dict):
        return top.get("forecast") or []
    if isinstance(top, list):
        return top
    return []


def collect_events(data: dict) -> list[dict]:
    """Collect every tournament we know about: current + majors schedule."""
    events: list[dict] = []
    seen_slugs: set[str] = set()
    courses = data.get("courses") or {}

    ce = data.get("currentEvent") or {}
    if ce.get("name"):
        slug = slugify(ce["name"])
        # Match weather/course key
        course_key = (data.get("weather") or {}).get("course")
        info = course_info(course_key, ce.get("course"), courses)
        events.append({
            "slug": slug,
            "name": ce.get("name"),
            "course": ce.get("course") or info.get("name") or "TBD",
            "course_key": course_key,
            "city": ce.get("city") or "",
            "state": ce.get("state") or "",
            "startDate": ce.get("startDate") or "",
            "endDate": ce.get("endDate") or "",
            "par": info.get("par"),
            "yards": info.get("yards"),
            "status": ce.get("status") or "UPCOMING",
            "weather": _extract_forecast(ce, data),
            "isCurrent": True,
        })
        seen_slugs.add(slug)

    for m in data.get("majorsSchedule") or []:
        slug = slugify(m.get("name", ""))
        if not slug or slug in seen_slugs:
            continue
        ck = m.get("course_key")
        info = course_info(ck, m.get("venue"), courses)
        events.append({
            "slug": slug,
            "name": m.get("name"),
            "course": m.get("venue") or info.get("name") or "TBD",
            "course_key": ck,
            "city": m.get("city") or "",
            "state": m.get("state") or "",
            "startDate": m.get("startDate") or "",
            "endDate": m.get("endDate") or "",
            "par": m.get("par") or info.get("par"),
            "yards": m.get("yards") or info.get("yards"),
            "status": m.get("status") or "UPCOMING",
            "weather": [],
            "isCurrent": False,
        })
        seen_slugs.add(slug)

    return events


# ---------------------------------------------------------------------------
# Section renderers
# ---------------------------------------------------------------------------

CSS = """
:root {
  --bg: #070e1a;
  --bg-2: #0b1525;
  --teal: #15ffc2;
  --ink: #e6edf7;
  --muted: #8a97ad;
  --line: rgba(255,255,255,0.08);
}
* { box-sizing: border-box; }
html,body { margin:0; padding:0; background:var(--bg); color:var(--ink);
  font-family: 'Inter', system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
  line-height:1.55; font-size:15px; }
a { color: var(--teal); text-decoration: none; }
a:hover { text-decoration: underline; }
.mono { font-family: 'JetBrains Mono', ui-monospace, Menlo, monospace; }
.wrap { max-width: 1120px; margin: 0 auto; padding: 32px 20px 64px; }
header.top { display:flex; align-items:center; justify-content:space-between; margin-bottom:22px; }
header.top .brand { font-weight:700; letter-spacing:0.02em; }
header.top .brand b { color: var(--teal); }
nav.crumbs { font-size:12px; color:var(--muted); }
nav.crumbs a { color: var(--muted); }
.glass { background: linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.015));
  border: 1px solid var(--line); border-radius: 18px; padding: 20px; backdrop-filter: blur(8px); }
.rounded-2xl { border-radius: 18px; }
.banner { display:grid; grid-template-columns: 1.4fr 1fr; gap: 18px; margin-bottom: 22px; }
.banner h1 { margin:0 0 6px; font-size: 30px; letter-spacing:-0.01em; }
.banner .sub { color: var(--muted); font-size: 14px; }
.stats { display:grid; grid-template-columns: repeat(3, 1fr); gap:10px; margin-top:14px; }
.stat { background: rgba(21,255,194,0.05); border:1px solid rgba(21,255,194,0.18); border-radius:12px; padding:10px 12px; }
.stat .k { font-size: 10px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em; }
.stat .v { font-family: 'JetBrains Mono', monospace; font-size: 16px; color: var(--teal); margin-top:2px; }
.grid-2 { display:grid; grid-template-columns: 1fr 1fr; gap:18px; }
.grid-3 { display:grid; grid-template-columns: repeat(3, 1fr); gap:18px; }
@media (max-width:820px) { .banner, .grid-2, .grid-3 { grid-template-columns: 1fr; } .stats { grid-template-columns: repeat(2, 1fr); } }
h2 { font-size: 18px; margin: 0 0 12px; letter-spacing: 0.01em; }
h2 .badge { font-size:10px; color:var(--teal); background:rgba(21,255,194,0.08);
  border:1px solid rgba(21,255,194,0.25); padding:2px 8px; border-radius:999px; margin-left:8px;
  font-family: 'JetBrains Mono', monospace; letter-spacing:0.05em; }
.pick { display:grid; grid-template-columns: 26px 1fr auto; gap:10px; align-items:center;
  padding:10px 0; border-bottom:1px dashed var(--line); }
.pick:last-child { border-bottom:none; }
.pick .rank { font-family:'JetBrains Mono', monospace; color: var(--teal); font-size:13px; }
.pick .name { font-weight:600; }
.pick .why { color: var(--muted); font-size: 12.5px; font-family:'JetBrains Mono', monospace; }
.pick .side { color: var(--teal); font-family:'JetBrains Mono', monospace; font-size:13px; }
.weather-row { display:grid; grid-template-columns: repeat(6, 1fr); gap:6px;
  font-family:'JetBrains Mono', monospace; font-size:12px; padding:8px 0; border-bottom:1px dashed var(--line); }
.weather-row .lbl { color: var(--muted); text-transform: uppercase; letter-spacing: 0.06em; font-size: 10px; }
.matchup { padding:10px 0; border-bottom:1px dashed var(--line); font-family:'JetBrains Mono', monospace; font-size:13px; }
.matchup:last-child { border-bottom:none; }
.matchup .m-meta { color: var(--muted); font-size: 11px; margin-top:2px; }
footer.bottom { margin-top:40px; color: var(--muted); font-size:12px; text-align:center;
  border-top: 1px solid var(--line); padding-top: 18px; }
.list-events .row { display:grid; grid-template-columns: 1fr auto auto; gap:12px; padding:12px 0;
  border-bottom:1px dashed var(--line); align-items:center; }
.list-events .row:last-child { border-bottom:none; }
.list-events .row .when { color: var(--muted); font-family:'JetBrains Mono', monospace; font-size:12px; }
.list-events .row .tag { font-size: 10px; padding:2px 8px; border-radius: 999px;
  border:1px solid rgba(21,255,194,0.3); color: var(--teal); background: rgba(21,255,194,0.05);
  font-family:'JetBrains Mono', monospace; }
.tag.past { color: var(--muted); border-color: var(--line); background: transparent; }
.notice { font-size:11px; color:var(--muted); margin-top:6px; }
"""


def head(title: str, description: str, canonical: str, og_title: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>{esc(title)}</title>
<meta name="description" content="{esc(description)}" />
<meta name="theme-color" content="#070e1a" />
<link rel="canonical" href="{esc(canonical)}" />
<meta property="og:type" content="article" />
<meta property="og:title" content="{esc(og_title)}" />
<meta property="og:description" content="{esc(description)}" />
<meta property="og:image" content="{esc(OG_IMAGE)}" />
<meta property="og:url" content="{esc(canonical)}" />
<meta property="og:site_name" content="PropsBot Golf Intelligence" />
<meta name="twitter:card" content="summary_large_image" />
<meta name="twitter:title" content="{esc(og_title)}" />
<meta name="twitter:description" content="{esc(description)}" />
<meta name="twitter:image" content="{esc(OG_IMAGE)}" />
<link rel="preconnect" href="https://fonts.googleapis.com" />
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet" />
<style>{CSS}</style>
</head>
<body>
<div class="wrap">
<header class="top">
  <div class="brand">PropsBot <b>Golf</b> Intelligence</div>
  <nav class="crumbs"><a href="/">Home</a> · <a href="/previews/">Previews</a></nav>
</header>
"""


def render_banner(e: dict) -> str:
    par = esc(e.get("par") if e.get("par") is not None else "TBD")
    yards = esc(f"{e['yards']:,}") if isinstance(e.get("yards"), (int, float)) else "TBD"
    loc = ", ".join([p for p in [e.get("city"), e.get("state")] if p]) or "TBD"
    dates = date_range(e.get("startDate"), e.get("endDate"))
    defender = e.get("defendingChamp") or "TBD"
    return f"""
<section class="banner">
  <div class="glass rounded-2xl">
    <div class="sub">TOURNAMENT PREVIEW · {esc(e.get('status') or 'UPCOMING')}</div>
    <h1>{esc(e.get('name'))}</h1>
    <div class="sub">{esc(e.get('course'))} · {esc(loc)}</div>
    <div class="sub mono" style="margin-top:6px;">{esc(dates)}</div>
    <div class="stats">
      <div class="stat"><div class="k">Par</div><div class="v">{par}</div></div>
      <div class="stat"><div class="k">Yardage</div><div class="v">{yards}</div></div>
      <div class="stat"><div class="k">Defending Champ</div><div class="v">{esc(defender)}</div></div>
    </div>
  </div>
  <div class="glass rounded-2xl">
    <h2>About this preview</h2>
    <p style="color:var(--muted); font-size:13.5px; margin:0;">Generated from PropsBot's PGA model: strokes-gained, course fit, recent form, book odds, and 10k-sim matchup projections. Numbers refresh every scrape cycle.</p>
  </div>
</section>
"""


def render_top_picks(players: list[dict], course_key: str | None) -> str:
    picks = sorted(players, key=confscore_of, reverse=True)[:5]
    rows = []
    for i, p in enumerate(picks, 1):
        rows.append(
            f'<div class="pick"><div class="rank">#{i}</div>'
            f'<div><div class="name">{esc(p.get("name"))}</div>'
            f'<div class="why">{esc(reason_top_pick(p, course_key))}</div></div>'
            f'<div class="side">{esc((p.get("odds") or {}).get("dk","N/A"))}</div></div>'
        )
    return (
        '<div class="glass rounded-2xl"><h2>Top Picks <span class="badge">confScore</span></h2>'
        + "".join(rows) + "</div>"
    )


def render_value_plays(players: list[dict]) -> str:
    # Positive edge players, sort by edge desc
    valued = [p for p in players if edge_of(p) > -900]
    valued.sort(key=edge_of, reverse=True)
    top = valued[:5]
    rows = []
    for i, p in enumerate(top, 1):
        rows.append(
            f'<div class="pick"><div class="rank">V{i}</div>'
            f'<div><div class="name">{esc(p.get("name"))}</div>'
            f'<div class="why">{esc(reason_value(p))}</div></div>'
            f'<div class="side">{edge_of(p):+.1f}%</div></div>'
        )
    return (
        '<div class="glass rounded-2xl"><h2>Value Plays <span class="badge">edge</span></h2>'
        + ("".join(rows) or '<div class="notice">No value edges detected this cycle.</div>')
        + "</div>"
    )


def render_make_cut(players: list[dict]) -> str:
    cands = [p for p in players if make_cut_score(p) > 0]
    cands.sort(key=make_cut_score, reverse=True)
    top = cands[:5]
    rows = []
    for i, p in enumerate(top, 1):
        rows.append(
            f'<div class="pick"><div class="rank">MC{i}</div>'
            f'<div><div class="name">{esc(p.get("name"))}</div>'
            f'<div class="why">{esc(reason_make_cut(p))}</div></div>'
            f'<div class="side">{make_cut_score(p):.0f}</div></div>'
        )
    return (
        '<div class="glass rounded-2xl"><h2>Make-Cut Picks <span class="badge">makeCut score</span></h2>'
        + ("".join(rows) or '<div class="notice">No cut picks available.</div>')
        + "</div>"
    )


def render_matchups(three_balls: list[dict]) -> str:
    scored = []
    for tb in three_balls:
        players = tb.get("players") or []
        if len(players) < 2:
            continue
        probs = [pl.get("pClearWin") for pl in players if isinstance(pl.get("pClearWin"), (int, float))]
        if len(probs) < 2:
            continue
        probs.sort(reverse=True)
        spread = probs[0] - probs[1]
        scored.append((spread, tb))
    scored.sort(key=lambda x: x[0], reverse=True)

    rows = []
    for spread, tb in scored[:3]:
        names = " vs ".join(esc(pl.get("name", "?")) for pl in tb.get("players", []))
        meta = f"R{tb.get('round','?')} · {esc(tb.get('type','matchup'))} · edge spread {spread*100:.1f} pts"
        top = max(tb["players"], key=lambda x: x.get("pClearWin") or 0)
        top_line = (
            f'pick {esc(top.get("name"))} · model win {float(top.get("pClearWin") or 0)*100:.1f}% · '
            f'fair {esc(top.get("fairOdds"))}'
        )
        rows.append(
            f'<div class="matchup"><div>{names}</div>'
            f'<div class="m-meta">{meta}</div>'
            f'<div class="m-meta">{top_line}</div></div>'
        )
    return (
        '<div class="glass rounded-2xl"><h2>Key Matchups <span class="badge">win-prob spread</span></h2>'
        + ("".join(rows) or '<div class="notice">No matchup markets posted yet.</div>')
        + "</div>"
    )


def render_weather(forecast: list[dict]) -> str:
    if not forecast:
        return (
            '<div class="glass rounded-2xl"><h2>Weather</h2>'
            '<div class="notice">Forecast not yet available.</div></div>'
        )
    rows = []
    rows.append(
        '<div class="weather-row">'
        '<div class="lbl">Date</div>'
        '<div class="lbl">High °F</div>'
        '<div class="lbl">Low °F</div>'
        '<div class="lbl">Wind avg</div>'
        '<div class="lbl">Wind max</div>'
        '<div class="lbl">Rain %</div>'
        "</div>"
    )
    for d in forecast[:5]:
        rows.append(
            f'<div class="weather-row">'
            f'<div>{esc(d.get("date",""))}</div>'
            f'<div>{esc(d.get("tempHigh","-"))}</div>'
            f'<div>{esc(d.get("tempLow","-"))}</div>'
            f'<div>{esc(d.get("windAvg","-"))} mph</div>'
            f'<div>{esc(d.get("windMax","-"))} mph</div>'
            f'<div>{esc(d.get("rainPct","-"))}%</div>'
            f'</div>'
        )
    return '<div class="glass rounded-2xl"><h2>Weather</h2>' + "".join(rows) + "</div>"


def render_jsonld(event: dict) -> str:
    loc_name = event.get("course") or "TBD"
    locality = event.get("city") or ""
    region = event.get("state") or ""
    data = {
        "@context": "https://schema.org",
        "@type": "SportsEvent",
        "name": event.get("name"),
        "startDate": event.get("startDate"),
        "endDate": event.get("endDate") or event.get("startDate"),
        "eventStatus": "https://schema.org/EventScheduled",
        "sport": "Golf",
        "location": {
            "@type": "Place",
            "name": loc_name,
            "address": {
                "@type": "PostalAddress",
                "addressLocality": locality,
                "addressRegion": region,
                "addressCountry": "US",
            },
        },
        "organizer": {
            "@type": "Organization",
            "name": "PGA TOUR",
            "url": "https://www.pgatour.com/",
        },
        "url": f"{SITE_URL}/previews/{event['slug']}.html",
    }
    return (
        '<script type="application/ld+json">'
        + json.dumps(data, ensure_ascii=False)
        + "</script>"
    )


def render_footer(generated_at: str) -> str:
    return f"""
<footer class="bottom">
  Updated {esc(generated_at)}. Not betting advice. 21+.
  Data via PropsBot Golf Scraper · <a href="/methodology">methodology</a> · <a href="/">home</a>.
</footer>
</div></body></html>
"""


# ---------------------------------------------------------------------------
# Page builders
# ---------------------------------------------------------------------------

def build_preview_page(event: dict, players: list[dict], three_balls: list[dict],
                       generated_at: str) -> str:
    title = f"{event['name']} Preview — Top Picks, Value Plays & Matchups | PropsBot Golf"
    loc = ", ".join([p for p in [event.get("city"), event.get("state")] if p]) or "PGA TOUR"
    description = (
        f"Model-driven preview for {event['name']} at {event.get('course','TBD')} ({loc}). "
        f"Top 5 picks by confScore, value plays by market edge, key matchups, make-cut picks, and weather."
    )
    canonical = f"{SITE_URL}/previews/{event['slug']}.html"

    # Filter players in the field (if currentEvent) — otherwise use all scored players
    field_players = [p for p in players if (p.get("fieldInfo") or {}).get("inField") is not False]
    if not field_players:
        field_players = players

    body = (
        head(title, description, canonical, f"{event['name']} — PropsBot Preview")
        + render_banner(event)
        + '<div class="grid-2">'
        + render_top_picks(field_players, event.get("course_key"))
        + render_value_plays(field_players)
        + "</div><div style='height:18px'></div><div class='grid-2'>"
        + render_matchups(three_balls if event.get("isCurrent") else [])
        + render_make_cut(field_players)
        + "</div><div style='height:18px'></div>"
        + render_weather(event.get("weather") or [])
        + render_jsonld(event)
        + render_footer(generated_at)
    )
    return body


def build_index_page(events: list[dict], generated_at: str) -> str:
    # Sort: current first (most recent startDate desc overall)
    def key(e):
        return (0 if e.get("isCurrent") else 1, -_date_epoch(e.get("startDate")))

    ordered = sorted(events, key=key)
    rows = []
    for e in ordered:
        tag = "CURRENT" if e.get("isCurrent") else (e.get("status") or "UPCOMING")
        tag_class = "tag" if (e.get("isCurrent") or tag == "UPCOMING" or tag == "NOT_STARTED") else "tag past"
        rows.append(
            f'<div class="row">'
            f'<div><a href="./{esc(e["slug"])}.html"><strong>{esc(e["name"])}</strong></a>'
            f'<div class="when">{esc(e.get("course") or "")} · {esc(date_range(e.get("startDate"), e.get("endDate")))}</div></div>'
            f'<div class="when">{esc((e.get("city") or "") + (", " + e["state"] if e.get("state") else ""))}</div>'
            f'<div class="{tag_class}">{esc(tag)}</div>'
            f"</div>"
        )

    canonical = f"{SITE_URL}/previews/"
    title = "Tournament Previews — PropsBot Golf Intelligence"
    description = "Weekly model-driven PGA TOUR tournament previews: top picks, value plays, matchups, make-cut picks, and weather."

    return (
        head(title, description, canonical, "PropsBot Golf Tournament Previews")
        + '<section class="glass rounded-2xl">'
        + '<h2>Tournament Previews</h2>'
        + '<div class="list-events">'
        + "".join(rows)
        + '</div></section>'
        + render_footer(generated_at)
    )


def _date_epoch(s: str) -> float:
    try:
        return datetime.fromisoformat((s or "").replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Sitemap
# ---------------------------------------------------------------------------

def build_sitemap(events: list[dict]) -> str:
    today = datetime.now(timezone.utc).date()
    lines = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']

    def entry(loc: str, priority: str, changefreq: str, lastmod: str | None = None) -> str:
        parts = [f"  <url>", f"    <loc>{loc}</loc>"]
        if lastmod:
            parts.append(f"    <lastmod>{lastmod}</lastmod>")
        parts.append(f"    <changefreq>{changefreq}</changefreq>")
        parts.append(f"    <priority>{priority}</priority>")
        parts.append("  </url>")
        return "\n".join(parts)

    today_str = today.isoformat()
    lines.append(entry(f"{SITE_URL}/", "1.0", "hourly", today_str))
    lines.append(entry(f"{SITE_URL}/methodology.html", "0.6", "weekly", today_str))
    lines.append(entry(f"{SITE_URL}/calculator.html", "0.6", "weekly", today_str))
    lines.append(entry(f"{SITE_URL}/api.html", "0.5", "weekly", today_str))
    lines.append(entry(f"{SITE_URL}/previews/", "0.9", "daily", today_str))

    for e in events:
        loc = f"{SITE_URL}/previews/{e['slug']}.html"
        is_past = False
        try:
            end = e.get("endDate") or e.get("startDate")
            if end:
                end_date = datetime.fromisoformat(str(end).replace("Z", "+00:00")).date()
                is_past = end_date < today and not e.get("isCurrent")
        except Exception:
            is_past = False
        if e.get("isCurrent"):
            lines.append(entry(loc, "0.8", "daily", today_str))
        elif is_past:
            lines.append(entry(loc, "0.3", "monthly", today_str))
        else:
            lines.append(entry(loc, "0.8", "daily", today_str))

    lines.append("</urlset>")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not DATA_PATH.exists():
        raise SystemExit(f"golf-data.json not found at {DATA_PATH}")

    with DATA_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    players = data.get("players") or []
    three_balls = data.get("threeBalls") or []
    generated_at = data.get("generatedAt") or datetime.now(timezone.utc).isoformat()

    events = collect_events(data)
    if not events:
        raise SystemExit("No events found in golf-data.json")

    PREVIEWS_DIR.mkdir(parents=True, exist_ok=True)

    written = []
    for e in events:
        page = build_preview_page(e, players, three_balls, generated_at)
        out_path = PREVIEWS_DIR / f"{e['slug']}.html"
        out_path.write_text(page, encoding="utf-8")
        written.append(out_path)

    idx_path = PREVIEWS_DIR / "index.html"
    idx_path.write_text(build_index_page(events, generated_at), encoding="utf-8")
    written.append(idx_path)

    # Sitemap (committed at repo root + copied to docs by workflow)
    sitemap_xml = build_sitemap(events)
    (ROOT / "sitemap.xml").write_text(sitemap_xml, encoding="utf-8")
    # Also mirror to docs/ if it exists (so local `python scripts/generate_preview.py`
    # before workflow also produces a preview of the deployed sitemap).
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    (DOCS_DIR / "sitemap.xml").write_text(sitemap_xml, encoding="utf-8")

    print(f"[generate_preview] wrote {len(written)} files to {PREVIEWS_DIR}")
    for p in written:
        print(f"  - {p.relative_to(ROOT)}")
    print(f"[generate_preview] sitemap: {len(events) + 5} url entries")


if __name__ == "__main__":
    main()
