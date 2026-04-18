#!/usr/bin/env python3
"""
Generate per-player HTML pages at /player/<slug>.html for long-tail SEO.

Reads golf-data.json + history/*.json and writes:
  - docs/player/<slug>.html    (one per player)
  - docs/player/index.html     (listing of all players)
  - appends <url> entries to sitemap.xml

These pages are SEPARATE static routes — they do NOT share code with
the main app's in-app Player detail panel (owned by another dev).
"""

from __future__ import annotations

import datetime as dt
import glob
import html
import json
import os
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
DATA_FILE = ROOT / "golf-data.json"
HISTORY_DIR = ROOT / "history"
OUT_DIR = ROOT / "player"  # will be copied into docs/ by workflow
SITEMAP_FILE = ROOT / "sitemap.xml"
SITE_BASE = "https://golf.propsbot.ai"

MAX_HISTORY_EVENTS = 12


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def slugify(name: str) -> str:
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    name = name.lower().strip()
    name = re.sub(r"[^a-z0-9]+", "-", name)
    return name.strip("-") or "player"


def esc(v: Any) -> str:
    if v is None:
        return ""
    return html.escape(str(v), quote=True)


def fmt(v: Any, nd: int = 2, dash: str = "—") -> str:
    if v is None or v == "":
        return dash
    try:
        f = float(v)
        if f != f:  # nan
            return dash
        return f"{f:.{nd}f}"
    except (TypeError, ValueError):
        return str(v)


def fmt_int(v: Any, dash: str = "—") -> str:
    if v is None or v == "":
        return dash
    try:
        return str(int(round(float(v))))
    except (TypeError, ValueError):
        return str(v)


def pga_profile_url(name: str) -> str:
    return f"https://www.pgatour.com/players/player.{slugify(name)}"


def load_history() -> list[tuple[str, dict]]:
    """Return list of (date-string, data-dict) sorted newest first."""
    out: list[tuple[str, dict]] = []
    if not HISTORY_DIR.is_dir():
        return out
    for p in sorted(glob.glob(str(HISTORY_DIR / "*.json")), reverse=True):
        try:
            with open(p, encoding="utf-8") as f:
                out.append((Path(p).stem, json.load(f)))
        except (OSError, json.JSONDecodeError):
            continue
    return out


def collect_history_for(name: str, history: list[tuple[str, dict]]) -> list[dict]:
    """Walk history files, pull out each event-day's snapshot for this player.

    Returns rows {date, event, position, score, total} — dedup by event.
    """
    rows: list[dict] = []
    seen_events: set[str] = set()
    for date, snap in history:
        players = snap.get("players") or []
        cur = snap.get("currentEvent") or {}
        event_name = cur.get("name") or ""
        if event_name in seen_events:
            continue
        lb = cur.get("leaderboard") or []
        # leaderboard is richest — position + score
        lb_row = next((r for r in lb if (r.get("name") or "").lower() == name.lower()), None)
        p_row = next((p for p in players if (p.get("name") or "").lower() == name.lower()), None)
        if not p_row and not lb_row:
            continue
        position = (lb_row or {}).get("position") or ""
        score = (lb_row or {}).get("score") or ""
        total = (lb_row or {}).get("totalStrokes") or ""
        if not position and p_row:
            rf = (p_row.get("recentForm") or {})
            lp = rf.get("lastPosition")
            if lp:
                position = f"#{lp}"
        rows.append({
            "date": date,
            "event": event_name or "—",
            "position": position or "—",
            "score": score or "—",
            "total": fmt_int(total),
        })
        if event_name:
            seen_events.add(event_name)
        if len(rows) >= MAX_HISTORY_EVENTS:
            break
    return rows


def derive_headline(p: dict) -> str:
    notes = (p.get("notes") or "").strip()
    if notes:
        # Take first sentence
        first = re.split(r"(?<=[.!?])\s+", notes, maxsplit=1)[0]
        return first
    conf = p.get("confScore")
    if conf is not None:
        return f"PropsBot confidence score {fmt(conf, 1)} / 100."
    return "PGA Tour player profile."


def tee_times_for(name: str, tee_times: list[dict]) -> list[dict]:
    out = []
    for t in tee_times or []:
        if (t.get("player") or "").lower() == name.lower():
            out.append(t)
    out.sort(key=lambda x: x.get("round") or 0)
    return out


def leaderboard_for(name: str, event: dict) -> dict | None:
    if not event:
        return None
    for r in event.get("leaderboard") or []:
        if (r.get("name") or "").lower() == name.lower():
            return r
    return None


# ---------------------------------------------------------------------------
# HTML fragments
# ---------------------------------------------------------------------------

BASE_HEAD_CSS = """
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700;800;900&family=Inter:wght@400;500;600;700;800&display=swap');
:root {
  --mono: 'JetBrains Mono', monospace;
  --sans: 'Inter', -apple-system, sans-serif;
  --teal: #15ffc2;
  --teal-dim: rgba(21,255,194,0.65);
  --gold: #f0c040;
  --red: #ff5975;
  --bg: #070e1a;
  --glass: rgba(18,28,45,0.92);
  --text-primary: #eef2f7;
  --text-secondary: rgba(200,215,230,0.75);
  --text-muted: rgba(150,180,210,0.45);
  --border: rgba(120,160,200,0.12);
}
* { box-sizing: border-box; margin: 0; padding: 0; }
html, body { background: var(--bg); color: var(--text-primary); font-family: var(--sans); line-height: 1.5; }
body { min-height: 100vh; background: radial-gradient(1000px 600px at 20% -10%, rgba(21,255,194,0.06), transparent 60%), radial-gradient(900px 500px at 90% 10%, rgba(34,169,236,0.05), transparent 60%), var(--bg); }
a { color: var(--teal); text-decoration: none; }
a:hover { text-decoration: underline; }
.mono { font-family: var(--mono); }
.wrap { max-width: 1100px; margin: 0 auto; padding: 24px 20px 80px; }
.topbar { display: flex; align-items: center; justify-content: space-between; padding: 14px 20px; border-bottom: 1px solid var(--border); background: linear-gradient(135deg, rgba(9,213,160,0.08), rgba(34,169,236,0.05)); }
.topbar .brand { font-family: var(--mono); font-weight: 800; color: var(--text-primary); font-size: 14px; letter-spacing: 0.03em; }
.topbar .brand span { color: var(--teal); }
.topbar .links a { font-family: var(--mono); font-size: 12px; margin-left: 16px; color: var(--text-secondary); }
.topbar .links a:hover { color: var(--teal); }
.glass { background: var(--glass); backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px); border: 1px solid var(--border); border-radius: 12px; box-shadow: 0 8px 32px rgba(0,0,0,0.45); }
.card { padding: 18px 20px; margin-bottom: 18px; }
.section-label { font-family: var(--mono); font-size: 10px; font-weight: 600; letter-spacing: 0.08em; text-transform: uppercase; color: var(--text-secondary); margin-bottom: 10px; }
h1 { font-size: 34px; font-weight: 800; letter-spacing: -0.01em; }
h2 { font-size: 18px; font-weight: 700; margin-bottom: 4px; }
.headline { color: var(--text-secondary); margin-top: 6px; font-size: 14px; max-width: 780px; }
.chip { display: inline-block; font-family: var(--mono); font-size: 11px; padding: 3px 10px; border-radius: 20px; border: 1px solid rgba(21,255,194,0.25); background: rgba(21,255,194,0.08); color: var(--teal); margin-right: 6px; }
.chip.gold { border-color: rgba(240,192,64,0.3); background: rgba(240,192,64,0.1); color: var(--gold); }
.chip.muted { border-color: var(--border); background: rgba(255,255,255,0.02); color: var(--text-secondary); }
.grid { display: grid; gap: 12px; }
.grid.cols-6 { grid-template-columns: repeat(6, minmax(0,1fr)); }
.grid.cols-5 { grid-template-columns: repeat(5, minmax(0,1fr)); }
.grid.cols-4 { grid-template-columns: repeat(4, minmax(0,1fr)); }
.grid.cols-3 { grid-template-columns: repeat(3, minmax(0,1fr)); }
.grid.cols-2 { grid-template-columns: repeat(2, minmax(0,1fr)); }
@media (max-width: 760px) {
  .grid.cols-6, .grid.cols-5, .grid.cols-4 { grid-template-columns: repeat(2, minmax(0,1fr)); }
  .grid.cols-3 { grid-template-columns: repeat(2, minmax(0,1fr)); }
  h1 { font-size: 26px; }
}
.stat { padding: 12px; border: 1px solid var(--border); border-radius: 10px; background: rgba(255,255,255,0.015); }
.stat .v { font-family: var(--mono); font-size: 20px; font-weight: 700; color: var(--teal); }
.stat .v.muted { color: var(--text-primary); }
.stat .k { font-size: 10px; font-weight: 600; letter-spacing: 0.06em; text-transform: uppercase; color: var(--text-secondary); margin-top: 4px; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th, td { padding: 8px 10px; text-align: left; border-bottom: 1px solid var(--border); }
th { font-family: var(--mono); font-size: 10px; text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-secondary); font-weight: 600; }
td.num, th.num { text-align: right; font-family: var(--mono); }
.bar-row { display: grid; grid-template-columns: 110px 1fr 56px; gap: 10px; align-items: center; margin-bottom: 6px; font-family: var(--mono); font-size: 12px; }
.bar-track { height: 10px; background: rgba(255,255,255,0.04); border-radius: 5px; overflow: hidden; border: 1px solid var(--border); }
.bar-fill { height: 100%; background: linear-gradient(90deg, rgba(21,255,194,0.9), rgba(34,169,236,0.9)); }
.bar-val { text-align: right; color: var(--teal); font-weight: 600; }
.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }
@media (max-width: 760px) { .two-col { grid-template-columns: 1fr; } }
.odds { font-family: var(--mono); font-size: 22px; font-weight: 800; color: var(--gold); }
.footer-note { margin-top: 36px; padding-top: 18px; border-top: 1px solid var(--border); color: var(--text-muted); font-size: 11px; text-align: center; line-height: 1.6; }
.back-link { display: inline-block; font-family: var(--mono); font-size: 12px; color: var(--text-secondary); margin-bottom: 14px; }
.back-link:hover { color: var(--teal); }
.player-list { list-style: none; display: grid; grid-template-columns: repeat(3, minmax(0,1fr)); gap: 8px; }
@media (max-width: 760px) { .player-list { grid-template-columns: 1fr; } }
.player-list li a { display: flex; justify-content: space-between; padding: 10px 12px; border: 1px solid var(--border); border-radius: 8px; background: rgba(255,255,255,0.015); color: var(--text-primary); font-size: 13px; }
.player-list li a:hover { background: rgba(21,255,194,0.06); border-color: rgba(21,255,194,0.35); text-decoration: none; }
.player-list .rnk { font-family: var(--mono); color: var(--teal); font-size: 11px; }
"""


def render_topbar() -> str:
    return (
        '<div class="topbar">'
        '<a href="/" class="brand">PropsBot <span>Golf Intelligence</span></a>'
        '<div class="links">'
        '<a href="/">Main tool</a>'
        '<a href="/methodology">Methodology</a>'
        '<a href="/player/">All players</a>'
        '</div></div>'
    )


def render_stats_grid(p: dict) -> str:
    items = [
        ("SG: Total", fmt(p.get("sgTotal"), 2)),
        ("SG: OTT", fmt(p.get("sgOtt"), 2)),
        ("SG: APP", fmt(p.get("sgApp"), 2)),
        ("SG: ARG", fmt(p.get("sgArg"), 2)),
        ("SG: Putt", fmt(p.get("sgPutt"), 2)),
        ("Rank", fmt_int(p.get("rank"))),
        ("Birdie Avg", fmt(p.get("birdieAvg"), 2)),
        ("Bogey Avg", fmt(p.get("bogeyAvg"), 2)),
        ("Scoring Avg", fmt(p.get("scoringAvg"), 2)),
        ("GIR %", fmt(p.get("gir"), 1)),
        ("Fairways %", fmt(p.get("fairways"), 1)),
        ("Scramble %", fmt(p.get("scramble"), 1)),
    ]
    cells = "".join(
        f'<div class="stat"><div class="v">{esc(v)}</div><div class="k">{esc(k)}</div></div>'
        for k, v in items
    )
    return f'<div class="grid cols-6">{cells}</div>'


def render_this_week(p: dict, event: dict, tee_times: list[dict]) -> str:
    if not event:
        return ""
    lb = leaderboard_for(p.get("name", ""), event)
    my_tt = tee_times_for(p.get("name", ""), tee_times)
    parts = []
    parts.append(
        f'<div style="display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;align-items:baseline;">'
        f'<h2>{esc(event.get("name") or "This Week")}</h2>'
        f'<span class="mono" style="color:var(--text-secondary);font-size:12px;">{esc(event.get("course") or "")}</span>'
        f'</div>'
    )
    chips = []
    if lb:
        chips.append(f'<span class="chip gold">Pos {esc(lb.get("position"))} · {esc(lb.get("score"))}</span>')
    if my_tt:
        rounds = ", ".join(f'R{t.get("round")}: {esc(t.get("teeTime"))}' for t in my_tt[:4])
        chips.append(f'<span class="chip muted">{esc(rounds)}</span>')
    if p.get("fieldInfo", {}).get("entryStatus"):
        chips.append(f'<span class="chip">{esc(p["fieldInfo"]["entryStatus"])}</span>')
    if chips:
        parts.append('<div style="margin-top:10px;">' + "".join(chips) + "</div>")
    if lb:
        rd_row = "".join(
            f"<td class='num'>{fmt_int(lb.get(f'round{i}')) if lb.get(f'round{i}') else '—'}</td>"
            for i in range(1, 5)
        )
        parts.append(
            '<table style="margin-top:14px;">'
            '<thead><tr><th>Thru</th><th class="num">R1</th><th class="num">R2</th><th class="num">R3</th><th class="num">R4</th><th class="num">Total</th></tr></thead>'
            f'<tbody><tr><td>{esc(lb.get("thru") or "—")}</td>{rd_row}<td class="num">{fmt_int(lb.get("totalStrokes"))}</td></tr></tbody>'
            '</table>'
        )
    return '<div class="card glass">' + '<div class="section-label">This Week</div>' + "".join(parts) + "</div>"


def render_odds(p: dict) -> str:
    odds = p.get("odds") or {}
    if not odds:
        return ""
    rows = "".join(
        f'<tr><td class="mono">{esc(k.upper())}</td><td class="num odds">{esc(v)}</td></tr>'
        for k, v in odds.items()
    )
    return (
        '<div class="card glass">'
        '<div class="section-label">Outright Odds</div>'
        '<table>' + rows + '</table>'
        '</div>'
    )


def render_course_fit(p: dict, courses: dict) -> str:
    fit = p.get("courseFit") or {}
    if not fit:
        return ""
    sorted_fit = sorted(fit.items(), key=lambda kv: (kv[1] if isinstance(kv[1], (int, float)) else 0), reverse=True)
    top5 = sorted_fit[:5]
    bot5 = list(reversed(sorted_fit[-5:]))

    def _row(key: str, score: Any) -> str:
        meta = courses.get(key) or {}
        name = meta.get("name") or key.replace("_", " ").title()
        return (
            f'<tr><td>{esc(name)}</td>'
            f'<td class="mono" style="color:var(--text-secondary);font-size:11px;">{esc(meta.get("event") or "")}</td>'
            f'<td class="num">{esc(score)}</td></tr>'
        )

    top_tbl = '<table><thead><tr><th>Course</th><th>Event</th><th class="num">Fit</th></tr></thead><tbody>' + "".join(_row(k, v) for k, v in top5) + "</tbody></table>"
    bot_tbl = '<table><thead><tr><th>Course</th><th>Event</th><th class="num">Fit</th></tr></thead><tbody>' + "".join(_row(k, v) for k, v in bot5) + "</tbody></table>"

    return (
        '<div class="card glass">'
        '<div class="section-label">Course Fit</div>'
        '<div class="two-col">'
        f'<div><h2 style="color:var(--teal);">Top 5 Fits</h2>{top_tbl}</div>'
        f'<div><h2 style="color:var(--red);">Weak Fits</h2>{bot_tbl}</div>'
        '</div></div>'
    )


def render_recent_form(p: dict) -> str:
    rf = p.get("recentForm") or {}
    if not rf:
        return ""
    rows = [
        ("Last 5 avg finish", fmt(rf.get("l5AvgFinish"), 1)),
        ("Last 10 avg finish", fmt(rf.get("l10AvgFinish"), 1)),
        ("Trend", esc(rf.get("trend") or "—")),
        ("Last result", esc(rf.get("lastResult") or "—")),
        ("Events", fmt_int(rf.get("events"))),
        ("Make cut %", fmt(rf.get("l5McPct"), 0)),
    ]
    stats = "".join(f'<div class="stat"><div class="v muted">{v}</div><div class="k">{esc(k)}</div></div>' for k, v in rows)
    extra = ""
    ra = rf.get("roundAvgs") or {}
    if ra:
        ra_items = "".join(
            f'<div class="stat"><div class="v muted">{fmt(ra.get(k), 2)}</div><div class="k">{esc(k.upper())}</div></div>'
            for k in ("r1Avg", "r2Avg", "r3Avg", "r4Avg") if k in ra
        )
        if ra_items:
            extra = f'<div class="grid cols-4" style="margin-top:12px;">{ra_items}</div>'
    return (
        '<div class="card glass">'
        '<div class="section-label">Recent Form</div>'
        f'<div class="grid cols-3">{stats}</div>'
        f'{extra}'
        '</div>'
    )


def render_prop_bars(p: dict) -> str:
    ps = p.get("propScores") or {}
    if not ps:
        return ""
    order = [("win", "Win"), ("top5", "Top 5"), ("top10", "Top 10"), ("top20", "Top 20"), ("makeCut", "Make Cut")]
    rows = []
    for k, label in order:
        if k not in ps:
            continue
        v = ps[k]
        try:
            pct = max(0, min(100, float(v)))
        except (TypeError, ValueError):
            pct = 0
        rows.append(
            f'<div class="bar-row"><div>{esc(label)}</div>'
            f'<div class="bar-track"><div class="bar-fill" style="width:{pct:.0f}%"></div></div>'
            f'<div class="bar-val">{fmt_int(v)}</div></div>'
        )
    if not rows:
        return ""
    return (
        '<div class="card glass">'
        '<div class="section-label">PropsBot Prop Scores (0-100)</div>'
        + "".join(rows) +
        '</div>'
    )


def render_history_table(rows: list[dict]) -> str:
    if not rows:
        return ""
    body = "".join(
        f'<tr><td class="mono">{esc(r["date"])}</td>'
        f'<td>{esc(r["event"])}</td>'
        f'<td>{esc(r["position"])}</td>'
        f'<td class="num">{esc(r["total"])}</td>'
        f'<td class="num">{esc(r["score"])}</td></tr>'
        for r in rows
    )
    return (
        '<div class="card glass">'
        '<div class="section-label">Recent Event History</div>'
        '<table><thead><tr><th>Date</th><th>Event</th><th>Pos</th><th class="num">Strokes</th><th class="num">To Par</th></tr></thead>'
        f'<tbody>{body}</tbody></table>'
        '</div>'
    )


def render_player_page(p: dict, data: dict, history: list[tuple[str, dict]], generated_at: str) -> str:
    name = p.get("name") or "Player"
    slug = slugify(name)
    event = data.get("currentEvent") or {}
    tee_times = data.get("teeTimes") or []
    courses = data.get("courses") or {}

    headline = derive_headline(p)
    conf = p.get("confScore")
    edge = p.get("edgeScore")

    # Meta description packs the top stats
    meta_desc_parts = [
        f"{name} PGA Tour profile.",
        f"SG Total {fmt(p.get('sgTotal'), 2)}, Rank #{fmt_int(p.get('rank'))}.",
    ]
    if conf is not None:
        meta_desc_parts.append(f"PropsBot confidence {fmt(conf, 1)}.")
    if event.get("name"):
        meta_desc_parts.append(f"This week: {event['name']}.")
    meta_desc = " ".join(meta_desc_parts)[:300]

    title = f"{name} — PGA Tour Stats, Odds, Course Fit | PropsBot"
    canonical = f"{SITE_BASE}/player/{slug}.html"

    # JSON-LD Person
    ld = {
        "@context": "https://schema.org",
        "@type": "Person",
        "name": name,
        "jobTitle": "Professional Golfer",
        "url": canonical,
        "sameAs": [pga_profile_url(name)],
    }

    # Stats
    hist_rows = collect_history_for(name, history)

    body_sections = [
        render_topbar(),
        '<div class="wrap">',
        '<a class="back-link" href="/player/">&larr; All players</a>',
        '<div class="card glass">',
        '<div class="section-label">Player Profile</div>',
        f'<h1>{esc(name)}</h1>',
        '<div style="margin-top:10px;">',
    ]
    chips = []
    if p.get("rank"):
        chips.append(f'<span class="chip">Rank #{fmt_int(p.get("rank"))}</span>')
    if p.get("owgr"):
        chips.append(f'<span class="chip muted">OWGR #{fmt_int(p.get("owgr"))}</span>')
    if conf is not None:
        chips.append(f'<span class="chip gold">Confidence {fmt(conf, 1)}</span>')
    if edge is not None:
        try:
            ev = float(edge)
            cls = "chip" if ev >= 0 else "chip muted"
            chips.append(f'<span class="{cls}">Edge {ev:+.1f}%</span>')
        except (TypeError, ValueError):
            pass
    body_sections.append("".join(chips))
    body_sections.append('</div>')
    body_sections.append(f'<p class="headline">{esc(headline)}</p>')
    body_sections.append('</div>')  # close profile card

    body_sections.append('<div class="card glass">')
    body_sections.append('<div class="section-label">Core Stats</div>')
    body_sections.append(render_stats_grid(p))
    body_sections.append('</div>')

    body_sections.append(render_this_week(p, event, tee_times))
    body_sections.append(render_odds(p))
    body_sections.append(render_course_fit(p, courses))
    body_sections.append(render_recent_form(p))
    body_sections.append(render_prop_bars(p))
    body_sections.append(render_history_table(hist_rows))

    # Internal link to current tournament preview page
    if event.get("name"):
        preview_slug = slugify(event["name"])
        body_sections.append(
            '<div class="card glass">'
            '<div class="section-label">Event Preview</div>'
            f'<p>Deeper breakdown of <strong>{esc(event["name"])}</strong>: '
            f'<a href="/previews/{preview_slug}.html">{esc(event["name"])} preview &rarr;</a></p>'
            '<p style="margin-top:6px;color:var(--text-muted);font-size:12px;">'
            'Preview pages are rolling out — link may 404 until that event ships.'
            '</p>'
            '</div>'
        )

    body_sections.append(
        '<div class="footer-note">'
        'PropsBot Golf Intelligence · Educational tool only · Not betting advice · '
        'Not affiliated with any sportsbook or DFS platform.<br>'
        f'Updated {esc(generated_at)}.'
        '</div>'
    )
    body_sections.append('</div>')  # close wrap

    return (
        '<!DOCTYPE html>\n'
        '<html lang="en">\n<head>\n'
        '<meta charset="UTF-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        f'<title>{esc(title)}</title>\n'
        f'<meta name="description" content="{esc(meta_desc)}">\n'
        f'<link rel="canonical" href="{esc(canonical)}">\n'
        f'<meta property="og:title" content="{esc(title)}">\n'
        f'<meta property="og:description" content="{esc(meta_desc)}">\n'
        f'<meta property="og:url" content="{esc(canonical)}">\n'
        '<meta property="og:type" content="profile">\n'
        f'<script type="application/ld+json">{json.dumps(ld)}</script>\n'
        f'<style>{BASE_HEAD_CSS}</style>\n'
        '</head>\n<body>\n'
        + "\n".join(body_sections)
        + '\n</body>\n</html>\n'
    )


def render_index_page(players: list[dict], generated_at: str) -> str:
    title = "All PGA Tour Players — PropsBot Profiles"
    meta_desc = "Browse PropsBot player profiles: stats, odds, course fit, and recent form for every player in the field."
    players_sorted = sorted(players, key=lambda p: (p.get("rank") or 9999, p.get("name") or ""))
    items = "".join(
        f'<li><a href="/player/{slugify(p.get("name",""))}.html">'
        f'<span>{esc(p.get("name") or "?")}</span>'
        f'<span class="rnk">#{fmt_int(p.get("rank"))}</span></a></li>'
        for p in players_sorted
    )
    canonical = f"{SITE_BASE}/player/"
    return (
        '<!DOCTYPE html>\n'
        '<html lang="en">\n<head>\n'
        '<meta charset="UTF-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        f'<title>{esc(title)}</title>\n'
        f'<meta name="description" content="{esc(meta_desc)}">\n'
        f'<link rel="canonical" href="{esc(canonical)}">\n'
        f'<style>{BASE_HEAD_CSS}</style>\n'
        '</head>\n<body>\n'
        + render_topbar()
        + '<div class="wrap">'
        + '<div class="card glass">'
        + '<div class="section-label">Directory</div>'
        + f'<h1>All Players</h1><p class="headline">{len(players_sorted)} players currently tracked. '
          'Click any player for full stats, odds, course fit, and event history.</p>'
        + f'<ul class="player-list" style="margin-top:16px;">{items}</ul>'
        + '</div>'
        + f'<div class="footer-note">Updated {esc(generated_at)} · <a href="/">Back to PropsBot Golf</a></div>'
        + '</div>\n</body>\n</html>\n'
    )


# ---------------------------------------------------------------------------
# Sitemap
# ---------------------------------------------------------------------------

SITEMAP_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>{base}/</loc>
    <changefreq>hourly</changefreq>
    <priority>1.0</priority>
  </url>
  <url>
    <loc>{base}/player/</loc>
    <changefreq>daily</changefreq>
    <priority>0.7</priority>
  </url>
{entries}
</urlset>
"""


def write_sitemap(players: list[dict]) -> int:
    entries = []
    for p in players:
        name = p.get("name")
        if not name:
            continue
        slug = slugify(name)
        entries.append(
            f"  <url>\n"
            f"    <loc>{SITE_BASE}/player/{slug}.html</loc>\n"
            f"    <changefreq>daily</changefreq>\n"
            f"    <priority>0.6</priority>\n"
            f"  </url>"
        )
    xml = SITEMAP_TEMPLATE.format(base=SITE_BASE, entries="\n".join(entries))
    SITEMAP_FILE.write_text(xml, encoding="utf-8")
    return len(entries)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    if not DATA_FILE.exists():
        print(f"[generate_player_pages] golf-data.json not found at {DATA_FILE}", file=sys.stderr)
        return 1
    with open(DATA_FILE, encoding="utf-8") as f:
        data = json.load(f)

    players = data.get("players") or []
    if not players:
        print("[generate_player_pages] no players in golf-data.json", file=sys.stderr)
        return 1

    generated_at = data.get("generatedAt") or dt.datetime.utcnow().isoformat()
    history = load_history()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    written = 0
    slugs_seen: set[str] = set()
    for p in players:
        name = p.get("name")
        if not name:
            continue
        slug = slugify(name)
        # Deduplicate slugs by appending id
        if slug in slugs_seen:
            slug = f"{slug}-{p.get('id') or written}"
        slugs_seen.add(slug)
        html_out = render_player_page(p, data, history, generated_at)
        (OUT_DIR / f"{slug}.html").write_text(html_out, encoding="utf-8")
        written += 1

    # Index page
    (OUT_DIR / "index.html").write_text(render_index_page(players, generated_at), encoding="utf-8")

    # Sitemap
    sitemap_n = write_sitemap(players)

    print(f"[generate_player_pages] wrote {written} player pages + index to {OUT_DIR}")
    print(f"[generate_player_pages] sitemap.xml updated with {sitemap_n} player URLs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
