# PropsBot Golf Intelligence — Data Pipeline

Automated scraping pipeline that pulls free PGA Tour player stats, course data, and scoring info weekly, then publishes a JSON data file consumed by the PropsBot Golf Intelligence frontend.

## Architecture

```
GitHub Actions (weekly cron)
    │
    ├── scraper.py → pulls from free sources
    │     ├── DataGolf.com (rankings, SG breakdowns)
    │     ├── PGATour.com (official stats pages)
    │     └── ESPN API (current leaderboard)
    │
    ├── golf-data.json → structured output
    │
    └── GitHub Pages → hosts JSON at public URL
                          │
                          └── propsbot.ai embeds the HTML tool
                              which fetches golf-data.json on load
```

## Setup (15 minutes)

### Step 1: Create the GitHub Repository

1. Go to [github.com/new](https://github.com/new)
2. Name it `propsbot-golf-data` (or whatever you prefer)
3. Make it **Public** (required for free GitHub Pages)
4. Check "Add a README file"
5. Click "Create repository"

### Step 2: Upload the Pipeline Files

Upload these files to the root of your new repo:

```
propsbot-golf-data/
├── .github/
│   └── workflows/
│       └── weekly-scrape.yml    ← GitHub Actions workflow
├── scraper.py                    ← Python scraper
├── PropsBot-GolfIntel.html       ← The frontend tool
└── README.md                     ← This file
```

You can drag-and-drop upload via GitHub's web interface, or:

```bash
git clone https://github.com/YOUR_USERNAME/propsbot-golf-data.git
cd propsbot-golf-data
# Copy all files from this package into the repo
git add .
git commit -m "Initial pipeline setup"
git push
```

### Step 3: Enable GitHub Pages

1. Go to your repo → **Settings** → **Pages**
2. Under "Source", select **GitHub Actions**
3. (The workflow will auto-deploy to Pages on each run)

### Step 4: Run the Pipeline for the First Time

1. Go to your repo → **Actions** tab
2. Click "Weekly Golf Data Update" in the left sidebar
3. Click "Run workflow" → "Run workflow" (green button)
4. Wait ~60 seconds for it to complete

### Step 5: Verify It Works

After the first run, your data is live at:

```
https://YOUR_USERNAME.github.io/propsbot-golf-data/golf-data.json
```

And the full tool is viewable at:

```
https://YOUR_USERNAME.github.io/propsbot-golf-data/
```

### Step 6: Embed on propsbot.ai

In your WordPress/Elementor site, add a **Custom HTML** widget with:

```html
<iframe
  src="https://YOUR_USERNAME.github.io/propsbot-golf-data/"
  width="100%"
  height="900"
  style="border: none; border-radius: 12px;"
  loading="lazy"
></iframe>
```

Or, for a tighter integration, copy the HTML file into an Elementor Custom HTML widget and update the `DATA_URL` constant at the top of the `<script>` to point to your GitHub Pages JSON URL.

## How Updates Work

- **Automatic**: Every Tuesday at 6 AM UTC (~1 AM EST), GitHub Actions runs the scraper, pulls fresh data, and deploys updated JSON to GitHub Pages
- **Manual**: Click "Run workflow" in the Actions tab anytime you want an immediate refresh
- **On push**: Any change to `scraper.py` triggers a fresh scrape automatically

The Tuesday schedule is chosen because most PGA Tour events end Sunday, final stats are published Monday, and Tuesday gives the data sources time to update.

## What Gets Scraped (Free Sources Only)

| Source | Data | Method | Cost |
|--------|------|--------|------|
| DataGolf.com | Player rankings, SG breakdowns | HTML scraping (Next.js __NEXT_DATA__) | Free |
| PGATour.com/stats | Scoring avg, birdie avg, GIR, SG categories | HTML scraping | Free |
| ESPN API | Current tournament leaderboard | Public JSON API | Free |
| Manual curation | Course fit scores, miss direction, ball flight, betting notes | Hardcoded in scraper.py | Free |

### What's NOT automated (manual curation needed):

- **Course fit scores** — These are subjective assessments based on course characteristics + player tendencies. Update the `get_fallback_players()` function in scraper.py
- **Betting notes** — Player-specific prop betting insights. Update in the same function
- **Pin sheet data** — Captured manually from broadcast during tournament weeks
- **New courses** — Add to `get_course_data()` function when you want to cover a new venue

## Extending the Scraper

### Adding a New Data Source

In `scraper.py`, add a new function:

```python
def scrape_new_source():
    url = "https://example.com/golf-stats"
    html = fetch_url(url)
    if not html:
        return None
    # Parse the HTML/JSON
    # Return structured data
```

Then call it from `run_pipeline()` and merge the results into the output.

### Adding a New Course

In the `get_course_data()` function, add a new course entry with hole-by-hole data. Sources for this data:

1. **DataGolf past results pages** — free, shows hole-level scoring by round
2. **PGA Tour stats** — course-specific scoring data
3. **GolfStats.com** — historical tournament records

### Increasing Player Count

The fallback database has 12 players. To expand:

1. Add more players to `get_fallback_players()` with curated fields
2. The DataGolf scraper already pulls top 50 — new scraped players auto-merge with defaults for missing fields

## Troubleshooting

**"Scraper returned fallback data"** — This means the live scraping sources were unreachable or their HTML structure changed. The fallback data is still accurate (manually curated), just not real-time. Check if DataGolf or PGA Tour changed their page structure.

**"GitHub Actions workflow failed"** — Check the Actions tab for error logs. Common issues:
- Rate limiting (increase `REQUEST_DELAY` in scraper.py)
- Site structure changes (update parsing regex in scraper functions)
- Network timeouts (increase `TIMEOUT`)

**"CORS error loading JSON"** — If your propsbot.ai site can't fetch the JSON, ensure:
- The repo is public
- GitHub Pages is enabled
- The `_headers` file is deployed with CORS headers

## Cost

**$0.** GitHub Actions gives 2,000 free minutes/month for public repos. This pipeline uses ~1 minute per run, so even running daily would be well within limits. GitHub Pages hosting is free for public repos.
