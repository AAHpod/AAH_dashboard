# Accountants After Hours — Dashboard

## What This Is

A monthly performance dashboard for the Accountants After Hours podcast and brand.
Tracks YouTube, podcast downloads, email subscribers, website visitors, and social media followers across Instagram, Facebook, and LinkedIn.

**Live URL:** https://aahpod.github.io/AAH_dashboard
**Local URL:** http://localhost:8080 (run `python3 server.py` from this directory)

---

## Data Sources

### Automatic (pulled via API each month)

| Platform | What's Fetched | API Used |
|----------|---------------|----------|
| **YouTube** | Subscribers, total views, episode list, shorts list (sorted by views) | YouTube Data API v3 |
| **HubSpot** | Total email subscriber/contact count | HubSpot CRM API v3 |

### Manual (entered in Google Sheet, read automatically)

Google Sheet: https://docs.google.com/spreadsheets/d/1vRdlPTCv_i3sA-WBlheHDKvu6KNNCZri
Tab: Sheet GID `1571762833`

| Column | Platform |
|--------|----------|
| Podcast Downloads | Acast (no public API) |
| Website Visitors | Wix (API was unreliable, dropped) |
| Instagram Followers | Meta (requires Business API — not wired up) |
| Facebook Followers | Meta (manual) |
| LinkedIn Followers | Manual |
| Podcast Subscribers - Apple | Manual |
| Podcast Subscribers - Spotify | Manual (Spotify returns "cannot obtain") |

**Episode download totals** are entered in a second table in the same sheet (rows starting with `Episode 1:`, `Episode 2:` etc.).

---

## Monthly Workflow

### What you need to do (by the 1st of each month)
Fill in the Google Sheet with the previous month's figures for:
- Podcast Downloads
- Website Visitors
- Instagram Followers
- Facebook Followers
- LinkedIn Followers
- Podcast Subscriber counts (Apple / Spotify)
- Episode download totals (update the episode table)

### What happens automatically (2nd of each month, 9am AEST)
1. GitHub Actions runs `update_data.py`
2. Fetches live YouTube stats (subscribers, views, all videos)
3. Fetches live HubSpot contact count
4. Reads the Google Sheet CSV export
5. Regenerates `data.js` with all data embedded
6. Commits and pushes to GitHub → GitHub Pages updates automatically
7. Sends an HTML email report via Brevo to:
   - hello@accountantsafterhours.com.au
   - sarah@hottoast.com.au
   - natalie@twosides.com.au

If the 2nd falls on a weekend, the script runs on the next Monday.

---

## Files

| File | Purpose |
|------|---------|
| `index.html` | The dashboard — all UI, charts, and logic |
| `data.js` | Auto-generated data file. **Do not edit manually** — it gets overwritten each month |
| `update_data.py` | GitHub Actions script — fetches APIs, reads sheet, writes data.js, sends email |
| `monthly_report.py` | Local version of the same script — saves HTML report locally and opens in browser |
| `server.py` | Local dev server — serves dashboard at localhost:8080 with live API data |
| `config.js` | Placeholder config (API keys are in environment variables / GitHub Secrets) |
| `com.aah.monthlyreport.plist` | macOS launchd plist — schedules monthly_report.py locally |
| `.github/workflows/monthly-update.yml` | GitHub Actions workflow — runs daily at 11pm UTC, script self-limits to 2nd |
| `reports/` | Local HTML report snapshots saved by monthly_report.py |

---

## GitHub Secrets (set in repo Settings → Secrets)

| Secret | Used For |
|--------|---------|
| `YOUTUBE_API_KEY` | YouTube Data API v3 |
| `HUBSPOT_TOKEN` | HubSpot CRM API |
| `BREVO_API_KEY` | Brevo email sending |

---

## Email

Sent via **Brevo** (brevo.com) using the verified sender `hello@accountantsafterhours.com.au` (listed as "AAH Bot").

> **Note:** Gmail/Google Workspace SMTP was blocked (App Passwords disabled by admin). Resend was blocked due to Wix domain. Brevo works without DNS changes because the sender email was verified directly.

---

## Scheduling

### GitHub Actions (production — updates the live site)
- Workflow: `.github/workflows/monthly-update.yml`
- Cron: `0 23 * * *` (runs daily at 11pm UTC = 9am AEST)
- Script guards: only executes on the 2nd business day of the month
- Repo: https://github.com/AAHpod/AAH_dashboard

### Local launchd (backup — updates local data.js only)
- Plist: `com.aah.monthlyreport.plist` → installed at `~/Library/LaunchAgents/`
- Runs: 9am on the 1st, 2nd, and 3rd of each month
- Script: `monthly_report.py` (saves HTML report locally, no email)
- To reload: `launchctl unload ~/Library/LaunchAgents/com.aah.monthlyreport.plist && launchctl load ~/Library/LaunchAgents/com.aah.monthlyreport.plist`
- Log: `monthly_report.log` in this directory

---

## Gotchas

- **Google Sheet must be public** ("Anyone with the link can view") for the GitHub Actions script to read it without authentication.
- **data.js is overwritten every month** — never manually edit it as changes will be lost. Edit the Google Sheet instead.
- **YouTube shorts are sorted by views** (highest first) in both the dashboard and email.
- **The dashboard tries `/api/all` first** (works locally with server.py). On GitHub Pages this 404s and falls back to `data.js`. This is intentional — GitHub Pages returns a 200 with HTML for unknown routes, so the fetch fails at JSON parse and the catch block handles it silently.
- **Facebook and Instagram are manual** — Meta's API requires Business account OAuth which was not set up. Update these in the Google Sheet each month.
- **Wix Analytics API** was tested but dropped — the available endpoints didn't return reliable visitor data. Website visitors are entered manually in the Google Sheet.
- **HubSpot subscriber count** is the total contacts count, not newsletter subscribers specifically. It's a reasonable proxy for email list size.
- **The workflow runs daily** but the `should_run()` guard in `update_data.py` exits immediately on every day except the 2nd business day, so it only does real work once a month.
