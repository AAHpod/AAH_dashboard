#!/usr/bin/env python3
"""
GitHub Actions data updater.
Reads API keys from environment variables (GitHub Secrets),
fetches YouTube & HubSpot live, reads Google Sheet,
then writes updated data.js.
"""

import csv, io, json, os, ssl
import urllib.request
from datetime import date, datetime, timedelta

# ── Config (from GitHub Secrets) ──────────────────────────────────────────────
YOUTUBE_API_KEY    = os.environ.get("YOUTUBE_API_KEY", "")
YOUTUBE_CHANNEL_ID = "UCddISEE5dB84mW4o8uDtkdw"
HUBSPOT_TOKEN      = os.environ.get("HUBSPOT_TOKEN", "")
SHEET_ID           = "1vRdlPTCv_i3sA-WBlheHDKvu6KNNCZri"
SHEET_GID          = "1571762833"

DIR          = os.path.dirname(os.path.abspath(__file__))
DATA_JS_PATH = os.path.join(DIR, "data.js")

# ── SSL ────────────────────────────────────────────────────────────────────────
ssl_ctx = ssl.create_default_context()
try:
    import certifi
    ssl_ctx.load_verify_locations(certifi.where())
except ImportError:
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode    = ssl.CERT_NONE


# ── Is this the right day to run? ─────────────────────────────────────────────
def should_run():
    """Run on the 2nd, or next Monday if the 2nd is a weekend."""
    today = date.today()
    if today.day > 5:
        return False
    for d in range(2, 6):
        try:
            candidate = today.replace(day=d)
        except ValueError:
            continue
        if candidate.weekday() < 5:   # Mon–Fri
            return candidate == today
    return False


# ── Google Sheet ───────────────────────────────────────────────────────────────
def fetch_sheet_rows():
    url = (f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
           f"/export?format=csv&gid={SHEET_GID}")
    with urllib.request.urlopen(url, context=ssl_ctx) as r:
        return list(csv.reader(io.StringIO(r.read().decode("utf-8"))))


def parse_sheet(rows):
    header_idx = next((i for i, r in enumerate(rows) if r and r[0] == "Month"), None)
    if header_idx is None:
        raise ValueError("Could not find 'Month' header row in Google Sheet")
    headers = [h.strip() for h in rows[header_idx]]
    monthly = {}
    for row in rows[header_idx + 1:]:
        if not row or not row[0].strip():
            break
        monthly[row[0].strip()] = dict(zip(headers, row))

    dl_idx = next((i for i, r in enumerate(rows) if r and r[0] == "Downloads"), None)
    episodes = []
    if dl_idx is not None:
        col_headers = rows[dl_idx]
        total_col   = next((j for j, h in enumerate(col_headers) if h.strip() == "TOTAL"), None)
        for row in rows[dl_idx + 1:]:
            if not row or not row[0].strip():
                break
            title = row[0].strip()
            dl = 0
            if total_col is not None and len(row) > total_col and row[total_col].strip():
                try:
                    dl = int(float(row[total_col]))
                except (ValueError, TypeError):
                    pass
            if dl > 0:
                episodes.append({"title": title, "downloads": dl})
    return monthly, episodes


# ── API fetchers ───────────────────────────────────────────────────────────────
def fetch_youtube():
    url = (f"https://www.googleapis.com/youtube/v3/channels"
           f"?part=statistics,snippet&id={YOUTUBE_CHANNEL_ID}&key={YOUTUBE_API_KEY}")
    with urllib.request.urlopen(url, context=ssl_ctx) as r:
        data = json.loads(r.read())
    stats   = data["items"][0]["statistics"]
    snippet = data["items"][0]["snippet"]
    return {
        "subscribers": int(stats.get("subscriberCount", 0)),
        "totalViews":  int(stats.get("viewCount", 0)),
        "title":       snippet.get("title", ""),
    }


def fetch_hubspot():
    url     = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    payload = json.dumps({"filterGroups": [], "limit": 1}).encode()
    req     = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Authorization", f"Bearer {HUBSPOT_TOKEN}")
    req.add_header("Content-Type",  "application/json")
    with urllib.request.urlopen(req, context=ssl_ctx) as r:
        return json.loads(r.read()).get("total", 0)


# ── data.js writer ─────────────────────────────────────────────────────────────
def _int(val, default=0):
    try:
        return int(float(str(val).replace(",", ""))) if val and str(val).strip() else default
    except (ValueError, TypeError):
        return default


def write_data_js(monthly, episodes, yt, hs_count, report_label):
    month_order = sorted(monthly.keys(), key=lambda m: datetime.strptime(m, "%b %Y"))

    def curr(field):
        return _int(monthly.get(report_label, {}).get(field, 0))

    def prev(field):
        idx = month_order.index(report_label) if report_label in month_order else -1
        return _int(monthly[month_order[idx-1]].get(field, 0)) if idx > 0 else 0

    def _pod_subs(row):
        return (_int(row.get("Podcast Subscribers - Apple", 0))
                + _int(row.get("Podcast Subscribers - Spotify ", 0)))

    yt_monthly  = [{"month": m, "views":     _int(monthly[m].get("YouTube Subscribers",      0))} for m in month_order]
    web_monthly = [{"month": m, "visitors":  _int(monthly[m].get("Website Visitors",         0))} for m in month_order]
    hs_monthly  = [{"month": m, "count":     _int(monthly[m].get("Mailing List Subscribers", 0))} for m in month_order]
    ig_monthly  = [{"month": m, "count":     _int(monthly[m].get("Instagram Followers",      0))} for m in month_order]
    fb_monthly  = [{"month": m, "count":     _int(monthly[m].get("Facebook Followers ",      0))} for m in month_order]
    li_monthly  = [{"month": m, "count":     _int(monthly[m].get("LinkedIn Followers",       0))} for m in month_order]
    pod_monthly = [{"month": m, "downloads": _int(monthly[m].get("Podcast Downloads",        0))} for m in month_order]

    curr_yt_prev = _int(monthly[month_order[-2]].get("YouTube Subscribers", 0)) if len(month_order) > 1 else 0
    curr_web     = curr("Website Visitors")
    curr_pod_dl  = curr("Podcast Downloads")
    curr_pod_sub = _pod_subs(monthly.get(report_label, {}))
    prev_pod_sub = _pod_subs(monthly.get(month_order[month_order.index(report_label)-1], {}) if report_label in month_order and month_order.index(report_label) > 0 else {})
    curr_ig      = curr("Instagram Followers")
    curr_fb      = _int(monthly.get(report_label, {}).get("Facebook Followers ", 0))
    curr_li      = curr("LinkedIn Followers")
    total_pod    = sum(e["downloads"] for e in episodes)
    top_eps      = sorted(episodes, key=lambda e: e["downloads"], reverse=True)[:5]
    today_str    = date.today().strftime("%Y-%m-%d")

    try:
        rm_full = datetime.strptime(report_label, "%b %Y").strftime("%B %Y")
    except ValueError:
        rm_full = report_label

    js = f"""// ============================================================
// ACCOUNTANTS AFTER HOURS - DASHBOARD DATA
// Auto-generated by GitHub Actions on {today_str}
// YouTube & HubSpot are pulled LIVE via API.
// ============================================================

const DASHBOARD_DATA = {{
  lastUpdated: "{today_str}",
  reportingMonth: "{rm_full}",

  youtube: {{
    subscribers: {yt["subscribers"]},
    subscriberGrowth: {yt["subscribers"] - curr_yt_prev},
    totalViews: {yt["totalViews"]},
    channelUrl: "https://www.youtube.com/@AccountantsAfterHours",
    episodes: [],
    monthlyViews: {json.dumps(yt_monthly, indent=4)},
  }},

  website: {{
    visitors: {curr_web},
    visitorGrowth: {curr_web - prev("Website Visitors")},
    topPages: [],
    monthlyVisitors: {json.dumps(web_monthly, indent=4)},
  }},

  hubspot: {{
    subscribers: {hs_count},
    subscriberGrowth: {hs_count - prev("Mailing List Subscribers")},
    openRate: "0%",
    clickRate: "0%",
    monthlySubscribers: {json.dumps(hs_monthly, indent=4)},
  }},

  podcast: {{
    platform: "Acast",
    totalDownloads: {total_pod},
    monthlyDownloads: {curr_pod_dl},
    downloadGrowth: {curr_pod_dl - prev("Podcast Downloads")},
    subscribers: {curr_pod_sub},
    subscriberGrowth: {curr_pod_sub - prev_pod_sub},
    topEpisodes: {json.dumps(top_eps, indent=4)},
    monthlyDownloads_history: {json.dumps(pod_monthly, indent=4)},
  }},

  instagram: {{
    followers: {curr_ig},
    followerGrowth: {curr_ig - prev("Instagram Followers")},
    postsThisMonth: 0,
    reach: 0,
    impressions: 0,
    engagement: "0%",
    profileUrl: "https://www.instagram.com/accountantsafterhours",
    monthlyFollowers: {json.dumps(ig_monthly, indent=4)},
  }},

  facebook: {{
    followers: {curr_fb},
    followerGrowth: {curr_fb - prev("Facebook Followers ")},
    reach: 0,
    engagement: "0%",
    profileUrl: "https://www.facebook.com/accountantsafterhours",
    monthlyFollowers: {json.dumps(fb_monthly, indent=4)},
  }},

  linkedin: {{
    followers: {curr_li},
    followerGrowth: {curr_li - prev("LinkedIn Followers")},
    impressions: 0,
    engagement: "0%",
    profileUrl: "https://www.linkedin.com/company/accountantsafterhours",
    monthlyFollowers: {json.dumps(li_monthly, indent=4)},
  }},
}};
"""
    with open(DATA_JS_PATH, "w") as f:
        f.write(js)
    print(f"  data.js updated for {report_label}")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # In GitHub Actions, workflow_dispatch allows manual runs anytime.
    # Scheduled runs check the day logic.
    is_manual = os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"

    if not is_manual and not should_run():
        print(f"Not the right day to run ({date.today()}). Skipping.")
        raise SystemExit(0)

    # Report is for the month that just ended
    first_of_today = date.today().replace(day=1)
    last_month_end = first_of_today - timedelta(days=1)
    report_label   = last_month_end.strftime("%b %Y")
    print(f"Reporting month: {report_label}")

    print("Reading Google Sheet...")
    rows = fetch_sheet_rows()
    monthly, episodes = parse_sheet(rows)

    if report_label not in monthly:
        print(f"WARNING: '{report_label}' not found in sheet. Please fill in the row. Aborting.")
        raise SystemExit(1)

    print("Fetching YouTube...")
    yt = fetch_youtube()
    print(f"  Subscribers: {yt['subscribers']:,}  Views: {yt['totalViews']:,}")

    print("Fetching HubSpot...")
    hs_count = fetch_hubspot()
    print(f"  Contacts: {hs_count:,}")

    print("Writing data.js...")
    write_data_js(monthly, episodes, yt, hs_count, report_label)

    print("Done!")
