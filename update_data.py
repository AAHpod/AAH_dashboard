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

BREVO_API_KEY = os.environ.get("BREVO_API_KEY", "")
EMAIL_FROM_NAME  = "AAH Bot"
EMAIL_FROM       = "hello@accountantsafterhours.com.au"
EMAIL_TO         = ["hello@accountantsafterhours.com.au", "sarah@hottoast.com.au", "natalie@twosides.com.au"]

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
def parse_duration(duration_str):
    import re
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
    if not match:
        return 0
    return int(match.group(1) or 0)*3600 + int(match.group(2) or 0)*60 + int(match.group(3) or 0)


def fetch_youtube():
    base = "https://www.googleapis.com/youtube/v3"

    # Channel stats
    url = f"{base}/channels?part=statistics,snippet&id={YOUTUBE_CHANNEL_ID}&key={YOUTUBE_API_KEY}"
    with urllib.request.urlopen(url, context=ssl_ctx) as r:
        data = json.loads(r.read())
    stats   = data["items"][0]["statistics"]
    snippet = data["items"][0]["snippet"]

    # All videos from uploads playlist
    uploads_id = "UU" + YOUTUBE_CHANNEL_ID[2:]
    url = f"{base}/playlistItems?part=snippet&playlistId={uploads_id}&maxResults=50&key={YOUTUBE_API_KEY}"
    with urllib.request.urlopen(url, context=ssl_ctx) as r:
        playlist_data = json.loads(r.read())

    video_ids = [item["snippet"]["resourceId"]["videoId"] for item in playlist_data.get("items", [])]

    episodes = []
    shorts   = []
    if video_ids:
        ids_str = ",".join(video_ids)
        url = f"{base}/videos?part=statistics,snippet,contentDetails&id={ids_str}&key={YOUTUBE_API_KEY}"
        with urllib.request.urlopen(url, context=ssl_ctx) as r:
            videos_data = json.loads(r.read())
        for v in videos_data.get("items", []):
            seconds = parse_duration(v["contentDetails"]["duration"])
            entry = {
                "title":         v["snippet"]["title"],
                "views":         int(v["statistics"].get("viewCount", 0)),
                "likes":         int(v["statistics"].get("likeCount", 0)),
                "publishedDate": v["snippet"]["publishedAt"][:10],
                "duration":      seconds,
            }
            if seconds <= 600:
                shorts.append(entry)
            else:
                episodes.append(entry)

    return {
        "subscribers": int(stats.get("subscriberCount", 0)),
        "totalViews":  int(stats.get("viewCount", 0)),
        "title":       snippet.get("title", ""),
        "episodes":    episodes,
        "shorts":      shorts,
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
    fb_monthly  = [{"month": m, "count":     _int(monthly[m].get("Facebook Followers",      0))} for m in month_order]
    li_monthly  = [{"month": m, "count":     _int(monthly[m].get("LinkedIn Followers",       0))} for m in month_order]
    pod_monthly = [{"month": m, "downloads": _int(monthly[m].get("Podcast Downloads",        0))} for m in month_order]

    curr_yt_prev = _int(monthly[month_order[-2]].get("YouTube Subscribers", 0)) if len(month_order) > 1 else 0
    curr_web     = curr("Website Visitors")
    curr_pod_dl  = curr("Podcast Downloads")
    curr_pod_sub = _pod_subs(monthly.get(report_label, {}))
    prev_pod_sub = _pod_subs(monthly.get(month_order[month_order.index(report_label)-1], {}) if report_label in month_order and month_order.index(report_label) > 0 else {})
    curr_ig      = curr("Instagram Followers")
    curr_fb      = _int(monthly.get(report_label, {}).get("Facebook Followers", 0))
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
    episodes: {json.dumps(sorted(yt.get("episodes", []), key=lambda e: e["views"], reverse=True), indent=4)},
    shorts: {json.dumps(sorted(yt.get("shorts", []), key=lambda e: e["views"], reverse=True), indent=4)},
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
    followerGrowth: {curr_fb - prev("Facebook Followers")},
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


# ── Email HTML ────────────────────────────────────────────────────────────────
def _delta(current, prev):
    d = current - prev
    if d > 0:  return f'<span style="color:#22C55E;font-size:13px">&#9650; +{d:,}</span>'
    if d < 0:  return f'<span style="color:#EF4444;font-size:13px">&#9660; {d:,}</span>'
    return '<span style="color:#888;font-size:13px">&#8212; no change</span>'

def build_email_html(monthly, episodes, yt, hs_count, report_label):
    month_order = sorted(monthly.keys(), key=lambda m: datetime.strptime(m, "%b %Y"))
    idx = month_order.index(report_label) if report_label in month_order else -1
    curr_row = monthly.get(report_label, {})

    def cv(f): return _int(curr_row.get(f, 0))
    def pv(f): return _int(monthly[month_order[idx-1]].get(f, 0)) if idx > 0 else 0

    try:
        rm_full = datetime.strptime(report_label, "%b %Y").strftime("%B %Y")
    except ValueError:
        rm_full = report_label

    def card(label, value, delta_html="", color="#000"):
        return f"""<td width="30%" style="padding:14px 16px;background:#F7F7F7;border-radius:10px;text-align:center;vertical-align:top">
          <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1.2px;color:#888;margin-bottom:6px">{label}</div>
          <div style="font-size:28px;font-weight:900;color:{color};line-height:1.1">{value:,}</div>
          <div style="margin-top:4px">{delta_html}</div>
        </td>"""

    sp = '<td width="5%"></td>'
    total_pod = sum(e["downloads"] for e in episodes)
    ep_rows = "".join(
        f'<tr><td style="padding:10px 14px;border-bottom:1px solid #f0f0f0;font-size:13px">{e["title"]}</td>'
        f'<td style="padding:10px 14px;border-bottom:1px solid #f0f0f0;font-size:13px;font-weight:800;text-align:right">{e["downloads"]:,}</td></tr>'
        for e in sorted(episodes, key=lambda e: e["downloads"], reverse=True)[:10]
    )

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#F0F0F0;font-family:Arial,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="padding:32px 0"><tr><td>
<table width="620" cellpadding="0" cellspacing="0" align="center"
       style="background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.10)">
  <tr><td style="background:#000;padding:28px 36px">
    <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:2px;color:#FFE500;margin-bottom:6px">Monthly Performance Report</div>
    <div style="font-size:26px;font-weight:900;color:#fff"">Accountants After Hours</div>
    <div style="font-size:14px;color:#888;margin-top:6px">{rm_full}</div>
  </td></tr>
  <tr><td style="background:#FFD5D0;padding:14px 36px;font-size:14px;font-weight:700;color:#000;text-align:center">
    Here&rsquo;s your monthly snapshot &mdash; <span style="background:#FFE500;padding:2px 8px">{rm_full}</span>
  </td></tr>
  <tr><td style="padding:32px 36px">
    <div style="font-size:13px;font-weight:900;text-transform:uppercase;letter-spacing:2px;color:#888;margin-bottom:12px">Platforms</div>
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:12px"><tr>
      {card("YouTube Subs",     yt["subscribers"], _delta(yt["subscribers"], pv("YouTube Subscribers")))}
      {sp}{card("Email Subs", hs_count, _delta(hs_count, pv("Mailing List Subscribers")))}
      {sp}{card("Website Visitors", cv("Website Visitors"), _delta(cv("Website Visitors"), pv("Website Visitors")))}
    </tr></table>
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px"><tr>
      {card("Podcast Downloads", cv("Podcast Downloads"), _delta(cv("Podcast Downloads"), pv("Podcast Downloads")))}
      {sp}{card("YouTube Total Views", yt["totalViews"])}
      {sp}<td width="30%"></td>
    </tr></table>
    <div style="font-size:13px;font-weight:900;text-transform:uppercase;letter-spacing:2px;color:#888;margin-bottom:12px">Social Media</div>
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px"><tr>
      {card("Instagram",  cv("Instagram Followers"),  _delta(cv("Instagram Followers"),  pv("Instagram Followers")))}
      {sp}{card("LinkedIn", cv("LinkedIn Followers"), _delta(cv("LinkedIn Followers"), pv("LinkedIn Followers")))}
      {sp}{card("Facebook", _int(curr_row.get("Facebook Followers", 0)), _delta(_int(curr_row.get("Facebook Followers", 0)), pv("Facebook Followers")))}
    </tr></table>
    <div style="font-size:13px;font-weight:900;text-transform:uppercase;letter-spacing:2px;color:#888;margin-bottom:4px">Podcast Episodes</div>
    <div style="font-size:12px;color:#888;margin-bottom:12px">All-time total: <strong style="color:#000">{total_pod:,} downloads</strong></div>
    {'<table width="100%" style="border-collapse:collapse"><thead><tr>'
     '<th style="text-align:left;padding:10px 14px;font-size:10px;font-weight:700;text-transform:uppercase;color:#888;background:#F7F7F7;border-bottom:2px solid #eee">Episode</th>'
     '<th style="text-align:right;padding:10px 14px;font-size:10px;font-weight:700;text-transform:uppercase;color:#888;background:#F7F7F7;border-bottom:2px solid #eee">Downloads</th>'
     f'</tr></thead><tbody>{ep_rows}</tbody></table>' if ep_rows else '<p style="color:#aaa;font-size:13px">No episode data.</p>'}
  </td></tr>
  <tr><td style="background:#000;padding:20px 36px;text-align:center">
    <p style="margin:0;color:#555;font-size:12px">Accountants After Hours &bull; Generated {date.today().strftime("%-d %B %Y")}</p>
    <p style="margin:6px 0 0"><a href="https://aahpod.github.io/AAH_dashboard" style="color:#FFE500;font-size:12px;text-decoration:none">View Live Dashboard</a></p>
  </td></tr>
</table></td></tr></table></body></html>"""


def send_email(subject, html_body):
    if not BREVO_API_KEY:
        print("  No BREVO_API_KEY set — skipping email.")
        return
    payload = json.dumps({
        "sender": {"name": EMAIL_FROM_NAME, "email": EMAIL_FROM},
        "to": [{"email": e} for e in EMAIL_TO],
        "subject": subject,
        "htmlContent": html_body,
    }).encode("utf-8")
    req = urllib.request.Request("https://api.brevo.com/v3/smtp/email", data=payload, method="POST")
    req.add_header("api-key",      BREVO_API_KEY)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, context=ssl_ctx) as r:
            resp = json.loads(r.read())
        print(f"  Email sent via Brevo (messageId: {resp.get('messageId', '?')})")
    except urllib.error.HTTPError as e:
        print(f"  Brevo error {e.code}: {e.read().decode()}")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Report label = the most recent month that has data in the sheet
    # (always the month before today if it's been filled in)
    first_of_today = date.today().replace(day=1)
    last_month_end = first_of_today - timedelta(days=1)
    report_label   = last_month_end.strftime("%b %Y")
    print(f"Reporting month: {report_label}")

    print("Reading Google Sheet...")
    rows = fetch_sheet_rows()
    monthly, episodes = parse_sheet(rows)

    # Fall back to most recent available month if current month not filled in yet
    if report_label not in monthly:
        available = [m for m in monthly if any(v.strip() for v in monthly[m].values())]
        if not available:
            print("No data in sheet yet. Aborting.")
            raise SystemExit(1)
        report_label = sorted(available, key=lambda m: datetime.strptime(m, "%b %Y"))[-1]
        print(f"  Sheet doesn't have {report_label} yet — using {report_label}")

    print("Fetching YouTube...")
    yt = fetch_youtube()
    print(f"  Subscribers: {yt['subscribers']:,}  Views: {yt['totalViews']:,}  Episodes: {len(yt['episodes'])}  Shorts: {len(yt['shorts'])}")

    print("Fetching HubSpot...")
    hs_count = fetch_hubspot()
    print(f"  Contacts: {hs_count:,}")

    print("Writing data.js...")
    write_data_js(monthly, episodes, yt, hs_count, report_label)

    # Only email on the 2nd of the month (or next Monday if weekend)
    is_manual = os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
    if is_manual or should_run():
        print("Sending email...")
        subject = f"AAH Monthly Report — {datetime.strptime(report_label, '%b %Y').strftime('%B %Y')}"
        html = build_email_html(monthly, episodes, yt, hs_count, report_label)
        send_email(subject, html)
    else:
        print(f"Not email day ({date.today()}) — data updated, no email sent.")

    print("Done!")
