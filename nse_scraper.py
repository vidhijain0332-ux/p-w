import os
import json
import time
import logging
import requests
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
from telegram import Bot
import asyncio

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
SEEN_IDS_FILE = "seen_ids.json"
NSE_API_URL   = "https://www.nseindia.com/api/corp-announcements"
SCREENER_URL  = "https://www.screener.in/api/company/{symbol}/announcements/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

# Keywords to filter (case-insensitive)
KEYWORDS = ["preferential", "warrants", "allotment of securities"]

# Google Sheets column order (matches your sheet headers exactly)
SHEET_COLUMNS = [
    "Logged at",
    "Company Name",
    "Symbol",
    "Category",
    "Title",
    "Full Subject/Topic",
    "NSE Dates",
    "First Disclosure",
    "NSE Link",
    "Screener Link",
]

# ── Secrets from environment ──────────────────────────────────────────────────
TELEGRAM_TOKEN      = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHANNEL_ID = os.environ["TELEGRAM_CHANNEL_ID"]   # e.g. @mychannel or -1001234567890
GOOGLE_SHEET_ID     = os.environ["GOOGLE_SHEET_ID"]
SHEET_TAB_NAME      = os.environ.get("SHEET_TAB_NAME", "Preferential & Warrants")
GOOGLE_CREDS_JSON   = os.environ["GOOGLE_CREDS_JSON"]     # full JSON string of service account


# ── Seen IDs ─────────────────────────────────────────────────────────────────
def load_seen_ids() -> set:
    if os.path.exists(SEEN_IDS_FILE):
        with open(SEEN_IDS_FILE, "r") as f:
            return set(json.load(f))
    return set()


def save_seen_ids(ids: set):
    with open(SEEN_IDS_FILE, "w") as f:
        json.dump(list(ids), f)


# ── NSE Session ───────────────────────────────────────────────────────────────
def get_nse_session() -> requests.Session:
    """Create a session with NSE cookies (required to avoid 403)."""
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get("https://www.nseindia.com", timeout=15)
        time.sleep(2)
    except Exception as e:
        log.warning(f"Could not pre-fetch NSE home: {e}")
    return session


# ── Fetch NSE Announcements ───────────────────────────────────────────────────
def fetch_nse_announcements(session: requests.Session) -> list[dict]:
    """Fetch corporate announcements from NSE API."""
    params = {"index": "equities"}
    try:
        r = session.get(NSE_API_URL, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return data
        # sometimes wrapped in a dict
        return data.get("data", data.get("announcements", []))
    except Exception as e:
        log.error(f"NSE fetch error: {e}")
        return []


# ── Filter by keyword ─────────────────────────────────────────────────────────
def is_relevant(announcement: dict) -> bool:
    text_fields = [
        announcement.get("subject", ""),
        announcement.get("desc", ""),
        announcement.get("anndesc", ""),
        announcement.get("attchmntText", ""),
    ]
    combined = " ".join(str(f) for f in text_fields).lower()
    return any(kw in combined for kw in KEYWORDS)


# ── Within last 24 hours ──────────────────────────────────────────────────────
def within_24h(announcement: dict) -> bool:
    cutoff = datetime.utcnow() - timedelta(hours=24)
    for key in ("date", "bfDate", "excDate", "an_dt"):
        raw = announcement.get(key, "")
        if not raw:
            continue
        for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
            try:
                dt = datetime.strptime(str(raw).strip(), fmt)
                return dt >= cutoff
            except ValueError:
                pass
    # If no date found, include it (safe default)
    return True


# ── Build row dict ────────────────────────────────────────────────────────────
def build_row(ann: dict) -> dict:
    symbol    = ann.get("symbol", "").strip()
    company   = ann.get("desc", ann.get("sm_name", symbol)).strip()
    subject   = ann.get("subject", "").strip()
    anndesc   = ann.get("anndesc", "").strip()
    category  = ann.get("categoryName", ann.get("category", "")).strip()
    an_dt     = ann.get("an_dt", ann.get("date", "")).strip()
    exc_date  = ann.get("excDate", "").strip()
    attach    = ann.get("attchmntFile", "")

    nse_link      = f"https://www.nseindia.com/api/announcements-attachments?filename={attach}" if attach else ""
    screener_link = f"https://www.screener.in/company/{symbol}/announcements/" if symbol else ""

    return {
        "Logged at":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Company Name":       company,
        "Symbol":             symbol,
        "Category":           category,
        "Title":              subject,
        "Full Subject/Topic": anndesc or subject,
        "NSE Dates":          an_dt,
        "First Disclosure":   exc_date,
        "NSE Link":           nse_link,
        "Screener Link":      screener_link,
    }


# ── Google Sheets ─────────────────────────────────────────────────────────────
def get_sheet():
    creds_dict = json.loads(GOOGLE_CREDS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds  = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    sheet  = client.open_by_key(GOOGLE_SHEET_ID)

    # Open or create the tab
    try:
        ws = sheet.worksheet(SHEET_TAB_NAME)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=SHEET_TAB_NAME, rows=1000, cols=len(SHEET_COLUMNS))
        ws.append_row(SHEET_COLUMNS)
        log.info(f"Created new tab: {SHEET_TAB_NAME}")

    # Ensure headers exist
    existing = ws.row_values(1)
    if existing != SHEET_COLUMNS:
        ws.insert_row(SHEET_COLUMNS, 1)

    return ws


def append_to_sheet(ws, row_dict: dict):
    row = [row_dict.get(col, "") for col in SHEET_COLUMNS]
    ws.append_row(row, value_input_option="USER_ENTERED")


# ── Telegram ──────────────────────────────────────────────────────────────────
async def send_telegram(bot: Bot, channel: str, row: dict):
    lines = [
        f"📢 *NSE Announcement*",
        f"🏢 *{row['Company Name']}* (`{row['Symbol']}`)",
        f"🏷 Category: {row['Category']}",
        f"📋 {row['Title']}",
        f"📅 NSE Date: {row['NSE Dates']}",
        f"📅 First Disclosure: {row['First Disclosure']}",
    ]
    if row["NSE Link"]:
        lines.append(f"🔗 [NSE Link]({row['NSE Link']})")
    if row["Screener Link"]:
        lines.append(f"🔍 [Screener]({row['Screener Link']})")
    lines.append(f"_Logged: {row['Logged at']}_")

    msg = "\n".join(lines)
    await bot.send_message(
        chat_id=channel,
        text=msg,
        parse_mode="Markdown",
        disable_web_page_preview=True,
    )


# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    log.info("=== NSE Announcement Bot starting ===")

    seen_ids = load_seen_ids()
    session  = get_nse_session()
    bot      = Bot(token=TELEGRAM_TOKEN)

    try:
        ws = get_sheet()
    except Exception as e:
        log.error(f"Google Sheets connection failed: {e}")
        raise

    announcements = fetch_nse_announcements(session)
    log.info(f"Fetched {len(announcements)} total announcements from NSE")

    new_count = 0
    for ann in announcements:
        # Build a unique ID from symbol + date + subject
        uid = f"{ann.get('symbol','')}_{ann.get('an_dt', ann.get('date',''))}_{ann.get('subject','')[:40]}"

        if uid in seen_ids:
            continue
        if not within_24h(ann):
            continue
        if not is_relevant(ann):
            continue

        row = build_row(ann)
        log.info(f"New announcement: {row['Symbol']} — {row['Title'][:60]}")

        try:
            append_to_sheet(ws, row)
            log.info("  → Added to Google Sheet")
        except Exception as e:
            log.error(f"  Sheet write error: {e}")
            continue

        try:
            await send_telegram(bot, TELEGRAM_CHANNEL_ID, row)
            log.info("  → Sent to Telegram channel")
        except Exception as e:
            log.error(f"  Telegram send error: {e}")

        seen_ids.add(uid)
        new_count += 1
        time.sleep(1)   # polite delay between Telegram messages

    save_seen_ids(seen_ids)
    log.info(f"=== Done. {new_count} new announcements processed. ===")


if __name__ == "__main__":
    asyncio.run(main())
