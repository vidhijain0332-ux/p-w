import os
import json
import time
import logging
import requests
import httpx
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

SEEN_IDS_FILE = "seen_ids.json"
NSE_API_URL   = "https://www.nseindia.com/api/corp-announcements"

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

KEYWORDS = ["preferential", "warrants", "allotment of securities"]

SHEET_COLUMNS = [
    "Logged at", "Company Name", "Symbol", "Category", "Title",
    "Full Subject/Topic", "NSE Dates", "First Disclosure", "NSE Link", "Screener Link",
]

TELEGRAM_TOKEN      = os.environ["TELEGRAM_TOKEN"].strip()
TELEGRAM_CHANNEL_ID = os.environ["TELEGRAM_CHANNEL_ID"].strip()
GOOGLE_SHEET_ID     = os.environ["GOOGLE_SHEET_ID"].strip()
SHEET_TAB_NAME      = os.environ.get("SHEET_TAB_NAME", "Preferential & Warrants").strip()
GOOGLE_CREDS_JSON   = os.environ["GOOGLE_CREDS_JSON"].strip()

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"


def load_seen_ids():
    if os.path.exists(SEEN_IDS_FILE):
        with open(SEEN_IDS_FILE, "r") as f:
            return set(json.load(f))
    return set()


def save_seen_ids(ids):
    with open(SEEN_IDS_FILE, "w") as f:
        json.dump(list(ids), f)


def escape_md(text):
    for ch in ["_", "*", "[", "]", "`"]:
        text = text.replace(ch, f"\\{ch}")
    return text


def send_telegram(row):
    lines = [
        "📢 *NSE Announcement*",
        f"🏢 *{escape_md(row['Company Name'])}* (`{row['Symbol']}`)",
        f"🏷 Category: {escape_md(row['Category'])}",
        f"📋 {escape_md(row['Title'])}",
        f"📅 NSE Date: {row['NSE Dates']}",
        f"📅 First Disclosure: {row['First Disclosure']}",
    ]
    if row["NSE Link"]:
        lines.append(f"🔗 [NSE Link]({row['NSE Link']})")
    if row["Screener Link"]:
        lines.append(f"🔍 [Screener]({row['Screener Link']})")
    lines.append(f"_Logged: {row['Logged at']}_")

    payload = {
        "chat_id": TELEGRAM_CHANNEL_ID,
        "text": "\n".join(lines),
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    resp = httpx.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=15)
    if not resp.is_success:
        log.error(f"Telegram error {resp.status_code}: {resp.text}")
        resp.raise_for_status()


def get_nse_session():
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    try:
        session.get("https://www.nseindia.com", timeout=15)
        time.sleep(2)
    except Exception as e:
        log.warning(f"NSE home prefetch failed: {e}")
    return session


def fetch_nse_announcements(session):
    try:
        r = session.get(NSE_API_URL, params={"index": "equities"}, timeout=20)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return data
        return data.get("data", data.get("announcements", []))
    except Exception as e:
        log.error(f"NSE fetch error: {e}")
        return []


def is_relevant(ann):
    combined = " ".join([
        ann.get("subject", ""), ann.get("desc", ""),
        ann.get("anndesc", ""), ann.get("attchmntText", ""),
    ]).lower()
    return any(kw in combined for kw in KEYWORDS)


def within_24h(ann):
    cutoff = datetime.utcnow() - timedelta(hours=24)
    for key in ("date", "bfDate", "excDate", "an_dt"):
        raw = ann.get(key, "")
        if not raw:
            continue
        for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(str(raw).strip(), fmt) >= cutoff
            except ValueError:
                pass
    return True


def build_row(ann):
    symbol   = ann.get("symbol", "").strip()
    company  = ann.get("desc", ann.get("sm_name", symbol)).strip()
    subject  = ann.get("subject", "").strip()
    anndesc  = ann.get("anndesc", "").strip()
    category = ann.get("categoryName", ann.get("category", "")).strip()
    an_dt    = ann.get("an_dt", ann.get("date", "")).strip()
    exc_date = ann.get("excDate", "").strip()
    attach   = ann.get("attchmntFile", "")
    nse_link = (f"https://www.nseindia.com/api/announcements-attachments?filename={attach}" if attach else "")
    screener = f"https://www.screener.in/company/{symbol}/announcements/" if symbol else ""
    return {
        "Logged at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Company Name": company, "Symbol": symbol, "Category": category,
        "Title": subject, "Full Subject/Topic": anndesc or subject,
        "NSE Dates": an_dt, "First Disclosure": exc_date,
        "NSE Link": nse_link, "Screener Link": screener,
    }


def get_sheet():
    creds = Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDS_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"]
    )
    client = gspread.authorize(creds)
    sheet  = client.open_by_key(GOOGLE_SHEET_ID)
    try:
        ws = sheet.worksheet(SHEET_TAB_NAME)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=SHEET_TAB_NAME, rows=1000, cols=len(SHEET_COLUMNS))
        ws.append_row(SHEET_COLUMNS)
        log.info(f"Created tab: {SHEET_TAB_NAME}")
    if ws.row_values(1) != SHEET_COLUMNS:
        ws.insert_row(SHEET_COLUMNS, 1)
    return ws


def append_to_sheet(ws, row_dict):
    ws.append_row([row_dict.get(c, "") for c in SHEET_COLUMNS], value_input_option="USER_ENTERED")


def main():
    log.info("=== NSE Announcement Bot starting ===")
    seen_ids = load_seen_ids()
    session  = get_nse_session()

    try:
        ws = get_sheet()
        log.info("Google Sheet connected")
    except Exception as e:
        log.error(f"Sheet connection failed: {e}")
        raise

    announcements = fetch_nse_announcements(session)
    log.info(f"Fetched {len(announcements)} announcements from NSE")

    new_count = 0
    for ann in announcements:
        uid = f"{ann.get('symbol','')}_{ann.get('an_dt', ann.get('date',''))}_{ann.get('subject','')[:40]}"
        if uid in seen_ids:
            continue
        if not within_24h(ann):
            continue
        if not is_relevant(ann):
            continue

        row = build_row(ann)
        log.info(f"New: {row['Symbol']} — {row['Title'][:60]}")

        try:
            append_to_sheet(ws, row)
            log.info("  → Sheet: OK")
        except Exception as e:
            log.error(f"  Sheet error: {e}")
            continue

        try:
            send_telegram(row)
            log.info("  → Telegram: OK")
        except Exception as e:
            log.error(f"  Telegram error: {e}")

        seen_ids.add(uid)
        new_count += 1
        time.sleep(1)

    save_seen_ids(seen_ids)
    log.info(f"=== Done. {new_count} new announcements processed. ===")


if __name__ == "__main__":
    main()
