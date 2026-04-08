#!/usr/bin/env python3
"""
Short Interest Tracker — scrapes finviz for short interest data,
stores top 12 by short-interest % in SQLite, serves a clean web UI.
Production-ready with gunicorn + optional TLS.
"""

import os
import sys
import json
import time
import hashlib
import sqlite3
import logging
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
from functools import wraps
from flask import Flask, render_template, jsonify, send_from_directory, request, Response
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService

EST = ZoneInfo("America/New_York")
GECKODRIVER_PATH = "/usr/local/sbin/geckodriver"

# ── Config ──────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "short_interest.db")
CHART_DIR = os.path.join(BASE_DIR, "static", "charts")

# Load config.json
CONFIG_PATH = os.path.join(BASE_DIR, "tracker_config.json")
with open(CONFIG_PATH, "r") as f:
    CONFIG = json.load(f)

HOST = CONFIG.get("host", "0.0.0.0")
PORT = CONFIG.get("port", 60914)
KEEP_DAYS = CONFIG.get("keep_days", 10)

_watchlist_path = os.path.join(BASE_DIR, "watchlist.json")
with open(_watchlist_path, "r") as _wf:
    TICKERS = sorted(json.load(_wf))

FINVIZ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(BASE_DIR, "data", "tracker.log")),
    ],
)
log = logging.getLogger("tracker")

# ── Database ────────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS short_interest (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            short_pct REAL,
            short_dollar TEXT,
            market_cap TEXT,
            price REAL,
            put_call_ratio REAL,
            poll_date TEXT NOT NULL,
            poll_ts INTEGER NOT NULL,
            UNIQUE(ticker, poll_date)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_si_date ON short_interest(poll_date)
    """)
    # Migrate: add put_call_ratio column if missing (existing DBs)
    try:
        conn.execute("SELECT put_call_ratio FROM short_interest LIMIT 1")
    except sqlite3.OperationalError:
        log.info("Migrating DB: adding put_call_ratio column...")
        conn.execute("ALTER TABLE short_interest ADD COLUMN put_call_ratio REAL")
    conn.commit()
    conn.close()


def prune_old_data():
    cutoff = (datetime.now(EST) - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM short_interest WHERE poll_date < ?", (cutoff,))
    conn.commit()
    conn.close()
    # prune old chart images
    for fname in os.listdir(CHART_DIR):
        if fname.endswith(".json"):
            continue
        fpath = os.path.join(CHART_DIR, fname)
        parts = fname.rsplit("_", 1)
        if len(parts) == 2:
            date_part = parts[1].replace(".png", "")
            if date_part < cutoff:
                try:
                    os.remove(fpath)
                    log.info(f"Pruned old chart: {fname}")
                except OSError:
                    pass


# ── Finviz Scraper ──────────────────────────────────────────────────────

def parse_pct(text):
    """Parse '15.23%' → 15.23"""
    if not text:
        return 0.0
    text = text.strip().replace("%", "").replace(",", "")
    try:
        return float(text)
    except ValueError:
        return 0.0


def parse_float(text):
    """Parse a numeric string like '1.23' or '-' → float or 0.0"""
    if not text or text.strip() == "-":
        return 0.0
    text = text.strip().replace(",", "")
    try:
        return float(text)
    except ValueError:
        return 0.0


def parse_price(text):
    """Parse price like '123.45' → float"""
    if not text:
        return 0.0
    text = text.strip().replace(",", "")
    try:
        return float(text)
    except ValueError:
        return 0.0


def calc_short_dollar(short_float_pct, market_cap_str, price):
    """Estimate dollar value of short interest from market cap × short %."""
    if not market_cap_str or market_cap_str == "-":
        return "N/A"
    mc = market_cap_str.strip().upper()
    multiplier = 1
    if mc.endswith("B"):
        multiplier = 1_000_000_000
        mc = mc[:-1]
    elif mc.endswith("M"):
        multiplier = 1_000_000
        mc = mc[:-1]
    elif mc.endswith("K"):
        multiplier = 1_000
        mc = mc[:-1]
    try:
        mc_val = float(mc.replace(",", "")) * multiplier
    except ValueError:
        return "N/A"

    short_val = mc_val * (short_float_pct / 100.0)
    if short_val >= 1_000_000_000:
        return f"${short_val / 1_000_000_000:.2f}B"
    elif short_val >= 1_000_000:
        return f"${short_val / 1_000_000:.2f}M"
    elif short_val >= 1_000:
        return f"${short_val / 1_000:.1f}K"
    else:
        return f"${short_val:,.0f}"


# ── Chart Capture ───────────────────────────────────────────────────────

def capture_charts(tickers, date_str):
    """Capture 1D chart screenshots for a list of tickers using selenium."""
    log.info(f"Capturing charts for {len(tickers)} tickers...")
    opts = FirefoxOptions()
    opts.add_argument("--headless")
    opts.add_argument("--width=360")
    opts.add_argument("--height=240")
    try:
        svc = FirefoxService(GECKODRIVER_PATH)
        driver = webdriver.Firefox(options=opts, service=svc)
        driver.set_window_size(360, 240)
    except Exception as e:
        log.error(f"Failed to start Firefox for chart capture: {e}")
        return

    for ticker in tickers:
        try:
            url = (
                f"https://s.tradingview.com/widgetembed/"
                f"?symbol={ticker}&interval=D&hideideas=1"
                f"&hide_side_toolbar=1&allow_symbol_change=0"
                f"&save_image=0&theme=light&style=1"
                f"&timezone=exchange&withdateranges=0&locale=en"
            )
            driver.get(url)
            time.sleep(6)
            fname = f"{ticker}_{date_str}.png"
            fpath = os.path.join(CHART_DIR, fname)
            driver.save_screenshot(fpath)
            log.info(f"  Chart saved: {fname}")
        except Exception as e:
            log.error(f"  Chart capture failed for {ticker}: {e}")

    try:
        driver.quit()
    except Exception:
        pass
    log.info("Chart capture complete.")


# ── Finviz Fetch ────────────────────────────────────────────────────────

def fetch_ticker_data(ticker):
    """Fetch short-interest + put/call ratio for a single ticker from finviz."""
    url = f"https://finviz.com/quote.ashx?t={ticker}&p=d"
    try:
        resp = requests.get(url, headers=FINVIZ_HEADERS, timeout=15)
        if resp.status_code != 200:
            log.warning(f"Finviz returned {resp.status_code} for {ticker}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Collect label→value pairs from the snapshot table
        data = {}
        FIELDS = ("Short Float", "Short Interest", "Shs Float",
                  "Market Cap", "Price", "Shs Outstand", "P/C")
        table = soup.find("table", class_="snapshot-table2")
        if not table:
            tables = soup.find_all("table")
            for t in tables:
                cells = t.find_all("td")
                for i, cell in enumerate(cells):
                    text = cell.get_text(strip=True)
                    if text in FIELDS and i + 1 < len(cells):
                        data[text] = cells[i + 1].get_text(strip=True)
        else:
            cells = table.find_all("td")
            for i, cell in enumerate(cells):
                text = cell.get_text(strip=True)
                if text in FIELDS and i + 1 < len(cells):
                    data[text] = cells[i + 1].get_text(strip=True)

        short_pct = parse_pct(data.get("Short Float", ""))
        market_cap = data.get("Market Cap", "")
        price = parse_price(data.get("Price", ""))
        short_dollar = calc_short_dollar(short_pct, market_cap, price)
        put_call = parse_float(data.get("P/C", ""))

        return {
            "ticker": ticker,
            "short_pct": short_pct,
            "short_dollar": short_dollar,
            "market_cap": market_cap,
            "price": price,
            "put_call_ratio": put_call,
        }

    except Exception as e:
        log.error(f"Error fetching {ticker}: {e}")
        return None


# ── Polling ─────────────────────────────────────────────────────────────

def poll_all_tickers():
    """Poll finviz for all tickers and store top 12 by short interest."""
    log.info("Starting data poll...")
    today = datetime.now(EST).strftime("%Y-%m-%d")
    ts = int(time.time())
    results = []

    for i, ticker in enumerate(TICKERS):
        log.info(f"  Fetching {ticker} ({i+1}/{len(TICKERS)})...")
        data = fetch_ticker_data(ticker)
        if data:
            results.append(data)
        time.sleep(1.5)

    results.sort(key=lambda x: x["short_pct"], reverse=True)
    top12 = results[:12]

    if not top12:
        log.warning("No data collected — skipping DB write")
        return

    conn = sqlite3.connect(DB_PATH)
    for row in top12:
        try:
            conn.execute("""
                INSERT OR REPLACE INTO short_interest
                (ticker, short_pct, short_dollar, market_cap, price, put_call_ratio,
                 poll_date, poll_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row["ticker"], row["short_pct"], row["short_dollar"],
                row["market_cap"], row["price"], row["put_call_ratio"],
                today, ts,
            ))
        except Exception as e:
            log.error(f"DB insert error for {row['ticker']}: {e}")
    conn.commit()
    conn.close()
    log.info(f"Stored top 12 for {today}: {[r['ticker'] for r in top12]}")

    # Capture chart screenshots for the top 12
    try:
        capture_charts([r["ticker"] for r in top12], today)
    except Exception as e:
        log.error(f"Chart capture phase failed: {e}")

    prune_old_data()


# ── Technical Analysis ─────────────────────────────────────────────────

def generate_trade_analysis(ticker, current_price, short_pct, put_call_ratio):
    """Generate a basic trade plan with technical analysis.
    
    This is a simplified version that calculates support/resistance levels
    based on the current price and generates a trade plan for a potential
    short squeeze setup.
    """
    # Calculate support and resistance levels based on price
    # These are simplified calculations - in a real system you'd use
    # historical price data and proper technical analysis
    
    # Support levels (below current price)
    support_levels = [
        {"name": "Strong Support", "price": round(current_price * 0.85, 2)},
        {"name": "Major Support", "price": round(current_price * 0.90, 2)},
        {"name": "Recent Low", "price": round(current_price * 0.95, 2)},
    ]
    
    # Resistance levels (above current price)
    resistance_levels = [
        {"name": "Recent High", "price": round(current_price * 1.05, 2)},
        {"name": "Major Resistance", "price": round(current_price * 1.10, 2)},
        {"name": "Strong Resistance", "price": round(current_price * 1.15, 2)},
    ]
    
    # Determine market sentiment based on short interest and put/call ratio
    if short_pct > 20:
        sentiment = "Highly Shorted - Potential Squeeze Setup"
        squeeze_potential = "High"
    elif short_pct > 10:
        sentiment = "Moderately Shorted - Watch for Catalysts"
        squeeze_potential = "Medium"
    else:
        sentiment = "Normal Short Interest - Technical Trade"
        squeeze_potential = "Low"
    
    # Put/Call ratio analysis
    if put_call_ratio > 1.5:
        options_sentiment = "Extreme Bearish (high put volume)"
    elif put_call_ratio > 1.0:
        options_sentiment = "Bearish"
    elif put_call_ratio < 0.7:
        options_sentiment = "Bullish (high call volume)"
    else:
        options_sentiment = "Neutral"
    
    # Generate trade plan
    ideal_entry = round(current_price * 0.97, 2)  # Slight pullback entry
    stop_loss = round(ideal_entry * 0.93, 2)      # 7% stop loss
    target = round(ideal_entry * 1.15, 2)         # 15% target
    
    overview = f"{ticker} shows {short_pct:.1f}% short interest with {options_sentiment.lower()} options flow. " \
               f"{sentiment}. Squeeze potential: {squeeze_potential}."
    
    notes = ""
    if short_pct > 15:
        notes = "High short interest increases squeeze potential. Monitor for positive catalysts or breaking above key resistance levels."
    elif put_call_ratio > 1.2:
        notes = "Elevated put volume suggests bearish sentiment. Wait for reversal confirmation before entry."
    else:
        notes = "Standard technical setup. Focus on price action around key support/resistance levels."
    
    return {
        "overview": overview,
        "support_levels": support_levels,
        "resistance_levels": resistance_levels,
        "ideal_entry": ideal_entry,
        "stop_loss": stop_loss,
        "stop_loss_pct": 7.0,
        "target": target,
        "target_pct": 15.0,
        "notes": notes,
        "sentiment": sentiment,
        "squeeze_potential": squeeze_potential,
        "options_sentiment": options_sentiment
    }


# ── Flask App ───────────────────────────────────────────────────────────

app = Flask(__name__, template_folder="templates", static_folder="static")

# ── HTTP Basic Auth ─────────────────────────────────────────────────────
AUTH_CFG = CONFIG.get("auth", {})
AUTH_ENABLED = AUTH_CFG.get("enabled", False)
AUTH_REALM = AUTH_CFG.get("realm", "Login Required")
AUTH_USERS = AUTH_CFG.get("users", {})


def verify_pbkdf2(password, stored):
    """Verify a password against a pbkdf2:salt_hex:hash_hex string."""
    try:
        _, salt_hex, hash_hex = stored.split(":")
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(hash_hex)
        dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 600_000)
        return dk == expected
    except Exception:
        return False


def check_auth(username, password):
    """Verify username/password against config. Supports plaintext or pbkdf2 hashes."""
    stored = AUTH_USERS.get(username)
    if stored is None:
        return False
    if stored.startswith("pbkdf2:"):
        return verify_pbkdf2(password, stored)
    return stored == password


def auth_required(f):
    """Decorator that enforces HTTP Basic Auth when enabled."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not AUTH_ENABLED:
            return f(*args, **kwargs)
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return Response(
                "Authentication required.\n", 401,
                {"WWW-Authenticate": f'Basic realm="{AUTH_REALM}"'},
            )
        return f(*args, **kwargs)
    return decorated


@app.before_request
def before_request_auth():
    """Global auth check on every request when auth is enabled."""
    if not AUTH_ENABLED:
        return
    auth = request.authorization
    if not auth or not check_auth(auth.username, auth.password):
        return Response(
            "Authentication required.\n", 401,
            {"WWW-Authenticate": f'Basic realm="{AUTH_REALM}"'},
        )


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/dates")
def api_dates():
    """Return list of available poll dates (most recent first, max 10).
    Only includes Monday-Friday dates (ignores weekends)."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT DISTINCT poll_date FROM short_interest WHERE ticker IS NOT NULL ORDER BY poll_date DESC LIMIT 15"  # Get extra to filter
    ).fetchall()
    conn.close()
    
    # Filter to only Monday-Friday dates (0=Monday, 4=Friday)
    valid_dates = []
    for date_str in [r[0] for r in rows]:
        try:
            # Parse date and get weekday (0=Monday, 6=Sunday)
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            if dt.weekday() <= 4:  # Monday (0) through Friday (4)
                valid_dates.append(date_str)
            # else: skip weekend dates
        except ValueError:
            # Invalid date format, skip
            continue
    
    # Return most recent 10 trading days
    return jsonify(valid_dates[:10])


@app.route("/api/data/<date>")
def api_data(date):
    """Return top 12 short interest records for a given date."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT ticker, short_pct, short_dollar, market_cap, price, poll_ts,
               put_call_ratio
        FROM short_interest
        WHERE poll_date = ?
        ORDER BY short_pct DESC
        LIMIT 12
    """, (date,)).fetchall()
    conn.close()

    data = []
    for r in rows:
        data.append({
            "ticker": r[0],
            "short_pct": r[1],
            "short_dollar": r[2],
            "market_cap": r[3],
            "price": r[4],
            "poll_ts": r[5],
            "put_call_ratio": r[6] or 0.0,
        })
    return jsonify(data)


@app.route("/api/latest")
def api_latest():
    """Return the most recent date's data."""
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT poll_date FROM short_interest ORDER BY poll_date DESC LIMIT 1"
    ).fetchone()
    if not row:
        conn.close()
        return jsonify([])
    date = row[0]
    rows = conn.execute("""
        SELECT ticker, short_pct, short_dollar, market_cap, price, poll_ts,
               put_call_ratio
        FROM short_interest
        WHERE poll_date = ?
        ORDER BY short_pct DESC
        LIMIT 12
    """, (date,)).fetchall()
    conn.close()

    data = []
    for r in rows:
        data.append({
            "ticker": r[0],
            "short_pct": r[1],
            "short_dollar": r[2],
            "market_cap": r[3],
            "price": r[4],
            "poll_ts": r[5],
            "put_call_ratio": r[6] or 0.0,
        })
    return jsonify({"date": date, "data": data})


@app.route("/api/chart/<ticker>/<date>")
def api_chart(ticker, date):
    """Serve a saved chart image, or return 404 if none exists."""
    fname = f"{ticker}_{date}.png"
    fpath = os.path.join(CHART_DIR, fname)
    if os.path.exists(fpath):
        return send_from_directory(CHART_DIR, fname, mimetype="image/png")
    return "", 404

@app.route("/api/trade-plan/<ticker>/<date>")
def api_trade_plan(ticker, date):
    """Generate a trade plan with technical analysis for a ticker."""
    # Try to get data from database first
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("""
        SELECT price, short_pct, put_call_ratio
        FROM short_interest
        WHERE ticker = ? AND poll_date = ?
    """, (ticker, date)).fetchone()
    conn.close()
    
    if row:
        # Found in database
        current_price = row[0]
        short_pct = row[1]
        put_call_ratio = row[2] or 0.0
    else:
        # Not in database, fetch live from Finviz
        try:
            data = fetch_ticker_data(ticker)
            if not data:
                return jsonify({"error": f"No data found for {ticker}"}), 404
            
            current_price = data.get("price", 0.0)
            short_pct = data.get("short_pct", 0.0)
            put_call_ratio = data.get("put_call_ratio", 0.0)
        except Exception as e:
            log.error(f"Error fetching trade plan data for {ticker}: {e}")
            return jsonify({"error": "Failed to fetch ticker data"}), 500
    
    # Generate technical analysis
    analysis = generate_trade_analysis(ticker, current_price, short_pct, put_call_ratio)
    
    return jsonify({
        "ticker": ticker,
        "date": date,
        "current_price": current_price,
        "short_pct": short_pct,
        "put_call_ratio": put_call_ratio,
        "analysis": analysis
    })


@app.route("/api/poll", methods=["POST"])
def api_poll():
    """Trigger a manual data poll."""
    poll_all_tickers()
    return jsonify({"status": "ok"})


# ── Scheduler (runs once in the gunicorn preload master process) ────────

_scheduler_started = False

def start_scheduler():
    global _scheduler_started
    if _scheduler_started:
        return
    _scheduler_started = True

    init_db()

    # Check if we have data for today
    today = datetime.now(EST).strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    existing = conn.execute(
        "SELECT COUNT(*) FROM short_interest WHERE poll_date = ?", (today,)
    ).fetchone()[0]
    conn.close()

    if existing == 0:
        log.info("No data for today — scheduling initial poll in background...")
        import threading
        threading.Thread(target=poll_all_tickers, daemon=True).start()
    else:
        log.info(f"Already have {existing} records for {today}")

    # Schedule polls from config
    schedule_cfg = CONFIG.get("poll_schedule", {})
    hours = schedule_cfg.get("hours", [10, 15])
    days = schedule_cfg.get("days", "mon-fri")

    scheduler = BackgroundScheduler(timezone=EST)
    for h in hours:
        job_id = f"poll_{h:02d}00"
        scheduler.add_job(
            poll_all_tickers,
            CronTrigger(hour=h, minute=0, day_of_week=days, timezone=EST),
            id=job_id,
            name=f"Poll at {h}:00 EST ({days})",
        )
    scheduler.start()
    log.info(f"Scheduler set: {hours} EST, {days}")


# Start scheduler when module loads (gunicorn --preload calls this once)
start_scheduler()


# ── Standalone fallback (python3 app.py) ────────────────────────────────

if __name__ == "__main__":
    log.info(f"Starting dev server on {HOST}:{PORT}")
    app.run(host=HOST, port=PORT, debug=False, use_reloader=False)
