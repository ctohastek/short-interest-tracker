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
import re
import pandas as pd
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
    
    # Main short interest table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS short_interest (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            short_pct REAL,
            short_dollar TEXT,
            market_cap TEXT,
            price REAL,
            put_call_ratio REAL,
            implied_volatility REAL,
            poll_date TEXT NOT NULL,
            poll_ts INTEGER NOT NULL,
            UNIQUE(ticker, poll_date)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_si_date ON short_interest(poll_date)
    """)
    
    # Gap gainers table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gap_gainers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            pct_change REAL,
            dollar_change REAL,
            price REAL,
            implied_volatility REAL,
            market_cap TEXT,
            gap_type TEXT,
            poll_date TEXT NOT NULL,
            poll_ts INTEGER NOT NULL,
            rank INTEGER,
            UNIQUE(ticker, poll_date)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_gap_gainers_date ON gap_gainers(poll_date)
    """)
    
    # Gap losers table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gap_losers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            pct_change REAL,
            dollar_change REAL,
            price REAL,
            implied_volatility REAL,
            market_cap TEXT,
            gap_type TEXT,
            poll_date TEXT NOT NULL,
            poll_ts INTEGER NOT NULL,
            rank INTEGER,
            UNIQUE(ticker, poll_date)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_gap_losers_date ON gap_losers(poll_date)
    """)
    
    # Intraday movers table (watchlist top 10 gainers + losers, 4x daily)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS intraday_movers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            pct_change REAL,
            dollar_change REAL,
            price REAL,
            implied_volatility REAL,
            market_cap TEXT,
            short_pct REAL,
            put_call_ratio REAL,
            mover_type TEXT,
            poll_date TEXT NOT NULL,
            poll_ts INTEGER NOT NULL,
            poll_slot TEXT NOT NULL,
            rank INTEGER,
            UNIQUE(ticker, poll_date, poll_slot, mover_type)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_intraday_date ON intraday_movers(poll_date)
    """)

    # Migrate: add put_call_ratio column if missing (existing DBs)
    try:
        conn.execute("SELECT put_call_ratio FROM short_interest LIMIT 1")
    except sqlite3.OperationalError:
        log.info("Migrating DB: adding put_call_ratio column...")
        conn.execute("ALTER TABLE short_interest ADD COLUMN put_call_ratio REAL")
    
    # Migrate: add implied_volatility column if missing
    try:
        conn.execute("SELECT implied_volatility FROM short_interest LIMIT 1")
    except sqlite3.OperationalError:
        log.info("Migrating DB: adding implied_volatility column...")
        conn.execute("ALTER TABLE short_interest ADD COLUMN implied_volatility REAL")

    # Migrate: add sector column to short_interest table
    try:
        conn.execute("SELECT sector FROM short_interest LIMIT 1")
    except sqlite3.OperationalError:
        log.info("Migrating DB: adding sector column to short_interest...")
        conn.execute("ALTER TABLE short_interest ADD COLUMN sector TEXT DEFAULT ''")

    # Migrate: add sector column to gap tables if missing
    try:
        conn.execute("SELECT sector FROM gap_gainers LIMIT 1")
    except sqlite3.OperationalError:
        log.info("Migrating DB: adding sector column to gap_gainers...")
        conn.execute("ALTER TABLE gap_gainers ADD COLUMN sector TEXT DEFAULT ''")
    try:
        conn.execute("SELECT sector FROM gap_losers LIMIT 1")
    except sqlite3.OperationalError:
        log.info("Migrating DB: adding sector column to gap_losers...")
        conn.execute("ALTER TABLE gap_losers ADD COLUMN sector TEXT DEFAULT ''")

    conn.commit()
    conn.close()


def prune_old_data():
    cutoff = (datetime.now(EST) - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM short_interest WHERE poll_date < ?", (cutoff,))
    conn.execute("DELETE FROM gap_gainers WHERE poll_date < ?", (cutoff,))
    conn.execute("DELETE FROM gap_losers WHERE poll_date < ?", (cutoff,))
    conn.execute("DELETE FROM intraday_movers WHERE poll_date < ?", (cutoff,))
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
                  "Market Cap", "Price", "Shs Outstand", "P/C", "Volatility")
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
        
        # Get Implied Volatility from yfinance
        implied_volatility = 0.0
        try:
            import yfinance as yf
            import time as _time
            from datetime import datetime, timezone
            stock = yf.Ticker(ticker)

            MIN_OI = 50
            MIN_VOL = 1
            MIN_DTE = 20
            IV_CAP = 300.0  # Cap displayed IV; mark above as illiquid

            def _dte(expiry_str):
                exp = datetime.strptime(expiry_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                now = datetime.now(timezone.utc)
                return (exp - now).days

            def _best_atm_iv(chain, current_price):
                """Return blended ATM IV (call+put avg) for a chain with liquidity filter."""
                for df in (chain.calls, chain.puts):
                    df['strike_diff'] = abs(df['strike'] - current_price)

                # Try strikes closest-to-ATM first, require liquidity
                calls_sorted = chain.calls.sort_values('strike_diff')
                puts_sorted  = chain.puts.sort_values('strike_diff')

                call_iv = None
                for _, row in calls_sorted.iterrows():
                    oi  = row.get('openInterest', 0) or 0
                    vol = row.get('volume', 0) or 0
                    iv  = row.get('impliedVolatility', None)
                    if oi >= MIN_OI and vol >= MIN_VOL and iv and iv > 0:
                        call_iv = iv * 100
                        break

                put_iv = None
                for _, row in puts_sorted.iterrows():
                    oi  = row.get('openInterest', 0) or 0
                    vol = row.get('volume', 0) or 0
                    iv  = row.get('impliedVolatility', None)
                    if oi >= MIN_OI and vol >= MIN_VOL and iv and iv > 0:
                        put_iv = iv * 100
                        break

                if call_iv and put_iv:
                    return (call_iv + put_iv) / 2
                return call_iv or put_iv  # fallback to whichever we got

            options = stock.options
            if options and price > 0:
                # Prefer first expiration with DTE >= MIN_DTE; fall back to nearest
                chosen_expiry = None
                for exp in options:
                    if _dte(exp) >= MIN_DTE:
                        chosen_expiry = exp
                        break
                if chosen_expiry is None:
                    chosen_expiry = options[0]  # last resort: nearest

                opt_chain = stock.option_chain(chosen_expiry)
                blended = _best_atm_iv(opt_chain, price)

                # If liquid contract not found in chosen expiry, try next expirations
                if blended is None:
                    for exp in options:
                        if exp == chosen_expiry:
                            continue
                        try:
                            chain2 = stock.option_chain(exp)
                            blended = _best_atm_iv(chain2, price)
                            if blended is not None:
                                break
                        except Exception:
                            continue

                if blended is not None:
                    if blended > IV_CAP:
                        implied_volatility = -1.0  # sentinel: illiquid / capped
                    else:
                        implied_volatility = blended

            # Small delay to avoid rate limiting
            _time.sleep(0.2)
        except Exception as e:
            log.warning(f"Failed to fetch IV for {ticker}: {e}")

        sector = _fetch_sector(ticker)
        return {
            "ticker": ticker,
            "short_pct": short_pct,
            "short_dollar": short_dollar,
            "market_cap": market_cap,
            "price": price,
            "put_call_ratio": put_call,
            "implied_volatility": implied_volatility,
            "sector": sector,
        }

    except Exception as e:
        log.error(f"Error fetching {ticker}: {e}")
        return None


# ── Shared Robust IV Helper ─────────────────────────────────────────────

def _fetch_robust_iv(ticker: str, price: float) -> float:
    """
    Fetch blended ATM (call+put) IV for `ticker` at `price`.
    Returns -1.0 as sentinel when IV > 300% (illiquid/capped).
    Returns 0.0 on failure or no data.
    """
    import yfinance as yf
    import time as _time
    from datetime import datetime, timezone

    MIN_OI  = 50
    MIN_VOL = 1
    MIN_DTE = 20
    IV_CAP  = 300.0

    def _dte(expiry_str):
        exp = datetime.strptime(expiry_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return (exp - datetime.now(timezone.utc)).days

    def _best_atm_iv(chain, cur_price):
        for df in (chain.calls, chain.puts):
            df['strike_diff'] = abs(df['strike'] - cur_price)
        call_iv = put_iv = None
        for _, row in chain.calls.sort_values('strike_diff').iterrows():
            if (row.get('openInterest', 0) or 0) >= MIN_OI and \
               (row.get('volume', 0) or 0) >= MIN_VOL and \
               row.get('impliedVolatility') and row['impliedVolatility'] > 0:
                call_iv = row['impliedVolatility'] * 100
                break
        for _, row in chain.puts.sort_values('strike_diff').iterrows():
            if (row.get('openInterest', 0) or 0) >= MIN_OI and \
               (row.get('volume', 0) or 0) >= MIN_VOL and \
               row.get('impliedVolatility') and row['impliedVolatility'] > 0:
                put_iv = row['impliedVolatility'] * 100
                break
        if call_iv and put_iv:
            return (call_iv + put_iv) / 2
        return call_iv or put_iv

    try:
        stock   = yf.Ticker(ticker)
        options = stock.options
        if not options or price <= 0:
            return 0.0

        chosen = next((e for e in options if _dte(e) >= MIN_DTE), options[0])
        blended = _best_atm_iv(stock.option_chain(chosen), price)

        if blended is None:
            for exp in options:
                if exp == chosen:
                    continue
                try:
                    blended = _best_atm_iv(stock.option_chain(exp), price)
                    if blended is not None:
                        break
                except Exception:
                    continue

        _time.sleep(0.2)
        if blended is None:
            return 0.0
        return -1.0 if blended > IV_CAP else blended
    except Exception as e:
        log.warning(f"IV fetch failed for {ticker}: {e}")
        return 0.0


# ── Gap Data Collection ─────────────────────────────────────────────────

def _format_market_cap(mcap_val):
    """Format raw market cap number into human-readable string."""
    if not mcap_val or mcap_val <= 0:
        return "N/A"
    if mcap_val >= 1_000_000_000_000:
        return f"{mcap_val / 1_000_000_000_000:.2f}T"
    if mcap_val >= 1_000_000_000:
        return f"{mcap_val / 1_000_000_000:.2f}B"
    if mcap_val >= 1_000_000:
        return f"{mcap_val / 1_000_000:.1f}M"
    return f"{mcap_val:,.0f}"


def _fetch_sector(ticker: str) -> str:
    """Fetch abbreviated sector name for a ticker using yfinance."""
    _ABBREV = {
        "Technology": "Tech",
        "Financial Services": "Financials",
        "Healthcare": "Healthcare",
        "Consumer Cyclical": "Cons Cyc",
        "Consumer Defensive": "Cons Def",
        "Communication Services": "Comm Svcs",
        "Industrials": "Industrials",
        "Basic Materials": "Materials",
        "Energy": "Energy",
        "Real Estate": "Real Est",
        "Utilities": "Utilities",
    }
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        sector = info.get("sector") or ""
        return _ABBREV.get(sector, sector)
    except Exception:
        return ""


def _yahoo_screener(scr_id, count=100):
    """Fetch quotes from Yahoo Finance predefined screener."""
    url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
    params = {"scrIds": scr_id, "count": count}
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data["finance"]["result"][0].get("quotes", [])


def fetch_gap_data():
    """Fetch top gap gainers and losers from Yahoo Finance screener (10B-200B mkt cap)."""
    log.info("Fetching gap data from Yahoo Finance...")

    MIN_MCAP = 10e9
    MAX_MCAP = 200e9
    MAX_RESULTS = 30  # per side

    gap_gainers = []
    gap_losers = []

    try:
        # ── Gainers ──
        quotes = _yahoo_screener("day_gainers", count=100)
        for q in quotes:
            mcap = q.get("marketCap") or 0
            if mcap < MIN_MCAP or mcap > MAX_MCAP:
                continue
            ticker = q.get("symbol", "")
            if not ticker or len(ticker) > 5:
                continue
            price = q.get("regularMarketPrice") or 0
            pct = q.get("regularMarketChangePercent") or 0
            dollar = q.get("regularMarketChange") or 0
            if price <= 0 or pct <= 0:
                continue

            iv = _fetch_robust_iv(ticker, price)
            sector = _fetch_sector(ticker)

            gap_gainers.append({
                "ticker": ticker,
                "pct_change": round(pct, 4),
                "dollar_change": round(dollar, 4),
                "price": round(price, 2),
                "implied_volatility": iv,
                "market_cap": _format_market_cap(mcap),
                "sector": sector,
                "gap_type": "up",
            })
            if len(gap_gainers) >= MAX_RESULTS:
                break

        # ── Losers ──
        quotes = _yahoo_screener("day_losers", count=100)
        for q in quotes:
            mcap = q.get("marketCap") or 0
            if mcap < MIN_MCAP or mcap > MAX_MCAP:
                continue
            ticker = q.get("symbol", "")
            if not ticker or len(ticker) > 5:
                continue
            price = q.get("regularMarketPrice") or 0
            pct = q.get("regularMarketChangePercent") or 0
            dollar = q.get("regularMarketChange") or 0
            if price <= 0 or pct >= 0:
                continue

            iv = _fetch_robust_iv(ticker, price)
            sector = _fetch_sector(ticker)
            # Ensure dollar change is negative for losers
            if dollar > 0:
                dollar = -dollar

            gap_losers.append({
                "ticker": ticker,
                "pct_change": round(pct, 4),
                "dollar_change": round(dollar, 4),
                "price": round(price, 2),
                "implied_volatility": iv,
                "market_cap": _format_market_cap(mcap),
                "sector": sector,
                "gap_type": "down",
            })
            if len(gap_losers) >= MAX_RESULTS:
                break

    except Exception as e:
        log.error(f"Yahoo Finance screener error: {e}")

    # Sort: gainers desc, losers asc
    gap_gainers.sort(key=lambda x: x["pct_change"], reverse=True)
    gap_losers.sort(key=lambda x: x["pct_change"])

    for i, item in enumerate(gap_gainers):
        item["rank"] = i + 1
    for i, item in enumerate(gap_losers):
        item["rank"] = i + 1

    log.info(f"Found {len(gap_gainers)} gap gainers and {len(gap_losers)} gap losers (Yahoo Finance)")
    return gap_gainers[:MAX_RESULTS], gap_losers[:MAX_RESULTS]


def poll_gap_data():
    """Poll and store gap data at market open (9:30 AM EST)."""
    log.info("Starting gap data poll...")
    today = datetime.now(EST).strftime("%Y-%m-%d")
    ts = int(time.time())
    
    gap_gainers, gap_losers = fetch_gap_data()
    
    if not gap_gainers and not gap_losers:
        log.warning("No gap data collected — skipping DB write")
        return
    
    conn = sqlite3.connect(DB_PATH)
    
    # Delete existing gap data for today before inserting new data
    conn.execute("DELETE FROM gap_gainers WHERE poll_date = ?", (today,))
    conn.execute("DELETE FROM gap_losers WHERE poll_date = ?", (today,))
    
    # Store gap gainers
    for item in gap_gainers:
        try:
            conn.execute("""
                INSERT OR REPLACE INTO gap_gainers 
                (ticker, pct_change, dollar_change, price, implied_volatility, 
                 market_cap, sector, gap_type, poll_date, poll_ts, rank)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                item["ticker"], item["pct_change"], item["dollar_change"],
                item["price"], item["implied_volatility"], item["market_cap"],
                item.get("sector", ""), item["gap_type"], today, ts, item["rank"]
            ))
        except Exception as e:
            log.error(f"Error storing gap gainer {item['ticker']}: {e}")
    
    # Store gap losers
    for item in gap_losers:
        try:
            conn.execute("""
                INSERT OR REPLACE INTO gap_losers 
                (ticker, pct_change, dollar_change, price, implied_volatility, 
                 market_cap, sector, gap_type, poll_date, poll_ts, rank)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                item["ticker"], item["pct_change"], item["dollar_change"],
                item["price"], item["implied_volatility"], item["market_cap"],
                item.get("sector", ""), item["gap_type"], today, ts, item["rank"]
            ))
        except Exception as e:
            log.error(f"Error storing gap loser {item['ticker']}: {e}")
    
    conn.commit()
    conn.close()
    log.info(f"Stored {len(gap_gainers)} gap gainers and {len(gap_losers)} gap losers")


# ── Intraday Movers ─────────────────────────────────────────────────────────

def _intraday_slot() -> str:
    """Label for the current poll slot."""
    now = datetime.now(EST)
    h, m = now.hour, now.minute
    if h < 11:
        return "945"
    elif h < 14:
        return "1145"
    elif h < 15 or (h == 15 and m < 30):
        return "1345"
    else:
        return "1555"


def fetch_intraday_movers():
    """
    Fetch intraday % change for all watchlist tickers via yfinance.
    Returns (gainers[:20], losers[:20]) sorted by pct_change desc/asc.
    """
    import yfinance as yf
    results = []
    log.info(f"Intraday poll: fetching {len(TICKERS)} tickers...")

    try:
        raw = yf.download(
            TICKERS, period="2d", interval="1d",
            auto_adjust=True, progress=False, threads=True,
        )
        closes      = raw["Close"].iloc[-1] if len(raw) >= 1 else None
        prev_closes = raw["Close"].iloc[-2] if len(raw) >= 2 else None
    except Exception as e:
        log.warning(f"yfinance batch download failed: {e}")
        closes = None
        prev_closes = None

    for ticker in TICKERS:
        try:
            price = prev_close = 0.0
            if closes is not None and ticker in closes.index:
                v = closes[ticker]
                price = float(v) if not pd.isna(v) else 0.0
            if prev_closes is not None and ticker in prev_closes.index:
                v = prev_closes[ticker]
                prev_close = float(v) if not pd.isna(v) else 0.0

            if price <= 0 or prev_close <= 0:
                info       = yf.Ticker(ticker).fast_info
                price      = float(getattr(info, "last_price", 0) or 0)
                prev_close = float(getattr(info, "previous_close", 0) or 0)

            if price <= 0 or prev_close <= 0:
                continue

            dollar_change = price - prev_close
            pct_change    = (dollar_change / prev_close) * 100

            try:
                mc = float(getattr(yf.Ticker(ticker).fast_info, "market_cap", 0) or 0)
                if mc >= 1e12:
                    market_cap = f"{mc/1e12:.2f}T"
                elif mc >= 1e9:
                    market_cap = f"{mc/1e9:.2f}B"
                elif mc >= 1e6:
                    market_cap = f"{mc/1e6:.2f}M"
                else:
                    market_cap = "N/A"
            except Exception:
                market_cap = "N/A"

            iv = _fetch_robust_iv(ticker, price)

            short_pct = put_call_ratio = 0.0
            try:
                fd = fetch_ticker_data(ticker)
                if fd:
                    short_pct      = fd.get("short_pct", 0.0)
                    put_call_ratio = fd.get("put_call_ratio", 0.0)
            except Exception:
                pass

            results.append({
                "ticker":             ticker,
                "pct_change":         round(pct_change, 2),
                "dollar_change":      round(dollar_change, 4),
                "price":              round(price, 2),
                "implied_volatility": iv,
                "market_cap":         market_cap,
                "short_pct":          short_pct,
                "put_call_ratio":     put_call_ratio,
            })
        except Exception as e:
            log.warning(f"Intraday fetch failed for {ticker}: {e}")

    results.sort(key=lambda x: x["pct_change"], reverse=True)
    gainers = results[:20]
    losers  = sorted(results, key=lambda x: x["pct_change"])[:20]
    log.info(f"Intraday: {len(gainers)} gainers, {len(losers)} losers")
    return gainers, losers


def poll_intraday_movers():
    """Fetch and store intraday movers (4x daily)."""
    log.info("Starting intraday movers poll...")
    today = datetime.now(EST).strftime("%Y-%m-%d")
    ts    = int(time.time())
    slot  = _intraday_slot()

    gainers, losers = fetch_intraday_movers()
    if not gainers and not losers:
        log.warning("No intraday data — skipping DB write")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "DELETE FROM intraday_movers WHERE poll_date = ? AND poll_slot = ?",
        (today, slot)
    )
    for rank, item in enumerate(gainers, 1):
        try:
            conn.execute("""
                INSERT OR REPLACE INTO intraday_movers
                (ticker,pct_change,dollar_change,price,implied_volatility,
                 market_cap,short_pct,put_call_ratio,mover_type,
                 poll_date,poll_ts,poll_slot,rank)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                item["ticker"], item["pct_change"], item["dollar_change"],
                item["price"], item["implied_volatility"], item["market_cap"],
                item["short_pct"], item["put_call_ratio"], "gainer",
                today, ts, slot, rank
            ))
        except Exception as e:
            log.error(f"Intraday gainer DB error {item['ticker']}: {e}")
    for rank, item in enumerate(losers, 1):
        try:
            conn.execute("""
                INSERT OR REPLACE INTO intraday_movers
                (ticker,pct_change,dollar_change,price,implied_volatility,
                 market_cap,short_pct,put_call_ratio,mover_type,
                 poll_date,poll_ts,poll_slot,rank)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                item["ticker"], item["pct_change"], item["dollar_change"],
                item["price"], item["implied_volatility"], item["market_cap"],
                item["short_pct"], item["put_call_ratio"], "loser",
                today, ts, slot, rank
            ))
        except Exception as e:
            log.error(f"Intraday loser DB error {item['ticker']}: {e}")
    conn.commit()
    conn.close()
    log.info(f"Stored intraday slot={slot}: {len(gainers)} gainers, {len(losers)} losers")


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
                 implied_volatility, sector, poll_date, poll_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row["ticker"], row["short_pct"], row["short_dollar"],
                row["market_cap"], row["price"], row["put_call_ratio"],
                row["implied_volatility"], row.get("sector", ""), today, ts,
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
app.config['TEMPLATES_AUTO_RELOAD'] = True  # Auto-reload templates
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0  # Disable caching of static files

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


def is_mobile():
    """Detect if request is from mobile device."""
    user_agent = request.headers.get('User-Agent', '').lower()
    mobile_indicators = [
        'mobile', 'android', 'iphone', 'ipad', 'ipod', 'blackberry',
        'windows phone', 'webos', 'opera mini', 'iemobile'
    ]
    
    # Check user agent
    for indicator in mobile_indicators:
        if indicator in user_agent:
            return True
    
    # Check viewport width via cookie or header
    viewport_width = request.headers.get('X-Viewport-Width')
    if viewport_width:
        try:
            return int(viewport_width) < 768
        except ValueError:
            pass
    
    return False


@app.route("/")
def index():
    """Serve mobile or desktop version based on detection."""
    if is_mobile():
        return render_template("index-mobile.html")
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
    """Return top short interest records for a given date."""
    limit = 8 if is_mobile() else 12
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(f"""
        SELECT ticker, short_pct, short_dollar, market_cap, price, poll_ts,
               put_call_ratio, implied_volatility, COALESCE(sector, '') as sector
        FROM short_interest
        WHERE poll_date = ?
        ORDER BY short_pct DESC
        LIMIT {limit}
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
            "implied_volatility": r[7] or 0.0,
            "sector": r[8],
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
    limit = 8 if is_mobile() else 12
    rows = conn.execute(f"""
        SELECT ticker, short_pct, short_dollar, market_cap, price, poll_ts,
               put_call_ratio, implied_volatility, COALESCE(sector, '') as sector
        FROM short_interest
        WHERE poll_date = ?
        ORDER BY short_pct DESC
        LIMIT {limit}
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
            "implied_volatility": r[7] or 0.0,
            "sector": r[8],
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


@app.route("/api/custom-ticker/<ticker>/<date>")
def api_custom_ticker(ticker, date):
    """Fetch data for a custom ticker (not in watchlist)."""
    # Fetch data using same function as regular polling
    data = fetch_ticker_data(ticker)
    if not data:
        return jsonify({"error": f"No data found for {ticker}"}), 404
    
    # Return in same format as regular data
    return jsonify({
        "ticker": data["ticker"],
        "short_pct": data["short_pct"],
        "short_dollar": data["short_dollar"],
        "market_cap": data["market_cap"],
        "price": data["price"],
        "put_call_ratio": data["put_call_ratio"],
        "implied_volatility": data["implied_volatility"]
    })


@app.route("/api/poll", methods=["POST"])
def api_poll():
    """Trigger a manual data poll."""
    poll_all_tickers()
    return jsonify({"status": "ok"})


@app.route("/api/watchlist", methods=["GET"])
def api_watchlist_get():
    """Return the current watchlist."""
    try:
        with open(_watchlist_path, "r") as f:
            tickers = json.load(f)
        return jsonify(sorted(tickers))
    except Exception as e:
        log.error(f"Error reading watchlist: {e}")
        return jsonify([]), 500


@app.route("/api/watchlist", methods=["POST"])
def api_watchlist_save():
    """Save an updated watchlist. Validates all entries."""
    TICKER_RE = re.compile(r'^[A-Z]{1,5}$')
    try:
        data = request.get_json(silent=True)
        if not isinstance(data, list):
            return jsonify({"error": "Expected a JSON array"}), 400
        # Sanitize: only uppercase letters, 1-5 chars, max 100 items
        clean = []
        seen = set()
        for item in data:
            if not isinstance(item, str):
                continue
            t = re.sub(r'[^A-Z]', '', item.upper())[:5]
            if t and TICKER_RE.match(t) and t not in seen:
                clean.append(t)
                seen.add(t)
                if len(clean) >= 100:
                    break
        clean.sort()
        with open(_watchlist_path, "w") as f:
            json.dump(clean, f, indent=2)
        # Reload in-memory ticker list
        global TICKERS
        TICKERS = clean
        log.info(f"Watchlist updated: {len(clean)} tickers")
        return jsonify({"status": "ok", "count": len(clean)})
    except Exception as e:
        log.error(f"Error saving watchlist: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/gap-data/<date>")
def api_gap_data(date):
    """Return gap gainers and losers for a given date."""
    conn = sqlite3.connect(DB_PATH)
    
    # Get gap gainers
    gainers_rows = conn.execute("""
        SELECT ticker, pct_change, dollar_change, price, implied_volatility, 
               market_cap, rank, COALESCE(sector, '') as sector
        FROM gap_gainers
        WHERE poll_date = ?
        ORDER BY rank
        LIMIT 30
    """, (date,)).fetchall()
    
    # Get gap losers
    losers_rows = conn.execute("""
        SELECT ticker, pct_change, dollar_change, price, implied_volatility, 
               market_cap, rank, COALESCE(sector, '') as sector
        FROM gap_losers
        WHERE poll_date = ?
        ORDER BY rank
        LIMIT 30
    """, (date,)).fetchall()
    
    # Get actual gap poll timestamp from gap tables
    pts_row = conn.execute(
        "SELECT poll_ts FROM gap_gainers WHERE poll_date = ? ORDER BY poll_ts DESC LIMIT 1",
        (date,)
    ).fetchone()
    if not pts_row:
        pts_row = conn.execute(
            "SELECT poll_ts FROM gap_losers WHERE poll_date = ? ORDER BY poll_ts DESC LIMIT 1",
            (date,)
        ).fetchone()
    gap_poll_ts = pts_row[0] if pts_row else None

    conn.close()

    gainers = []
    for row in gainers_rows:
        gainers.append({
            "ticker": row[0],
            "pct_change": row[1],
            "dollar_change": row[2],
            "price": row[3],
            "implied_volatility": row[4],
            "market_cap": row[5],
            "rank": row[6],
            "sector": row[7]
        })

    losers = []
    for row in losers_rows:
        raw_dollar = row[2]
        # Losers must have negative dollar change; clamp corrupt/positive legacy values
        # If null (cleaned corrupt record), pass None → displays as N/A on frontend
        if raw_dollar is None:
            safe_dollar = None
        elif abs(raw_dollar) > 1000:  # sanity: per-share change can't be >$1000
            safe_dollar = None
        else:
            safe_dollar = -abs(raw_dollar)
        losers.append({
            "ticker": row[0],
            "pct_change": row[1],
            "dollar_change": safe_dollar,
            "price": row[3],
            "implied_volatility": row[4],
            "market_cap": row[5],
            "rank": row[6],
            "sector": row[7]
        })

    return jsonify({
        "gainers": gainers,
        "losers": losers,
        "date": date,
        "poll_ts": gap_poll_ts,
    })


@app.route("/api/gap-trade-plan/<ticker>/<date>")
def api_gap_trade_plan(ticker, date):
    """Generate a trade plan for a gap stock."""
    conn = sqlite3.connect(DB_PATH)
    
    # Try to find in gap gainers first
    row = conn.execute("""
        SELECT ticker, pct_change, dollar_change, price, implied_volatility, 
               market_cap, gap_type
        FROM gap_gainers
        WHERE ticker = ? AND poll_date = ?
    """, (ticker, date)).fetchone()
    
    if not row:
        # Try gap losers
        row = conn.execute("""
            SELECT ticker, pct_change, dollar_change, price, implied_volatility, 
                   market_cap, gap_type
            FROM gap_losers
            WHERE ticker = ? AND poll_date = ?
        """, (ticker, date)).fetchone()
    
    conn.close()
    
    if not row:
        return jsonify({"error": "Ticker not found in gap data for this date"}), 404
    
    ticker, pct_change, dollar_change, price, iv, market_cap, gap_type = row
    
    # Generate gap-specific trade plan
    if gap_type == "up":
        direction = "gap up"
        sentiment = "bullish"
        # Format dollar amount with commas
        formatted_dollar = f"{abs(dollar_change):,.2f}"
        gap_desc = f"Gapped up {abs(pct_change):.2f}% (${formatted_dollar})"
        
        # Analysis for gap ups
        if pct_change > 5:
            analysis = "Strong gap up suggests continuation potential. Look for consolidation above gap level."
            entry = price * 0.99  # Slightly below current price
            stop = price * 0.94   # Below gap fill level
            target = price * 1.08  # 8% target
        elif pct_change > 2:
            analysis = "Moderate gap up. Watch for partial fill before continuation."
            entry = price * 0.985
            stop = price * 0.96
            target = price * 1.06
        else:
            analysis = "Small gap up. Could be noise. Wait for confirmation."
            entry = price * 0.99
            stop = price * 0.97
            target = price * 1.04
    else:  # gap_type == "down"
        direction = "gap down"
        sentiment = "bearish"
        # Format dollar amount with commas
        formatted_dollar = f"{abs(dollar_change):,.2f}"
        gap_desc = f"Gapped down {abs(pct_change):.2f}% (${formatted_dollar})"
        
        # Analysis for gap downs
        if pct_change < -5:
            analysis = "Strong gap down suggests further downside. Resistance at gap fill level."
            entry = price * 1.01  # Slightly above current price for short
            stop = price * 1.06   # Above gap fill level
            target = price * 0.92  # 8% target for short
        elif pct_change < -2:
            analysis = "Moderate gap down. May see dead cat bounce before continuation."
            entry = price * 1.015
            stop = price * 1.04
            target = price * 0.94
        else:
            analysis = "Small gap down. Could be normal pullback. Wait for breakdown confirmation."
            entry = price * 1.01
            stop = price * 1.03
            target = price * 0.97
    
    # Add IV analysis (iv == -1 means illiquid/capped sentinel)
    if iv < 0:
        iv_analysis = "IV data unavailable (illiquid options or >300% — reading unreliable)."
    elif iv > 50:
        iv_analysis = f"High IV ({iv:.1f}%) suggests elevated option premiums. Consider defined-risk strategies."
    elif iv > 25:
        iv_analysis = f"Moderate IV ({iv:.1f}%). Options fairly priced."
    else:
        iv_analysis = f"Low IV ({iv:.1f}%). Options relatively cheap."
    
    trade_plan = {
        "ticker": ticker,
        "direction": direction,
        "sentiment": sentiment,
        "gap_description": gap_desc,
        "analysis": analysis,
        "iv_analysis": iv_analysis,
        "current_price": price,
        "gap_percent": pct_change,
        "gap_dollar": dollar_change,
        "implied_volatility": iv,
        "market_cap": market_cap,
        "entry": round(entry, 2),
        "stop": round(stop, 2),
        "target": round(target, 2),
        "risk_reward": round(abs(target - entry) / abs(entry - stop), 2)
    }
    
    return jsonify(trade_plan)



@app.route("/api/intraday/<date>")
def api_intraday_data(date):
    """Return intraday movers for a given date (latest poll slot)."""
    conn = sqlite3.connect(DB_PATH)
    # Get the most recent slot for this date
    slot_row = conn.execute(
        "SELECT poll_slot FROM intraday_movers WHERE poll_date = ? ORDER BY poll_ts DESC LIMIT 1",
        (date,)
    ).fetchone()
    slot = slot_row[0] if slot_row else None

    poll_ts_row = conn.execute(
        "SELECT poll_ts FROM intraday_movers WHERE poll_date = ? ORDER BY poll_ts DESC LIMIT 1",
        (date,)
    ).fetchone()
    poll_ts = poll_ts_row[0] if poll_ts_row else None

    gainers_rows = conn.execute("""
        SELECT ticker, pct_change, dollar_change, price, implied_volatility,
               market_cap, short_pct, put_call_ratio, rank
        FROM intraday_movers
        WHERE poll_date = ? AND mover_type = ? AND poll_slot = ?
        ORDER BY pct_change DESC LIMIT 20
    """, (date, "gainer", slot)).fetchall() if slot else []

    losers_rows = conn.execute("""
        SELECT ticker, pct_change, dollar_change, price, implied_volatility,
               market_cap, short_pct, put_call_ratio, rank
        FROM intraday_movers
        WHERE poll_date = ? AND mover_type = ? AND poll_slot = ?
        ORDER BY pct_change ASC LIMIT 20
    """, (date, "loser", slot)).fetchall() if slot else []
    conn.close()

    def row_to_dict(r):
        return {
            "ticker": r[0], "pct_change": r[1], "dollar_change": r[2],
            "price": r[3], "implied_volatility": r[4], "market_cap": r[5],
            "short_pct": r[6], "put_call_ratio": r[7], "rank": r[8],
        }

    return jsonify({
        "gainers":  [row_to_dict(r) for r in gainers_rows],
        "losers":   [row_to_dict(r) for r in losers_rows],
        "date":     date,
        "slot":     slot,
        "poll_ts":  poll_ts,
    })


@app.route("/api/intraday-trade-plan/<ticker>/<date>")
def api_intraday_trade_plan(ticker, date):
    """Generate a trade plan for an intraday mover."""
    conn = sqlite3.connect(DB_PATH)
    slot_row = conn.execute(
        "SELECT poll_slot FROM intraday_movers WHERE poll_date = ? ORDER BY poll_ts DESC LIMIT 1",
        (date,)
    ).fetchone()
    slot = slot_row[0] if slot_row else None

    row = conn.execute("""
        SELECT ticker, pct_change, dollar_change, price, implied_volatility,
               market_cap, short_pct, put_call_ratio, mover_type
        FROM intraday_movers
        WHERE ticker = ? AND poll_date = ? AND poll_slot = ?
        ORDER BY poll_ts DESC LIMIT 1
    """, (ticker, date, slot)).fetchone() if slot else None
    conn.close()

    if not row:
        return jsonify({"error": "Ticker not found in intraday data for this date"}), 404

    ticker, pct_change, dollar_change, price, iv, market_cap, short_pct, put_call_ratio, mover_type = row

    if mover_type == "gainer":
        direction = "intraday gainer"
        sentiment = "bullish"
        move_desc = f"Up {abs(pct_change):.2f}% intraday (${abs(dollar_change):.2f})"
        if pct_change > 5:
            analysis = "Strong intraday surge. Watch for continuation above VWAP with volume."
            entry  = price * 0.99
            stop   = price * 0.94
            target = price * 1.08
        elif pct_change > 2:
            analysis = "Moderate intraday gain. Look for pullback to VWAP as entry opportunity."
            entry  = price * 0.985
            stop   = price * 0.96
            target = price * 1.06
        else:
            analysis = "Small intraday move. Wait for volume confirmation before entering."
            entry  = price * 0.99
            stop   = price * 0.97
            target = price * 1.04
    else:
        direction = "intraday loser"
        sentiment = "bearish"
        move_desc = f"Down {abs(pct_change):.2f}% intraday (${abs(dollar_change):.2f})"
        if pct_change < -5:
            analysis = "Strong intraday selloff. Resistance at VWAP. Consider short on bounce."
            entry  = price * 1.01
            stop   = price * 1.06
            target = price * 0.92
        elif pct_change < -2:
            analysis = "Moderate intraday decline. Watch for dead-cat bounce before continuation."
            entry  = price * 1.015
            stop   = price * 1.04
            target = price * 0.94
        else:
            analysis = "Minor intraday dip. Wait for breakdown confirmation below support."
            entry  = price * 1.01
            stop   = price * 1.03
            target = price * 0.97

    if iv < 0:
        iv_analysis = "IV data unavailable (illiquid options or >300% — reading unreliable)."
    elif iv > 50:
        iv_analysis = f"High IV ({iv:.1f}%) — elevated premiums. Consider defined-risk strategies."
    elif iv > 25:
        iv_analysis = f"Moderate IV ({iv:.1f}%). Options fairly priced."
    else:
        iv_analysis = f"Low IV ({iv:.1f}%). Options relatively cheap."

    si_note = ""
    if short_pct > 20:
        si_note = f"High short interest ({short_pct:.1f}%) — squeeze risk elevated."
    elif short_pct > 10:
        si_note = f"Moderate short interest ({short_pct:.1f}%)."

    return jsonify({
        "ticker":           ticker,
        "direction":        direction,
        "sentiment":        sentiment,
        "move_description": move_desc,
        "analysis":         analysis,
        "iv_analysis":      iv_analysis,
        "si_note":          si_note,
        "current_price":    price,
        "pct_change":       pct_change,
        "dollar_change":    dollar_change,
        "implied_volatility": iv,
        "market_cap":       market_cap,
        "short_pct":        short_pct,
        "put_call_ratio":   put_call_ratio,
        "entry":   round(entry, 2),
        "stop":    round(stop, 2),
        "target":  round(target, 2),
        "risk_reward": round(abs(target - entry) / abs(entry - stop), 2),
    })


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

    # Schedule gap polls from config
    gap_schedule_cfg = CONFIG.get("gap_poll_schedule", {})
    gap_hour = gap_schedule_cfg.get("hour", 9)
    gap_minute = gap_schedule_cfg.get("minute", 30)
    gap_days = gap_schedule_cfg.get("days", "mon-fri")

    scheduler = BackgroundScheduler(timezone=EST)
    
    # Schedule regular short interest polls
    for h in hours:
        job_id = f"poll_{h:02d}00"
        scheduler.add_job(
            poll_all_tickers,
            CronTrigger(hour=h, minute=0, day_of_week=days, timezone=EST),
            id=job_id,
            name=f"Poll at {h}:00 EST ({days})",
        )
    
    # Schedule gap data poll at market open
    gap_job_id = f"gap_poll_{gap_hour:02d}{gap_minute:02d}"
    scheduler.add_job(
        poll_gap_data,
        CronTrigger(hour=gap_hour, minute=gap_minute, day_of_week=gap_days, timezone=EST),
        id=gap_job_id,
        name=f"Gap poll at {gap_hour}:{gap_minute:02d} EST ({gap_days})",
    )
    
    # Schedule intraday movers polls: 9:45, 11:45, 14:45, 15:55 EDT (mon-fri)
    intraday_slots = [(9, 45, "945"), (11, 45, "1145"), (14, 45, "1345"), (15, 55, "1555")]
    for ih, im, islot in intraday_slots:
        scheduler.add_job(
            poll_intraday_movers,
            CronTrigger(hour=ih, minute=im, day_of_week=days, timezone=EST),
            id=f"intraday_{islot}",
            name=f"Intraday movers at {ih}:{im:02d} EST ({days})",
        )

    scheduler.start()
    log.info(f"Scheduler set: short interest at {hours} EST, gap at {gap_hour}:{gap_minute:02d} EST, intraday at 9:45/11:45/14:45/15:55 EST, {days}")


# Start scheduler when module loads (gunicorn --preload calls this once)
start_scheduler()


# ── Standalone fallback (python3 app.py) ────────────────────────────────

if __name__ == "__main__":
    log.info(f"Starting dev server on {HOST}:{PORT}")
    app.run(host=HOST, port=PORT, debug=False, use_reloader=False)
