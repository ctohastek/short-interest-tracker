"""Gunicorn configuration — reads from tracker_config.json for port + SSL."""

import json
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "tracker_config.json")

with open(CONFIG_PATH, "r") as f:
    _cfg = json.load(f)

# ── Bind ──
_host = _cfg.get("host", "0.0.0.0")
_port = _cfg.get("port", 60914)
bind = f"{_host}:{_port}"

# ── Workers ──
workers = _cfg.get("workers", 4)
worker_class = "sync"
timeout = 300  # long timeout for poll endpoint

# ── Preload app so scheduler starts once in master ──
preload_app = True

# ── SSL ──
_ssl = _cfg.get("ssl", {})
if _ssl.get("enabled", False):
    certfile = _ssl.get("certfile")
    keyfile = _ssl.get("keyfile")
    ca_certs = _ssl.get("ca_certs")

# ── Logging ──
accesslog = os.path.join(BASE_DIR, "data", "access.log")
errorlog = os.path.join(BASE_DIR, "data", "gunicorn-error.log")
loglevel = "info"

# ── Process naming ──
proc_name = "squeezetracker"
