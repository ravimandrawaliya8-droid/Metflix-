#!/usr/bin/env python3
"""
sync.py — Telegram channel -> Postgres indexer (bot-only) + Flask uptime endpoint
Designed for Render / VPS. Run as a web service (python sync.py).
"""

import os
import time
import json
import logging
import threading
from datetime import datetime
from typing import Optional

import requests
from flask import Flask, jsonify, request

# Pyrogram (sync client)
from pyrogram import Client
from pyrogram.errors import FloodWait

# psycopg (psycopg3 binary). Ensure requirements install psycopg[binary]
try:
    import psycopg
    from psycopg.rows import dict_row
except Exception as e:
    psycopg = None
    dict_row = None

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("sync")

# ---------- Config (env) ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")                 # required: BotFather token
DB_CHANNEL = os.getenv("DB_CHANNEL")               # required: channel id like -100123... or @name
DATABASE_URL = os.getenv("DATABASE_URL")           # required: full postgres url
BATCH_LIMIT = int(os.getenv("BATCH_LIMIT", "5000"))  # how many messages to index per run
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300")) # how often (seconds) to auto-run index
PORT = int(os.getenv("PORT", "10000"))             # Flask port (Render: $PORT usually)
API_ID = os.getenv("API_ID")                       # optional for pyrogram user session (not used here)
API_HASH = os.getenv("API_HASH")                   # optional
APP_NAME = os.getenv("APP_NAME", "metflic-sync")

if not BOT_TOKEN or not DB_CHANNEL or not DATABASE_URL:
    log.error("BOT_TOKEN, DB_CHANNEL and DATABASE_URL are required environment variables.")
    raise SystemExit("Missing required env variables")

API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

# ---------- DB helpers ----------
def get_db_conn():
    """
    Return a new psycopg connection using psycopg3.
    Assumes psycopg[binary] is installed.
    """
    if not psycopg:
        raise RuntimeError("psycopg (psycopg3) not installed. Install psycopg[binary] in requirements.")
    # connect with dsn string; use autocommit mode for simplicity
    conn = psycopg.connect(DATABASE_URL, autocommit=True, row_factory=dict_row)
    return conn

def ensure_table():
    q = """
    CREATE TABLE IF NOT EXISTS movies (
        id SERIAL PRIMARY KEY,
        slug TEXT,
        title TEXT,
        year INTEGER,
        file_id TEXT,
        file_unique_id TEXT UNIQUE,
        caption TEXT,
        file_size BIGINT,
        mime_type TEXT,
        channel_id BIGINT,
        message_id BIGINT,
        extra JSONB,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(q)
    finally:
        conn.close()
    log.info("DB table ensured")

def insert_movie_record(record: dict) -> bool:
    """
    Insert record dict into movies table.
    record keys: slug,title,year,file_id,file_unique_id,caption,file_size,mime_type,channel_id,message_id,extra
    Returns True if inserted (or already exists) — False if error.
    """
    sql = """
    INSERT INTO movies
      (slug,title,year,file_id,file_unique_id,caption,file_size,mime_type,channel_id,message_id,extra)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (file_unique_id) DO NOTHING
    RETURNING id;
    """
    params = (
        record.get("slug"),
        record.get("title"),
        record.get("year"),
        record.get("file_id"),
        record.get("file_unique_id"),
        record.get("caption"),
        record.get("file_size"),
        record.get("mime_type"),
        record.get("channel_id"),
        record.get("message_id"),
        json.dumps(record.get("extra", {}))
    )
    try:
        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                r = cur.fetchone()
                if r and r.get("id"):
                    log.info("Inserted DB id=%s msg=%s title=%s", r["id"], record.get("message_id"), record.get("title"))
                    return True
                else:
                    # conflict or nothing returned (already existing)
                    log.debug("Already present (or no id returned) for message_id=%s", record.get("message_id"))
                    return True
        finally:
            conn.close()
    except Exception:
        log.exception("DB insert error for message %s", record.get("message_id"))
        return False

# ---------- Telegram / Pyrogram client ----------
# Use Bot-only Pyrogram client (needs to be admin to read channel history)
pyro = Client("sync_bot", bot_token=BOT_TOKEN)

def start_pyro():
    try:
        if not pyro.is_running:
            pyro.start()
            log.info("Pyrogram started")
    except Exception:
        log.exception("Failed to start Pyrogram client")
        raise

def stop_pyro():
    try:
        if pyro.is_running:
            pyro.stop()
            log.info("Pyrogram stopped")
    except Exception:
        log.exception("Failed to stop Pyrogram client")

# ---------- Utils: clean caption, slug ----------
import re
def clean_caption(caption: Optional[str]) -> str:
    if not caption:
        return ""
    line = caption.splitlines()[0].strip()
    line = re.sub(r"@[\w_]+", "", line)                # remove @usernames
    line = re.sub(r"join premium.*", "", line, flags=re.I)
    line = re.sub(r"[_\.\+\-×\u00D7]+", " ", line)
    line = re.sub(r"\.(mkv|mp4|avi|mov|zip|rar)$", "", line, flags=re.I)
    return re.sub(r"\s+", " ", line).strip()

def slugify(text: str) -> str:
    s = (text or "").lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s[:150]

# ---------- Indexing logic ----------
def index_channel_history(limit=1000, start_from: Optional[int]=None):
    """
    Iterate channel history and insert items to DB.
    If start_from provided, start after that message_id (useful for resume).
    Returns number inserted.
    """
    inserted = 0
    start_pyro_if_needed = False
    try:
        start_pyro()
        start_pyro_if_needed = True
    except Exception:
        log.exception("Cannot start Pyrogram")
        return 0

    try:
        kwargs = {"limit": limit}
        # Pyrogram iter_history supports offset_id, but we'll just fetch recent messages.
        # If start_from provided, we can use offset_id=start_from to continue from later ids.
        if start_from:
            kwargs["offset_id"] = start_from

        log.info("Iterating history for channel=%s limit=%s", DB_CHANNEL, limit)
        # iter_history yields pyrogram Message objects
        for m in pyro.iter_history(DB_CHANNEL, **kwargs):
            # find media object
            media = None
            for k in ("document","video","animation","audio","voice","photo"):
                if getattr(m, k, None):
                    media = getattr(m, k)
                    break
            if not media:
                continue
            # prepare record
            raw_caption = (m.caption or m.text or "")[:1000]
            title = clean_caption(raw_caption) or f"file-{m.message_id}"
            rec = {
                "slug": slugify(title),
                "title": title,
                "year": None,
                "file_id": getattr(media, "file_id", None),
                "file_unique_id": getattr(media, "file_unique_id", None),
                "caption": raw_caption,
                "file_size": getattr(media, "file_size", None),
                "mime_type": getattr(media, "mime_type", None),
                "channel_id": m.chat.id if m.chat else None,
                "message_id": m.message_id,
                "extra": {}
            }
            ok = insert_movie_record(rec)
            if ok:
                inserted += 1
            # throttle small sleep to avoid hitting limits
            time.sleep(0.01)
            # if we've inserted many, respect batch limits externally
            if inserted >= limit:
                break
    except FloodWait as e:
        # if Telegram asks to wait
        wait = int(e.value) + 1
        log.warning("FloodWait encountered. Sleeping %s seconds", wait)
        time.sleep(wait)
    except Exception:
        log.exception("Error while iterating channel history")
    finally:
        # do not stop pyro here — keep running for reuse
        log.info("Indexing done, inserted=%s", inserted)
    return inserted

# ---------- Background worker & Flask endpoints ----------
app = Flask(__name__)
_index_lock = threading.Lock()
_last_run = {"time": None, "inserted": 0}

def background_loop():
    while True:
        try:
            log.info("Auto-sync loop: starting indexing run")
            inserted = 0
            with _index_lock:
                inserted = index_channel_history(limit=BATCH_LIMIT)
                _last_run["time"] = datetime.utcnow().isoformat()
                _last_run["inserted"] = inserted
            log.info("Auto-sync loop completed inserted=%s", inserted)
        except Exception:
            log.exception("Auto-sync run failed")
        # sleep until next scheduled run
        time.sleep(POLL_INTERVAL)

@app.route("/")
def root():
    return jsonify({"ok": True, "service": APP_NAME, "status": "running", "last_run": _last_run})

@app.route("/health")
def health():
    return jsonify({"ok": True, "time": datetime.utcnow().isoformat()})

@app.route("/sync", methods=["POST","GET"])
def manual_sync():
    # manual trigger
    if _index_lock.locked():
        return jsonify({"ok": False, "error": "sync already running"}), 409
    t = threading.Thread(target=_manual_sync_task, daemon=True)
    t.start()
    return jsonify({"ok": True, "started": True})

def _manual_sync_task():
    with _index_lock:
        inserted = index_channel_history(limit=BATCH_LIMIT)
        _last_run["time"] = datetime.utcnow().isoformat()
        _last_run["inserted"] = inserted
        log.info("Manual sync finished inserted=%s", inserted)

# ---------- Entrypoint ----------
def start_background():
    # ensure DB table
    try:
        ensure_table()
    except Exception:
        log.exception("Could not ensure DB table; exiting")
        raise

    # start pyro (best-effort)
    try:
        start_pyro()
    except Exception:
        log.exception("Pyrogram start failed (will retry later)")

    t = threading.Thread(target=background_loop, daemon=True)
    t.start()
    log.info("Background indexing thread started")

if __name__ == "__main__":
    log.info("Starting sync.py web service")
    start_background()
    # Run flask (Render will call this process)
    app.run(host="0.0.0.0", port=PORT)
