#!/usr/bin/env python3
"""
sync.py — Telegram channel -> Postgres indexer (bot-only)
Webhook-compatible + Background indexing
Designed for Render free plan (Web Service)
"""

import os
import time
import json
import logging
import threading
from datetime import datetime
from typing import Optional

from flask import Flask, request, jsonify

# Pyrogram
from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait

# psycopg3 binary
import psycopg
from psycopg.rows import dict_row

import re

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("sync")

# ---------- Config ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_CHANNEL = os.getenv("DB_CHANNEL")  # e.g., -100123456789
DATABASE_URL = os.getenv("DATABASE_URL")
BATCH_LIMIT = int(os.getenv("BATCH_LIMIT", "5000"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))
PORT = int(os.getenv("PORT", "10000"))
APP_NAME = os.getenv("APP_NAME", "metflic-sync")

if not BOT_TOKEN or not DB_CHANNEL or not DATABASE_URL:
    raise SystemExit("BOT_TOKEN, DB_CHANNEL and DATABASE_URL are required")

# ---------- DB helpers ----------
def get_db_conn():
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
    sql = """
    INSERT INTO movies
      (slug,title,year,file_id,file_unique_id,caption,file_size,mime_type,channel_id,message_id,extra)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                    log.info("Inserted id=%s msg=%s title=%s", r["id"], record.get("message_id"), record.get("title"))
                    return True
                return True
        finally:
            conn.close()
    except Exception:
        log.exception("DB insert error for message %s", record.get("message_id"))
        return False

# ---------- Utils ----------
def clean_caption(caption: Optional[str]) -> str:
    if not caption:
        return ""
    line = caption.splitlines()[0].strip()
    line = re.sub(r"@[\w_]+", "", line)
    line = re.sub(r"join premium.*", "", line, flags=re.I)
    line = re.sub(r"[_\.\+\-×\u00D7]+", " ", line)
    line = re.sub(r"\.(mkv|mp4|avi|mov|zip|rar)$", "", line, flags=re.I)
    return re.sub(r"\s+", " ", line).strip()

def slugify(text: str) -> str:
    s = (text or "").lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s[:150]

def prepare_record(msg: Message) -> dict:
    media = None
    for k in ("document","video","animation","audio","voice","photo"):
        if getattr(msg, k, None):
            media = getattr(msg, k)
            break
    if not media:
        return {}
    raw_caption = (msg.caption or msg.text or "")[:1000]
    title = clean_caption(raw_caption) or f"file-{msg.message_id}"
    rec = {
        "slug": slugify(title),
        "title": title,
        "year": None,
        "file_id": getattr(media, "file_id", None),
        "file_unique_id": getattr(media, "file_unique_id", None),
        "caption": raw_caption,
        "file_size": getattr(media, "file_size", None),
        "mime_type": getattr(media, "mime_type", None),
        "channel_id": msg.chat.id if msg.chat else None,
        "message_id": msg.message_id,
        "extra": {}
    }
    return rec

# ---------- Pyrogram client ----------
pyro = Client("sync_bot", bot_token=BOT_TOKEN)
_index_lock = threading.Lock()
_last_run = {"time": None, "inserted": 0}

def start_pyro():
    pyro.start()
    log.info("Pyrogram started")

# ---------- Indexing ----------
def index_channel_history(limit=1000, start_from: Optional[int]=None):
    inserted = 0
    try:
        start_pyro()
        kwargs = {"limit": limit}
        if start_from:
            kwargs["offset_id"] = start_from
        for m in pyro.iter_history(DB_CHANNEL, **kwargs):
            rec = prepare_record(m)
            if not rec:
                continue
            if insert_movie_record(rec):
                inserted += 1
            time.sleep(0.01)
            if inserted >= limit:
                break
    except FloodWait as e:
        wait = int(e.value)+1
        log.warning("FloodWait sleeping %s sec", wait)
        time.sleep(wait)
    except Exception:
        log.exception("Indexing error")
    log.info("Indexing done inserted=%s", inserted)
    return inserted

def background_loop():
    while True:
        with _index_lock:
            inserted = index_channel_history(limit=BATCH_LIMIT)
            _last_run["time"] = datetime.utcnow().isoformat()
            _last_run["inserted"] = inserted
        time.sleep(POLL_INTERVAL)

# ---------- Flask web service ----------
app = Flask(__name__)

@app.route("/")
def root():
    return jsonify({"ok": True, "service": APP_NAME, "last_run": _last_run})

@app.route("/health")
def health():
    return jsonify({"ok": True, "time": datetime.utcnow().isoformat()})

@app.route("/sync", methods=["POST","GET"])
def manual_sync():
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
        log.info("Manual sync done inserted=%s", inserted)

# ---------- Webhook endpoint ----------
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True)
    if not data:
        return jsonify({"ok": False, "error": "empty"}), 400
    # Pyrogram Message object simulation
    from pyrogram.types import Message
    from pyrogram.raw import functions
    # We'll insert manually using file_unique_id if available
    try:
        # Minimal: expect {"file_unique_id":..., "file_id":..., "caption":..., "chat_id":..., "message_id":...}
        rec = {
            "slug": slugify(data.get("caption", f"file-{data.get('message_id')}")),
            "title": data.get("caption", f"file-{data.get('message_id')}"),
            "year": None,
            "file_id": data.get("file_id"),
            "file_unique_id": data.get("file_unique_id"),
            "caption": data.get("caption"),
            "file_size": data.get("file_size"),
            "mime_type": data.get("mime_type"),
            "channel_id": data.get("chat_id"),
            "message_id": data.get("message_id"),
            "extra": {}
        }
        inserted = insert_movie_record(rec)
        return jsonify({"ok": True, "inserted": inserted})
    except Exception:
        log.exception("Webhook insert error")
        return jsonify({"ok": False, "error": "internal"}), 500

# ---------- Entrypoint ----------
def start_background():
    ensure_table()
    t = threading.Thread(target=background_loop, daemon=True)
    t.start()
    log.info("Background thread started")
    start_pyro()

if __name__ == "__main__":
    log.info("Starting sync.py service")
    start_background()
    app.run(host="0.0.0.0", port=PORT)
