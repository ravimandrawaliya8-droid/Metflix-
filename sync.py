#!/usr/bin/env python3
"""
Render Movie Indexer - FINAL VERSION
Indexes all files posted in your Telegram movie channel
and inserts them into Render PostgreSQL.
"""

import os
import time
import json
import psycopg2
from urllib.request import urlopen, Request
from urllib.parse import urlencode

# ============================
# ENVIRONMENT VARIABLES
# ============================

BOT_TOKEN = os.getenv("BOT_TOKEN")
MOVIE_CHANNEL_ID = int(os.getenv("MOVIE_CHANNEL_ID", 0))
DATABASE_URL = os.getenv("DATABASE_URL")

if not BOT_TOKEN or not MOVIE_CHANNEL_ID or not DATABASE_URL:
    print("[FATAL] Missing env variables.")
    exit()

print("\n===== MOVIE INDEXER STARTED ON RENDER =====")
print(f"Channel: {MOVIE_CHANNEL_ID}")
print("============================================\n")

# ============================
# DB CONNECTION
# ============================

def db_connect():
    """Connect to Render PostgreSQL"""
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        print("[DB] Connected ✓")
        return conn
    except Exception as e:
        print("[DB ERROR]", e)
        exit()


conn = db_connect()
cur = conn.cursor()

# ============================
# DB INSERT
# ============================

def insert_movie(file_id, file_unique_id, title, caption, size, mime, msg_id):
    try:
        cur.execute("SELECT id FROM movies WHERE file_unique_id=%s", (file_unique_id,))
        if cur.fetchone():
            print(f"[SKIP] Already indexed: {title}")
            return

        cur.execute("""
            INSERT INTO movies (file_id, file_unique_id, title, caption, file_size, mime_type, message_id, channel_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (file_id, file_unique_id, title, caption, size, mime, msg_id, MOVIE_CHANNEL_ID))

        conn.commit()
        print(f"[OK] Indexed: {title}")

    except Exception as e:
        print("[INSERT ERROR]", e)
        conn.rollback()

# ============================
# TELEGRAM API CALL
# ============================

def tg(method, params=None):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    if params:
        url += "?" + urlencode(params)

    try:
        req = Request(url)
        data = urlopen(req).read()
        return json.loads(data)
    except:
        return {"ok": False}

# ============================
# MAIN INDEX LOOP
# ============================

offset = 0

print("[START] Listening for channel posts...\n")

while True:
    try:
        updates = tg("getUpdates", {
            "offset": offset,
            "timeout": 10,
            "allowed_updates": ["channel_post"]
        })

        if not updates.get("ok"):
            print("[WARN] Telegram returned error, retrying...")
            time.sleep(2)
            continue

        updates = updates.get("result", [])

        if not updates:
            time.sleep(2)
            continue

        for up in updates:
            offset = up["update_id"] + 1

            if "channel_post" not in up:
                continue

            msg = up["channel_post"]

            if msg["chat"]["id"] != MOVIE_CHANNEL_ID:
                continue

            caption = msg.get("caption", "")
            message_id = msg.get("message_id", 0)

            # ============================
            # FILE TYPES
            # ============================

            file_obj = None

            if "document" in msg:
                file_obj = msg["document"]
            elif "video" in msg:
                file_obj = msg["video"]
            elif "audio" in msg:
                file_obj = msg["audio"]
            else:
                continue

            file_id = file_obj["file_id"]
            file_unique_id = file_obj["file_unique_id"]
            mime_type = file_obj.get("mime_type", "")
            size = file_obj.get("file_size", 0)
            title = file_obj.get("file_name", "Untitled")

            insert_movie(
                file_id=file_id,
                file_unique_id=file_unique_id,
                title=title,
                caption=caption,
                size=size,
                mime=mime_type,
                msg_id=message_id,
            )

    except Exception as e:
        print("[LOOP ERROR]", e)
        time.sleep(3)
