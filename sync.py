import os
import sqlite3
import logging
import threading
import time
from pyrogram import Client, filters
from pyrogram.types import Message

# ---------------------------
# LOGGING
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [INFO] %(message)s"
)

logging.info("Starting sync.py service")

# ---------------------------
# ENV VARIABLES
# ---------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_CHANNEL_ID = int(os.getenv("DB_CHANNEL_ID"))
DB_PATH = os.getenv("DB_PATH", "./movies.db")

# ---------------------------
# DATABASE SETUP
# ---------------------------
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS movies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id TEXT UNIQUE,
    file_name TEXT,
    caption TEXT,
    file_size INTEGER,
    message_id INTEGER,
    timestamp INTEGER
)
""")
conn.commit()

logging.info("DB table ensured")

# ---------------------------
# PYROGRAM BOT CLIENT
# ---------------------------
pyro = Client(
    "sync-bot",
    bot_token=BOT_TOKEN
)

# ---------------------------
# INSERT FUNCTION
# ---------------------------
def insert_movie(file_id, file_name, caption, file_size, message_id):
    try:
        cursor.execute("""
            INSERT OR IGNORE INTO movies (file_id, file_name, caption, file_size, message_id, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (file_id, file_name, caption, file_size, message_id, int(time.time())))
        conn.commit()
        logging.info(f"Inserted → {file_name}")
    except Exception as e:
        logging.error(f"Insert error: {e}")

# ---------------------------
# MESSAGE HANDLER
# ---------------------------
@pyro.on_message(filters.chat(DB_CHANNEL_ID))
def handler(client: Client, message: Message):

    # Only media messages allowed
    media = (
        message.document or
        message.video or
        message.audio or
        message.voice or
        message.video_note
    )

    if not media:
        return

    file_id = media.file_id
    file_name = getattr(media, "file_name", None)
    caption = message.caption or ""
    file_size = getattr(media, "file_size", 0)
    msg_id = message.id

    insert_movie(file_id, file_name, caption, file_size, msg_id)

# ---------------------------
# BACKGROUND THREAD
# (KEEPS BOT RUNNING)
# ---------------------------
def run_bot():
    logging.info("Background thread started")
    pyro.run()

# ---------------------------
# START SYNC SERVICE
# ---------------------------
def start_background():
    t = threading.Thread(target=run_bot, daemon=True)
    t.start()

    # Keep Render web service alive
    while True:
        time.sleep(60)

# ---------------------------
# MAIN
# ---------------------------
if __name__ == "__main__":
    start_background()
