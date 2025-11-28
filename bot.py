# bot.py
import os
from pyrogram import Client, filters
from flask import Flask, request

# ---------------- Environment Variables ----------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
MOVIE_CHANNEL_ID = int(os.getenv("MOVIE_CHANNEL_ID"))  # Private DB channel, bot must be admin

# ---------------- Flask App ----------------
app = Flask(__name__)

# ---------------- Pyrogram Client ----------------
bot = Client("movie_bot", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH)

# ---------------- Bot Command Handlers ----------------
@bot.on_message(filters.private & filters.text)
def handle_message(client, message):
    movie_name = message.text.strip().lower()
    if movie_name.startswith("/start"):
        message.reply("👋 Welcome! Type the movie name to get the file.")
        return

    found = False
    try:
        # Fetch last 500 messages from the private channel for search
        for m in client.get_chat_history(MOVIE_CHANNEL_ID, limit=500):
            caption = (m.caption or "").lower()
            if movie_name in caption:
                # Forward movie to user
                client.forward_messages(chat_id=message.chat.id,
                                        from_chat_id=MOVIE_CHANNEL_ID,
                                        message_ids=m.message_id)
                found = True
                break
    except Exception as e:
        message.reply(f"❌ Error accessing channel or forwarding: {e}")
        return

    if not found:
        message.reply("❌ Movie not available in the channel.")

# ---------------- Flask Webhook for Render ----------------
@app.route("/", methods=["POST", "GET"])
def webhook():
    if request.method == "POST":
        update = request.get_json()
        if update:
            bot.process_new_updates([update])
        return "OK"
    return "Bot is running."

# ---------------- Main ----------------
if __name__ == "__main__":
    # Start bot (local testing)
    bot.start()
    port = int(os.environ.get("PORT", 5000))  # Render assigns dynamic PORT
    app.run(host="0.0.0.0", port=port)
