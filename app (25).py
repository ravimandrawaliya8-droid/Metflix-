#!/usr/bin/env python3
# app.py — Metflic unified backend (Flask + Postgres + optional Pyrogram)
import os
import time
import json
import logging
import threading
import re
import random
from datetime import datetime
from urllib.parse import quote_plus

import requests
from flask import Flask, request, jsonify, render_template, abort, redirect, url_for, flash, session

# Optional imports
try:
    from sqlalchemy import create_engine, text
    SQLA = True
except Exception:
    SQLA = False 

try:
    from pyrogram import Client as PyroClient
    PYRO = True
except Exception:
    PYRO = False

# --- logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("metflic")

# --- config (env) ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
WEBSITE_URL = os.getenv("WEBSITE_URL", "").rstrip("/") if os.getenv("WEBSITE_URL") else ""
DB_CHANNEL = os.getenv("DB_CHANNEL", "").strip()  # -100... or @channelname
REQUIRED_CHANNELS = [c.strip() for c in os.getenv("REQUIRED_CHANNELS", "").split(",") if c.strip()]
API_ID = os.getenv("API_ID", "")
API_HASH = os.getenv("API_HASH", "")
PYRO_SESSION = os.getenv("PYRO_SESSION", "")
TMDB_KEY = os.getenv("TMDB_KEY", "")
OMDB_API_KEY = os.getenv("OMDB_API_KEY", "")
WEBHOOK_SECRET_TOKEN = os.getenv("WEBHOOK_SECRET_TOKEN", "")
PORT = int(os.getenv("PORT", "10000"))
DELETE_AFTER_SECONDS = int(os.getenv("DELETE_AFTER_SECONDS", "300"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "86400"))
BACKUP_HOURS = int(os.getenv("BACKUP_HOURS", "12"))
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
FLASK_SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "your-secret-key-change-this")

# ================= TMDB HELPERS =================

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMG = "https://image.tmdb.org/t/p/w500"

def tmdb_get(path, params=None):
    if not TMDB_KEY:
        return {}
    try:
        params = params or {}
        params["api_key"] = TMDB_KEY
        r = requests.get(f"{TMDB_BASE}/{path}", params=params, timeout=8)
        return r.json() if r.ok else {}
    except Exception:
        return {}

def tmdb_safe_title(title: str) -> str:
    if not title:
        return ""

    t = title.lower()

    # remove extensions
    t = re.sub(r'\.(mkv|mp4|avi|mov|webm)$', '', t)

    # remove quality / codec / language junk
    junk = [
        r'\b\d{3,4}p\b', r'\bbluray\b', r'\bhdtc\b', r'\bwebrip\b',
        r'\bx264\b', r'\bx265\b', r'\bhevc\b', r'\bhdrip\b',
        r'\bhindi\b', r'\benglish\b', r'\btamil\b', r'\btelugu\b',
        r'\bdual\b', r'\bline\b', r'\baudio\b', r'\bdubbed\b',
        r'\bms\b', r'\blol\b'
    ]

    for j in junk:
        t = re.sub(j, ' ', t)

    # remove year if exists
    t = re.sub(r'\b(19|20)\d{2}\b', ' ', t)

    # cleanup
    t = re.sub(r'[^a-z0-9 ]+', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()

    return t

def tmdb_best_match(title):
    if not title:
        return None
    data = tmdb_get("search/movie", {
        "query": title,
        "include_adult": False,
        "page": 1
    })
    if not data or not data.get("results"):
        return None

    # best match = first result
    return data["results"][0]

def poster_from_title(title):
    m = tmdb_best_match(title)
    if not m:
        return ""
    if not m.get("poster_path"):
        return ""
    return "https://image.tmdb.org/t/p/w500" + m["poster_path"]

def normalize_tmdb(m):
    if not m:
        return {}
    return {
        "title": m.get("title") or m.get("name") or "",
        "slug": f"tmdb-{m['id']}" if m.get('id') else "",
        "poster": (
            f"https://image.tmdb.org/t/p/w500{m['poster_path']}"
            if m.get("poster_path")
            else "https://via.placeholder.com/500x750?text=No+Poster"
        ),
        "backdrop": (
            f"https://image.tmdb.org/t/p/original{m['backdrop_path']}"
            if m.get("backdrop_path") else None
        ),
        "overview": m.get("overview", ""),
        "year": (m.get("release_date") or m.get("first_air_date") or "")[:4]
    }
     
def normalize_db(row):
    title = row.get("title", "")
    return {
        "slug": row.get("slug"),
        "title": title,
        "overview": row.get("caption", ""),
        "poster": best_poster(title),
        "year": row.get("year", "") or ""
    }
    
def tmdb_search_movie(title):
    if not TMDB_KEY or not title:
        return []

    try:
        r = requests.get(
            "https://api.themoviedb.org/3/search/movie",
            params={
                "api_key": TMDB_KEY,
                "query": title,
                "page": 1
            },
            timeout=6
        )
        if r.ok:
            return r.json().get("results", [])
    except Exception:
        logger.exception("TMDB search failed")

    return []

def tmdb_movie_full(tid):
    """
    Fetch full TMDB movie data:
    - details
    - images
    - trailer (YouTube)
    """
    base = "https://api.themoviedb.org/3"

    d = requests.get(
        f"{base}/movie/{tid}",
        params={"api_key": TMDB_KEY},
        timeout=8
    ).json()

    imgs = requests.get(
        f"{base}/movie/{tid}/images",
        params={"api_key": TMDB_KEY},
        timeout=8
    ).json()

    vids = requests.get(
        f"{base}/movie/{tid}/videos",
        params={"api_key": TMDB_KEY},
        timeout=8
    ).json()

    # 🎬 trailer (YouTube only)
    trailer = None
    for v in vids.get("results", []):
        if v.get("site") == "YouTube" and v.get("type") in ("Trailer", "Teaser"):
            trailer = v.get("key")
            break

    gallery = [
        "https://image.tmdb.org/t/p/original" + i["file_path"]
        for i in imgs.get("backdrops", [])[:10]
    ]

    return {
        "title": d.get("title"),
        "year": (d.get("release_date") or "")[:4],
        "overview": d.get("overview"),
        "poster": (
            "https://image.tmdb.org/t/p/w500" + d["poster_path"]
            if d.get("poster_path") else ""
        ),
        "rating": d.get("vote_average"),
        "runtime": d.get("runtime"),
        "language": (d.get("spoken_languages") or [{}])[0].get("english_name"),
        "images": gallery,
        "trailer": trailer
    }

def tmdb_tv_full(tid):
    """
    Fetch full TMDB TV show data
    """
    base = "https://api.themoviedb.org/3"

    d = requests.get(
        f"{base}/tv/{tid}",
        params={"api_key": TMDB_KEY},
        timeout=8
    ).json()

    imgs = requests.get(
        f"{base}/tv/{tid}/images",
        params={"api_key": TMDB_KEY},
        timeout=8
    ).json()

    vids = requests.get(
        f"{base}/tv/{tid}/videos",
        params={"api_key": TMDB_KEY},
        timeout=8
    ).json()

    # seasons
    seasons = []
    for s in d.get("seasons", []):
        if s.get("season_number", 0) > 0:
            seasons.append({
                "season_number": s["season_number"],
                "episode_count": s.get("episode_count", 0),
                "name": s.get("name", ""),
                "overview": s.get("overview", ""),
                "poster": f"https://image.tmdb.org/t/p/w500{s['poster_path']}" if s.get("poster_path") else ""
            })

    # trailer
    trailer = None
    for v in vids.get("results", []):
        if v.get("site") == "YouTube" and v.get("type") in ("Trailer", "Teaser"):
            trailer = v.get("key")
            break

    gallery = [
        "https://image.tmdb.org/t/p/original" + i["file_path"]
        for i in imgs.get("backdrops", [])[:10]
    ]

    return {
        "title": d.get("name"),
        "year": (d.get("first_air_date") or "")[:4],
        "overview": d.get("overview"),
        "poster": (
            "https://image.tmdb.org/t/p/w500" + d["poster_path"]
            if d.get("poster_path") else ""
        ),
        "rating": d.get("vote_average"),
        "seasons": seasons,
        "images": gallery,
        "trailer": trailer,
        "status": d.get("status", ""),
        "type": "tv"
    }

# ==============================================
if not BOT_TOKEN:
    logger.error("BOT_TOKEN required")
    raise SystemExit("BOT_TOKEN missing")
if not DATABASE_URL:
    logger.error("DATABASE_URL required")
    raise SystemExit("DATABASE_URL missing")

API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
BOT_USERNAME = os.getenv("BOT_USERNAME", BOT_TOKEN.split(":", 1)[0])

# --- DB / SQLAlchemy setup ---
USE_DB = False
engine = None
if SQLA:
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        USE_DB = True
        logger.info("SQLAlchemy engine ready")
    except Exception:
        logger.exception("Failed to create SQLAlchemy engine; running without persistent DB")
        USE_DB = False

if USE_DB:
    # ensure tables
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS movies (
                    id SERIAL PRIMARY KEY,
                    slug TEXT UNIQUE,
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
                    created_at TIMESTAMP DEFAULT now()
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS delete_queue (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    message_id BIGINT NOT NULL,
                    delete_at BIGINT NOT NULL
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    first_seen BIGINT
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS categories (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    slug TEXT UNIQUE,
                    type TEXT,
                    count INTEGER DEFAULT 0
                );
            """))
        logger.info("DB tables ensured")
    except Exception:
        logger.exception("Error ensuring DB tables")

# --- simple in-memory cache ---
_cache = {}
def cache_get(k):
    rec = _cache.get(k)
    if not rec: return None
    v, exp = rec
    if time.time() > exp:
        _cache.pop(k, None)
        return None
    return v

def cache_set(k, v, ttl=CACHE_TTL):
    _cache[k] = (v, time.time() + ttl)

# --- poster helpers ---
def poster_from_tmdb_path(p):
    return f"https://image.tmdb.org/t/p/w500{p}" if p else ""

def tmdb_search_poster(title):
    if not TMDB_KEY or not title:
        return "https://via.placeholder.com/500x750?text=No+Poster"

    key = f"tmdb:{title.lower()}"
    cached = cache_get(key)
    if cached is not None:
        return cached

    try:
        r = requests.get(
            "https://api.themoviedb.org/3/search/movie",
            params={"api_key": TMDB_KEY, "query": title, "page": 1},
            timeout=8
        )
        if r.ok:
            j = r.json()
            if j.get("results"):
                p = j["results"][0].get("poster_path")
                url = poster_from_tmdb_path(p)
                if url:
                    cache_set(key, url)
                    return url
    except Exception:
        logger.debug("tmdb search fail", exc_info=True)

    fallback = "https://via.placeholder.com/500x750?text=No+Poster"
    cache_set(key, fallback)
    return fallback

def omdb_poster(title):
    if not OMDB_API_KEY or not title: return ""
    key = f"omdb:{title.lower()}"
    cached = cache_get(key)
    if cached is not None:
        return cached
    try:
        r = requests.get("http://www.omdbapi.com/", params={"t": title, "apikey": OMDB_API_KEY}, timeout=6)
        if r.ok:
            j = r.json()
            if j.get("Response") == "True" and j.get("Poster") and j.get("Poster") != "N/A":
                cache_set(key, j.get("Poster"))
                return j.get("Poster")
    except Exception:
        logger.debug("omdb fail", exc_info=True)
    cache_set(key, "")
    return ""

def best_poster(title: str) -> str:
    """यह function TMDB से poster fetch करेगा"""
    if not title:
        return "https://via.placeholder.com/500x750?text=No+Poster"
    
    clean_title = tmdb_safe_title(title)
    if not clean_title:
        clean_title = title.strip()[:100]
    
    try:
        response = requests.get(
            "https://api.themoviedb.org/3/search/movie",
            params={
                "api_key": TMDB_KEY,
                "query": clean_title,
                "page": 1
            },
            timeout=5
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("results") and len(data["results"]) > 0:
                movie = data["results"][0]
                if movie.get("poster_path"):
                    return f"https://image.tmdb.org/t/p/w500{movie['poster_path']}"
    except Exception:
        pass
    
    return "https://via.placeholder.com/500x750?text=No+Poster"

# --- Telegram helpers (bot API) ---
def tg_post(method, payload=None, files=None, timeout=20):
    url = f"{API_BASE}/{method}"
    try:
        if files:
            r = requests.post(url, data=payload or {}, files=files, timeout=timeout)
        else:
            r = requests.post(url, json=payload or {}, timeout=timeout)
        try:
            return r.json()
        except Exception:
            return {"ok": False, "http_status": r.status_code, "text": r.text}
    except Exception:
        logger.exception("tg_post exception")
        return {"ok": False, "error": "request error"}

def send_message(chat_id, text, reply_markup=None, parse_mode="HTML"):
    payload = {"chat_id": int(chat_id), "text": text, "parse_mode": parse_mode}
    if reply_markup:
        payload["reply_markup"] = reply_markup if isinstance(reply_markup, str) else json.dumps(reply_markup)
    return tg_post("sendMessage", payload)

def forward_message(chat_id, from_chat_id, message_id):
    return tg_post("forwardMessage", {"chat_id": int(chat_id), "from_chat_id": int(from_chat_id), "message_id": int(message_id)})

def delete_message(chat_id, message_id):
    return tg_post("deleteMessage", {"chat_id": int(chat_id), "message_id": int(message_id)})

# --- delete queue ---
_mem_delete_queue = []
_delete_lock = threading.Lock()

def schedule_delete(chat_id, message_id, delay=DELETE_AFTER_SECONDS):
    delete_at = int(time.time()) + int(delay)
    if USE_DB:
        try:
            with engine.begin() as conn:
                conn.execute(text("INSERT INTO delete_queue (chat_id,message_id,delete_at) VALUES (:c,:m,:d)"),
                             {"c": int(chat_id), "m": int(message_id), "d": int(delete_at)})
            return
        except Exception:
            logger.exception("schedule_delete DB failed — fallback to mem")
    with _delete_lock:
        _mem_delete_queue.append((int(chat_id), int(message_id), int(delete_at)))

def run_delete_pass():
    now = int(time.time())
    # DB-backed
    if USE_DB:
        try:
            with engine.begin() as conn:
                rows = conn.execute(text("SELECT id,chat_id,message_id FROM delete_queue WHERE delete_at <= :t"), {"t": now}).fetchall()
                for r in rows:
                    try:
                        delete_message(r[1], r[2])
                    except Exception:
                        logger.exception("delete_message failed")
                    conn.execute(text("DELETE FROM delete_queue WHERE id = :id"), {"id": r[0]})
        except Exception:
            logger.exception("run_delete_pass db error")
    # mem-backed
    with _delete_lock:
        keep = []
        for cid, mid, at in _mem_delete_queue:
            if at <= now:
                try:
                    delete_message(cid, mid)
                except Exception:
                    logger.exception("delete_message mem failed")
            else:
                keep.append((cid, mid, at))
        _mem_delete_queue[:] = keep

def delete_worker():
    while True:
        try:
            run_delete_pass()
        except Exception:
            logger.exception("delete_worker top")
        time.sleep(5)

threading.Thread(target=delete_worker, daemon=True).start()

# --- DB helpers: insert/search/get by slug ---
def make_slug(text: str) -> str:
    text = text.lower()
    text = re.sub(r'\.(mkv|mp4|avi|mov)$', '', text)
    text = re.sub(r'[^a-z0-9]+', '-', text)
    text = text.strip('-')
    return text[:180] or str(int(time.time()))

def clean_caption(caption):
    if not caption:
        return ""
    line = caption.splitlines()[0].strip()
    line = re.sub(r"@[\w_]+", " ", line)
    line = re.sub(r"join premium.*", " ", line, flags=re.I)
    line = re.sub(r"[_\.\+\-×\u00D7]+", " ", line)
    line = re.sub(r"\.(mkv|mp4|avi|mov|zip|rar)$", "", line, flags=re.I)
    line = re.sub(r"\s+", " ", line).strip()
    return line[:300]

def db_insert_movie_from_webhook(slug, title, year, message_id, channel_id, file_id, file_unique_id, caption, file_size, mime_type, extra_json):
    if not USE_DB:
        logger.debug("db_insert_movie skipped; no DB")
        return
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO movies (slug,title,year,file_id,file_unique_id,caption,file_size,mime_type,channel_id,message_id,extra,created_at)
                VALUES (:slug,:title,:year,:file_id,:file_unique_id,:caption,:file_size,:mime_type,:channel_id,:message_id,:extra,now())
                ON CONFLICT (file_unique_id) DO NOTHING
            """), {
                "slug": slug, "title": title, "year": year or None, "file_id": file_id,
                "file_unique_id": file_unique_id, "caption": caption, "file_size": file_size,
                "mime_type": mime_type, "channel_id": int(channel_id) if channel_id else None, "message_id": int(message_id) if message_id else None,
                "extra": json.dumps(extra_json) if extra_json else json.dumps({})
            })
    except Exception:
        logger.exception("db_insert_movie failed for %s", title)

def db_get_movie_by_slug(slug):
    if not USE_DB:
        return None
    try:
        with engine.connect() as conn:
            row = conn.execute(text("SELECT * FROM movies WHERE slug = :s LIMIT 1"), {"s": slug}).fetchone()
            return dict(row._mapping) if row else None
    except Exception:
        logger.exception("db_get_movie_by_slug failed")
        return None

def db_search_smart(q, limit=40, category=None):
    if not q: return []
    q = q.strip()
    tokens = [t for t in re.split(r"\W+", q.lower()) if t and len(t) > 1]
    if not tokens: return []
    if not USE_DB: return []
    try:
        like_clauses = []
        params = {}
        for i, tk in enumerate(tokens):
            key = f"t{i}"
            like_clauses.append(f"LOWER(title) LIKE :{key}")
            params[key] = f"%{tk}%"
        clause = " OR ".join(like_clauses)
        
        if category:
            sql = f"SELECT id,slug,title,year,caption,channel_id,message_id FROM movies WHERE ({clause}) AND LOWER(title) LIKE :cat LIMIT :lim"
            params["cat"] = f"%{category}%"
        else:
            sql = f"SELECT id,slug,title,year,caption,channel_id,message_id FROM movies WHERE ({clause}) LIMIT :lim"
        
        params["lim"] = max(200, limit*5)
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        scored = []
        for r in rows:
            title_lower = (r[2] or "").lower()
            match_count = sum(1 for tk in tokens if tk in title_lower)
            score = match_count - (len(title_lower)/500.0)
            if match_count > 0:
                scored.append((score, r))
        scored.sort(key=lambda x: x[0], reverse=True)
        results = [r for (_, r) in scored][:limit]
        clean = []
        for r in results:
            title = r[2] or ""
            clean.append({
                "slug": r[1],
                "title": title,
                "year": r[3] or "",
                "caption": r[4] or "",
                "poster": best_poster(title),
                "channel_id": r[5],
                "message_id": r[6]
            })
        return clean
    except Exception:
        logger.exception("db_search_smart failed")
        return []

def db_get_latest_movies(limit=20, offset=0):
    if not USE_DB:
        return []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT slug, title, year, caption, channel_id, message_id 
                FROM movies 
                ORDER BY created_at DESC 
                LIMIT :lim OFFSET :off
            """), {"lim": limit, "off": offset}).fetchall()
            return [dict(row._mapping) for row in rows]
    except Exception:
        logger.exception("db_get_latest_movies failed")
        return []

def db_get_movies_by_category(category, limit=20, offset=0):
    if not USE_DB:
        return []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT slug, title, year, caption, channel_id, message_id 
                FROM movies 
                WHERE LOWER(title) LIKE :cat OR LOWER(caption) LIKE :cat
                ORDER BY created_at DESC 
                LIMIT :lim OFFSET :off
            """), {"cat": f"%{category}%", "lim": limit, "off": offset}).fetchall()
            return [dict(row._mapping) for row in rows]
    except Exception:
        logger.exception("db_get_movies_by_category failed")
        return []

# --- optional Pyrogram client for direct fetch/iter (only if configured) ---
pyro = None
_pyro_started = False
def make_pyro():
    global pyro, _pyro_started
    if _pyro_started:
        return pyro
    if not PYRO:
        logger.info("Pyrogram not installed — skipping pyro features")
        return None
    try:
        if PYRO_SESSION:
            pyro = PyroClient("metflic_user", session_string=PYRO_SESSION, api_id=int(API_ID) if API_ID else None, api_hash=API_HASH)
        elif API_ID and API_HASH:
            pyro = PyroClient("metflic_bot", api_id=int(API_ID), api_hash=API_HASH, bot_token=BOT_TOKEN)
        else:
            logger.info("No Pyrogram credentials provided — skip pyro features")
            pyro = None
            _pyro_started = False
            return None
        pyro.start()
        _pyro_started = True
        logger.info("Pyrogram started")
        return pyro
    except Exception:
        logger.exception("pyrogram start failed")
        pyro = None
        _pyro_started = False
        return None

if DB_CHANNEL:
    try:
        make_pyro()
    except Exception:
        pass

def iter_channel_messages(limit=200):
    client = pyro or make_pyro()
    if not client:
        return []
    try:
        msgs = []
        for m in client.iter_history(DB_CHANNEL, limit=limit):
            msgs.append(m)
        return msgs
    except Exception:
        logger.exception("iter_channel_messages failed")
        return []

# ======================= FLASK APP =======================

app = Flask(__name__, static_folder="static", template_folder="templates")
app.secret_key = FLASK_SECRET_KEY

# ======================= MAIN ROUTES =======================

@app.route("/")
def home():
    """Home page - main landing page"""
    return render_template("index.html")

@app.route("/index")
def index():
    """Alternative home page route"""
    return redirect(url_for('home'))

@app.route("/about")
def about():
    """About us page"""
    return render_template("about.html")

@app.route("/contact", methods=["GET", "POST"])
def contact():
    """Contact page with form"""
    if request.method == "POST":
        # Process contact form
        name = request.form.get("name", "").strip()
        email = request.form.get("email", "").strip()
        message = request.form.get("message", "").strip()
        
        # Here you would typically save to database or send email
        flash("Thank you for your message! We'll get back to you soon.", "success")
        return redirect(url_for('contact'))
    
    return render_template("contact.html")

@app.route("/privacy")
def privacy():
    """Privacy policy page"""
    return render_template("privacy.html")

@app.route("/terms")
def terms():
    """Terms and conditions page"""
    return render_template("terms.html")

# ======================= MOVIES ROUTES =======================

@app.route("/movies")
def movies_page():
    """Movies listing page"""
    return render_template("movies.html")

@app.route("/movies/<slug>")
def movies_detail_page(slug):
    """Movie detail page"""
    return render_template("movies_detail.html", slug=slug)

@app.route("/movie/<slug>")
def movie_page(slug):
    """Alternative movie detail route"""
    return render_template("movie.html", slug=slug)

@app.route("/detail")
def detail_page():
    """Generic detail page (can be used as fallback)"""
    slug = request.args.get("slug", "")
    if slug:
        return render_template("detail.html", slug=slug)
    return render_template("detail.html")

# ======================= TV ROUTES =======================

@app.route("/tv")
def tv_page():
    """TV shows listing page"""
    return render_template("tv.html")

@app.route("/tv/<slug>")
def tv_detail_page(slug):
    """TV show detail page"""
    return render_template("tv_detail.html", slug=slug)

# ======================= ANIME ROUTES =======================

@app.route("/anime")
def anime_page():
    """Anime listing page"""
    return render_template("anime.html")

@app.route("/anime/<slug>")
def anime_detail_page(slug):
    """Anime detail page"""
    return render_template("anime_detail.html", slug=slug)

# ======================= CARTOON ROUTES =======================

@app.route("/cartoon")
def cartoon_page():
    """Cartoon listing page"""
    return render_template("cartoon.html")

@app.route("/cartoon/<slug>")
def cartoon_detail_page(slug):
    """Cartoon detail page"""
    return render_template("cartoon_detail.html", slug=slug)

# ======================= DRAMA ROUTES =======================

@app.route("/drama")
def drama_page():
    """Drama listing page"""
    return render_template("drama.html")

@app.route("/drama/<slug>")
def drama_detail_page(slug):
    """Drama detail page"""
    return render_template("drama_detail.html", slug=slug)

# ======================= WEBSERIES ROUTES =======================

@app.route("/webseries")
def webseries_page():
    """Web series listing page"""
    return render_template("webseries.html")

@app.route("/webseries/<slug>")
def webseries_detail_page(slug):
    """Web series detail page"""
    return render_template("webseries_detail.html", slug=slug)

# ======================= OTHER PAGES =======================

@app.route("/list")
def list_page():
    """List page (could be for playlists or collections)"""
    return render_template("list.html")

@app.route("/episodes")
def episodes_page():
    """Episodes listing page"""
    series_id = request.args.get("series_id", "")
    season = request.args.get("season", "1")
    return render_template("episodes.html", series_id=series_id, season=season)

@app.route("/search")
def search_page():
    """Search results page"""
    query = request.args.get("q", "")
    category = request.args.get("category", "all")
    return render_template("search.html", query=query, category=category)

# ======================= API ROUTES =======================

@app.route("/api/status")
def api_status():
    """API status endpoint"""
    return jsonify({
        "ok": True,
        "service": "Metflic API",
        "version": "2.0.0",
        "website": WEBSITE_URL or "not-set",
        "database": "connected" if USE_DB else "not-connected",
        "tmdb": "available" if TMDB_KEY else "not-available"
    })

@app.route("/api/home")
def api_home():
    """Homepage data - Netflix style"""
    try:
        # Latest movies from database
        latest_movies = []
        if USE_DB:
            rows = db_get_latest_movies(limit=20)
            for row in rows:
                latest_movies.append({
                    "slug": row["slug"],
                    "title": row["title"],
                    "year": row.get("year", ""),
                    "poster": best_poster(row["title"]),
                    "overview": row.get("caption", "")[:150] + "..." if len(row.get("caption", "")) > 150 else row.get("caption", "")
                })
        
        # Trending from TMDB
        trending = []
        if TMDB_KEY:
            try:
                response = requests.get(
                    "https://api.themoviedb.org/3/trending/movie/week",
                    params={"api_key": TMDB_KEY},
                    timeout=5
                )
                if response.status_code == 200:
                    data = response.json()
                    for m in data.get("results", [])[:15]:
                        trending.append({
                            "slug": f"tmdb-{m['id']}",
                            "title": m.get("title", ""),
                            "year": (m.get("release_date") or "")[:4],
                            "poster": f"https://image.tmdb.org/t/p/w500{m['poster_path']}" if m.get("poster_path") else "https://via.placeholder.com/500x750?text=No+Poster",
                            "overview": m.get("overview", "")[:100] + "..." if len(m.get("overview", "")) > 100 else m.get("overview", "")
                        })
            except Exception:
                pass
        
        # Hero movie (random from trending or latest)
        hero_movie = None
        if trending:
            hero_movie = random.choice(trending[:5])
        elif latest_movies:
            hero_movie = random.choice(latest_movies[:5])
        
        # Popular movies from TMDB
        popular = []
        if TMDB_KEY:
            try:
                response = requests.get(
                    "https://api.themoviedb.org/3/movie/popular",
                    params={"api_key": TMDB_KEY, "page": 1},
                    timeout=5
                )
                if response.status_code == 200:
                    data = response.json()
                    for m in data.get("results", [])[:15]:
                        popular.append({
                            "slug": f"tmdb-{m['id']}",
                            "title": m.get("title", ""),
                            "year": (m.get("release_date") or "")[:4],
                            "poster": f"https://image.tmdb.org/t/p/w500{m['poster_path']}" if m.get("poster_path") else "https://via.placeholder.com/500x750?text=No+Poster"
                        })
            except Exception:
                pass
        
        return jsonify({
            "ok": True,
            "hero": hero_movie,
            "latest": latest_movies[:10],
            "trending": trending[:10],
            "popular": popular[:10],
            "categories": [
                {"name": "Movies", "slug": "movies", "count": len(latest_movies)},
                {"name": "TV Shows", "slug": "tv", "count": 50},
                {"name": "Anime", "slug": "anime", "count": 30},
                {"name": "Drama", "slug": "drama", "count": 25},
                {"name": "Cartoon", "slug": "cartoon", "count": 20}
            ]
        })
        
    except Exception as e:
        logger.exception("Home API error")
        return jsonify({"ok": False, "error": str(e)})

@app.route("/api/latest")
def api_latest():
    """Latest movies from database"""
    try:
        page = int(request.args.get("page", "1"))
        per_page = int(request.args.get("per_page", "20"))
        offset = (page - 1) * per_page
        
        movies = []
        if USE_DB:
            rows = db_get_latest_movies(limit=per_page, offset=offset)
            for row in rows:
                movies.append({
                    "slug": row["slug"],
                    "title": row["title"],
                    "year": row.get("year", ""),
                    "poster": best_poster(row["title"]),
                    "overview": row.get("caption", "")[:100] + "..." if len(row.get("caption", "")) > 100 else row.get("caption", "")
                })
        
        return jsonify({
            "ok": True,
            "page": page,
            "per_page": per_page,
            "total": len(movies),
            "movies": movies
        })
    except Exception:
        logger.exception("api_latest failed")
        return jsonify({"ok": False, "error": "internal"}), 500

@app.route("/api/search")
def api_search():
    """Search movies"""
    q = (request.args.get("q") or "").strip()
    category = request.args.get("category", "")
    page = int(request.args.get("page", "1"))
    per_page = int(request.args.get("per_page", "20"))
    
    if len(q) < 2:
        return jsonify({"ok": True, "results": [], "total": 0})
    
    try:
        # Database search
        results = []
        if USE_DB:
            rows = db_search_smart(q, limit=100, category=category if category else None)
            for row in rows:
                results.append({
                    "slug": row["slug"],
                    "title": row["title"],
                    "year": row["year"],
                    "poster": row["poster"],
                    "overview": row.get("caption", "")[:150] + "..." if len(row.get("caption", "")) > 150 else row.get("caption", ""),
                    "category": "movie"
                })
        
        # TMDB search (if no results in database)
        if not results and TMDB_KEY and not category:
            try:
                response = requests.get(
                    "https://api.themoviedb.org/3/search/movie",
                    params={"api_key": TMDB_KEY, "query": q, "page": 1},
                    timeout=5
                )
                if response.status_code == 200:
                    data = response.json()
                    for m in data.get("results", [])[:10]:
                        results.append({
                            "slug": f"tmdb-{m['id']}",
                            "title": m.get("title", ""),
                            "year": (m.get("release_date") or "")[:4],
                            "poster": f"https://image.tmdb.org/t/p/w500{m['poster_path']}" if m.get("poster_path") else "https://via.placeholder.com/500x750?text=No+Poster",
                            "overview": m.get("overview", "")[:150] + "..." if len(m.get("overview", "")) > 150 else m.get("overview", ""),
                            "category": "tmdb"
                        })
            except Exception:
                pass
        
        # Pagination
        total = len(results)
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_results = results[start_idx:end_idx]
        
        return jsonify({
            "ok": True,
            "query": q,
            "page": page,
            "per_page": per_page,
            "total": total,
            "results": paginated_results
        })
        
    except Exception:
        logger.exception("api_search failed")
        return jsonify({"ok": False, "error": "search failed"}), 500

@app.route("/api/movie/<slug>")
def api_movie_detail(slug):
    """Get movie details by slug"""
    try:
        # Check database first
        movie = None
        if USE_DB:
            row = db_get_movie_by_slug(slug)
            if row:
                movie = {
                    "slug": slug,
                    "title": row["title"],
                    "year": row.get("year", ""),
                    "poster": best_poster(row["title"]),
                    "overview": row.get("caption", ""),
                    "download_link": f"https://t.me/{BOT_USERNAME}?start={quote_plus(slug)}",
                    "source": "database",
                    "type": "movie"
                }
        
        # If not in database, check TMDB
        if not movie and slug.startswith("tmdb-") and TMDB_KEY:
            try:
                tid = int(slug.split("-", 1)[1])
                tmdb_data = tmdb_movie_full(tid)
                if tmdb_data:
                    movie = {
                        "slug": slug,
                        "title": tmdb_data["title"],
                        "year": tmdb_data["year"],
                        "poster": tmdb_data["poster"],
                        "overview": tmdb_data["overview"],
                        "rating": tmdb_data.get("rating"),
                        "runtime": tmdb_data.get("runtime"),
                        "language": tmdb_data.get("language"),
                        "trailer": f"https://www.youtube.com/watch?v={tmdb_data['trailer']}" if tmdb_data.get("trailer") else None,
                        "images": tmdb_data.get("images", []),
                        "download_link": f"https://t.me/{BOT_USERNAME}?start={quote_plus(slug)}",
                        "source": "tmdb",
                        "type": "movie"
                    }
            except Exception:
                pass
        
        if movie:
            return jsonify({"ok": True, "movie": movie})
        else:
            return jsonify({"ok": False, "error": "Movie not found"}), 404
            
    except Exception:
        logger.exception("api_movie_detail failed")
        return jsonify({"ok": False, "error": "internal"}), 500

@app.route("/api/tv/<slug>")
def api_tv_detail(slug):
    """Get TV show details by slug"""
    try:
        if slug.startswith("tmdb-") and TMDB_KEY:
            tid = int(slug.split("-", 1)[1])
            tmdb_data = tmdb_tv_full(tid)
            if tmdb_data:
                tv_show = {
                    "slug": slug,
                    "title": tmdb_data["title"],
                    "year": tmdb_data["year"],
                    "poster": tmdb_data["poster"],
                    "overview": tmdb_data["overview"],
                    "rating": tmdb_data.get("rating"),
                    "seasons": tmdb_data.get("seasons", []),
                    "trailer": f"https://www.youtube.com/watch?v={tmdb_data['trailer']}" if tmdb_data.get("trailer") else None,
                    "images": tmdb_data.get("images", []),
                    "status": tmdb_data.get("status", ""),
                    "download_link": f"https://t.me/{BOT_USERNAME}?start={quote_plus(slug)}",
                    "source": "tmdb",
                    "type": "tv"
                }
                return jsonify({"ok": True, "tv_show": tv_show})
        
        return jsonify({"ok": False, "error": "TV show not found"}), 404
        
    except Exception:
        logger.exception("api_tv_detail failed")
        return jsonify({"ok": False, "error": "internal"}), 500

@app.route("/api/anime")
def api_anime_list():
    """Get anime list"""
    try:
        page = int(request.args.get("page", "1"))
        per_page = int(request.args.get("per_page", "20"))
        offset = (page - 1) * per_page
        
        anime_list = []
        if USE_DB:
            rows = db_get_movies_by_category("anime", limit=per_page, offset=offset)
            for row in rows:
                anime_list.append({
                    "slug": row["slug"],
                    "title": row["title"],
                    "year": row.get("year", ""),
                    "poster": best_poster(row["title"]),
                    "overview": row.get("caption", "")[:100] + "..." if len(row.get("caption", "")) > 100 else row.get("caption", "")
                })
        
        # If no anime in database, get from TMDB (animation genre)
        if not anime_list and TMDB_KEY:
            try:
                response = requests.get(
                    "https://api.themoviedb.org/3/discover/movie",
                    params={
                        "api_key": TMDB_KEY,
                        "with_genres": "16",  # Animation genre
                        "page": page,
                        "sort_by": "popularity.desc"
                    },
                    timeout=5
                )
                if response.status_code == 200:
                    data = response.json()
                    for m in data.get("results", []):
                        anime_list.append({
                            "slug": f"tmdb-{m['id']}",
                            "title": m.get("title", ""),
                            "year": (m.get("release_date") or "")[:4],
                            "poster": f"https://image.tmdb.org/t/p/w500{m['poster_path']}" if m.get("poster_path") else "https://via.placeholder.com/500x750?text=No+Poster",
                            "overview": m.get("overview", "")[:100] + "..." if len(m.get("overview", "")) > 100 else m.get("overview", "")
                        })
            except Exception:
                pass
        
        return jsonify({
            "ok": True,
            "page": page,
            "per_page": per_page,
            "total": len(anime_list),
            "anime": anime_list
        })
        
    except Exception:
        logger.exception("api_anime_list failed")
        return jsonify({"ok": False, "error": "internal"}), 500

@app.route("/api/cartoon")
def api_cartoon_list():
    """Get cartoon list"""
    try:
        page = int(request.args.get("page", "1"))
        per_page = int(request.args.get("per_page", "20"))
        offset = (page - 1) * per_page
        
        cartoons = []
        if USE_DB:
            rows = db_get_movies_by_category("cartoon", limit=per_page, offset=offset)
            for row in rows:
                cartoons.append({
                    "slug": row["slug"],
                    "title": row["title"],
                    "year": row.get("year", ""),
                    "poster": best_poster(row["title"]),
                    "overview": row.get("caption", "")[:100] + "..." if len(row.get("caption", "")) > 100 else row.get("caption", "")
                })
        
        # TMDB fallback
        if not cartoons and TMDB_KEY:
            try:
                response = requests.get(
                    "https://api.themoviedb.org/3/discover/movie",
                    params={
                        "api_key": TMDB_KEY,
                        "with_genres": "16",  # Animation
                        "page": page,
                        "sort_by": "popularity.desc"
                    },
                    timeout=5
                )
                if response.status_code == 200:
                    data = response.json()
                    for m in data.get("results", []):
                        cartoons.append({
                            "slug": f"tmdb-{m['id']}",
                            "title": m.get("title", ""),
                            "year": (m.get("release_date") or "")[:4],
                            "poster": f"https://image.tmdb.org/t/p/w500{m['poster_path']}" if m.get("poster_path") else "https://via.placeholder.com/500x750?text=No+Poster",
                            "overview": m.get("overview", "")[:100] + "..." if len(m.get("overview", "")) > 100 else m.get("overview", "")
                        })
            except Exception:
                pass
        
        return jsonify({
            "ok": True,
            "page": page,
            "per_page": per_page,
            "total": len(cartoons),
            "cartoons": cartoons
        })
        
    except Exception:
        logger.exception("api_cartoon_list failed")
        return jsonify({"ok": False, "error": "internal"}), 500

@app.route("/api/drama")
def api_drama_list():
    """Get drama list"""
    try:
        page = int(request.args.get("page", "1"))
        per_page = int(request.args.get("per_page", "20"))
        offset = (page - 1) * per_page
        
        dramas = []
        if USE_DB:
            rows = db_get_movies_by_category("drama", limit=per_page, offset=offset)
            for row in rows:
                dramas.append({
                    "slug": row["slug"],
                    "title": row["title"],
                    "year": row.get("year", ""),
                    "poster": best_poster(row["title"]),
                    "overview": row.get("caption", "")[:100] + "..." if len(row.get("caption", "")) > 100 else row.get("caption", "")
                })
        
        # TMDB fallback (drama genre = 18)
        if not dramas and TMDB_KEY:
            try:
                response = requests.get(
                    "https://api.themoviedb.org/3/discover/movie",
                    params={
                        "api_key": TMDB_KEY,
                        "with_genres": "18",
                        "page": page,
                        "sort_by": "popularity.desc"
                    },
                    timeout=5
                )
                if response.status_code == 200:
                    data = response.json()
                    for m in data.get("results", []):
                        dramas.append({
                            "slug": f"tmdb-{m['id']}",
                            "title": m.get("title", ""),
                            "year": (m.get("release_date") or "")[:4],
                            "poster": f"https://image.tmdb.org/t/p/w500{m['poster_path']}" if m.get("poster_path") else "https://via.placeholder.com/500x750?text=No+Poster",
                            "overview": m.get("overview", "")[:100] + "..." if len(m.get("overview", "")) > 100 else m.get("overview", "")
                        })
            except Exception:
                pass
        
        return jsonify({
            "ok": True,
            "page": page,
            "per_page": per_page,
            "total": len(dramas),
            "dramas": dramas
        })
        
    except Exception:
        logger.exception("api_drama_list failed")
        return jsonify({"ok": False, "error": "internal"}), 500

@app.route("/api/webseries")
def api_webseries_list():
    """Get web series list"""
    try:
        page = int(request.args.get("page", "1"))
        per_page = int(request.args.get("per_page", "20"))
        
        webseries = []
        if TMDB_KEY:
            try:
                # Search for TV shows (web series are usually TV shows)
                response = requests.get(
                    "https://api.themoviedb.org/3/discover/tv",
                    params={
                        "api_key": TMDB_KEY,
                        "page": page,
                        "sort_by": "popularity.desc"
                    },
                    timeout=5
                )
                if response.status_code == 200:
                    data = response.json()
                    for m in data.get("results", []):
                        webseries.append({
                            "slug": f"tmdb-{m['id']}",
                            "title": m.get("name", ""),
                            "year": (m.get("first_air_date") or "")[:4],
                            "poster": f"https://image.tmdb.org/t/p/w500{m['poster_path']}" if m.get("poster_path") else "https://via.placeholder.com/500x750?text=No+Poster",
                            "overview": m.get("overview", "")[:100] + "..." if len(m.get("overview", "")) > 100 else m.get("overview", ""),
                            "type": "tv"
                        })
            except Exception:
                pass
        
        return jsonify({
            "ok": True,
            "page": page,
            "per_page": per_page,
            "total": len(webseries),
            "webseries": webseries
        })
        
    except Exception:
        logger.exception("api_webseries_list failed")
        return jsonify({"ok": False, "error": "internal"}), 500

@app.route("/api/episodes")
def api_episodes():
    """Get episodes for a TV series"""
    series_id = request.args.get("series_id", "")
    season = request.args.get("season", "1")
    
    if not series_id or not series_id.startswith("tmdb-"):
        return jsonify({"ok": False, "error": "Invalid series ID"}), 400
    
    try:
        tid = int(series_id.split("-", 1)[1])
        
        # Get season details from TMDB
        response = requests.get(
            f"https://api.themoviedb.org/3/tv/{tid}/season/{season}",
            params={"api_key": TMDB_KEY},
            timeout=5
        )
        
        if response.status_code == 200:
            data = response.json()
            episodes = []
            for ep in data.get("episodes", []):
                episodes.append({
                    "episode_number": ep.get("episode_number", 0),
                    "title": ep.get("name", ""),
                    "overview": ep.get("overview", ""),
                    "air_date": ep.get("air_date", ""),
                    "still_path": f"https://image.tmdb.org/t/p/w500{ep['still_path']}" if ep.get("still_path") else "",
                    "runtime": ep.get("runtime", 0)
                })
            
            return jsonify({
                "ok": True,
                "series_id": series_id,
                "season": season,
                "season_name": data.get("name", f"Season {season}"),
                "overview": data.get("overview", ""),
                "poster": f"https://image.tmdb.org/t/p/w500{data['poster_path']}" if data.get("poster_path") else "",
                "episodes": episodes
            })
        else:
            return jsonify({"ok": False, "error": "Season not found"}), 404
            
    except Exception:
        logger.exception("api_episodes failed")
        return jsonify({"ok": False, "error": "internal"}), 500

@app.route("/api/trending")
def api_trending():
    """Get trending movies/TV shows"""
    media_type = request.args.get("type", "movie")
    time_window = request.args.get("time_window", "week")
    
    try:
        if not TMDB_KEY:
            return jsonify({"ok": False, "error": "TMDB API key not configured"}), 500
        
        response = requests.get(
            f"https://api.themoviedb.org/3/trending/{media_type}/{time_window}",
            params={"api_key": TMDB_KEY},
            timeout=5
        )
        
        if response.status_code == 200:
            data = response.json()
            results = []
            for item in data.get("results", [])[:20]:
                if media_type == "movie":
                    results.append({
                        "slug": f"tmdb-{item['id']}",
                        "title": item.get("title", ""),
                        "year": (item.get("release_date") or "")[:4],
                        "poster": f"https://image.tmdb.org/t/p/w500{item['poster_path']}" if item.get("poster_path") else "https://via.placeholder.com/500x750?text=No+Poster",
                        "overview": item.get("overview", "")[:150] + "..." if len(item.get("overview", "")) > 150 else item.get("overview", ""),
                        "rating": item.get("vote_average", 0),
                        "type": "movie"
                    })
                else:  # tv
                    results.append({
                        "slug": f"tmdb-{item['id']}",
                        "title": item.get("name", ""),
                        "year": (item.get("first_air_date") or "")[:4],
                        "poster": f"https://image.tmdb.org/t/p/w500{item['poster_path']}" if item.get("poster_path") else "https://via.placeholder.com/500x750?text=No+Poster",
                        "overview": item.get("overview", "")[:150] + "..." if len(item.get("overview", "")) > 150 else item.get("overview", ""),
                        "rating": item.get("vote_average", 0),
                        "type": "tv"
                    })
            
            return jsonify({
                "ok": True,
                "type": media_type,
                "time_window": time_window,
                "total": len(results),
                "results": results
            })
        else:
            return jsonify({"ok": False, "error": "TMDB API error"}), 500
            
    except Exception:
        logger.exception("api_trending failed")
        return jsonify({"ok": False, "error": "internal"}), 500

@app.route("/api/popular")
def api_popular():
    """Get popular movies/TV shows"""
    media_type = request.args.get("type", "movie")
    
    try:
        if not TMDB_KEY:
            return jsonify({"ok": False, "error": "TMDB API key not configured"}), 500
        
        endpoint = "movie/popular" if media_type == "movie" else "tv/popular"
        response = requests.get(
            f"https://api.themoviedb.org/3/{endpoint}",
            params={"api_key": TMDB_KEY, "page": 1},
            timeout=5
        )
        
        if response.status_code == 200:
            data = response.json()
            results = []
            for item in data.get("results", [])[:20]:
                if media_type == "movie":
                    results.append({
                        "slug": f"tmdb-{item['id']}",
                        "title": item.get("title", ""),
                        "year": (item.get("release_date") or "")[:4],
                        "poster": f"https://image.tmdb.org/t/p/w500{item['poster_path']}" if item.get("poster_path") else "https://via.placeholder.com/500x750?text=No+Poster",
                        "overview": item.get("overview", "")[:150] + "..." if len(item.get("overview", "")) > 150 else item.get("overview", ""),
                        "rating": item.get("vote_average", 0),
                        "type": "movie"
                    })
                else:  # tv
                    results.append({
                        "slug": f"tmdb-{item['id']}",
                        "title": item.get("name", ""),
                        "year": (item.get("first_air_date") or "")[:4],
                        "poster": f"https://image.tmdb.org/t/p/w500{item['poster_path']}" if item.get("poster_path") else "https://via.placeholder.com/500x750?text=No+Poster",
                        "overview": item.get("overview", "")[:150] + "..." if len(item.get("overview", "")) > 150 else item.get("overview", ""),
                        "rating": item.get("vote_average", 0),
                        "type": "tv"
                    })
            
            return jsonify({
                "ok": True,
                "type": media_type,
                "total": len(results),
                "results": results
            })
        else:
            return jsonify({"ok": False, "error": "TMDB API error"}), 500
            
    except Exception:
        logger.exception("api_popular failed")
        return jsonify({"ok": False, "error": "internal"}), 500

# ======================= TELEGRAM WEBHOOK =======================

@app.route("/webhook", methods=["POST"])
def webhook():
    """Telegram bot webhook endpoint"""
    if WEBHOOK_SECRET_TOKEN:
        header = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
        if header != WEBHOOK_SECRET_TOKEN:
            logger.warning("Invalid webhook secret token")
            return jsonify({"ok": False}), 403
    data = request.get_json(force=True)
    threading.Thread(target=handle_update, args=(data,), daemon=True).start()
    return jsonify({"ok": True})

# ======================= UTILITY FUNCTIONS =======================

def handle_update(update):
    """Handle Telegram updates"""
    try:
        # Clean delete queue on each update
        try:
            run_delete_pass()
        except Exception:
            pass

        # Detect message type
        m = None
        if "channel_post" in update:
            m = update["channel_post"]
        elif "message" in update:
            m = update["message"]
        else:
            return

        chat = m.get("chat") or {}
        cid = chat.get("id")
        text = m.get("text", "") or ""
        uid = (m.get("from") or {}).get("id")

        # DB CHANNEL → INSERT MOVIE INTO DATABASE
        if DB_CHANNEL and (
            str(cid) == str(DB_CHANNEL)
            or (
                isinstance(DB_CHANNEL, str)
                and DB_CHANNEL.startswith("@")
                and chat.get("username") == DB_CHANNEL.lstrip("@")
            )
        ):
            media = None
            for k in ("document", "video", "animation", "audio", "voice", "photo"):
                if m.get(k):
                    media = m.get(k)
                    break

            if media:
                file_id = media.get("file_id")
                file_unique_id = media.get("file_unique_id")
                caption = (m.get("caption") or "")[:1000]

                title_guess = clean_caption(caption) or file_id

                year_match = re.search(r'\b(19|20)\d{2}\b', caption)
                year = int(year_match.group()) if year_match else None

                slug = make_slug(title_guess)

                file_size = media.get("file_size")
                mime_type = media.get("mime_type")

                db_insert_movie_from_webhook(
                    slug=slug,
                    title=title_guess,
                    year=year,
                    message_id=m.get("message_id"),
                    channel_id=cid,
                    file_id=file_id,
                    file_unique_id=file_unique_id,
                    caption=caption,
                    file_size=file_size,
                    mime_type=mime_type,
                    extra_json={"caption": caption},
                )

                logger.info("🎬 Movie inserted: %s | %s", title_guess, slug)

            return

        # PRIVATE CHAT — /start <slug>
        if isinstance(text, str) and text.startswith("/start"):
            parts = text.split(" ", 1)
            if len(parts) > 1:
                param = parts[1].strip()
                threading.Thread(
                    target=handle_start_with_slug,
                    args=(uid or cid, cid, param),
                    daemon=True,
                ).start()
                return
            else:
                send_message(
                    cid,
                    f"👋 Welcome to Metflic!\nVisit website: {WEBSITE_URL}",
                )
                return

        # Private quick search
        if (chat.get("type") == "private") and text and not text.startswith("/"):
            q = text.strip()[:200]
            if USE_DB:
                rows = db_search_smart(q, limit=8)
                if not rows:
                    send_message(cid, "❌ Movie not found. Try the website:")
                    if WEBSITE_URL:
                        send_message(cid, WEBSITE_URL)
                else:
                    kb_buttons = []
                    for r in rows:
                        title_display = r["title"]
                        if r.get("year"):
                            title_display += f" ({r['year']})"
                        kb_buttons.append([{"text": title_display[:64], "url": f"{WEBSITE_URL}/movie/{r['slug']}"}])
                    
                    kb = {"inline_keyboard": kb_buttons}
                    send_message(cid, f"🔍 Results for: <b>{q}</b>\n\nClick to open on website:", reply_markup=kb)
            else:
                found = []
                msgs = iter_channel_messages(limit=500)
                for mm in msgs:
                    title_raw = (getattr(mm, "caption", None) or getattr(mm, "text", ""))[:300]
                    if not title_raw: continue
                    title = clean_caption(title_raw)
                    if q.lower() in title.lower():
                        found.append({"title": title, "slug": f"msg-{mm.message_id}"})
                        if len(found) >= 8: break
                if not found:
                    send_message(cid, "❌ Movie not found. Try the website:")
                    if WEBSITE_URL:
                        send_message(cid, WEBSITE_URL)
                else:
                    kb_buttons = []
                    for r in found:
                        kb_buttons.append([{"text": r["title"][:64], "url": f"{WEBSITE_URL}/movie/{r['slug']}"}])
                    kb = {"inline_keyboard": kb_buttons}
                    send_message(cid, f"🔍 Results for: <b>{q}</b>\n\nClick to open on website:", reply_markup=kb)
            return

        # Group heuristic quick search
        if chat.get("type") in ("group","supergroup") and text and not text.startswith("/"):
            q = text.strip()[:200]
            if len(q) < 120 and re.search(r"[A-Za-z0-9]", q):
                try:
                    rows = db_search_smart(q, limit=4) if USE_DB else []
                    if rows:
                        kb_buttons = []
                        for r in rows:
                            title_display = r["title"]
                            if r.get("year"):
                                title_display += f" ({r['year']})"
                            kb_buttons.append([{"text": title_display[:64], "url": f"{WEBSITE_URL}/movie/{r['slug']}"}])
                        
                        kb = {"inline_keyboard": kb_buttons}
                        send_message(chat.get("id"), f"🔍 Found {len(rows)} results for: <b>{q}</b>\nOpen on website to download:", reply_markup=kb)
                except Exception:
                    logger.exception("group quick search failed")
            return

    except Exception:
        logger.exception("handle_update error")

def check_user_join(uid):
    """Check if user has joined required channels"""
    try:
        for ch in REQUIRED_CHANNELS:
            res = tg_post("getChatMember", {"chat_id": ch, "user_id": uid})
            if not res.get("ok"):
                return False
            status = res.get("result", {}).get("status")
            if status in ("left","kicked"):
                return False
        return True
    except Exception:
        logger.exception("check_user_join failed")
        return False

def handle_start_with_slug(uid, chat_id, slug):
    """Handle /start command with slug parameter"""
    # Verify join
    if REQUIRED_CHANNELS:
        if not check_user_join(uid):
            kb = {"inline_keyboard": [[{"text": "Join Channel", "url": f"https://t.me/{REQUIRED_CHANNELS[0].lstrip('@')}"}],
                                     [{"text": "I've Joined", "callback_data": f"joined:{slug}"}]]}
            send_message(uid, "🔒 Please join required channels to access files.", reply_markup=kb)
            return
    
    # Try DB forward
    if USE_DB:
        row = db_get_movie_by_slug(slug)
        if row and row.get("channel_id") and row.get("message_id"):
            res = forward_message(uid, row["channel_id"], row["message_id"])
            if res.get("ok"):
                msg_id = res.get("result", {}).get("message_id")
                if msg_id:
                    schedule_delete(uid, msg_id)
                send_message(uid, f"⏳ Sent: {row['title']} — Auto-delete in {DELETE_AFTER_SECONDS} seconds.")
                return
            else:
                logger.warning("forward failed via Bot API: %s", res)
    
    # Fallback: if slug is msg-<id>, try direct pyrogram fetch then forward
    if slug.startswith("msg-"):
        try:
            mid = int(slug.split("-",1)[1])
            client = pyro or make_pyro()
            if client:
                m = client.get_messages(DB_CHANNEL, mid)
                if m:
                    res = forward_message(uid, m.chat.id, m.message_id)
                    if res.get("ok"):
                        msg_id = res.get("result", {}).get("message_id")
                        if msg_id:
                            schedule_delete(uid, msg_id)
                        send_message(uid, f"⏳ Sent: {clean_caption(getattr(m,'caption',None) or getattr(m,'text',''))} — Auto-delete in {DELETE_AFTER_SECONDS} seconds.")
                        return
        except Exception:
            logger.exception("direct channel forwarding failed")
    
    # Last fallback
    send_message(uid, f"You can view the movie on the website: {WEBSITE_URL}/movie?slug={quote_plus(slug)}")

# ======================= STARTUP FUNCTIONS =======================

def set_webhook():
    """Set Telegram webhook"""
    if not WEBSITE_URL:
        logger.info("WEBSITE_URL not set — skipping webhook set")
        return
    payload = {"url": f"{WEBSITE_URL}/webhook"}
    if WEBHOOK_SECRET_TOKEN:
        payload["secret_token"] = WEBHOOK_SECRET_TOKEN
    try:
        r = requests.post(f"{API_BASE}/setWebhook", json=payload, timeout=10)
        logger.info("set_webhook: %s", r.text)
    except Exception:
        logger.exception("set_webhook failed")

def cache_warmer():
    """Warm up cache with TMDB calls"""
    while True:
        try:
            if TMDB_KEY:
                requests.get("https://api.themoviedb.org/3/trending/movie/day", params={"api_key": TMDB_KEY}, timeout=6)
        except Exception:
            pass
        time.sleep(1800)

def backup_and_send():
    """Backup database and send to admin"""
    if not USE_DB:
        logger.warning("backup skipped — no DB")
        return
    try:
        fname = f"movies_backup_{int(time.time())}.csv"
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT id,slug,title,file_id,created_at FROM movies ORDER BY id")).fetchall()
        with open(fname, "w", newline="", encoding="utf-8") as f:
            import csv
            writer = csv.writer(f)
            writer.writerow(["id","slug","title","file_id","created_at"])
            for r in rows:
                writer.writerow([r[0], r[1], r[2], r[3], r[4]])
        if ADMIN_ID:
            with open(fname, "rb") as fh:
                files = {"document": fh}
                data = {"chat_id": ADMIN_ID, "caption": f"Backup: {fname}"}
                requests.post(f"{API_BASE}/sendDocument", data=data, files=files, timeout=60)
        try:
            os.remove(fname)
        except Exception:
            pass
        logger.info("Backup completed")
    except Exception:
        logger.exception("backup failed")

def backup_worker(interval_hours=BACKUP_HOURS):
    """Background worker for backups"""
    while True:
        time.sleep(interval_hours*3600)
        try:
            backup_and_send()
        except Exception:
            logger.exception("backup worker failed")

# ======================= ERROR HANDLERS =======================

@app.errorhandler(404)
def page_not_found(error):
    """404 error handler"""
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_server_error(error):
    """500 error handler"""
    return render_template('500.html'), 500

# ======================= MAIN ENTRY POINT =======================

if __name__ == "__main__":
    logger.info("Starting Metflic backend")
    logger.info("BOT_USERNAME: %s", BOT_USERNAME)
    logger.info("WEBSITE_URL: %s", WEBSITE_URL or "not-set")
    logger.info("DATABASE_URL present: %s", bool(DATABASE_URL))
    logger.info("DB_CHANNEL: %s", DB_CHANNEL)
    logger.info("PYRO available: %s", PYRO)
    logger.info("TMDB_KEY present: %s", bool(TMDB_KEY))

    # Create necessary directories
    os.makedirs("templates", exist_ok=True)
    os.makedirs("static", exist_ok=True)
    os.makedirs("static/css", exist_ok=True)
    os.makedirs("static/js", exist_ok=True)
    os.makedirs("static/images", exist_ok=True)

    try:
        set_webhook()
    except Exception:
        logger.exception("set_webhook error")
    
    # Start cache warmer
    threading.Thread(target=cache_warmer, daemon=True).start()
    
    # Start backup worker if DB
    if USE_DB and ADMIN_ID:
        threading.Thread(target=backup_worker, args=(BACKUP_HOURS,), daemon=True).start()

    # Start Flask app
    app.run(host="0.0.0.0", port=PORT, debug=True)
