#!/usr/bin/env python3
"""
File To Link Telegram MTProto Streaming Bot
Supports files up to 2 GB (and 4 GB for Telegram Premium).
Features:
- Telethon MTProto client
- aiohttp HTTP streaming server with HTTP Range support (resumable downloads & seeking)
- Web video/audio player page
- SQLite file store
- CLI utility for sending messages and uploading files
"""

import os
import sys
import time
import math
import secrets
import sqlite3
import asyncio
import mimetypes
import argparse
import re
import json
import html
import shutil
from typing import Optional, Tuple
from urllib.parse import quote
from dotenv import load_dotenv

from aiohttp import web
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors.rpcerrorlist import (
    FloodWaitError,
    UserIsBlockedError,
    InputUserDeactivatedError,
)
from telethon.tl.types import (
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    DocumentAttributeAudio,
    MessageMediaDocument,
    MessageMediaPhoto,
    MessageMediaWebPage,
)

from url_downloader import extract_url_from_text, download_url_media, MAX_URL_DOWNLOAD_MB

# Load environment
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "8870014336:AAGVMVJy6CmC7ZooGULxE5whsFv0AgqM0Jg").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "6558775028"))
API_ID = int(os.getenv("API_ID", "38517263"))
API_HASH = os.getenv("API_HASH", "da981346b2708e5f8fefeee726b7bfd1").strip()

PORT = int(os.getenv("PORT", "8080"))
BIND_ADDRESS = os.getenv("BIND_ADDRESS", "0.0.0.0")
SERVER_URL = os.getenv("SERVER_URL", "").strip().rstrip("/")
LINK_EXPIRY_DAYS = int(os.getenv("LINK_EXPIRY_DAYS", "3"))
LINK_EXPIRY_SECONDS = LINK_EXPIRY_DAYS * 86400  # Default 3 days in seconds

# Permanent Storage / Bin Channel ID or Username
DEFAULT_STORAGE_CHANNEL = -1004441486082

def parse_channel_target(val):
    if not val:
        return None
    val = str(val).strip().strip("'\"")
    if not val:
        return None
    clean_username = val.lstrip("@").lower()
    if clean_username in ["xx_kake_xx", "filetolink_kakebot"]:
        return None
    if "t.me/" in val:
        val = val.split("t.me/")[-1].strip("/")
    if val.startswith("c/"):
        parts = val.split("/")
        val = parts[1] if len(parts) > 1 else val[2:]
        val = val.split("/")[0]
        clean_c = val.lstrip("-")
        if clean_c.isdigit():
            return -int(f"100{clean_c}")
    clean = val.lstrip("-")
    if clean.isdigit():
        num = int(clean)
        if num == OWNER_ID or num == 6558775028 or clean == f"100{OWNER_ID}" or clean == "1006558775028":
            return None
        if num == 4441486082:
            return -1004441486082
        if val.startswith("-100"):
            return -num
        elif val.startswith("-"):
            if len(clean) >= 9:
                return -int(f"100{clean}")
            return -num
        else:
            if str(num).startswith("100") and len(str(num)) >= 10:
                return -num
            return None
    if not val.startswith("@"):
        val = "@" + val
    return val

BIN_CHANNEL_TARGET = parse_channel_target(os.getenv("BIN_CHANNEL", ""))

DB_PATH = os.path.join(os.path.dirname(__file__), "file_store.db")
SESSION_NAME = os.path.join(os.path.dirname(__file__), "file_to_link_bot")

CURRENT_PUBLIC_URL = ""
tunnel_process = None


def is_expired(created_at: int) -> bool:
    """Check if a file link has expired."""
    return (time.time() - created_at) > LINK_EXPIRY_SECONDS


def format_expiry_date(created_at: int) -> str:
    """Format expiration timestamp to human-readable UTC string."""
    expiry_timestamp = created_at + LINK_EXPIRY_SECONDS
    return time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(expiry_timestamp))


async def init_public_url() -> str:
    """Determine the public base URL using Cloudflare Tunnel or configuration."""
    global CURRENT_PUBLIC_URL, tunnel_process

    if SERVER_URL:
        CURRENT_PUBLIC_URL = SERVER_URL
        return CURRENT_PUBLIC_URL

    # Auto-detect Render deployment URL
    render_url = os.getenv("RENDER_EXTERNAL_URL")
    if render_url:
        CURRENT_PUBLIC_URL = render_url.rstrip("/")
        print(f"[✓] Render primary URL detected: {CURRENT_PUBLIC_URL}")
        return CURRENT_PUBLIC_URL

    # Auto-detect Koyeb deployment URL
    koyeb_domain = os.getenv("KOYEB_PUBLIC_DOMAIN")
    if koyeb_domain:
        CURRENT_PUBLIC_URL = f"https://{koyeb_domain}".rstrip("/")
        print(f"[✓] Koyeb public domain detected: {CURRENT_PUBLIC_URL}")
        return CURRENT_PUBLIC_URL

    cloudflared_bin = shutil.which("cloudflared") or os.path.expanduser("~/.local/bin/cloudflared")
    if os.path.isfile(cloudflared_bin):
        print("[*] Starting Cloudflare Tunnel for direct public HTTPS access...")
        try:
            tunnel_process = await asyncio.create_subprocess_exec(
                cloudflared_bin, "tunnel", "--url", f"http://127.0.0.1:{PORT}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            pattern = re.compile(r"https://[a-zA-Z0-9.-]+\.trycloudflare\.com")
            start_wait = time.time()
            while time.time() - start_wait < 15:
                line = await tunnel_process.stderr.readline()
                if not line:
                    break
                text = line.decode(errors="ignore")
                match = pattern.search(text)
                if match:
                    CURRENT_PUBLIC_URL = match.group(0).strip()
                    print(f"[✓] Cloudflare Tunnel established: {CURRENT_PUBLIC_URL}")
                    return CURRENT_PUBLIC_URL
        except Exception as e:
            print(f"[WARNING] Cloudflare Tunnel failed to start: {e}")

    codespace_name = os.getenv("CODESPACE_NAME")
    port_domain = os.getenv("GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN", "app.github.dev")
    if codespace_name:
        CURRENT_PUBLIC_URL = f"https://{codespace_name}-{PORT}.{port_domain}"
    else:
        CURRENT_PUBLIC_URL = f"http://localhost:{PORT}"

    return CURRENT_PUBLIC_URL


def get_server_url() -> str:
    """Retrieve the active public URL."""
    return CURRENT_PUBLIC_URL or SERVER_URL or f"http://localhost:{PORT}"


def format_size(bytes_size: int) -> str:
    """Format bytes into readable units."""
    if not bytes_size or bytes_size <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = int(math.floor(math.log(bytes_size, 1024)))
    p = math.pow(1024, i)
    s = round(bytes_size / p, 2)
    return f"{s} {units[i]}"


def make_progress_bar(current: int, total: int, length: int = 10) -> str:
    """Generate a clean visual text progress bar."""
    if not total or total <= 0:
        return "░" * length
    fraction = min(max(current / total, 0.0), 1.0)
    filled = int(fraction * length)
    empty = length - filled
    return "█" * filled + "░" * empty


def format_eta(seconds: Optional[float]) -> str:
    """Format estimated seconds remaining into MM:SS or HH:MM:SS."""
    if seconds is None or seconds <= 0:
        return "calculating..."
    s = int(seconds)
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


class ThrottledProgressTracker:
    """Tracks download/upload progress and updates Telegram message at a safe interval."""

    def __init__(self, message, action_title: str, update_interval: float = 2.5):
        self.message = message
        self.action_title = action_title
        self.update_interval = update_interval
        self.last_update_time = time.time()
        self.last_percent = -1
        self.start_time = time.time()
        self.loop = asyncio.get_running_loop()

    def on_download_progress(self, current: int, total: int, speed: float = 0, eta: float = 0):
        if not self.message:
            return
        now = time.time()
        percent = int((current / total * 100)) if total > 0 else 0
        if now - self.last_update_time < self.update_interval and percent < 100:
            return
        self.last_update_time = now
        asyncio.run_coroutine_threadsafe(
            self._async_edit(current, total, speed, eta, percent),
            self.loop
        )

    async def on_upload_progress(self, current: int, total: int):
        if not self.message:
            return
        now = time.time()
        percent = int((current / total * 100)) if total > 0 else 0
        if now - self.last_update_time < self.update_interval and percent < 100:
            return
        self.last_update_time = now
        elapsed = max(now - self.start_time, 0.1)
        speed = current / elapsed
        eta = (total - current) / speed if speed > 0 and total > current else 0
        await self._async_edit(current, total, speed, eta, percent)

    async def _async_edit(self, current: int, total: int, speed: float, eta: float, percent: int):
        if percent == self.last_percent:
            return
        self.last_percent = percent
        cur_str = format_size(current)
        tot_str = format_size(total) if total > 0 else ""
        speed_str = f"⚡ {format_size(int(speed))}/s" if speed > 0 else ""
        eta_str = f" • ETA {format_eta(eta)}" if eta > 0 else ""
        size_str = f" ({cur_str} of {tot_str})" if tot_str else f" ({cur_str})"
        stats_line = f"\n<code>{speed_str}{eta_str}</code>" if speed_str else ""

        text = f"<b>{self.action_title}</b> {percent}%{size_str}{stats_line}".strip()
        try:
            await self.message.edit(text, parse_mode="html")
        except Exception:
            pass


# Database helpers
def init_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_hash TEXT UNIQUE NOT NULL,
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            file_name TEXT NOT NULL,
            file_size INTEGER NOT NULL,
            mime_type TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            orig_chat_id INTEGER DEFAULT 0,
            orig_msg_id INTEGER DEFAULT 0
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            first_name TEXT,
            username TEXT,
            created_at INTEGER
        )
    """)
    try:
        cursor.execute("ALTER TABLE files ADD COLUMN orig_chat_id INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        cursor.execute("ALTER TABLE files ADD COLUMN orig_msg_id INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        cursor.execute("INSERT OR IGNORE INTO users (user_id, first_name, username, created_at) SELECT DISTINCT orig_chat_id, 'User', '', created_at FROM files WHERE orig_chat_id > 0;")
    except Exception:
        pass
    conn.commit()
    conn.close()


SEED_USERS_PATH = os.path.join(os.path.dirname(__file__), "seed_users.json")


def register_user(user_id: int, first_name: str = "", username: str = ""):
    """Save or update user in database for broadcasting."""
    if not user_id or user_id < 0:
        return
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("""
            INSERT INTO users (user_id, first_name, username, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                first_name = CASE WHEN excluded.first_name != '' THEN excluded.first_name ELSE users.first_name END,
                username = CASE WHEN excluded.username != '' THEN excluded.username ELSE users.username END;
        """, (user_id, first_name or "", username or "", int(time.time())))
        conn.commit()
        conn.close()
        sync_seed_users_file()
    except Exception as e:
        print(f"[DB] Error registering user {user_id}: {e}", file=sys.stderr)


def get_all_user_ids() -> list:
    """Retrieve list of all registered user IDs."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT user_id FROM users WHERE user_id > 0;")
    rows = cursor.fetchall()
    conn.close()
    uids = [r[0] for r in rows]
    if OWNER_ID and OWNER_ID not in uids:
        uids.append(OWNER_ID)
    return uids


def get_all_users() -> list:
    """Retrieve list of all registered users with their details."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, first_name, username, created_at FROM users WHERE user_id > 0 ORDER BY created_at DESC;")
    rows = cursor.fetchall()
    conn.close()
    return rows


def get_user_count() -> int:
    """Retrieve total count of registered users."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(DISTINCT user_id) FROM users WHERE user_id > 0;")
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else 0


def load_seed_users():
    """Load pre-seeded users from seed_users.json into SQLite on startup."""
    register_user(OWNER_ID, "Owner", "Xx_KaKe_xX")
    register_user(8848280741, "𝙺𝚎𝚗", "MrKen_Kaneki")

    if os.path.exists(SEED_USERS_PATH):
        try:
            with open(SEED_USERS_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    for u in data:
                        if isinstance(u, dict) and u.get("user_id"):
                            register_user(
                                int(u["user_id"]),
                                u.get("first_name", "") or "",
                                u.get("username", "") or ""
                            )
        except Exception as e:
            print(f"[SEED] Note loading seed_users.json: {e}", file=sys.stderr)


def sync_seed_users_file():
    """Sync current database users into seed_users.json so they persist across runs."""
    try:
        users = get_all_users()
        data = [
            {"user_id": uid, "first_name": fn, "username": un}
            for uid, fn, un, _ in users
        ]
        with open(SEED_USERS_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception:
        pass


def delete_user(user_id: int):
    """Remove user from database if blocked or deactivated."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()
    except Exception:
        pass


def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else default


def set_setting(key: str, value: str):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
    conn.commit()
    conn.close()


def get_storage_channel():
    """Retrieve the active storage channel target."""
    env_chan = os.getenv("BIN_CHANNEL", "").strip()
    if env_chan:
        parsed = parse_channel_target(env_chan)
        if parsed and (isinstance(parsed, str) or parsed < 0):
            return parsed

    saved_chan = get_setting("bin_channel")
    if saved_chan:
        parsed = parse_channel_target(saved_chan)
        if parsed and (isinstance(parsed, str) or parsed < 0):
            return parsed

    return DEFAULT_STORAGE_CHANNEL


def save_file_record(
    file_hash: str,
    chat_id: int,
    message_id: int,
    file_name: str,
    file_size: int,
    mime_type: str,
    orig_chat_id: int = 0,
    orig_msg_id: int = 0
):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute(
        "INSERT INTO files (file_hash, chat_id, message_id, file_name, file_size, mime_type, created_at, orig_chat_id, orig_msg_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (file_hash, chat_id, message_id, file_name, file_size, mime_type, int(time.time()), orig_chat_id, orig_msg_id)
    )
    conn.commit()
    conn.close()


def get_file_record(file_hash: str) -> Optional[dict]:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("SELECT * FROM files WHERE file_hash = ?", (file_hash,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def get_db_stats() -> Tuple[int, int, int]:
    """Retrieve total count of files, total size in bytes, and total registered users."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*), COALESCE(SUM(file_size), 0) FROM files;")
    row = cursor.fetchone()
    files_count = row[0] if row else 0
    total_size = row[1] if row else 0
    cursor.execute("SELECT COUNT(*) FROM users;")
    user_row = cursor.fetchone()
    users_count = user_row[0] if user_row else 0
    conn.close()
    return files_count, total_size, users_count


def extract_media_meta(message) -> Tuple[Optional[str], int, str]:
    """Extract (file_name, file_size, mime_type) from a Telethon message."""
    file_name = None
    file_size = 0
    mime_type = "application/octet-stream"

    if message.file:
        file_name = message.file.name
        file_size = message.file.size or 0
        mime_type = message.file.mime_type or "application/octet-stream"

    if not file_name:
        if isinstance(message.media, MessageMediaPhoto):
            file_name = f"photo_{message.id}.jpg"
            mime_type = "image/jpeg"
            file_size = message.file.size if message.file else 0
        elif is_video_file(None, mime_type, message):
            ext = mimetypes.guess_extension(mime_type) or ".mp4"
            file_name = f"video_{message.id}{ext}"
            if mime_type == "application/octet-stream":
                mime_type = "video/mp4"
        else:
            ext = mimetypes.guess_extension(mime_type) or ".bin"
            file_name = f"file_{message.id}{ext}"

    return file_name, file_size, mime_type


VIDEO_EXTENSIONS = {
    ".mp4", ".mkv", ".webm", ".avi", ".mov", ".flv",
    ".wmv", ".3gp", ".ts", ".m4v", ".mpg", ".mpeg",
    ".vob", ".ogv"
}


def is_video_file(file_name: Optional[str], mime_type: Optional[str], message=None) -> bool:
    """Accurately detect whether the file is a video."""
    if message:
        # Check Telethon document attributes
        doc = getattr(message, "document", None)
        if doc and getattr(doc, "attributes", None):
            for attr in doc.attributes:
                if isinstance(attr, DocumentAttributeVideo):
                    return True
        if getattr(message, "video", None):
            return True

    if mime_type and mime_type.lower().startswith("video/"):
        return True

    if file_name:
        _, ext = os.path.splitext(file_name.lower())
        if ext in VIDEO_EXTENSIONS:
            return True
        guessed_mime, _ = mimetypes.guess_type(file_name)
        if guessed_mime and guessed_mime.lower().startswith("video/"):
            return True

    return False


# Telethon Client (Uses StringSession to avoid MTProto authorization FloodWait on cloud restarts)
DEFAULT_SESSION_STRING = "1BVtsOJsBu1tB1hMluWUPWJ3LepVQ-YOVvQmrg-7xI_nesOmO_XN7ZTHHyEWT9fqFDvfWas6858hcF0hxw_saGOkFkNBGKBqcY327wrWDVva1FhK6__On7TLJrrurDWA1xD8R-xHXDFL3TUHW6x0mUSKbmXnQ87oQ43w00VymgqKZPbzq0qjCfAGa5jmc4lUSzY_N-xU74WRONCNdVJNfVx7y6RHIJL_xoRoQIF0OMPCiEyQY-RLtLILLEdndOBRiVuhRTT9_evAb1eDwdBa5UIZLUKK2X7hvzEcl2TK0F65uYm4E3NHJ7aCWOsHIC55ec3zNxrAR6y5nCh9zHOSql3r6hveBsLY="
SESSION_STRING = os.getenv("SESSION_STRING", DEFAULT_SESSION_STRING).strip()

if SESSION_STRING:
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
else:
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)


async def start_telegram_client():
    if SESSION_STRING:
        await client.connect()
        if not await client.is_user_authorized():
            await client.start(bot_token=BOT_TOKEN)
    else:
        await client.start(bot_token=BOT_TOKEN)


async def backup_to_storage_channel(message) -> Tuple[int, int]:
    """
    Backs up media directly to the storage channel so files are posted to your channel
    and links stay valid permanently. Supports files up to 2 GB via MTProto.
    Returns (storage_chat_id, storage_message_id).
    """
    target_channel = get_storage_channel()
    # Guard: Never allow target to be owner, user private chat, or invalid peer
    if not target_channel or target_channel in [OWNER_ID, 6558775028, -int(f"100{OWNER_ID}"), message.chat_id]:
        target_channel = DEFAULT_STORAGE_CHANNEL

    # 1. Try Telethon MTProto forward_messages with from_peer (supports files up to 2 GB server-side)
    try:
        fwd = await client.forward_messages(target_channel, message.id, from_peer=message.chat_id)
        fwd_msg = fwd[0] if isinstance(fwd, list) else fwd
        if fwd_msg:
            print(f"[STORAGE CHANNEL] Telethon forward successful to {target_channel} (ID: {fwd_msg.id})")
            return target_channel, fwd_msg.id
    except Exception as err:
        print(f"[STORAGE CHANNEL MTPROTO] Forward with from_peer failed ({err}), trying message object forward...", file=sys.stderr)
        try:
            fwd = await client.forward_messages(target_channel, message)
            fwd_msg = fwd[0] if isinstance(fwd, list) else fwd
            if fwd_msg:
                print(f"[STORAGE CHANNEL] Telethon message forward successful to {target_channel} (ID: {fwd_msg.id})")
                return target_channel, fwd_msg.id
        except Exception as err2:
            print(f"[STORAGE CHANNEL MTPROTO] Forward failed ({err2}), trying Bot API copyMessage...", file=sys.stderr)

    # 2. Try Telegram HTTP Bot API copyMessage
    try:
        import aiohttp
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/copyMessage"
        payload = {
            "chat_id": target_channel,
            "from_chat_id": message.chat_id,
            "message_id": message.id
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                data = await resp.json()
                if data.get("ok"):
                    channel_msg_id = data["result"]["message_id"]
                    print(f"[STORAGE CHANNEL] Bot API copied file to channel {target_channel} (ID: {channel_msg_id})")
                    return target_channel, channel_msg_id
                else:
                    print(f"[STORAGE CHANNEL] copyMessage error ({data}), attempting forwardMessage...", file=sys.stderr)
                    fwd_url = f"https://api.telegram.org/bot{BOT_TOKEN}/forwardMessage"
                    async with session.post(fwd_url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as fwd_resp:
                        fwd_data = await fwd_resp.json()
                        if fwd_data.get("ok"):
                            channel_msg_id = fwd_data["result"]["message_id"]
                            print(f"[STORAGE CHANNEL] Bot API forwarded to channel {target_channel} (ID: {channel_msg_id})")
                            return target_channel, channel_msg_id
    except Exception as e:
        print(f"[STORAGE CHANNEL EXCEPTION] HTTP Bot API backup failed: {e}", file=sys.stderr)

    # 3. Direct send_file fallback
    try:
        sent = await client.send_file(target_channel, file=message.media)
        if sent:
            print(f"[STORAGE CHANNEL] Telethon send_file successful to {target_channel} (ID: {sent.id})")
            return target_channel, sent.id
    except Exception as err:
        print(f"[STORAGE CHANNEL TELETHON ERROR] Telethon send_file failed: {err}", file=sys.stderr)

    # 4. If target_channel was custom and failed, fallback to DEFAULT_STORAGE_CHANNEL as last resort
    if target_channel != DEFAULT_STORAGE_CHANNEL:
        try:
            fwd = await client.forward_messages(DEFAULT_STORAGE_CHANNEL, message.id, from_peer=message.chat_id)
            fwd_msg = fwd[0] if isinstance(fwd, list) else fwd
            if fwd_msg:
                print(f"[STORAGE CHANNEL] Fallback to DEFAULT_STORAGE_CHANNEL successful (ID: {fwd_msg.id})")
                return DEFAULT_STORAGE_CHANNEL, fwd_msg.id
        except Exception:
            pass

    return message.chat_id, message.id


# --- HTTP Server Endpoints ---
async def handle_index(request: web.Request):
    return web.Response(
        text="<h2>⚡ File To Link Streaming Server is Running!</h2><p>Send a file to the bot on Telegram to generate streamable links.</p>",
        content_type="text/html"
    )


async def stream_media_response(request: web.Request, as_attachment: bool = False):
    file_hash = request.match_info.get("file_hash", "")
    record = get_file_record(file_hash)
    if not record:
        return web.Response(status=404, text="404: File not found.")

    if is_expired(record["created_at"]):
        return web.Response(
            status=410,
            text=f"410 Gone: This link has expired (links are valid for {LINK_EXPIRY_DAYS} days). Please generate a new link via the Telegram bot.",
            content_type="text/plain"
        )

    msg = None
    try:
        msg = await client.get_messages(record["chat_id"], ids=record["message_id"])
    except (ValueError, TypeError):
        try:
            entity = await client.get_entity(record["chat_id"])
            msg = await client.get_messages(entity, ids=record["message_id"])
        except Exception as e:
            print(f"[STREAM FETCH] Primary entity resolution failed: {e}", file=sys.stderr)
    except Exception as e:
        print(f"[STREAM FETCH] Primary get_messages failed ({record['chat_id']}): {e}", file=sys.stderr)

    # Fallback to original user chat message if storage channel fetch had no media
    if (not msg or not msg.media) and record.get("orig_chat_id") and record.get("orig_msg_id"):
        try:
            msg = await client.get_messages(record["orig_chat_id"], ids=record["orig_msg_id"])
        except Exception as e:
            print(f"[STREAM FETCH] Fallback to original chat failed: {e}", file=sys.stderr)

    if not msg or not msg.media:
        return web.Response(status=404, text="404: Media file is no longer available.")

    total_size = record["file_size"]
    file_name = record["file_name"]
    mime_type = record["mime_type"] or "application/octet-stream"

    # Handle HTTP Range
    range_header = request.headers.get("Range")
    offset = 0
    length = total_size
    status_code = 200

    if range_header and range_header.startswith("bytes="):
        range_spec = range_header.replace("bytes=", "").split("-")
        try:
            start_str, end_str = range_spec[0], range_spec[1]
            start = int(start_str) if start_str else 0
            end = int(end_str) if end_str else total_size - 1
            if start < total_size:
                offset = start
                length = (end - start) + 1
                status_code = 206
        except Exception:
            offset = 0
            length = total_size

    disposition = "attachment" if as_attachment else "inline"
    safe_filename = quote(file_name)

    headers = {
        "Content-Type": mime_type,
        "Accept-Ranges": "bytes",
        "Content-Length": str(length),
        "Content-Disposition": f'{disposition}; filename="{safe_filename}"; filename*=UTF-8\'\'{safe_filename}',
    }

    if status_code == 206:
        headers["Content-Range"] = f"bytes {offset}-{offset + length - 1}/{total_size}"

    response = web.StreamResponse(status=status_code, headers=headers)
    await response.prepare(request)

    # High-speed parallel MTProto streaming with connection pooling
    try:
        from fast_streamer import fast_stream_file
        async for chunk in fast_stream_file(client, msg, offset=offset, length=length, concurrency=6):
            await response.write(chunk)
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    except Exception as err:
        print(f"[FAST STREAM ERROR] {err}, falling back to standard...", file=sys.stderr)
        try:
            async for chunk in client.iter_download(
                msg.media,
                offset=offset,
                limit=length,
                chunk_size=1024 * 1024,
                request_size=1024 * 1024,
            ):
                await response.write(chunk)
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        except Exception as e2:
            print(f"[STREAM WRITE ERROR] {e2}", file=sys.stderr)

    return response


async def handle_download(request: web.Request):
    return await stream_media_response(request, as_attachment=True)


async def handle_raw_stream(request: web.Request):
    return await stream_media_response(request, as_attachment=False)


async def handle_watch(request: web.Request):
    file_hash = request.match_info.get("file_hash", "")
    record = get_file_record(file_hash)
    if not record:
        return web.Response(status=404, text="404: File not found.")

    if is_expired(record["created_at"]):
        expired_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Link Expired - File To Link Bot</title>
    <style>
        body {{
            margin: 0;
            padding: 20px;
            background-color: #0f1117;
            color: #e5e7eb;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 90vh;
        }}
        .container {{
            max-width: 500px;
            width: 100%;
            background: #1a1d24;
            border: 1px solid #ef4444;
            border-radius: 12px;
            padding: 32px 24px;
            box-sizing: border-box;
            text-align: center;
        }}
        .badge {{
            display: inline-block;
            padding: 6px 14px;
            background: rgba(239, 68, 68, 0.15);
            color: #f87171;
            border-radius: 20px;
            font-size: 0.85rem;
            font-weight: 600;
            margin-bottom: 16px;
        }}
        h2 {{
            color: #ef4444;
            margin-top: 0;
            margin-bottom: 12px;
        }}
        p {{
            color: #9ca3af;
            line-height: 1.5;
            margin: 8px 0;
        }}
        a {{
            color: #60a5fa;
            text-decoration: none;
        }}
        a:hover {{
            text-decoration: underline;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="badge">⏰ Link Expired</div>
        <h2>Link Has Expired</h2>
        <p>This stream and download link was valid for <b>{LINK_EXPIRY_DAYS} days</b> and has expired.</p>
        <p>To access this file again, send or forward it to <a href="https://t.me/Filetolink_kakebot">@Filetolink_kakebot</a> on Telegram to generate a fresh link.</p>
    </div>
</body>
</html>"""
        return web.Response(text=expired_html, content_type="text/html", status=410)

    base_url = get_server_url()
    stream_url = f"{base_url}/stream/{file_hash}"
    download_url = f"{base_url}/download/{file_hash}"
    file_name = record["file_name"]
    formatted_size = format_size(record["file_size"])
    mime = record["mime_type"]
    expiry_time_str = format_expiry_date(record["created_at"])

    is_video = is_video_file(file_name, mime)
    is_audio = mime.startswith("audio/") or any(file_name.lower().endswith(ext) for ext in [".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav", ".flac"])

    # If the file is neither video nor audio, redirect directly to download
    if not is_video and not is_audio:
        raise web.HTTPFound(download_url)

    safe_file_name = html.escape(file_name)

    if is_video:
        player_html = f"""
        <div class="player-wrapper">
            <video id="player" playsinline controls preload="metadata">
                <source src="{stream_url}" type="{mime}">
                Your browser does not support HTML5 video.
            </video>
        </div>
        """
    else:
        player_html = f"""
        <div class="player-wrapper audio-wrapper">
            <div class="audio-icon">🎵</div>
            <audio id="player" controls preload="metadata">
                <source src="{stream_url}" type="{mime}">
                Your browser does not support HTML5 audio.
            </audio>
        </div>
        """

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{safe_file_name} - Stream & Download</title>
    <link rel="stylesheet" href="https://cdn.plyr.io/3.7.8/plyr.css" />
    <style>
        :root {{
            --plyr-color-main: #3b82f6;
            --plyr-video-background: #000;
            --plyr-menu-background: rgba(18, 24, 38, 0.95);
            --plyr-menu-color: #f3f4f6;
        }}
        * {{
            box-sizing: border-box;
        }}
        body {{
            margin: 0;
            padding: 24px 12px;
            background: radial-gradient(circle at top, #141b29 0%, #080b11 100%);
            color: #e5e7eb;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
        }}
        .container {{
            max-width: 920px;
            width: 100%;
            background: rgba(20, 26, 38, 0.88);
            backdrop-filter: blur(16px);
            -webkit-backdrop-filter: blur(16px);
            border: 1px solid rgba(255, 255, 255, 0.08);
            border-radius: 18px;
            padding: 24px;
            box-shadow: 0 25px 60px rgba(0, 0, 0, 0.65);
        }}
        .header {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 14px;
            flex-wrap: wrap;
            gap: 10px;
        }}
        .tag-pill {{
            background: linear-gradient(135deg, #2563eb, #1d4ed8);
            color: white;
            font-size: 0.75rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            padding: 5px 12px;
            border-radius: 20px;
            display: inline-flex;
            align-items: center;
            gap: 6px;
        }}
        .file-title {{
            margin: 8px 0 14px 0;
            font-size: 1.25rem;
            font-weight: 700;
            color: #f9fafb;
            word-break: break-all;
            line-height: 1.4;
        }}
        .meta-pills {{
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
            margin-bottom: 18px;
        }}
        .pill {{
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(255, 255, 255, 0.08);
            color: #9ca3af;
            font-size: 0.85rem;
            padding: 5px 12px;
            border-radius: 8px;
        }}
        .player-wrapper {{
            border-radius: 12px;
            overflow: hidden;
            background: #000;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.5);
            position: relative;
        }}
        .audio-wrapper {{
            padding: 40px 20px;
            background: linear-gradient(180deg, #182032 0%, #0c1018 100%);
            text-align: center;
        }}
        .audio-icon {{
            font-size: 3.5rem;
            margin-bottom: 20px;
            animation: pulse 2s infinite ease-in-out;
        }}
        @keyframes pulse {{
            0%, 100% {{ transform: scale(1); opacity: 0.9; }}
            50% {{ transform: scale(1.08); opacity: 1; }}
        }}
        .actions-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
            gap: 10px;
            margin-top: 22px;
        }}
        .btn {{
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
            padding: 12px 18px;
            font-size: 0.9rem;
            font-weight: 600;
            border-radius: 10px;
            text-decoration: none;
            cursor: pointer;
            border: none;
            transition: all 0.2s ease;
        }}
        .btn-primary {{
            background: #2563eb;
            color: white;
            box-shadow: 0 4px 14px rgba(37, 99, 235, 0.4);
        }}
        .btn-primary:hover {{
            background: #1d4ed8;
            transform: translateY(-1px);
        }}
        .btn-secondary {{
            background: #1f2937;
            color: #e5e7eb;
            border: 1px solid rgba(255, 255, 255, 0.1);
        }}
        .btn-secondary:hover {{
            background: #374151;
            color: white;
        }}
        .btn-player {{
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(255, 255, 255, 0.1);
            color: #d1d5db;
        }}
        .btn-player:hover {{
            background: rgba(255, 255, 255, 0.12);
            color: white;
        }}
        .shortcuts {{
            margin-top: 22px;
            padding-top: 16px;
            border-top: 1px solid rgba(255, 255, 255, 0.08);
            display: flex;
            justify-content: center;
            flex-wrap: wrap;
            gap: 16px;
            font-size: 0.8rem;
            color: #6b7280;
        }}
        .shortcuts kbd {{
            background: rgba(255, 255, 255, 0.1);
            border-radius: 4px;
            padding: 2px 6px;
            color: #9ca3af;
            font-family: inherit;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <span class="tag-pill">File to Link</span>
            <span style="font-size: 0.85rem; color: #9ca3af;">Direct Stream & Download</span>
        </div>
        <h1 class="file-title">{safe_file_name}</h1>
        <div class="meta-pills">
            <span class="pill">📦 {formatted_size}</span>
            <span class="pill">🏷 {mime}</span>
            <span class="pill">⏳ Valid until {expiry_time_str}</span>
        </div>

        {player_html}

        <div class="actions-grid">
            <a href="{download_url}" class="btn btn-primary">
                <span>⬇️</span> Download File ({formatted_size})
            </a>
            <button onclick="copyDownloadLink()" id="copyBtn" class="btn btn-secondary">
                <span>📋</span> Copy Download Link
            </button>
            <a href="vlc://{stream_url}" class="btn btn-player">
                <span>🎬</span> Open in VLC
            </a>
            <a href="intent:{stream_url}#Intent;package=com.mxtech.videoplayer.ad;type=video/*;end" class="btn btn-player">
                <span>📱</span> Open in MX Player
            </a>
            <button onclick="downloadM3U()" class="btn btn-player">
                <span>🎵</span> Download M3U
            </button>
        </div>

        <div class="shortcuts">
            <span><kbd>Space</kbd> / <kbd>K</kbd> Play/Pause</span>
            <span><kbd>←</kbd> <kbd>→</kbd> Seek 10s</span>
            <span><kbd>↑</kbd> <kbd>↓</kbd> Volume</span>
            <span><kbd>F</kbd> Fullscreen</span>
            <span><kbd>M</kbd> Mute</span>
            <span><kbd>0-9</kbd> Jump %</span>
        </div>
    </div>

    <script src="https://cdn.plyr.io/3.7.8/plyr.polyfilled.js"></script>
    <script>
        document.addEventListener('DOMContentLoaded', () => {{
            const player = new Plyr('#player', {{
                controls: [
                    'play-large', 'restart', 'rewind', 'play', 'fast-forward',
                    'progress', 'current-time', 'duration', 'mute', 'volume',
                    'settings', 'pip', 'airplay', 'fullscreen'
                ],
                seekTime: 10,
                speed: {{ selected: 1, options: [0.5, 0.75, 1, 1.25, 1.5, 2] }},
                keyboard: {{ focused: true, global: true }},
                tooltips: {{ controls: true, seek: true }}
            }});
        }});

        function copyDownloadLink() {{
            const url = "{download_url}";
            navigator.clipboard.writeText(url).then(() => {{
                const btn = document.getElementById('copyBtn');
                btn.innerHTML = '<span>✅</span> Link Copied!';
                setTimeout(() => {{ btn.innerHTML = '<span>📋</span> Copy Download Link'; }}, 2000);
            }}).catch(() => {{
                prompt('Copy download link:', url);
            }});
        }}

        function downloadM3U() {{
            const title = {json.dumps(file_name)};
            const url = "{stream_url}";
            const content = '#EXTM3U\\n#EXTINF:-1,' + title + '\\n' + url + '\\n';
            const blob = new Blob([content], {{ type: 'audio/x-mpegurl' }});
            const a = document.createElement('a');
            a.href = URL.createObjectURL(blob);
            a.download = (title || 'stream') + '.m3u';
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
        }}
    </script>
</body>
</html>"""
    return web.Response(text=html_content, content_type="text/html")


async def handle_debug(request: web.Request):
    return web.json_response({
        "status": "online",
        "bin_channel_env": os.getenv("BIN_CHANNEL", ""),
        "owner_id": OWNER_ID,
        "saved_bin_channel": get_setting("bin_channel"),
        "active_storage_channel": get_storage_channel(),
        "default_storage_channel": DEFAULT_STORAGE_CHANNEL,
    })


def setup_web_app() -> web.Application:
    app = web.Application(client_max_size=1024**3 * 4)
    app.router.add_get("/", handle_index)
    app.router.add_get("/download/{file_hash}", handle_download)
    app.router.add_get("/stream/{file_hash}", handle_raw_stream)
    app.router.add_get("/watch/{file_hash}", handle_watch)
    app.router.add_get("/debug", handle_debug)
    return app


# --- Telethon Telegram Bot Handlers ---
@client.on(events.NewMessage(incoming=True))
async def user_activity_tracker(event):
    """Automatically track and register every user who interacts with the bot."""
    if event.is_private and event.sender_id and event.sender_id > 0:
        try:
            sender = await event.get_sender()
            if sender and not getattr(sender, "bot", False):
                register_user(
                    sender.id,
                    getattr(sender, "first_name", "") or "",
                    getattr(sender, "username", "") or ""
                )
            else:
                register_user(event.sender_id)
        except Exception:
            register_user(event.sender_id)


@client.on(events.CallbackQuery)
async def callback_activity_tracker(event):
    """Register users when they click any inline button."""
    if event.sender_id and event.sender_id > 0:
        try:
            sender = await event.get_sender()
            if sender and not getattr(sender, "bot", False):
                register_user(
                    sender.id,
                    getattr(sender, "first_name", "") or "",
                    getattr(sender, "username", "") or ""
                )
            else:
                register_user(event.sender_id)
        except Exception:
            register_user(event.sender_id)


@client.on(events.NewMessage(pattern="/start"))
async def start_handler(event):
    sender = await event.get_sender()
    name = sender.first_name if sender else "there"
    if sender and not getattr(sender, "bot", False):
        register_user(sender.id, sender.first_name or "", sender.username or "")

    owner_cmd = "\n• /ownerhelp - Admin settings" if (event.sender_id in [OWNER_ID, 6558775028]) else ""
    text = (
        f"Hey <b>{html.escape(name)}</b>!\n\n"
        "Send or forward any file (up to <b>2 GB</b>), or paste a video link from YouTube, Instagram, TikTok, Facebook, Pinterest, or Twitter/X.\n\n"
        "I'll give you a direct download link and a browser streaming player instantly.\n\n"
        "<b>Commands:</b>\n"
        "• /help - How to use\n"
        "• /ping - Check status\n"
        "• /id - Your Telegram ID"
        f"{owner_cmd}"
    )
    buttons = [
        [Button.url("Developer", f"tg://user?id={OWNER_ID}")]
    ]
    await event.reply(text, parse_mode="html", buttons=buttons)


@client.on(events.NewMessage(pattern="/help"))
async def help_handler(event):
    if event.is_private and event.sender_id:
        try:
            sender = await event.get_sender()
            if sender and not getattr(sender, "bot", False):
                register_user(sender.id, getattr(sender, "first_name", "") or "", getattr(sender, "username", "") or "")
            else:
                register_user(event.sender_id)
        except Exception:
            register_user(event.sender_id)
    text = (
        "<b>How to use:</b>\n\n"
        "1. Send or forward any file (up to <b>2 GB</b>) or paste any video link (up to <b>500 MB</b>).\n"
        "2. You'll receive a direct download link and an online streaming link.\n"
        "3. Works in browser, VLC, MX Player, and download managers.\n\n"
        f"Links remain active for <b>{LINK_EXPIRY_DAYS} days</b>."
    )
    await event.reply(text, parse_mode="html")


@client.on(events.NewMessage(pattern="/id"))
async def id_handler(event):
    sender = await event.get_sender()
    if sender and not getattr(sender, "bot", False):
        register_user(sender.id, getattr(sender, "first_name", "") or "", getattr(sender, "username", "") or "")
    elif event.sender_id:
        register_user(event.sender_id)
    user_id = sender.id if sender else "Unknown"
    chat_id = event.chat_id
    username = f"@{sender.username}" if sender and sender.username else "None"
    text = (
        f"<b>Your Info:</b>\n\n"
        f"• User ID: <code>{user_id}</code>\n"
        f"• Chat ID: <code>{chat_id}</code>\n"
        f"• Username: {username}"
    )
    if event.is_channel or event.is_group:
        text += f"\n\nStorage channel ID: <code>BIN_CHANNEL={chat_id}</code>"
    await event.reply(text, parse_mode="html")


@client.on(events.NewMessage(pattern="/ping"))
async def ping_handler(event):
    if event.is_private and event.sender_id:
        try:
            sender = await event.get_sender()
            if sender and not getattr(sender, "bot", False):
                register_user(sender.id, getattr(sender, "first_name", "") or "", getattr(sender, "username", "") or "")
            else:
                register_user(event.sender_id)
        except Exception:
            register_user(event.sender_id)
    t0 = time.time()
    msg = await event.reply("Ping...")
    latency = round((time.time() - t0) * 1000)
    await msg.edit(f"Pong! (<code>{latency}ms</code>)", parse_mode="html")


@client.on(events.NewMessage(pattern=r"^/(ownerhelp|adminhelp)"))
async def owner_help_handler(event):
    """Show owner control panel and administrative commands."""
    if event.sender_id != OWNER_ID and event.sender_id != 6558775028:
        await event.reply("Access denied.", parse_mode="html")
        return

    storage_chan = get_storage_channel()
    text = (
        "<b>Owner Control Panel:</b>\n\n"
        "• <code>/broadcast &lt;text&gt;</code> - Send message to all users\n"
        "• <code>/users</code> - List all registered users\n"
        "• <code>/adduser &lt;id&gt;</code> - Add user ID\n"
        f"• <code>/setchannel &lt;id&gt;</code> - Set storage channel (current: <code>{storage_chan}</code>)\n"
        "• <code>/stats</code> - View stats\n"
        "• <code>/ping</code> - Server status"
    )
    buttons = [
        [
            Button.inline("View Stats", data=b"view_stats")
        ]
    ]
    await event.reply(text, parse_mode="html", buttons=buttons)


@client.on(events.NewMessage(pattern=r"^/broadcast"))
async def broadcast_handler(event):
    """Broadcast a message or forwarded media to all registered bot users (Owner only)."""
    if event.sender_id != OWNER_ID and event.sender_id != 6558775028:
        await event.reply("⛔ <b>Access Denied:</b> This command is restricted to the bot owner.", parse_mode="html")
        return

    reply_msg = await event.get_reply_message()
    broadcast_text = ""
    parts = event.text.split(maxsplit=1)
    if len(parts) > 1:
        broadcast_text = parts[1].strip()

    if not reply_msg and not broadcast_text:
        usage = (
            "ℹ️ <b>How to Use /broadcast:</b>\n\n"
            "1. <b>Text Broadcast:</b>\n"
            "   <code>/broadcast &lt;Your announcement text&gt;</code>\n\n"
            "2. <b>Media / Post Broadcast:</b>\n"
            "   Reply to any photo, video, document, audio, or message with <code>/broadcast</code>!\n\n"
            "<i>The message will be delivered to every registered bot user.</i>"
        )
        await event.reply(usage, parse_mode="html")
        return

    user_ids = get_all_user_ids()
    if OWNER_ID not in user_ids:
        user_ids.append(OWNER_ID)

    total_targets = len(user_ids)
    if total_targets == 0:
        await event.reply("⚠️ No registered users found in the database yet.", parse_mode="html")
        return

    status_msg = await event.reply(f"⏳ <b>Starting broadcast to {total_targets} users...</b>", parse_mode="html")
    start_time = time.time()

    success_count = 0
    blocked_count = 0
    failed_count = 0

    for idx, uid in enumerate(user_ids, 1):
        try:
            if reply_msg:
                await client.send_message(uid, reply_msg)
            else:
                await client.send_message(uid, broadcast_text, parse_mode="html")
            success_count += 1
        except FloodWaitError as fe:
            print(f"[BROADCAST] FloodWait: sleeping {fe.seconds + 1}s...")
            await asyncio.sleep(fe.seconds + 1)
            try:
                if reply_msg:
                    await client.send_message(uid, reply_msg)
                else:
                    await client.send_message(uid, broadcast_text, parse_mode="html")
                success_count += 1
            except Exception:
                failed_count += 1
        except (UserIsBlockedError, InputUserDeactivatedError):
            blocked_count += 1
            delete_user(uid)
        except Exception as e:
            failed_count += 1
            print(f"[BROADCAST ERROR] User {uid}: {e}", file=sys.stderr)

        await asyncio.sleep(0.05)

        if idx % 20 == 0 or idx == total_targets:
            try:
                elapsed = round(time.time() - start_time, 1)
                await status_msg.edit(
                    f"📡 <b>Broadcasting in Progress...</b>\n\n"
                    f"👥 <b>Progress:</b> <code>{idx}/{total_targets}</code>\n"
                    f"✅ <b>Sent:</b> <code>{success_count}</code>\n"
                    f"🚫 <b>Blocked:</b> <code>{blocked_count}</code>\n"
                    f"⏱ <b>Elapsed:</b> <code>{elapsed}s</code>",
                    parse_mode="html"
                )
            except Exception:
                pass

    time_taken = round(time.time() - start_time, 1)
    report = (
        "📢 <b>Broadcast Completed Successfully!</b>\n\n"
        f"👥 <b>Total Target:</b> <code>{total_targets}</code>\n"
        f"✅ <b>Delivered:</b> <code>{success_count}</code>\n"
        f"🚫 <b>Blocked / Inactive:</b> <code>{blocked_count}</code>\n"
        f"❌ <b>Failed / Errors:</b> <code>{failed_count}</code>\n"
        f"⏱ <b>Time Taken:</b> <code>{time_taken}s</code>"
    )
    try:
        await status_msg.edit(report, parse_mode="html")
    except Exception:
        await event.reply(report, parse_mode="html")


@client.on(events.NewMessage(pattern=r"^/users"))
async def users_list_handler(event):
    """View registered users (Owner only)."""
    if event.sender_id != OWNER_ID and event.sender_id != 6558775028:
        return
    users = get_all_users()
    if not users:
        await event.reply("ℹ️ No users registered yet.", parse_mode="html")
        return

    text = f"👥 <b>Registered Users ({len(users)}):</b>\n\n"
    lines = []
    for uid, fname, uname, _ in users[:50]:
        name_part = f" ({fname})" if fname else ""
        user_part = f" [@{uname}]" if uname else ""
        lines.append(f"• <code>{uid}</code>{name_part}{user_part}")
    text += "\n".join(lines)
    if len(users) > 50:
        text += f"\n\n<i>...and {len(users) - 50} more</i>"
    await event.reply(text, parse_mode="html")


@client.on(events.NewMessage(pattern=r"^/adduser"))
async def add_user_handler(event):
    """Manually add a user ID to the broadcast database (Owner only)."""
    if event.sender_id != OWNER_ID and event.sender_id != 6558775028:
        return
    parts = event.text.split(maxsplit=1)
    if len(parts) > 1:
        ids_raw = parts[1].replace(",", " ").split()
        added = 0
        for item in ids_raw:
            item = item.strip().lstrip("-")
            if item.isdigit():
                uid = int(item)
                register_user(uid, f"User_{uid}")
                added += 1
        await event.reply(f"✅ Added {added} user(s) to the broadcast database!\n\nTotal Users: <code>{get_user_count()}</code>", parse_mode="html")
    else:
        await event.reply("ℹ️ Usage: <code>/adduser &lt;user_id&gt;</code> (or multiple IDs separated by space)", parse_mode="html")


@client.on(events.NewMessage(pattern=r"^/stats"))
async def stats_handler(event):
    """View bot database statistics (Owner only)."""
    if event.sender_id != OWNER_ID and event.sender_id != 6558775028:
        return
    total_files, total_bytes, total_users = get_db_stats()
    storage_chan = get_storage_channel()
    formatted_total_size = format_size(total_bytes)

    text = (
        "<b>Bot Statistics:</b>\n\n"
        f"• Users: <code>{total_users}</code>\n"
        f"• Files: <code>{total_files}</code>\n"
        f"• Total Size: <code>{formatted_total_size}</code>\n"
        f"• Storage Channel: <code>{storage_chan}</code>\n"
        f"• Server: <code>{get_server_url()}</code>\n"
        f"• Link Expiry: <code>{LINK_EXPIRY_DAYS} days</code>"
    )
    await event.reply(text, parse_mode="html")


@client.on(events.CallbackQuery(data=b"view_stats"))
async def view_stats_callback(event):
    if event.sender_id != OWNER_ID and event.sender_id != 6558775028:
        await event.answer("⛔ Owner only.", alert=True)
        return
    total_files, total_bytes, total_users = get_db_stats()
    formatted_total_size = format_size(total_bytes)
    await event.answer(f"👥 Users: {total_users} | 📁 Files: {total_files} | 📦 Size: {formatted_total_size}", alert=True)


@client.on(events.ChatAction)
async def chat_action_handler(event):
    """Automatically detect when bot is added to a channel and configure it as storage channel."""
    if event.is_channel and (event.user_added or event.user_joined):
        try:
            me = await client.get_me()
            if event.user_id == me.id and event.chat_id < 0:
                set_setting("bin_channel", str(event.chat_id))
                print(f"[AUTO-DETECT] Bot added to channel {event.chat_id}, configured as storage channel.")
        except Exception:
            pass


@client.on(events.NewMessage(pattern=r"^/(setchannel|setstorage)"))
async def set_channel_handler(event):
    """Allow setting storage channel via command inside a channel or private chat."""
    if (event.is_channel or event.is_group) and event.chat_id < 0:
        set_setting("bin_channel", str(event.chat_id))
        await event.reply("✅ <b>This channel is now set as the permanent storage channel!</b>", parse_mode="html")
        return

    parts = event.text.split(maxsplit=1)
    if len(parts) > 1:
        target = parse_channel_target(parts[1])
        if target and (isinstance(target, str) or target < 0):
            set_setting("bin_channel", str(target))
            await event.reply(f"✅ Storage channel updated to <code>{target}</code> successfully!", parse_mode="html")
        else:
            await event.reply("❌ Invalid channel format. Provide a channel ID (e.g. <code>-1004441486082</code>) or @channel_username. Users/private chats cannot be storage channels.", parse_mode="html")
    else:
        await event.reply("ℹ️ Send <code>/setchannel &lt;channel_id or @username&gt;</code> or use the command directly inside your channel.", parse_mode="html")


@client.on(events.NewMessage)
async def media_handler(event):
    # Ignore bot's own outgoing messages
    if event.message.out:
        return

    # Ignore explicit storage channel configuration commands
    if event.text and any(event.text.strip().startswith(c) for c in ["/setchannel", "/setstorage"]):
        return

    active_storage = get_storage_channel()

    # Register user in database
    sender = await event.get_sender()
    if sender and not getattr(sender, "bot", False):
        register_user(sender.id, sender.first_name or "", sender.username or "")

    # Check if message is a URL / web link or has no actual file media
    is_webpage_preview = isinstance(event.media, MessageMediaWebPage)
    if not event.media or is_webpage_preview:
        # Ignore bot slash commands for text-only messages (handled by respective command handlers)
        if event.text and event.text.startswith("/"):
            return

        if event.is_private:
            url = extract_url_from_text(event.text)
            if url:
                status_msg = None
                try:
                    status_msg = await event.reply("⏳ <i>Downloading link...</i>", parse_mode="html")
                except Exception as e:
                    print(f"[ERROR] Could not send initial link status: {e}", file=sys.stderr)

                meta = None
                try:
                    dl_tracker = ThrottledProgressTracker(status_msg, "📥 Downloading...")
                    file_path, meta, err = await download_url_media(url, progress_callback=dl_tracker.on_download_progress)
                    if err:
                        err_text = f"❌ Error: <code>{html.escape(err)}</code>"
                        if status_msg:
                            await status_msg.edit(err_text, parse_mode="html")
                        else:
                            await event.reply(err_text, parse_mode="html")
                        return

                    if not file_path or not meta:
                        err_text = "❌ Error: <code>Could not extract media from link.</code>"
                        if status_msg:
                            await status_msg.edit(err_text, parse_mode="html")
                        else:
                            await event.reply(err_text, parse_mode="html")
                        return

                    target_channel = active_storage
                    if not target_channel or target_channel in [OWNER_ID, 6558775028, -int(f"100{OWNER_ID}")]:
                        target_channel = DEFAULT_STORAGE_CHANNEL

                    clean_title = meta.get("title") or "video"
                    storage_msg = None
                    up_tracker = ThrottledProgressTracker(status_msg, "☁️ Uploading to storage...")
                    try:
                        storage_msg = await client.send_file(
                            target_channel,
                            file=file_path,
                            caption=f"🎥 <b>{html.escape(clean_title[:100])}</b>\n🔗 <i>Source: {html.escape(url[:80])}</i>",
                            parse_mode="html",
                            supports_streaming=True,
                            progress_callback=up_tracker.on_upload_progress
                        )
                        storage_chat_id = target_channel
                        storage_message_id = storage_msg.id
                    except Exception as upload_err:
                        print(f"[STORAGE UPLOAD ERROR] Primary channel failed: {upload_err}", file=sys.stderr)
                        if target_channel != DEFAULT_STORAGE_CHANNEL:
                            storage_msg = await client.send_file(
                                DEFAULT_STORAGE_CHANNEL,
                                file=file_path,
                                caption=f"🎥 <b>{html.escape(clean_title[:100])}</b>\n🔗 <i>Source: {html.escape(url[:80])}</i>",
                                parse_mode="html",
                                supports_streaming=True,
                                progress_callback=up_tracker.on_upload_progress
                            )
                            storage_chat_id = DEFAULT_STORAGE_CHANNEL
                            storage_message_id = storage_msg.id
                        else:
                            raise upload_err

                    raw_ext = meta.get("ext", "mp4").lstrip(".")
                    safe_title = re.sub(r'[/\\:*?"<>|]', '_', clean_title).strip() or "video"
                    file_name = f"{safe_title}.{raw_ext}"
                    file_size = meta.get("filesize", 0)
                    mime_type, _ = mimetypes.guess_type(file_name)
                    if not mime_type:
                        mime_type = "video/mp4" if raw_ext in ["mp4", "mkv", "webm", "mov"] else "application/octet-stream"

                    file_hash = secrets.token_urlsafe(8)
                    now_ts = int(time.time())

                    save_file_record(
                        file_hash=file_hash,
                        chat_id=storage_chat_id,
                        message_id=storage_message_id,
                        file_name=file_name,
                        file_size=file_size,
                        mime_type=mime_type,
                        orig_chat_id=event.chat_id,
                        orig_msg_id=event.message.id
                    )

                    base_url = get_server_url()
                    download_link = f"{base_url}/download/{file_hash}"
                    watch_link = f"{base_url}/watch/{file_hash}"
                    formatted_size = format_size(file_size)

                    reply_text = (
                        f"🎬 <b>{html.escape(file_name)}</b>\n\n"
                        f"📁 <b>Size:</b> <code>{formatted_size}</code>\n"
                        f"⏳ <b>Expires:</b> <code>{LINK_EXPIRY_DAYS} days</code>\n\n"
                        f"🍿 <b>Stream Online:</b>\n"
                        f"{watch_link}\n\n"
                        f"⚡ <b>Fast Download:</b>\n"
                        f"{download_link}\n\n"
                        f"<i>🚀 High-speed MTProto • Play in browser or VLC / MX Player</i>"
                    )
                    buttons = [
                        [Button.url("🍿 Stream Online", watch_link)],
                        [Button.url("⚡ Fast Download", download_link)],
                    ]

                    if status_msg:
                        await status_msg.edit(reply_text, parse_mode="html", buttons=buttons)
                    else:
                        await event.reply(reply_text, parse_mode="html", buttons=buttons)

                except Exception as link_err:
                    print(f"[ERROR processing URL] {link_err}", file=sys.stderr)
                    err_msg = f"❌ Error: <code>{html.escape(str(link_err)[:150])}</code>"
                    if status_msg:
                        try:
                            await status_msg.edit(err_msg, parse_mode="html")
                        except Exception:
                            await event.reply(err_msg, parse_mode="html")
                    else:
                        await event.reply(err_msg, parse_mode="html")
                finally:
                    if meta and meta.get("temp_dir") and os.path.isdir(meta["temp_dir"]):
                        shutil.rmtree(meta["temp_dir"], ignore_errors=True)
                return

            msg = (
                "Send or forward any file (up to <b>2 GB</b>), or paste a video link to generate direct download & stream links."
            )
            await event.reply(msg, parse_mode="html")
        return

    status_msg = None
    try:
        status_msg = await event.reply("⚡ <i>Generating links...</i>", parse_mode="html")
    except Exception as e:
        print(f"[ERROR] Could not send initial status: {e}", file=sys.stderr)

    try:
        file_name, file_size, mime_type = extract_media_meta(event.message)
        file_hash = secrets.token_urlsafe(8)
        now_ts = int(time.time())

        # If file was posted directly in the storage channel, use it directly
        if active_storage and event.chat_id == active_storage:
            storage_chat_id = event.chat_id
            storage_message_id = event.message.id
        else:
            # Backup to permanent storage channel
            storage_chat_id, storage_message_id = await backup_to_storage_channel(event.message)

        save_file_record(
            file_hash=file_hash,
            chat_id=storage_chat_id,
            message_id=storage_message_id,
            file_name=file_name,
            file_size=file_size,
            mime_type=mime_type,
            orig_chat_id=event.chat_id,
            orig_msg_id=event.message.id
        )

        base_url = get_server_url()
        download_link = f"{base_url}/download/{file_hash}"
        watch_link = f"{base_url}/watch/{file_hash}"
        formatted_size = format_size(file_size)

        is_video = is_video_file(file_name, mime_type, event.message)
        is_audio = mime_type.startswith("audio/")

        if is_video:
            reply_text = (
                f"🎬 <b>{html.escape(file_name)}</b>\n\n"
                f"📁 <b>Size:</b> <code>{formatted_size}</code>\n"
                f"⏳ <b>Expires:</b> <code>{LINK_EXPIRY_DAYS} days</code>\n\n"
                f"🍿 <b>Stream Online:</b>\n"
                f"{watch_link}\n\n"
                f"⚡ <b>Fast Download:</b>\n"
                f"{download_link}\n\n"
                f"<i>🚀 High-speed MTProto • Play in browser or VLC / MX Player</i>"
            )
            buttons = [
                [Button.url("🍿 Stream Online", watch_link)],
                [Button.url("⚡ Fast Download", download_link)],
            ]
        elif is_audio:
            reply_text = (
                f"🎵 <b>{html.escape(file_name)}</b>\n\n"
                f"📁 <b>Size:</b> <code>{formatted_size}</code>\n"
                f"⏳ <b>Expires:</b> <code>{LINK_EXPIRY_DAYS} days</code>\n\n"
                f"🎧 <b>Listen Online:</b>\n"
                f"{watch_link}\n\n"
                f"⚡ <b>Fast Download:</b>\n"
                f"{download_link}\n\n"
                f"<i>🚀 High-speed stream • Listen in browser or audio player</i>"
            )
            buttons = [
                [Button.url("🎧 Listen Online", watch_link)],
                [Button.url("⚡ Fast Download", download_link)],
            ]
        else:
            reply_text = (
                f"📄 <b>{html.escape(file_name)}</b>\n\n"
                f"📁 <b>Size:</b> <code>{formatted_size}</code>\n"
                f"⏳ <b>Expires:</b> <code>{LINK_EXPIRY_DAYS} days</code>\n\n"
                f"⚡ <b>Fast Download:</b>\n"
                f"{download_link}\n\n"
                f"<i>🚀 High-speed MTProto • Supports IDM & download managers</i>"
            )
            buttons = [
                [Button.url("⚡ Fast Download", download_link)],
            ]

        if status_msg:
            await status_msg.edit(reply_text, parse_mode="html", buttons=buttons)
        else:
            await event.reply(reply_text, parse_mode="html", buttons=buttons)
    except Exception as e:
        print(f"[ERROR in media_handler] {e}", file=sys.stderr)
        err_msg = f"❌ Error: <code>{html.escape(str(e))}</code>"
        if status_msg:
            try:
                await status_msg.edit(err_msg, parse_mode="html")
            except Exception:
                await event.reply(err_msg, parse_mode="html")
        else:
            await event.reply(err_msg, parse_mode="html")


# --- CLI Commands ---
async def cli_send_message(text: str, target_chat: int = OWNER_ID):
    if SESSION_STRING:
        cli_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        await cli_client.connect()
    else:
        from telethon.sessions import MemorySession
        cli_client = TelegramClient(MemorySession(), API_ID, API_HASH)
        await cli_client.start(bot_token=BOT_TOKEN)
    await cli_client.send_message(target_chat, text, parse_mode="html")
    print(f"[SUCCESS] Message sent to {target_chat}!")
    await cli_client.disconnect()


async def cli_send_file(file_path: str, caption: str = "", target_chat: int = OWNER_ID):
    if not os.path.isfile(file_path):
        print(f"[ERROR] File '{file_path}' not found!", file=sys.stderr)
        return

    if SESSION_STRING:
        cli_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        await cli_client.connect()
    else:
        from telethon.sessions import MemorySession
        cli_client = TelegramClient(MemorySession(), API_ID, API_HASH)
        await cli_client.start(bot_token=BOT_TOKEN)
    print(f"[*] Uploading '{file_path}' to chat {target_chat}...")
    msg = await cli_client.send_file(target_chat, file_path, caption=caption or f"Uploaded: {os.path.basename(file_path)}")

    file_name, file_size, mime_type = extract_media_meta(msg)
    file_hash = secrets.token_urlsafe(8)
    now_ts = int(time.time())
    save_file_record(
        file_hash=file_hash,
        chat_id=target_chat,
        message_id=msg.id,
        file_name=file_name,
        file_size=file_size,
        mime_type=mime_type
    )

    base_url = get_server_url()
    download_link = f"{base_url}/download/{file_hash}"
    watch_link = f"{base_url}/watch/{file_hash}"
    expiry_str = format_expiry_date(now_ts)
    is_video = is_video_file(file_name, mime_type, msg)

    print(f"[SUCCESS] File uploaded! Message ID: {msg.id}")
    print(f"Download Link: {download_link}")
    if is_video:
        print(f"Watch Link:    {watch_link}")
    print(f"Expires:       {expiry_str} ({LINK_EXPIRY_DAYS} days)")
    await cli_client.disconnect()


async def run_server():
    init_db()

    # Start Aiohttp Web Server first so port is ready
    app = setup_web_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, BIND_ADDRESS, PORT)
    await site.start()
    print(f"[*] HTTP Streaming server listening on {BIND_ADDRESS}:{PORT}")

    # Initialize public URL (Cloudflare Tunnel or custom domain)
    base_url = await init_public_url()

    # Start Telethon Bot
    try:
        await start_telegram_client()
    except FloodWaitError as fe:
        print(f"[!] Telegram FloodWait on login: sleeping {fe.seconds} seconds...", file=sys.stderr)
        await asyncio.sleep(fe.seconds)
        await start_telegram_client()
    me = await client.get_me()
    load_seed_users()

    # Auto-seed users from Telegram API getUpdates queue if available
    try:
        import aiohttp
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?limit=100", timeout=aiohttp.ClientTimeout(total=8)) as r:
                u_data = await r.json()
                if u_data.get("ok"):
                    for u in u_data.get("result", []):
                        m = u.get("message", {}) or {}
                        cq = u.get("callback_query", {}) or {}
                        f = cq.get("from", {}) or m.get("from", {})
                        if f.get("id") and not f.get("is_bot"):
                            register_user(f["id"], f.get("first_name", "") or "", f.get("username", "") or "")
    except Exception as e:
        print(f"[*] Note on initial user sync: {e}")

    active_chan = get_storage_channel()
    print("=" * 60)
    print("🚀 File To Link MTProto Streaming Bot Online!")
    print(f"Bot Name:      {me.first_name}")
    print(f"Bot Username:  @{me.username}")
    print(f"Owner ID:      {OWNER_ID}")
    print(f"Server Port:   {PORT}")
    print(f"Public URL:    {base_url}")
    print(f"Storage Chan:  {active_chan if active_chan else 'Disabled (Direct User Chat)'}")
    print(f"Capacity:      Files up to 2 GB (4 GB with Premium)")
    print("=" * 60)

    # Pre-cache storage channel in Telethon session
    if active_chan:
        try:
            await client.get_messages(active_chan, limit=1)
        except Exception as err:
            print(f"[*] Note on storage channel pre-cache: {err}")

    try:
        await client.send_message(
            OWNER_ID,
            f"🚀 <b>File To Link Bot is Online!</b>\n\n"
            f"⚡ MTProto Streaming: Up to 2 GB\n"
            f"🌐 Public Stream URL: {base_url}",
            parse_mode="html"
        )
    except Exception as e:
        print(f"[WARNING] Could not send startup message: {e}")

    # Auto-ping keep-alive loop to keep free cloud tiers (Render/Koyeb) awake 24/7
    async def keep_alive_loop():
        await asyncio.sleep(60)
        while True:
            current_url = get_server_url()
            if current_url and ("onrender.com" in current_url or "koyeb.app" in current_url):
                try:
                    import aiohttp
                    async with aiohttp.ClientSession() as s:
                        async with s.get(current_url, timeout=10) as r:
                            pass
                except Exception:
                    pass
            await asyncio.sleep(600)  # Ping every 10 minutes

    ping_task = asyncio.create_task(keep_alive_loop())

    print("[*] Bot is running. Press Ctrl+C to stop.")

    # Keep alive
    try:
        await client.run_until_disconnected()
    finally:
        if not ping_task.done():
            ping_task.cancel()
        if tunnel_process:
            try:
                tunnel_process.terminate()
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser(description="File To Link Telegram MTProto Bot")
    parser.add_argument("--send-msg", type=str, help="Send a text message to chat ID")
    parser.add_argument("--send-file", type=str, help="Upload a file and get links")
    parser.add_argument("--chat", type=int, default=OWNER_ID, help="Target Chat ID")
    parser.add_argument("--caption", type=str, default="", help="Caption for uploaded file")

    args = parser.parse_args()

    init_db()

    if args.send_msg:
        asyncio.run(cli_send_message(args.send_msg, target_chat=args.chat))
        return

    if args.send_file:
        asyncio.run(cli_send_file(args.send_file, caption=args.caption, target_chat=args.chat))
        return

    # Normal server running mode
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_server())
    except (KeyboardInterrupt, SystemExit):
        print("\n[*] Stopping bot...")
    finally:
        loop.close()


if __name__ == "__main__":
    main()
