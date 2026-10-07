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
import shutil
from typing import Optional, Tuple
from urllib.parse import quote
from dotenv import load_dotenv

from aiohttp import web
from telethon import TelegramClient, events, Button
from telethon.tl.types import (
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    DocumentAttributeAudio,
    MessageMediaDocument,
    MessageMediaPhoto,
)

# Load environment
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "8870014336:AAGVMVJy6CmC7ZooGULxE5whsFv0AgqM0Jg").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "6558775028"))
API_ID = int(os.getenv("API_ID", "38517263"))
API_HASH = os.getenv("API_HASH", "da981346b2708e5f8fefeee726b7bfd1").strip()

PORT = int(os.getenv("PORT", "8080"))
BIND_ADDRESS = os.getenv("BIND_ADDRESS", "0.0.0.0")
SERVER_URL = os.getenv("SERVER_URL", "").strip().rstrip("/")

DB_PATH = os.path.join(os.path.dirname(__file__), "file_store.db")
SESSION_NAME = os.path.join(os.path.dirname(__file__), "file_to_link_bot")

CURRENT_PUBLIC_URL = ""
tunnel_process = None


async def init_public_url() -> str:
    """Determine the public base URL using Cloudflare Tunnel or configuration."""
    global CURRENT_PUBLIC_URL, tunnel_process

    if SERVER_URL:
        CURRENT_PUBLIC_URL = SERVER_URL
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
            created_at INTEGER NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def save_file_record(file_hash: str, chat_id: int, message_id: int, file_name: str, file_size: int, mime_type: str):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute(
        "INSERT INTO files (file_hash, chat_id, message_id, file_name, file_size, mime_type, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (file_hash, chat_id, message_id, file_name, file_size, mime_type, int(time.time()))
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
        else:
            ext = mimetypes.guess_extension(mime_type) or ".bin"
            file_name = f"file_{message.id}{ext}"

    return file_name, file_size, mime_type


# Telethon Client
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)


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
        return web.Response(status=404, text="404: File not found or expired.")

    try:
        msg = await client.get_messages(record["chat_id"], ids=record["message_id"])
    except Exception as e:
        return web.Response(status=500, text=f"Error retrieving message from Telegram: {e}")

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

    # 1 MB chunk (maximum supported by Telegram MTProto)
    chunk_size = 1024 * 1024
    queue = asyncio.Queue(maxsize=4)

    async def prefetch_worker():
        try:
            async for chunk in client.iter_download(
                msg.media,
                offset=offset,
                limit=length,
                chunk_size=chunk_size,
                request_size=chunk_size
            ):
                await queue.put(chunk)
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        except Exception as err:
            print(f"[STREAM PREFETCH ERROR] {err}", file=sys.stderr)
        finally:
            await queue.put(None)  # Sentinel to indicate end of stream

    fetch_task = asyncio.create_task(prefetch_worker())

    try:
        while True:
            chunk = await queue.get()
            if chunk is None:
                break
            await response.write(chunk)
            queue.task_done()
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    except Exception as err:
        print(f"[STREAM WRITE ERROR] {err}", file=sys.stderr)
    finally:
        if not fetch_task.done():
            fetch_task.cancel()

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

    base_url = get_server_url()
    stream_url = f"{base_url}/stream/{file_hash}"
    download_url = f"{base_url}/download/{file_hash}"
    file_name = record["file_name"]
    formatted_size = format_size(record["file_size"])
    mime = record["mime_type"]

    is_video = "video" in mime
    is_audio = "audio" in mime
    is_image = "image" in mime

    player_html = ""
    if is_video:
        player_html = f"""
        <video controls autoplay playsinline style="max-width: 100%; border-radius: 8px; box-shadow: 0 4px 16px rgba(0,0,0,0.4);">
            <source src="{stream_url}" type="{mime}">
            Your browser does not support HTML5 video.
        </video>
        """
    elif is_audio:
        player_html = f"""
        <audio controls autoplay style="width: 100%; margin: 20px 0;">
            <source src="{stream_url}" type="{mime}">
            Your browser does not support HTML5 audio.
        </audio>
        """
    elif is_image:
        player_html = f"""
        <img src="{stream_url}" alt="{file_name}" style="max-width: 100%; max-height: 80vh; border-radius: 8px; box-shadow: 0 4px 16px rgba(0,0,0,0.4);" />
        """
    else:
        player_html = """
        <div style="padding: 40px; background: #222; border-radius: 8px; color: #bbb;">
            📁 Preview not available for this file type. Click download below.
        </div>
        """

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{file_name} - Stream & Download</title>
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
            max-width: 800px;
            width: 100%;
            background: #1a1d24;
            border: 1px solid #2d3342;
            border-radius: 12px;
            padding: 24px;
            box-sizing: border-box;
            text-align: center;
        }}
        h2 {{
            margin-top: 0;
            font-size: 1.3rem;
            word-break: break-all;
            color: #60a5fa;
        }}
        .meta {{
            font-size: 0.95rem;
            color: #9ca3af;
            margin-bottom: 20px;
        }}
        .actions {{
            margin-top: 24px;
            display: flex;
            gap: 12px;
            justify-content: center;
            flex-wrap: wrap;
        }}
        .btn {{
            display: inline-block;
            padding: 12px 24px;
            background: #2563eb;
            color: white;
            text-decoration: none;
            border-radius: 6px;
            font-weight: 600;
            transition: background 0.2s;
        }}
        .btn:hover {{
            background: #1d4ed8;
        }}
        .btn-copy {{
            background: #374151;
        }}
        .btn-copy:hover {{
            background: #4b5563;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h2>📁 {file_name}</h2>
        <div class="meta">📦 Size: {formatted_size} | 🏷 Type: {mime}</div>
        {player_html}
        <div class="actions">
            <a href="{download_url}" class="btn">⬇️ Download File</a>
            <button onclick="navigator.clipboard.writeText('{download_url}'); alert('Download link copied!');" class="btn btn-copy">📋 Copy Download Link</button>
        </div>
    </div>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


def setup_web_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/download/{file_hash}", handle_download)
    app.router.add_get("/stream/{file_hash}", handle_raw_stream)
    app.router.add_get("/watch/{file_hash}", handle_watch)
    return app


# --- Telethon Telegram Bot Handlers ---
@client.on(events.NewMessage(pattern="/start"))
async def start_handler(event):
    sender = await event.get_sender()
    name = sender.first_name if sender else "User"
    text = (
        f"👋 <b>Hello, {name}!</b>\n\n"
        f"🤖 Welcome to <b>File To Link Bot</b> (Up to 2 GB Support 🚀)!\n\n"
        f"Send or forward any file (documents, videos, audio, images, apk, archives), "
        f"and I will generate high-speed <b>Direct Download</b> and <b>Online Streaming</b> links for you!\n\n"
        f"<b>Features:</b>\n"
        f"• Supports files up to <b>2 GB</b> (and 4 GB for Premium)\n"
        f"• Instant Streaming & Web Player\n"
        f"• Resume-supported direct download links\n"
        f"• IDM & Download Manager compatible\n\n"
        f"<b>Commands:</b>\n"
        f"• /help - How to use\n"
        f"• /id - Check your Telegram ID\n"
        f"• /ping - Check bot & server status"
    )
    buttons = [
        [
            Button.url("👤 Developer / Owner", f"tg://user?id={OWNER_ID}"),
        ]
    ]
    await event.reply(text, parse_mode="html", buttons=buttons)


@client.on(events.NewMessage(pattern="/help"))
async def help_handler(event):
    text = (
        "📖 <b>How to Use:</b>\n\n"
        "1. Send or forward any file to this bot.\n"
        "2. The bot generates both a <b>Direct Download Link</b> and a <b>Stream / Watch</b> link.\n"
        "3. You can paste the link in your browser, VLC media player, or any download manager.\n\n"
        "⚡ <i>Powered by Telegram MTProto streaming — no 20 MB limits!</i>"
    )
    await event.reply(text, parse_mode="html")


@client.on(events.NewMessage(pattern="/id"))
async def id_handler(event):
    sender = await event.get_sender()
    user_id = sender.id if sender else "Unknown"
    chat_id = event.chat_id
    username = f"@{sender.username}" if sender and sender.username else "None"
    text = (
        f"🆔 <b>Account ID Info:</b>\n\n"
        f"• <b>User ID:</b> <code>{user_id}</code>\n"
        f"• <b>Chat ID:</b> <code>{chat_id}</code>\n"
        f"• <b>Username:</b> {username}\n"
    )
    await event.reply(text, parse_mode="html")


@client.on(events.NewMessage(pattern="/ping"))
async def ping_handler(event):
    t0 = time.time()
    msg = await event.reply("🏓 Pong...")
    latency = round((time.time() - t0) * 1000)
    await msg.edit(f"🏓 <b>Pong!</b>\n⏱ Latency: <code>{latency}ms</code>\n🌐 Server: <code>{get_server_url()}</code>", parse_mode="html")


@client.on(events.NewMessage)
async def media_handler(event):
    if not event.media or event.text.startswith("/"):
        return

    status_msg = await event.reply("⏳ <i>Generating links...</i>", parse_mode="html")

    file_name, file_size, mime_type = extract_media_meta(event.message)
    file_hash = secrets.token_urlsafe(8)

    save_file_record(
        file_hash=file_hash,
        chat_id=event.chat_id,
        message_id=event.message.id,
        file_name=file_name,
        file_size=file_size,
        mime_type=mime_type
    )

    base_url = get_server_url()
    download_link = f"{base_url}/download/{file_hash}"
    watch_link = f"{base_url}/watch/{file_hash}"
    formatted_size = format_size(file_size)

    reply_text = (
        f"✅ <b>File Link Generated!</b>\n\n"
        f"📁 <b>Name:</b> <code>{file_name}</code>\n"
        f"📦 <b>Size:</b> <code>{formatted_size}</code>\n"
        f"🏷 <b>Type:</b> <code>{mime_type}</code>\n\n"
        f"📥 <b>Direct Download Link:</b>\n{download_link}\n\n"
        f"🎬 <b>Online Stream / Watch:</b>\n{watch_link}"
    )

    buttons = [
        [Button.url("📥 Direct Download", download_link)],
        [Button.url("🎬 Stream / Watch Online", watch_link)],
    ]

    await status_msg.edit(reply_text, parse_mode="html", buttons=buttons)


# --- CLI Commands ---
async def cli_send_message(text: str, target_chat: int = OWNER_ID):
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

    from telethon.sessions import MemorySession
    cli_client = TelegramClient(MemorySession(), API_ID, API_HASH)
    await cli_client.start(bot_token=BOT_TOKEN)
    print(f"[*] Uploading '{file_path}' to chat {target_chat}...")
    msg = await cli_client.send_file(target_chat, file_path, caption=caption or f"Uploaded: {os.path.basename(file_path)}")

    file_name, file_size, mime_type = extract_media_meta(msg)
    file_hash = secrets.token_urlsafe(8)
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

    print(f"[SUCCESS] File uploaded! Message ID: {msg.id}")
    print(f"Download Link: {download_link}")
    print(f"Watch Link:    {watch_link}")
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
    await client.start(bot_token=BOT_TOKEN)
    me = await client.get_me()

    print("=" * 60)
    print("🚀 File To Link MTProto Streaming Bot Online!")
    print(f"Bot Name:      {me.first_name}")
    print(f"Bot Username:  @{me.username}")
    print(f"Owner ID:      {OWNER_ID}")
    print(f"Server Port:   {PORT}")
    print(f"Public URL:    {base_url}")
    print(f"Capacity:      Files up to 2 GB (4 GB with Premium)")
    print("=" * 60)

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

    print("[*] Bot is running. Press Ctrl+C to stop.")

    # Keep alive
    try:
        await client.run_until_disconnected()
    finally:
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
