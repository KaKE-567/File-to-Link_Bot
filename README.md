# 🚀 Telegram File To Link & Streaming Bot

High-performance Telegram MTProto file streaming bot supporting files up to **2 GB** (and **4 GB** with Telegram Premium).

Converts any file sent on Telegram into a direct, high-speed download link and an online streaming web player page.

---

## ✨ Features

- **No 20 MB File Limit:** Supports files up to **2 GB / 4 GB** using Telegram's native MTProto protocol.
- **High-Speed Streaming:** C-accelerated decryption (`cryptg`) and 1 MB chunk pipelining for maximum throughput.
- **HTTP Range Support:** Full seeking support for video players and multi-threaded downloading (IDM, 1DM, ADM, aria2, FDM).
- **Online Web Player:** Built-in web player page to watch videos and play audio directly in the browser without downloading first.
- **Automatic Cloudflare Tunnel:** Provisions a public, secure HTTPS URL out of the box with zero port-forwarding setup.
- **SQLite Database:** Stores file hashes permanently so links stay active across bot restarts.
- **CLI Utility:** Upload files or send messages directly from server terminal / scripts.

---

## 🛠️ Setup & Installation

### 1. Clone Repository
```bash
git clone https://github.com/KaKE-567/File-to-link_bot.git
cd File-to-link_bot
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables
Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```
Edit `.env` and fill in your details:
```env
BOT_TOKEN=your_bot_token_here
OWNER_ID=your_telegram_id_here
API_ID=your_api_id_here
API_HASH=your_api_hash_here
PORT=8080
BIND_ADDRESS=0.0.0.0
SERVER_URL=
```
- **BOT_TOKEN:** Get from [@BotFather](https://t.me/BotFather) on Telegram.
- **OWNER_ID:** Your numerical Telegram User ID.
- **API_ID & API_HASH:** Get free from [my.telegram.org](https://my.telegram.org) under *API development tools*.
- **SERVER_URL (Optional):** Set your custom domain or VPS IP (e.g. `https://example.com`). If empty, the bot automatically launches a free Cloudflare HTTPS tunnel.

---

## 🚀 Running the Bot

Use the included `tg.sh` management script:

```bash
# Start bot in background
./tg.sh start

# Check status
./tg.sh status

# Follow real-time logs
./tg.sh logs

# Stop the bot
./tg.sh stop

# Restart the bot
./tg.sh restart
```

---

## 💻 Terminal CLI Usage

You can also use the bot to send alerts or upload files directly from your terminal:

```bash
# Send a notification to your Telegram
./tg.sh msg "Build completed successfully!"

# Upload a file and get download & streaming links
./tg.sh upload /path/to/large_file.zip "File caption"
```

---

## 🤖 Telegram Bot Commands

- `/start` — Welcome message and instructions
- `/help` — How to use the bot
- `/id` — View your Telegram User ID & Chat ID
- `/ping` — Check server latency and active public URL
- **Send any file** — Send or forward any document, video, or audio to get download and stream links.

---

## 📄 License
MIT License