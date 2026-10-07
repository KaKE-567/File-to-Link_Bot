#!/usr/bin/env bash
#
# Telegram Bot Helper Script
# Bot: File To Link (@Filetolink_kakebot)
# Owner ID: 6558775028

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BOT_SCRIPT="${SCRIPT_DIR}/file_to_link_bot.py"
PID_FILE="${SCRIPT_DIR}/.bot.pid"
LOG_FILE="${SCRIPT_DIR}/bot.log"

# Default Token and Chat ID (can be overridden via .env)
if [ -f "${SCRIPT_DIR}/.env" ]; then
    export $(grep -v '^#' "${SCRIPT_DIR}/.env" | xargs)
fi

BOT_TOKEN="${BOT_TOKEN:-8870014336:AAGVMVJy6CmC7ZooGULxE5whsFv0AgqM0Jg}"
OWNER_ID="${OWNER_ID:-6558775028}"

usage() {
    echo "Usage: $0 {start|stop|restart|status|logs|msg|upload}"
    echo ""
    echo "Commands:"
    echo "  start                 Run bot in background"
    echo "  stop                  Stop the running bot"
    echo "  restart               Restart the bot"
    echo "  status                Check bot running status"
    echo "  logs                  View bot logs in real time"
    echo "  msg <text>            Send text message to Telegram ($OWNER_ID)"
    echo "  upload <file> [cap]   Upload file and get direct link"
    exit 1
}

start_bot() {
    if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        echo "[!] Bot is already running with PID $(cat "$PID_FILE")"
        exit 0
    fi

    echo "[*] Starting bot in the background..."
    setsid python3 -u "$BOT_SCRIPT" > "$LOG_FILE" 2>&1 &
    PID=$(pgrep -f "$BOT_SCRIPT" | tail -n 1)
    echo $PID > "$PID_FILE"
    echo "[✓] Bot started successfully (PID: $PID)"
    echo "[*] Log file: $LOG_FILE"
}

stop_bot() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            echo "[*] Stopping bot (PID: $PID)..."
            kill "$PID"
            rm -f "$PID_FILE"
            echo "[✓] Bot stopped."
        else
            echo "[!] Bot was not running. Cleaned stale PID file."
            rm -f "$PID_FILE"
        fi
    else
        echo "[!] No PID file found. Checking for matching processes..."
        PIDS=$(pgrep -f "file_to_link_bot.py")
        if [ -n "$PIDS" ]; then
            kill $PIDS
            echo "[✓] Bot processes stopped: $PIDS"
        else
            echo "[!] No running bot process found."
        fi
    fi
    pkill -f "cloudflared" 2>/dev/null || true
}

status_bot() {
    if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        echo "[✓] Bot is RUNNING (PID: $(cat "$PID_FILE"))"
    else
        PIDS=$(pgrep -f "file_to_link_bot.py")
        if [ -n "$PIDS" ]; then
            echo "[✓] Bot is running with PID: $PIDS"
        else
            echo "[✗] Bot is NOT RUNNING"
        fi
    fi
}

show_logs() {
    if [ -f "$LOG_FILE" ]; then
        tail -n 50 -f "$LOG_FILE"
    else
        echo "[!] Log file not found yet."
    fi
}

send_msg() {
    if [ -z "$1" ]; then
        echo "[ERROR] Please specify the message to send!"
        echo "Example: $0 msg \"Kernel build completed successfully!\""
        exit 1
    fi
    python3 "$BOT_SCRIPT" --send-msg "$1" --chat "$OWNER_ID"
}

upload_file() {
    if [ -z "$1" ]; then
        echo "[ERROR] Please specify the file path to upload!"
        echo "Example: $0 upload /path/to/file.zip \"File caption\""
        exit 1
    fi
    python3 "$BOT_SCRIPT" --send-file "$1" --caption "$2" --chat "$OWNER_ID"
}

case "$1" in
    start)
        start_bot
        ;;
    stop)
        stop_bot
        ;;
    restart)
        stop_bot
        sleep 1
        start_bot
        ;;
    status)
        status_bot
        ;;
    logs)
        show_logs
        ;;
    msg)
        shift
        send_msg "$*"
        ;;
    upload)
        shift
        upload_file "$1" "$2"
        ;;
    *)
        usage
        ;;
esac
