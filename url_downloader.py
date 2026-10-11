"""
Social Media & Web Link Video Downloader Module
Extracts media from YouTube, Instagram, TikTok, Facebook, Pinterest, Snapchat,
Twitter/X, Reddit, and direct URLs with a safe 500 MB file size limit.
"""

import os
import re
import sys
import shutil
import asyncio
import tempfile
from typing import Optional, Dict, Tuple
import yt_dlp

MAX_URL_DOWNLOAD_MB = 500
MAX_URL_DOWNLOAD_BYTES = MAX_URL_DOWNLOAD_MB * 1024 * 1024

URL_REGEX = re.compile(r'https?://[^\s<>"]+|www\.[^\s<>"]+')


class _QuietLogger:
    def debug(self, msg): pass
    def info(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): pass


def extract_url_from_text(text: str) -> Optional[str]:
    """Find the first URL in the user message text."""
    if not text:
        return None
    match = URL_REGEX.search(text)
    if match:
        url = match.group(0).rstrip('.,;!?"\')]}')
        if url.startswith("www."):
            url = "https://" + url
        return url
    return None


def is_supported_url(url: str) -> bool:
    """Check if the string looks like an HTTP(S) URL."""
    return bool(url and (url.startswith("http://") or url.startswith("https://")))


async def download_url_media(
    url: str,
    progress_callback=None
) -> Tuple[Optional[str], Optional[dict], Optional[str]]:
    """
    Downloads media from URL into a temporary file.
    Accepts optional progress_callback(current, total, speed, eta).
    Returns: (file_path, metadata_dict, error_message)
    """
    loop = asyncio.get_running_loop()

    def _sync_download():
        temp_dir = tempfile.mkdtemp(prefix="f2l_url_")
        outtmpl = os.path.join(temp_dir, "%(title).80s [%(id)s].%(ext)s")

        def _progress_hook(d):
            if progress_callback and d.get("status") == "downloading":
                total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                current = d.get("downloaded_bytes") or 0
                speed = d.get("speed") or 0
                eta = d.get("eta") or 0
                try:
                    progress_callback(current, total, speed, eta)
                except Exception:
                    pass

        ydl_opts = {
            "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best[ext=mp4]/best",
            "outtmpl": outtmpl,
            "max_filesize": MAX_URL_DOWNLOAD_BYTES,
            "noplaylist": True,
            "quiet": True,
            "no_warnings": True,
            "noprogress": True,
            "merge_output_format": "mp4",
            "socket_timeout": 30,
            "logger": _QuietLogger(),
            "progress_hooks": [_progress_hook] if progress_callback else [],
            "extractor_args": {
                "youtube": {
                    "player_client": ["android", "ios", "mweb"],
                }
            },
            "http_headers": {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            }
        }

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # 1. Probe metadata first to check size
                try:
                    info = ydl.extract_info(url, download=False)
                except Exception:
                    info = None

                if info:
                    filesize = info.get("filesize") or info.get("filesize_approx")
                    if filesize and filesize > MAX_URL_DOWNLOAD_BYTES:
                        shutil.rmtree(temp_dir, ignore_errors=True)
                        size_mb = round(filesize / (1024 * 1024), 1)
                        return None, None, f"Video size ({size_mb} MB) exceeds the {MAX_URL_DOWNLOAD_MB} MB limit for web links."

                # 2. Perform download
                info = ydl.extract_info(url, download=True)
                if not info:
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    return None, None, "Could not extract video from link."

                # Find downloaded file in temp_dir
                files = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if not f.endswith(".part") and not f.endswith(".ytdl")]
                if not files:
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    return None, None, f"File exceeds the {MAX_URL_DOWNLOAD_MB} MB limit or is unavailable."

                target_file = files[0]
                meta = {
                    "title": info.get("title") or os.path.basename(target_file),
                    "duration": info.get("duration"),
                    "filesize": os.path.getsize(target_file),
                    "ext": info.get("ext") or os.path.splitext(target_file)[1].lstrip("."),
                    "temp_dir": temp_dir
                }
                return target_file, meta, None

        except yt_dlp.utils.DownloadError as de:
            shutil.rmtree(temp_dir, ignore_errors=True)
            err_msg = str(de)
            if "larger than max-filesize" in err_msg.lower():
                return None, None, f"Video exceeds the {MAX_URL_DOWNLOAD_MB} MB limit."
            elif "unsupported url" in err_msg.lower():
                return None, None, "Unsupported link or no downloadable video found at this URL."
            elif "sign in" in err_msg.lower() or "login" in err_msg.lower():
                return None, None, "This platform requires account login or is restricted/private."
            elif "404" in err_msg or "not found" in err_msg.lower():
                return None, None, "Video not found or post was deleted."
            clean_err = re.sub(r'^(ERROR:\s*(\[[^\]]+\]\s*)?)+', '', err_msg).strip()
            return None, None, f"Download error: {clean_err[:120]}"
        except Exception as e:
            shutil.rmtree(temp_dir, ignore_errors=True)
            return None, None, f"Error processing link: {str(e)[:120]}"

    return await loop.run_in_executor(None, _sync_download)
