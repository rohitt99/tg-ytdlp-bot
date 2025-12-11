from urllib.parse import urlparse, parse_qs, urlencode
import re
import requests
from CONFIG.config import Config
from CONFIG.messages import Messages, safe_get_messages
from HELPERS.logger import logger

# Added imports for Spotify download support
import subprocess
import tempfile
import os
import shutil
import glob
import time


def youtube_to_short_url(url: str) -> str:
    """Converts youtube.com/watch?v=... to youtu.be/... while preserving query parameters."""
    parsed = urlparse(url)
    if 'youtube.com' in parsed.netloc and parsed.path == '/watch':
        qs = parse_qs(parsed.query)
        v = qs.get('v', [None])[0]
        if v:
            # Collect query without v
            query = {k: v for k, v in qs.items() if k != 'v'}
            query_str = urlencode(query, doseq=True)
            base = f'https://youtu.be/{v}'
            if query_str:
                return f'{base}?{query_str}'
            return base
    elif 'youtube.com' in parsed.netloc and parsed.path.startswith('/shorts/'):
        # For YouTube Shorts, convert to youtu.be format
        video_id = parsed.path.split('/')[2]  # /shorts/VIDEO_ID
        if video_id:
            return f'https://youtu.be/{video_id}'
    return url


def youtube_to_long_url(url: str) -> str:
    """Converts youtu.be/... to youtube.com/watch?v=... while preserving query parameters."""
    parsed = urlparse(url)
    if 'youtu.be' in parsed.netloc:
        video_id = parsed.path.lstrip('/')
        if video_id:
            qs = parsed.query
            base = f'https://www.youtube.com/watch?v={video_id}'
            if qs:
                return f'{base}&{qs}'
            return base
    elif 'youtube.com' in parsed.netloc and parsed.path.startswith('/shorts/'):
        # For YouTube Shorts, convert to watch format
        video_id = parsed.path.split('/')[2]  # /shorts/VIDEO_ID
        if video_id:
            return f'https://www.youtube.com/watch?v={video_id}'
    return url


def is_youtube_url(url: str) -> bool:
    parsed = urlparse(url)
    return 'youtube.com' in parsed.netloc or 'youtu.be' in parsed.netloc


def extract_youtube_id(url: str, user_id=None) -> str:
    """
    It extracts YouTube Video ID from different link formats.
    """
    patterns = [
        r"youtu\.be/([^?&/]+)",
        r"v=([^?&/]+)",
        r"embed/([^?&/]+)",
        r"youtube\.com/watch\?[^ ]*v=([^?&/]+)"
    ]
    for pat in patterns:
        m = re.search(pat, url)
        if m:
            return m.group(1)
    raise ValueError(safe_get_messages(user_id).YOUTUBE_FAILED_EXTRACT_ID_MSG)


def download_thumbnail(video_id: str, dest: str, url: str = None) -> None:
    """
    Downloads YouTube (Maxresdefault/Hqdefault) to the disk in the original size.
    URL - it is needed to determine Shorts by link (but now it is not used).
    """
    base = f"https://img.youtube.com/vi/{video_id}"
    img_bytes = None
    for name in ("maxresdefault.jpg", "hqdefault.jpg"):
        r = requests.get(f"{base}/{name}", timeout=10)
        if r.status_code == 200 and len(r.content) <= 1024 * 1024:
            with open(dest, "wb") as f:
                f.write(r.content)
            img_bytes = r.content
            break
    if not img_bytes:
        raise RuntimeError(safe_get_messages(user_id).YOUTUBE_FAILED_DOWNLOAD_THUMBNAIL_MSG)
    # We do nothing else - we keep the original size!


def youtube_to_piped_url(url: str) -> str:
    """Преобразует YouTube-ссылку к формату
    https://<Config.PIPED_DOMAIN>/api/video/download?v=<ID>&q=18
    1) youtu.be -> извлечь ID и собрать целевой URL
    2) youtube.com/watch?v= -> извлечь ID и собрать целевой URL
    3) youtube.com/shorts/ID -> привести к watch и собрать целевой URL
    Иные параметры из исходной ссылки игнорируются (по ТЗ).
    """
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        path = parsed.path
        query = parsed.query
        # 1) короткая форма youtu.be/ID
        if 'youtu.be' in domain:
            video_id = path.lstrip('/')
            return f"https://{Config.PIPED_DOMAIN}/api/video/download?v={video_id}&q=18"
        # 2) полная форма
        if 'youtube.com' in domain:
            # shorts -> watch
            if path.startswith('/shorts/'):
                parts = path.split('/')
                if len(parts) >= 3:
                    vid = parts[2]
                    return f"https://{Config.PIPED_DOMAIN}/api/video/download?v={vid}&q=18"
            # watch?v=ID
            qs = parse_qs(query)
            vid = (qs.get('v') or [None])[0]
            if vid:
                return f"https://{Config.PIPED_DOMAIN}/api/video/download?v={vid}&q=18"
        return url
    except Exception:
        return url


# ----------------------------
# Spotify / spotdl integration
# ----------------------------
def is_spotify_url(url: str) -> bool:
    """
    Detects whether the given URL is a Spotify track link or Spotify URI.
    Examples:
      - https://open.spotify.com/track/<id>
      - spotify:track:<id>
    """
    if not url:
        return False
    parsed = urlparse(url)
    if 'spotify.com' in parsed.netloc:
        # could be /track/, /album/, /playlist/ etc. We only handle /track/ (single song)
        if parsed.path.startswith('/track/'):
            return True
        # sometimes users include query params or trailing slashes
        if '/track/' in parsed.path:
            return True
    if url.startswith('spotify:track:'):
        return True
    return False


def _find_downloaded_audio(directory: str):
    """Find most recently modified audio file in directory."""
    audio_exts = ['*.mp3', '*.m4a', '*.flac', '*.ogg', '*.wav', '*.webm', '*.aac']
    candidates = []
    for pattern in audio_exts:
        candidates.extend(glob.glob(os.path.join(directory, pattern)))
    if not candidates:
        return None
    # choose newest
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]


def download_spotify_track(spotify_url: str, dest_dir: str, user_id=None, timeout: int = 300) -> str:
    """
    Downloads a Spotify track using the spotdl CLI into dest_dir.
    Returns full path to downloaded audio file.

    Requirements:
      - spotdl (CLI) must be installed and available in PATH.
      - ffmpeg must be installed (spotdl uses it).
    Behavior:
      - Only proceeds if spotify_url is recognized as a Spotify track link/URI.
      - Uses a temporary directory for spotdl output and moves the resulting file to dest_dir.
      - Raises RuntimeError on failure with an appropriate message from safe_get_messages(user_id)
        if available, otherwise a generic message.
    """
    if not is_spotify_url(spotify_url):
        raise ValueError("URL is not a Spotify track URL")

    # Ensure destination directory exists
    os.makedirs(dest_dir, exist_ok=True)

    tmp_dir = tempfile.mkdtemp(prefix="spotdl_")
    try:
        # Try modern spotdl invocation first, then fallback to legacy form.
        commands_to_try = [
            ['spotdl', 'download', spotify_url, '--output', tmp_dir],
            ['spotdl', spotify_url, '--output', tmp_dir],
            ['spotdl', 'download', spotify_url, '-o', tmp_dir],
            ['spotdl', spotify_url, '-o', tmp_dir],
        ]

        last_exc = None
        success = False
        for cmd in commands_to_try:
            try:
                logger.info(f"Running spotdl command: {' '.join(cmd)}")
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
                if proc.returncode == 0:
                    success = True
                    logger.info("spotdl reported success")
                    break
                else:
                    logger.warning(f"spotdl returned non-zero ({proc.returncode}). stdout: {proc.stdout} stderr: {proc.stderr}")
            except FileNotFoundError as fe:
                # spotdl not installed / not in PATH
                last_exc = fe
                logger.error("spotdl executable not found in PATH")
                break
            except subprocess.TimeoutExpired as te:
                last_exc = te
                logger.error("spotdl command timed out")
                break
            except Exception as e:
                last_exc = e
                logger.exception("Unexpected error running spotdl")

        if not success:
            # If spotdl binary missing, provide a helpful message if available from messages config
            if isinstance(last_exc, FileNotFoundError):
                msg_obj = safe_get_messages(user_id)
                # Use configurable message if present, otherwise fallback
                text = getattr(msg_obj, "SPOTDL_NOT_INSTALLED_MSG", None) or "spotdl is not installed or not found in PATH. Please install spotdl and try again."
                raise RuntimeError(text)
            # generic failure
            msg_obj = safe_get_messages(user_id)
            text = getattr(msg_obj, "SPOTIFY_FAILED_DOWNLOAD_MSG", None) or "Failed to download Spotify track via spotdl."
            raise RuntimeError(text)

        # Give the filesystem a tiny moment to flush files
        time.sleep(0.1)
        found = _find_downloaded_audio(tmp_dir)
        if not found:
            # Possibly spotdl saved into nested subfolder; search recursively
            found = None
            for root, _, files in os.walk(tmp_dir):
                for file in files:
                    if any(file.lower().endswith(ext) for ext in ['.mp3', '.m4a', '.flac', '.ogg', '.wav', '.webm', '.aac']):
                        found = os.path.join(root, file)
                        break
                if found:
                    break

        if not found:
            msg_obj = safe_get_messages(user_id)
            text = getattr(msg_obj, "SPOTIFY_FAILED_FIND_FILE_MSG", None) or "spotdl completed but no audio file was found."
            raise RuntimeError(text)

        # Move found file into dest_dir
        dest_path = os.path.join(dest_dir, os.path.basename(found))
        # Avoid overwriting: if exists, append a timestamp
        if os.path.exists(dest_path):
            base, ext = os.path.splitext(dest_path)
            dest_path = f"{base}_{int(time.time())}{ext}"
        shutil.move(found, dest_path)
        logger.info(f"Moved downloaded Spotify file to {dest_path}")
        return dest_path
    finally:
        # Clean up temporary directory (may still contain other files)
        try:
            shutil.rmtree(tmp_dir)
        except Exception:
            logger.debug("Failed to remove temporary spotdl directory; ignoring.")


# End of file
