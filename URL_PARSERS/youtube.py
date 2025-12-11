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


def download_thumbnail(video_id: str, dest: str, url: str = None, user_id=None) -> None:
    """
    Downloads YouTube (Maxresdefault/Hqdefault) to the disk in the original size.
    URL - it is needed to determine Shorts by link (but now it is not used).
    """
    base = f"https://img.youtube.com/vi/{video_id}"
    img_bytes = None
    for name in ("maxresdefault.jpg", "hqdefault.jpg"):
        try:
            r = requests.get(f"{base}/{name}", timeout=10)
        except Exception:
            r = None
        if r and r.status_code == 200 and len(r.content) <= 1024 * 1024:
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
    url = url.strip()
    parsed = urlparse(url)
    netloc = (parsed.netloc or "").lower()
    # open.spotify.com or play.spotify.com etc.
    if 'spotify.com' in netloc:
        # Handle typical track path /track/<id>
        if parsed.path.lower().startswith('/track/'):
            return True
        if '/track/' in parsed.path.lower():
            return True
    # spotify URI form
    if url.startswith('spotify:track:'):
        return True
    return False


def _find_downloaded_audio(directory: str):
    """Find most recently modified audio file in directory (recursive)."""
    audio_exts = ['*.mp3', '*.m4a', '*.flac', '*.ogg', '*.wav', '*.webm', '*.aac']
    candidates = []
    for root, _, files in os.walk(directory):
        for file in files:
            lower = file.lower()
            if any(lower.endswith(ext.lstrip('*')) for ext in audio_exts):
                candidates.append(os.path.join(root, file))
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
      - spotdl (CLI) must be installed and available in PATH (or accessible via python3 -m spotdl).
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
        # Use an explicit output template so files land inside tmp_dir with predictable names
        # spotdl accepts templates like '%(title)s.%(ext)s'
        output_template = os.path.join(tmp_dir, "%(title)s.%(ext)s")

        commands_to_try = [
            # modern: spotdl download <url> --output <template>
            ['spotdl', 'download', spotify_url, '--output', output_template],
            ['spotdl', 'download', spotify_url, '-o', output_template],
            # some older versions accept 'spotdl <url> --output ...'
            ['spotdl', spotify_url, '--output', output_template],
            ['spotdl', spotify_url, '-o', output_template],
            # fallback to python -m spotdl forms
            ['python3', '-m', 'spotdl', 'download', spotify_url, '--output', output_template],
            ['python3', '-m', 'spotdl', spotify_url, '--output', output_template],
        ]

        last_exc = None
        proc = None
        success = False
        for cmd in commands_to_try:
            try:
                logger.info(f"Running spotdl command: {' '.join(cmd)}")
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
                logger.debug(f"spotdl exit {proc.returncode}; stdout: {proc.stdout}; stderr: {proc.stderr}")
                if proc.returncode == 0:
                    success = True
                    logger.info("spotdl reported success")
                    break
                else:
                    # continue trying other forms, but remember last non-zero result
                    last_exc = RuntimeError(f"spotdl returned code {proc.returncode}. stderr: {proc.stderr.strip()}")
                    logger.warning(f"spotdl returned non-zero ({proc.returncode}). stdout: {proc.stdout} stderr: {proc.stderr}")
            except FileNotFoundError as fe:
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
            # generic failure with stderr if available
            msg_obj = safe_get_messages(user_id)
            stderr_text = ""
            if proc is not None:
                stderr_text = proc.stderr.strip()
            text = getattr(msg_obj, "SPOTIFY_FAILED_DOWNLOAD_MSG", None) or "Failed to download Spotify track via spotdl."
            if stderr_text:
                text = f"{text} spotdl stderr: {stderr_text}"
            raise RuntimeError(text)

        # Give the filesystem a short moment to flush files and allow spotdl to write nested directories.
        found = None
        # Try a few times for spotdl to finish writing and rename files
        for attempt in range(10):
            found = _find_downloaded_audio(tmp_dir)
            if found:
                break
            # recursive search as well
            for root, _, files in os.walk(tmp_dir):
                for file in files:
                    if any(file.lower().endswith(ext) for ext in ['.mp3', '.m4a', '.flac', '.ogg', '.wav', '.webm', '.aac']):
                        found = os.path.join(root, file)
                        break
                if found:
                    break
            if found:
                break
            time.sleep(0.3)

        if not found:
            msg_obj = safe_get_messages(user_id)
            text = getattr(msg_obj, "SPOTIFY_FAILED_FIND_FILE_MSG", None) or "spotdl completed but no audio file was found."
            # include last stderr if available for debugging
            if proc is not None:
                stderr_text = proc.stderr.strip()
                if stderr_text:
                    text = f"{text} Last spotdl stderr: {stderr_text}"
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
