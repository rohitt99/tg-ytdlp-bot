
import os
import math
import hashlib
import subprocess
import shutil
import logging
import time
import re
from moviepy.editor import VideoFileClip
from moviepy.video.fx.all import resize
from HELPERS.app_instance import get_app
from HELPERS.logger import logger, send_to_all, send_to_logger
from CONFIG.config import Config
from HELPERS.safe_messeger import safe_forward_messages
from pyrogram import enums

# Get app instance for decorators
app = get_app()

def get_ffmpeg_path():
    """Get FFmpeg path - first try system PATH, then fallback to local binary"""
    import shutil
    
    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    # First try to find ffmpeg in system PATH
    ffmpeg_path = shutil.which('ffmpeg')
    if not ffmpeg_path:
        # Fallback to local binary
        if os.name == 'nt':  # Windows
            ffmpeg_path = os.path.join(project_root, "ffmpeg.exe")
        else:  # Linux/Unix
            ffmpeg_path = os.path.join(project_root, "ffmpeg")
        
        if not os.path.exists(ffmpeg_path):
            logger.error("ffmpeg not found in PATH or project directory. Please install FFmpeg.")
            return None
    
    return ffmpeg_path

def normalize_path_for_ffmpeg(path, for_ffmpeg=True):
    """Normalize path for FFmpeg compatibility across platforms"""
    if os.name == 'nt':  # Windows
        # For Windows, normalize the path first
        normalized = os.path.normpath(path)
        
        # Convert to forward slashes for FFmpeg compatibility
        normalized = normalized.replace('\\', '/')
        
        # Only add quotes if this is for FFmpeg command line
        if for_ffmpeg and (' ' in normalized or any(char in normalized for char in ['(', ')', '[', ']', '{', '}', '&', '|', ';', '"', "'"])):
            # For Windows, use double quotes and escape internal quotes
            escaped_path = normalized.replace('"', '\\"')
            normalized = f'"{escaped_path}"'
        return normalized
    else:  # Linux/Unix
        # For Linux, normalize the path and use absolute path
        normalized = os.path.normpath(path)
        return os.path.abspath(normalized)

def create_safe_filename(original_path, prefix="safe", extension=None):
    """Create a safe filename for cross-platform compatibility"""
    import hashlib
    import time
    import re
    
    # Get original filename and extension
    base_name = os.path.basename(original_path)
    if extension is None:
        name, ext = os.path.splitext(base_name)
    else:
        name = os.path.splitext(base_name)[0]
        ext = extension
    
    # Create safe name using hash and timestamp
    file_hash = hashlib.md5(original_path.encode('utf-8')).hexdigest()[:8]
    timestamp = int(time.time())
    
    # Clean the original name - remove or replace problematic characters
    # Keep only alphanumeric characters, spaces, dots, and common punctuation
    safe_chars = re.sub(r'[^\w\s\-\.\(\)]', '_', name)
    # Limit length to avoid path issues
    safe_chars = safe_chars[:50] if len(safe_chars) > 50 else safe_chars
    
    # Use cleaned name + hash + timestamp for maximum compatibility
    safe_name = f"{prefix}_{safe_chars}_{file_hash}_{timestamp}{ext}"
    
    # Ensure no double underscores
    safe_name = re.sub(r'_+', '_', safe_name)
    
    return safe_name

def test_path_handling():
    """Test function to verify path handling with special characters"""
    # Use universal path format
    test_path = os.path.join("users", "7360853", "Ценам приказано не расти _ Послушаются ли они (Eng.en.srt")
    
    logger.info(f"Testing path: {test_path}")
    logger.info(f"Path exists: {os.path.exists(test_path)}")
    logger.info(f"Platform: {os.name}")
    
    # Test universal path handling
    normalized_path = os.path.normpath(test_path)
    logger.info(f"Normalized path: {normalized_path}")
    
    return True

def get_ytdlp_path():
    """Get yt-dlp binary path - first try system PATH, then fallback to local binary.
    This is used only for functions that need the binary directly (like /cookies_from_browser)"""
    import shutil
    
    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    # First try to find yt-dlp in system PATH
    ytdlp_path = shutil.which('yt-dlp')
    if not ytdlp_path:
        # Fallback to local binary
        if os.name == 'nt':  # Windows
            ytdlp_path = os.path.join(project_root, "yt-dlp.exe")
        else:  # Linux/Unix
            ytdlp_path = os.path.join(project_root, "yt-dlp")
        
        if not os.path.exists(ytdlp_path):
            logger.error("yt-dlp binary not found in PATH or project directory. Please install yt-dlp.")
            return None
    
    return ytdlp_path

def split_video_2(dir, video_name, video_path, video_size, max_size, duration):
    """
    Split a video into multiple parts

    Args:
        dir: Directory path
        video_name: Name for the video
        video_path: Path to the video file
        video_size: Size of the video in bytes
        max_size: Maximum size for each part
        duration: Duration of the video

    Returns:
        dict: Dictionary with video parts information
    """
    rounds = (math.floor(video_size / max_size)) + 1
    n = duration / rounds
    caption_lst = []
    path_lst = []

    try:
        if rounds > 20:
            logger.warning(f"Video will be split into {rounds} parts, which may be excessive")

        for x in range(rounds):
            start_time = float(x * n)
            end_time = float((x * n) + n)

            # Ensure end_time doesn't exceed duration
            end_time = min(end_time, float(duration))

            cap_name = video_name + " - Part " + str(x + 1)
            target_name = os.path.join(dir, cap_name + ".mp4")

            caption_lst.append(cap_name)
            path_lst.append(target_name)

            try:
                # Use progress logging
                logger.info(f"Splitting video part {x+1}/{rounds}: {start_time:.2f}s to {end_time:.2f}s")
                ffmpeg_extract_subclip(video_path, start_time, end_time, targetname=target_name)

                # Verify the split was successful
                if not os.path.exists(target_name) or os.path.getsize(target_name) == 0:
                    logger.error(f"Failed to create split part {x+1}: {target_name}")
                else:
                    logger.info(f"Successfully created split part {x+1}: {target_name} ({os.path.getsize(target_name)} bytes)")

            except Exception as e:
                logger.error(f"Error splitting video part {x+1}: {e}")
                # If a part fails, we continue with the others

        split_vid_dict = {
            "video": caption_lst,
            "path": path_lst
        }

        logger.info(f"Video split into {len(path_lst)} parts successfully")
        return split_vid_dict

    except Exception as e:
        logger.error(f"Error in video splitting process: {e}")
        # Return what we have so far
        split_vid_dict = {
            "video": caption_lst,
            "path": path_lst
        }
        return split_vid_dict


def get_duration_thumb_(dir, video_path, thumb_name):
    # Generate a short unique name for the thumbnail
    thumb_hash = hashlib.md5(thumb_name.encode()).hexdigest()[:10]
    thumb_dir = os.path.abspath(os.path.join(dir, thumb_hash + ".jpg"))
    try:
        width, height, duration = get_video_info_ffprobe(video_path)
        duration = int(duration)
        orig_w = width if width > 0 else 1920
        orig_h = height if height > 0 else 1080
    except Exception as e:
        logger.error(f"[FFPROBE BYPASS] Error while processing video {video_path}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        duration = 0
        orig_w, orig_h = 1920, 1080  # Default dimensions
    
    # Determine optimal thumbnail size based on video aspect ratio
    aspect_ratio = orig_w / orig_h
    max_dimension = 640  # Maximum width or height
    
    if aspect_ratio > 1.5:  # Wide/horizontal video (16:9, etc.)
        thumb_w = max_dimension
        thumb_h = int(max_dimension / aspect_ratio)
    elif aspect_ratio < 0.75:  # Tall/vertical video (9:16, etc.)
        thumb_h = max_dimension
        thumb_w = int(max_dimension * aspect_ratio)
    else:  # Square-ish video (1:1, 4:3, etc.)
        if orig_w >= orig_h:
            thumb_w = max_dimension
            thumb_h = int(max_dimension / aspect_ratio)
        else:
            thumb_h = max_dimension
            thumb_w = int(max_dimension * aspect_ratio)
    
    # Ensure minimum size
    thumb_w = max(thumb_w, 240)
    thumb_h = max(thumb_h, 240)
    
    # Create thumbnail using FFmpeg instead of moviepy
    try:
        # Get FFmpeg path using the common function
        ffmpeg_path = get_ffmpeg_path()
        if not ffmpeg_path:
            logger.error("ffmpeg not found in PATH or project directory.")
            create_default_thumbnail(thumb_dir, thumb_w, thumb_h)
            return duration, thumb_dir
        
        ffmpeg_command = [
            ffmpeg_path,
            "-y",
            "-i", video_path,
            "-ss", "2",         # Seek to 2 Seconds
            "-vframes", "1",    # Capture 1 Frame
            "-vf", f"scale={thumb_w}:{thumb_h}",  # Scale to exact thumbnail size
            thumb_dir
        ]
        subprocess.run(ffmpeg_command, check=True, capture_output=True, encoding='utf-8', errors='replace')
    except Exception as e:
        logger.error(f"Error creating thumbnail with FFmpeg: {e}")
        # Create default thumbnail as fallback
        create_default_thumbnail(thumb_dir, thumb_w, thumb_h)
    
    return duration, thumb_dir

def get_duration_thumb(message, dir_path, video_path, thumb_name):
    """
    Captures a thumbnail at 2 seconds into the video and retrieves video duration.
    Creates thumbnail with same aspect ratio as video (no black bars).

    Args:
        message: The message object
        dir_path: Directory path for the thumbnail
        video_path: Path to the video file
        thumb_name: Name for the thumbnail

    Returns:
        tuple: (duration, thumbnail_path) or None if error
    """
    # Generate a short unique name for the thumbnail
    thumb_hash = hashlib.md5(thumb_name.encode()).hexdigest()[:10]
    thumb_dir = os.path.abspath(os.path.join(dir_path, thumb_hash + ".jpg"))

    # Get FFmpeg path using the common function (we'll use ffmpeg instead of ffprobe)
    ffmpeg_path = get_ffmpeg_path()
    if not ffmpeg_path:
        logger.error("ffmpeg not found in PATH or project directory.")
        send_to_all(message, "❌ FFmpeg not found. Please install FFmpeg.")
        return None
    
    # Check if video file exists first (without quotes)
    if not os.path.exists(video_path):
        logger.error(f"Video file does not exist: {video_path}")
        send_to_all(message, f"❌ Video file not found: {os.path.basename(video_path)}")
        return None
    
    # Use absolute paths without quotes for better compatibility
    abs_video_path = os.path.abspath(video_path)
    abs_thumb_dir = os.path.abspath(thumb_dir)
    
    # For Windows, convert to forward slashes for FFmpeg
    if os.name == 'nt':
        abs_video_path = abs_video_path.replace('\\', '/')
        abs_thumb_dir = abs_thumb_dir.replace('\\', '/')
    
    # Use ffmpeg to get video info instead of ffprobe
    ffmpeg_info_command = [
        ffmpeg_path,
        "-i", abs_video_path,
        "-f", "null", "-"
    ]

    try:

        # Get video info using ffmpeg
        logger.info(f"Running ffmpeg info command for thumbnail: {' '.join(ffmpeg_info_command)}")
        logger.info(f"Original video path: {video_path}")
        logger.info(f"Absolute video path: {abs_video_path}")
        result = subprocess.run(ffmpeg_info_command, capture_output=True, text=True, timeout=int(30), encoding='utf-8', errors='replace')
        output = result.stderr  # ffmpeg outputs info to stderr
        logger.info(f"FFmpeg info return code: {result.returncode}")
        logger.info(f"FFmpeg info output: {output[:300]}...")  # Log first 300 chars
        
        # Extract video dimensions
        orig_w, orig_h = 1920, 1080  # Default dimensions
        stream_match = re.search(r'Stream.*Video.* (\d+)x(\d+)', output)
        if stream_match:
            orig_w, orig_h = map(int, stream_match.groups())
        else:
            logger.warning(f"Could not determine video dimensions, using default: {orig_w}x{orig_h}")
        
        # Extract duration
        duration = 0
        duration_match = re.search(r'Duration: (\d{2}):(\d{2}):(\d{2})\.(\d{2})', output)
        if duration_match:
            hours, minutes, seconds, centiseconds = map(int, duration_match.groups())
            duration = int(hours * 3600 + minutes * 60 + seconds + centiseconds / 100)
        
        # Determine optimal thumbnail size based on video aspect ratio
        aspect_ratio = orig_w / orig_h
        max_dimension = 640  # Maximum width or height
        
        if aspect_ratio > 1.5:  # Wide/horizontal video (16:9, etc.)
            thumb_w = max_dimension
            thumb_h = int(max_dimension / aspect_ratio)
        elif aspect_ratio < 0.75:  # Tall/vertical video (9:16, etc.)
            thumb_h = max_dimension
            thumb_w = int(max_dimension * aspect_ratio)
        else:  # Square-ish video (1:1, 4:3, etc.)
            if orig_w >= orig_h:
                thumb_w = max_dimension
                thumb_h = int(max_dimension / aspect_ratio)
            else:
                thumb_h = max_dimension
                thumb_w = int(max_dimension * aspect_ratio)
        
        # Ensure minimum size
        thumb_w = max(thumb_w, 240)
        thumb_h = max(thumb_h, 240)
        
        # FFMPEG Command to create thumbnail with calculated dimensions
        ffmpeg_command = [
            ffmpeg_path,
            "-y",
            "-i", abs_video_path,
            "-ss", "2",         # Seek to 2 Seconds
            "-vframes", "1",    # Capture 1 Frame
            "-vf", f"scale={thumb_w}:{thumb_h}",  # Scale to exact thumbnail size
            abs_thumb_dir
        ]

        # Run ffmpeg command to create thumbnail
        logger.info(f"Running ffmpeg thumbnail command: {' '.join(ffmpeg_command)}")
        ffmpeg_result = subprocess.run(ffmpeg_command, check=True, capture_output=True, text=True, encoding='utf-8', errors='replace')
        if ffmpeg_result.returncode != 0:
            logger.error(f"Error creating thumbnail: {ffmpeg_result.stderr}")
        else:
            logger.info("Thumbnail created successfully")

        # Verify thumbnail was created
        if not os.path.exists(thumb_dir):
            logger.warning(f"Thumbnail not created at {thumb_dir}, using default")
            # Create a blank thumbnail as fallback
            create_default_thumbnail(thumb_dir, thumb_w, thumb_h)

        return duration, thumb_dir
    except subprocess.CalledProcessError as e:
        logger.error(f"Command execution error: {e.stderr if hasattr(e, 'stderr') else e}")
        send_to_all(message, f"❌ Error processing video: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error processing video: {e}")
        send_to_all(message, f"❌ Error processing video: {e}")
        return None

def create_default_thumbnail(thumb_path, width=480, height=480):
    """Create a default thumbnail when normal thumbnail creation fails"""
    try:
        # Get FFmpeg path using the common function
        ffmpeg_path = get_ffmpeg_path()
        if not ffmpeg_path:
            logger.error("ffmpeg not found in PATH or project directory.")
            return
        
        # Create a black image with specified dimensions (square by default)
        ffmpeg_cmd = [
            ffmpeg_path, "-y",
            "-f", "lavfi",
            "-i", f"color=c=black:s={width}x{height}",
            "-frames:v", "1",
            thumb_path
        ]
        subprocess.run(ffmpeg_cmd, check=True, capture_output=True, encoding='utf-8', errors='replace')
        logger.info(f"Created default {width}x{height} thumbnail at {thumb_path}")
    except Exception as e:
        logger.error(f"Failed to create default thumbnail: {e}")


def ensure_utf8_srt(srt_path):
    """Ensure SRT file is in UTF-8 encoding"""
    try:
        with open(srt_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return srt_path
    except UnicodeDecodeError:
        try:
            # Try to detect encoding and convert to UTF-8
            import chardet
            with open(srt_path, 'rb') as f:
                raw_data = f.read()
                detected = chardet.detect(raw_data)
                encoding = detected['encoding'] or 'cp1252'
            
            with open(srt_path, 'r', encoding=encoding) as f:
                content = f.read()
            
            # Write back as UTF-8
            with open(srt_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return srt_path
        except Exception as e:
            logger.error(f"Error converting SRT to UTF-8: {e}")
            return None


def force_fix_arabic_encoding(srt_path, lang):
    """Fix Arabic subtitle encoding issues"""
    try:
        with open(srt_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Apply Arabic-specific fixes if needed
        if lang in {'ar', 'fa', 'ur', 'ps', 'iw', 'he'}:
            # Add any Arabic-specific text processing here
            pass
        
        with open(srt_path, 'w', encoding='utf-8') as f:
            f.write(content)
        return srt_path
    except Exception as e:
        logger.error(f"Error fixing Arabic encoding: {e}")
        return None


def ffmpeg_extract_subclip(video_path, start_time, end_time, targetname):
    """Extract a subclip from video using FFmpeg"""
    try:
        # Get FFmpeg path using the common function
        ffmpeg_path = get_ffmpeg_path()
        if not ffmpeg_path:
            logger.error("ffmpeg not found in PATH or project directory.")
            return False
        
        # Check if video file exists first (without quotes)
        if not os.path.exists(video_path):
            logger.error(f"Video file not found: {video_path}")
            return False
        
        # Normalize paths for universal compatibility (with quotes for FFmpeg)
        normalized_video_path = normalize_path_for_ffmpeg(video_path, for_ffmpeg=True)
        normalized_targetname = normalize_path_for_ffmpeg(targetname, for_ffmpeg=True)
        
        cmd = [
            ffmpeg_path, '-y',
            '-i', normalized_video_path,
            '-ss', str(start_time),
            '-t', str(end_time - start_time),
            '-c', 'copy',
            normalized_targetname
        ]
        
        logger.info(f"Running ffmpeg extract command: {' '.join(cmd)}")
        logger.info(f"Original video path: {video_path}")
        logger.info(f"Normalized video path: {normalized_video_path}")
        logger.info(f"Original target path: {targetname}")
        logger.info(f"Normalized target path: {normalized_targetname}")
        
        subprocess.run(cmd, check=True, capture_output=True, encoding='utf-8', errors='replace')
        return True
    except Exception as e:
        logger.error(f"Error extracting subclip: {e}")
        return False


# ####################################################################################
# ####################################################################################

def get_video_info_ffprobe(video_path):
    import json
    
    # Get FFmpeg path using the common function
    ffmpeg_path = get_ffmpeg_path()
    if not ffmpeg_path:
        return 0, 0, 0
    
    # Check if video file exists first (without quotes)
    if not os.path.exists(video_path):
        logger.error(f"Video file not found: {video_path}")
        video_dir = os.path.dirname(video_path)
        if video_dir and os.path.exists(video_dir):
            logger.info(f"Directory contents of {video_dir}: {os.listdir(video_dir)}")
            # Try to find the file with different encodings
            try:
                import glob
                video_name = os.path.basename(video_path)
                video_ext = os.path.splitext(video_name)[1]
                video_base = os.path.splitext(video_name)[0]
                # Look for files with similar names
                pattern = os.path.join(video_dir, f"*{video_ext}")
                matching_files = glob.glob(pattern)
                logger.info(f"Files with extension {video_ext} in directory: {matching_files}")
                if matching_files:
                    # Use the first matching file
                    video_path = matching_files[0]
                    logger.info(f"Using alternative video path: {video_path}")
                else:
                    # Try to find any video file in the directory
                    video_patterns = ["*.mp4", "*.mkv", "*.webm", "*.avi"]
                    for pattern in video_patterns:
                        video_files = glob.glob(os.path.join(video_dir, pattern))
                        if video_files:
                            video_path = video_files[0]
                            logger.info(f"Found video file with pattern {pattern}: {video_path}")
                            break
                    else:
                        logger.error("No video files found in directory")
                        return 0, 0, 0
            except Exception as e:
                logger.error(f"Error finding alternative video path: {e}")
                return 0, 0, 0
        else:
            logger.info(f"Directory {video_dir} does not exist")
            return 0, 0, 0
    
    # Now check if the video file exists after potential path correction
    if not os.path.exists(video_path):
        logger.error(f"Video file still not found after path correction: {video_path}")
        return 0, 0, 0
    
    logger.info(f"Video file found and exists: {video_path}")
    
    # Use absolute paths without quotes for better compatibility
    abs_video_path = os.path.abspath(video_path)
    
    # For Windows, convert to forward slashes for FFmpeg
    if os.name == 'nt':
        abs_video_path = abs_video_path.replace('\\', '/')
    
    try:
        # Use ffmpeg to get video info (ffmpeg can do what ffprobe does)
        cmd = [ffmpeg_path, '-i', abs_video_path, '-f', 'null', '-']
        
        logger.info(f"Running ffmpeg info command: {' '.join(cmd)}")
        logger.info(f"Original video path: {video_path}")
        logger.info(f"Absolute video path: {abs_video_path}")
        logger.info(f"Video path exists (original): {os.path.exists(video_path)}")
        logger.info(f"Video path exists (absolute): {os.path.exists(abs_video_path)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=int(60), encoding='utf-8', errors='replace')
        
        logger.info(f"FFmpeg return code: {result.returncode}")
        logger.info(f"FFmpeg stderr output: {result.stderr[:500]}...")  # Log first 500 chars
        
        # Parse the output to extract width, height, and duration
        output = result.stderr  # ffmpeg outputs info to stderr
        
        # Extract duration
        duration = 0
        duration_match = re.search(r'Duration: (\d{2}):(\d{2}):(\d{2})\.(\d{2})', output)
        if duration_match:
            hours, minutes, seconds, centiseconds = map(int, duration_match.groups())
            duration = int(hours * 3600 + minutes * 60 + seconds + centiseconds / 100)
        
        # Extract video stream info
        width, height = 0, 0
        stream_match = re.search(r'Stream.*Video.* (\d+)x(\d+)', output)
        if stream_match:
            width, height = map(int, stream_match.groups())
        
        return width, height, duration
    except subprocess.TimeoutExpired:
        logger.error('ffmpeg timeout - video file may be corrupted or too large')
    except FileNotFoundError:
        logger.error('ffmpeg not found. Please install FFmpeg.')
    except Exception as e:
        logger.error(f'ffmpeg error: {e}')
    return 0, 0, 0


def embed_subs_to_video(video_path, user_id, tg_update_callback=None, app=None, message=None, original_video_path=None):
    """
    Burning (hardcode) subtitles in a video file, if there is any .SRT file and subs.txt
    tg_update_callback (Progress: Float, ETA: StR) - Function for updating the status in Telegram
    original_video_path: Original video path for subtitle search (before renaming)
    """
    try:
        logger.info(f"Starting embed_subs_to_video for user {user_id}, video: {video_path}")
        logger.info(f"Original video path: {original_video_path}")
        logger.info(f"Current working directory: {os.getcwd()}")
        
        if not video_path:
            logger.error("Video path is None or empty")
            return False
        
        # Check if video file exists (without quotes)
        if not os.path.exists(video_path):
            logger.error(f"Video file not found: {video_path}")
            logger.info(f"Current working directory: {os.getcwd()}")
            video_dir = os.path.dirname(video_path)
            if video_dir and os.path.exists(video_dir):
                logger.info(f"Directory contents of {video_dir}: {os.listdir(video_dir)}")
                # Try to find the file with different encodings
                try:
                    import glob
                    video_name = os.path.basename(video_path)
                    video_ext = os.path.splitext(video_name)[1]
                    video_base = os.path.splitext(video_name)[0]
                    # Look for files with similar names
                    pattern = os.path.join(video_dir, f"*{video_ext}")
                    matching_files = glob.glob(pattern)
                    logger.info(f"Files with extension {video_ext} in directory: {matching_files}")
                    if matching_files:
                        # Use the first matching file
                        video_path = matching_files[0]
                        logger.info(f"Using alternative video path: {video_path}")
                    else:
                        # Try to find any video file in the directory
                        video_patterns = ["*.mp4", "*.mkv", "*.webm", "*.avi"]
                        for pattern in video_patterns:
                            video_files = glob.glob(os.path.join(video_dir, pattern))
                            if video_files:
                                video_path = video_files[0]
                                logger.info(f"Found video file with pattern {pattern}: {video_path}")
                                break
                        else:
                            logger.error("No video files found in directory")
                            return False
                except Exception as e:
                    logger.error(f"Error finding alternative video path: {e}")
                    return False
            else:
                logger.info(f"Directory {video_dir} does not exist")
                return False
        
        # Now check if the video file exists after potential path correction
        if not os.path.exists(video_path):
            logger.error(f"Video file still not found after path correction: {video_path}")
            return False
        
        user_dir = os.path.join("users", str(user_id))
        subs_file = os.path.join(user_dir, "subs.txt")
        if not os.path.exists(subs_file):
            logger.info(f"No subs.txt for user {user_id}, skipping embed_subs_to_video")
            return False
        
        with open(subs_file, "r", encoding="utf-8") as f:
            subs_lang = f.read().strip()
        if not subs_lang or subs_lang == "OFF":
            logger.info(f"Subtitles disabled for user {user_id}")
            return False
        
        video_dir = os.path.dirname(video_path)
        
        # We get video parameters via FFPRobe
        width, height, total_time = get_video_info_ffprobe(video_path)
        if width == 0 or height == 0:
            logger.error(f"Unable to determine video resolution via ffprobe: width={width}, height={height}")
            return False
        original_size = os.path.getsize(video_path)

        # Checking the duration of the video
        if total_time and total_time > Config.MAX_SUB_DURATION:
            logger.info(f"Video duration too long for subtitles: {total_time} sec > {Config.MAX_SUB_DURATION} sec limit")
            return False

        # Checking the file size
        original_size_mb = original_size / (1024 * 1024)
        if original_size_mb > Config.MAX_SUB_SIZE:
            logger.info(f"Video file too large for subtitles: {original_size_mb:.2f} MB > {Config.MAX_SUB_SIZE} MB limit")
            return False

        # Video quality testing on the smallest side
        # Log video parameters before checking quality
        logger.info(f"Quality check: width={width}, height={height}, min_side={min(width, height)}, limit={Config.MAX_SUB_QUALITY}")
        if min(width, height) > Config.MAX_SUB_QUALITY:
            logger.info(f"Video quality too high for subtitles: {width}x{height}, min side: {min(width, height)}p > {Config.MAX_SUB_QUALITY}p limit")
            return False

        # --- Enhanced search: look for subtitle files with various extensions and patterns ---
        # Define subtitle extensions first
        subtitle_extensions = ['.srt', '.vtt', '.ass', '.ssa']
        
        # Use original video path for subtitle search if provided
        search_video_path = original_video_path if original_video_path else video_path
        video_base = os.path.splitext(os.path.basename(search_video_path))[0]
        logger.info(f"Looking for subtitles for video: {video_base}")
        logger.info(f"Using search path: {search_video_path}")
        logger.info(f"Video directory: {video_dir}")
        logger.info(f"All files in directory: {os.listdir(video_dir)}")
        
        # Check if we have any subtitle files before searching
        subtitle_files = []
        for ext in subtitle_extensions:
            subtitle_files.extend([f for f in os.listdir(video_dir) if f.lower().endswith(ext)])
        logger.info(f"Found subtitle files before search: {subtitle_files}")
        
        # Search for subtitle files with various patterns
        srt_files = []
        
        # First, look for any subtitle files in the directory
        for ext in subtitle_extensions:
            srt_files.extend([f for f in os.listdir(video_dir) if f.lower().endswith(ext)])
        
        # If no general subtitle files found, look for files matching video name
        if not srt_files:
            for ext in subtitle_extensions:
                # Look for exact match with video name
                exact_match = f"{video_base}{ext}"
                if os.path.exists(os.path.join(video_dir, exact_match)):
                    srt_files.append(exact_match)
                    break
                
                # Look for files containing video name
                for f in os.listdir(video_dir):
                    if f.lower().endswith(ext) and video_base.lower() in f.lower():
                        srt_files.append(f)
                        break
        
        # If still no files found, look for any subtitle files with language codes
        if not srt_files:
            for f in os.listdir(video_dir):
                if any(f.lower().endswith(ext) for ext in subtitle_extensions):
                    srt_files.append(f)
        
        # If still no files found, try to find subtitles by original filename pattern
        # This handles cases where yt-dlp downloads with special characters
        if not srt_files:
            # Get the original filename from the video path
            original_filename = os.path.basename(video_path)
            # Remove extension
            original_base = os.path.splitext(original_filename)[0]
            logger.info(f"Trying to find subtitles for original filename: {original_base}")
            
            # Look for any subtitle files that might match the original pattern
            for f in os.listdir(video_dir):
                if any(f.lower().endswith(ext) for ext in subtitle_extensions):
                    # Check if the subtitle filename contains parts of the original video name
                    # This handles cases where yt-dlp adds special characters
                    if any(part.lower() in f.lower() for part in original_base.split() if len(part) > 2):
                        srt_files.append(f)
                        logger.info(f"Found subtitle by pattern matching: {f}")
                        break
        
        if not srt_files:
            logger.info(f"No subtitle files found in {video_dir}")
            logger.info(f"Available files in directory: {os.listdir(video_dir)}")
            return False
        
        logger.info(f"Found subtitle files: {srt_files}")
        
        # Prefer .srt files, then .vtt, then others
        preferred_extensions = ['.srt', '.vtt', '.ass', '.ssa']
        selected_file = None
        
        for ext in preferred_extensions:
            for file in srt_files:
                if file.lower().endswith(ext):
                    selected_file = file
                    break
            if selected_file:
                break
        
        if not selected_file:
            selected_file = srt_files[0]  # Take the first one if no preferred extension found
        
        subs_path = os.path.join(video_dir, selected_file)
        logger.info(f"Selected subtitle file: {subs_path}")
        
        if not os.path.exists(subs_path):
            logger.error(f"Subtitle file not found: {subs_path}")
            return False
        
        logger.info(f"Subtitle file size: {os.path.getsize(subs_path)} bytes")
        
        # Check subtitle file content
        try:
            with open(subs_path, 'r', encoding='utf-8') as f:
                first_lines = [f.readline().strip() for _ in range(5)]
            logger.info(f"First 5 lines of subtitle file: {first_lines}")
        except Exception as e:
            logger.error(f"Error reading subtitle file content: {e}")

        # Always bring .SRT to UTF-8
        subs_path = ensure_utf8_srt(subs_path)
        if not subs_path or not os.path.exists(subs_path) or os.path.getsize(subs_path) == 0:
            logger.error(f"Subtitle file after ensure_utf8_srt is missing or empty: {subs_path}")
            return False
        
        # Create a safe copy of the subtitle file with universal naming
        safe_filename = create_safe_filename(subs_path, prefix="subs", extension=".srt")
        safe_subs_path = os.path.join(video_dir, safe_filename)
        
        try:
            shutil.copy2(subs_path, safe_subs_path)
            subs_path = safe_subs_path
            logger.info(f"Created safe subtitle copy: {safe_subs_path}")
        except Exception as e:
            logger.error(f"Error creating safe subtitle copy: {e}")
            return False

        # Forcibly correcting Arab cracies
        if subs_lang in {'ar', 'fa', 'ur', 'ps', 'iw', 'he'}:
            subs_path = force_fix_arabic_encoding(subs_path, subs_lang)
        if not subs_path or not os.path.exists(subs_path) or os.path.getsize(subs_path) == 0:
            logger.error(f"Subtitle file after force_fix_arabic_encoding is missing or empty: {subs_path}")
            return False
        
        video_base = os.path.splitext(os.path.basename(video_path))[0]
        output_path = os.path.join(video_dir, f"{video_base}_with_subs_temp.mp4")
        
        # We get the duration of the video via FFmpeg
        def get_duration(path):
            try:
                # Get FFmpeg path using the common function
                ffmpeg_path = get_ffmpeg_path()
                if not ffmpeg_path:
                    logger.error("ffmpeg not found in PATH or project directory.")
                    return None
                
                result = subprocess.run([
                    ffmpeg_path, '-i', path, '-f', 'null', '-'
                ], capture_output=True, text=True, encoding='utf-8', errors='replace')
                
                # Parse the output to extract duration
                output = result.stderr  # ffmpeg outputs info to stderr
                duration_match = re.search(r'Duration: (\d{2}):(\d{2}):(\d{2})\.(\d{2})', output)
                if duration_match:
                    hours, minutes, seconds, centiseconds = map(int, duration_match.groups())
                    return hours * 3600 + minutes * 60 + seconds + centiseconds / 100
            except Exception as e:
                logger.error(f"ffmpeg error: {e}")
            return None
        
        # Get FFmpeg path using the common function
        ffmpeg_path = get_ffmpeg_path()
        if not ffmpeg_path:
            logger.error("ffmpeg not found in PATH or project directory.")
            return False
        
        # Field of subtitles with improved styling
        # Create safe output filename to avoid path issues
        # Use a simple timestamp-based name to avoid any path issues
        import time
        timestamp = int(time.time())
        safe_output_filename = f"video_with_subs_{timestamp}.mp4"
        safe_output_path = os.path.join(video_dir, safe_output_filename)
        
        # Use absolute paths without quotes for better compatibility
        abs_video_path = os.path.abspath(video_path)
        abs_subs_path = os.path.abspath(safe_subs_path)
        abs_output_path = os.path.abspath(safe_output_path)
        
        # For Windows, convert to forward slashes for FFmpeg
        if os.name == 'nt':
            abs_video_path = abs_video_path.replace('\\', '/')
            abs_subs_path = abs_subs_path.replace('\\', '/')
            abs_output_path = abs_output_path.replace('\\', '/')
        
        # AVOID original_size ERROR: Use dynamic SRT reading with original styling
        # Read SRT file and create synchronized drawtext filters with original parameters
        try:
            with open(abs_subs_path, 'r', encoding='utf-8') as f:
                srt_content = f.read()
            
            # Parse SRT and create drawtext filters for each subtitle
            import re
            subtitle_blocks = re.split(r'\n\s*\n', srt_content.strip())
            drawtext_filters = []
            
            # Limit to first 50 subtitles to prevent command line length issues
            max_subtitles = 50
            subtitle_count = 0
            
            for block in subtitle_blocks:
                if subtitle_count >= max_subtitles:
                    logger.warning(f"Limited to {max_subtitles} subtitles to prevent command line length issues")
                    break
                    
                lines = block.strip().split('\n')
                if len(lines) >= 3:
                    # Extract time and text
                    time_line = lines[1]
                    text = ' '.join(lines[2:])
                    
                    # Truncate text if too long (max 100 characters)
                    if len(text) > 100:
                        text = text[:97] + "..."
                    
                    # Parse time (00:00:00,000 --> 00:00:05,000) with better regex
                    time_match = re.match(r'(\d{2}):(\d{2}):(\d{2})[,.](\d{3})\s*-->\s*(\d{2}):(\d{2}):(\d{2})[,.](\d{3})', time_line)
                    if time_match:
                        start_h, start_m, start_s, start_ms = map(int, time_match.groups()[:4])
                        end_h, end_m, end_s, end_ms = map(int, time_match.groups()[4:])
                        
                        start_time = start_h * 3600 + start_m * 60 + start_s + start_ms / 1000
                        end_time = end_h * 3600 + end_m * 60 + end_s + end_ms / 1000
                        
                        # Validate time values - very permissive for longer videos
                        if start_time >= 0 and end_time > start_time and end_time <= 28800:  # Max 8 hours
                            logger.debug(f"Valid subtitle time: {start_time:.2f}s -> {end_time:.2f}s")
                        else:
                            logger.warning(f"Invalid subtitle time: {start_time:.2f}s -> {end_time:.2f}s, skipping")
                            continue
                        
                        # Escape text for drawtext
                        escaped_text = text.replace("'", "\\'").replace('"', '\\"')
                        
                        # Create drawtext filter with BEAUTIFUL styling
                        # Fix encoding issues and split text into multiple lines
                        # Clean problematic characters - more comprehensive
                        text = text.replace(''', "'").replace(''', "'").replace('"', '"').replace('"', '"')
                        text = text.replace('–', '-').replace('—', '-').replace('…', '...')
                        text = text.replace('!', '!').replace('?', '?').replace('(', '(').replace(')', ')')
                        text = text.replace('"', '"').replace('"', '"').replace(''', "'").replace(''', "'")
                        text = text.replace('…', '...').replace('–', '-').replace('—', '-')
                        # Remove any other problematic Unicode characters
                        text = ''.join(char for char in text if ord(char) < 128 or char in 'абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ')
                        
                        # Split text into multiple lines if too long
                        words = text.split()
                        lines = []
                        current_line = ""
                        
                        for word in words:
                            test_line = current_line + (" " + word) if current_line else word
                            if len(test_line) <= 35:  # Even shorter lines for better readability
                                current_line = test_line
                            else:
                                if current_line:
                                    lines.append(current_line.strip())
                                current_line = word
                        
                        if current_line:
                            lines.append(current_line.strip())
                        
                        # Ensure we have at least one line
                        if not lines:
                            lines = [text[:35] + "..." if len(text) > 35 else text]
                        
                        # Create multiline text with \n separator
                        multiline_text = "\\n".join(lines)
                        
                        # Use bold text with proper styling - no fontconfig option needed
                        drawtext_filter = f"drawtext=text='{multiline_text}':fontsize=24:fontcolor=white:box=1:boxcolor=black@0.7:boxborderw=4:shadowcolor=black:shadowx=2:shadowy=2:line_spacing=8:x=(w-text_w)/2:y=h-th-50:enable='between(t,{start_time},{end_time})'"
                        drawtext_filters.append(drawtext_filter)
                        subtitle_count += 1
            
            if drawtext_filters:
                filter_arg = ','.join(drawtext_filters)
                logger.info(f"Using DYNAMIC drawtext filters for {len(drawtext_filters)} subtitles with original styling")
            else:
                logger.warning(f"No valid subtitles found, skipping subtitle embedding")
                return False
                
        except Exception as e:
            logger.error(f"Failed to parse SRT file: {e}")
            logger.warning(f"SRT parsing failed, skipping subtitle embedding")
            return False
        
        logger.info(f"Original subtitle path: {subs_path}")
        logger.info(f"Safe subtitle path: {safe_subs_path}")
        logger.info(f"Absolute subtitle path: {abs_subs_path}")
        logger.info(f"Filter argument: {filter_arg}")
        logger.info(f"Original output path: {output_path}")
        logger.info(f"Safe output path: {safe_output_path}")
        logger.info(f"Absolute output path: {abs_output_path}")
        # Set environment to disable fontconfig errors
        env = os.environ.copy()
        env['FONTCONFIG_PATH'] = '/dev/null'  # Disable fontconfig on Windows
        
        cmd = [
            ffmpeg_path,
            '-y',
            '-i', abs_video_path,
            '-vf', filter_arg,
            '-c:a', 'copy',
            abs_output_path
        ]
        
        # Log the command for debugging
        logger.info(f"FFmpeg command before execution: {cmd}")
        
        logger.info(f"Running ffmpeg command: {' '.join(cmd)}")
        logger.info(f"Subtitle path: {subs_path}")
        logger.info(f"Safe subtitle path: {safe_subs_path}")
        logger.info(f"Absolute subtitle path: {abs_subs_path}")
        logger.info(f"Filter argument: {filter_arg}")
        logger.info(f"Output path: {abs_output_path}")
        
        # Ensure output directory exists
        output_dir = os.path.dirname(abs_output_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
            logger.info(f"Created output directory: {output_dir}")
        
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=env
        )
        progress = 0.0
        last_update = time.time()
        eta = "?"
        time_pattern = re.compile(r'time=([0-9:.]+)')
        
        while True:
            line = proc.stdout.readline()
            if not line:
                break
            logger.info(line.strip())
            match = time_pattern.search(line)
            if match and total_time:
                t = match.group(1)
                # Transform T (hh: mm: ss.xx) in seconds
                h, m, s = 0, 0, 0.0
                parts = t.split(':')
                if len(parts) == 3:
                    h, m, s = int(parts[0]), int(parts[1]), float(parts[2])
                elif len(parts) == 2:
                    m, s = int(parts[0]), float(parts[1])
                elif len(parts) == 1:
                    s = float(parts[0])
                cur_sec = h * 3600 + m * 60 + s
                progress = min(cur_sec / total_time, 1.0)
                # ETA
                if progress > 0:
                    elapsed = time.time() - last_update
                    eta_sec = int((1.0 - progress) * (elapsed / progress)) if progress > 0 else 0
                    eta = f"{eta_sec//60}:{eta_sec%60:02d}"
                # Update every 10 seconds or with a change in progress> 1%
                if tg_update_callback and (time.time() - last_update > 10 or progress >= 1.0):
                    tg_update_callback(progress, eta)
                    last_update = time.time()
        
        proc.wait()
        
        if proc.returncode != 0:
            logger.error(f"FFmpeg error: process exited with code {proc.returncode}")
            if os.path.exists(safe_output_path):
                os.remove(safe_output_path)
            return False
        
        # Check that the file exists and is not empty
        if not os.path.exists(safe_output_path):
            logger.error("Output file does not exist after ffmpeg")
            return False
        
        # We are waiting a little so that the file will definitely complete the recording
        time.sleep(1)
        
        output_size = os.path.getsize(safe_output_path)
        original_size = os.path.getsize(video_path)
        
        if output_size == 0:
            logger.error("Output file is empty")
            if os.path.exists(safe_output_path):
                os.remove(safe_output_path)
            return False
        
        # We check that the final file is not too small (there should be at least 50% of the original)
        if output_size < original_size * 0.5:
            logger.error(f"Output file too small: {output_size} bytes (original: {original_size} bytes)")
            if os.path.exists(safe_output_path):
                os.remove(safe_output_path)
            return False
        
        # Safely replace the file
        backup_path = video_path + ".backup"
        try:
            os.rename(video_path, backup_path)   # Create a backup
            os.rename(safe_output_path, video_path)   # Rename the result
            os.remove(backup_path)               # Delete backup
            
            logger.info(f"Successfully burned-in subtitles")
                    
        except Exception as e:
            logger.error(f"Error replacing video file: {e}")
            # Restore the source file
            if os.path.exists(backup_path):
                os.rename(backup_path, video_path)
            if os.path.exists(safe_output_path):
                os.remove(safe_output_path)
            return False
        
        # Send .SRT to the user before removing
        # Find original subtitle files (not our safe copies)
        original_srt_files = [f for f in os.listdir(video_dir) if f.lower().endswith('.srt') and not f.startswith('subs_')]
        if original_srt_files:
            original_srt_path = os.path.join(video_dir, original_srt_files[0])
            if os.path.exists(original_srt_path):
                try:
                    if app is not None and message is not None:
                        sent_msg = app.send_document(
                            chat_id=user_id,
                            document=original_srt_path,
                            caption="<blockquote>💬 Subtitles SRT-file</blockquote>",
                            reply_to_message_id=message.id,
                            parse_mode=enums.ParseMode.HTML
                        )
                        safe_forward_messages(Config.LOGS_ID, user_id, [sent_msg.id])
                        send_to_logger(message, "💬 Subtitles SRT-file sent to user.") 
                except Exception as e:
                    logger.error(f"Error sending srt file: {e}")
                try:
                    os.remove(original_srt_path)
                except Exception as e:
                    logger.error(f"Error deleting original srt file: {e}")
        
        # Clean up safe subtitle copy
        if os.path.exists(safe_subs_path):
            try:
                os.remove(safe_subs_path)
                logger.info("Safe subtitle copy removed")
            except Exception as e:
                logger.error(f"Error deleting safe srt file: {e}")
        
        logger.info("Successfully burned-in subtitles")
        return True
        
    except Exception as e:
        logger.error(f"Error in embed_subs_to_video: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False
