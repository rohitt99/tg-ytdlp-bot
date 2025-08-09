import math
import time
import threading
import os
from typing import Any, Dict, List, Optional

import requests
import firebase_admin
from firebase_admin import credentials, db as admin_db

from CONFIG.config import Config
from HELPERS.logger import logger
from HELPERS.filesystem_hlp import create_directory
from HELPERS.logger import send_to_all

# Global variable for timing
starting_point = []


def _get_database_url() -> str:
    try:
        database_url_local = Config.FIREBASE_CONF.get("databaseURL")
    except Exception:
        database_url_local = None
    if not database_url_local:
        raise RuntimeError("FIREBASE_CONF.databaseURL отсутствует в Config")
    return database_url_local


def _init_firebase_admin_if_needed() -> bool:
    """Initialize firebase_admin with databaseURL from Config.

    Prefers credentials from GOOGLE_APPLICATION_CREDENTIALS or
    Config.FIREBASE_SERVICE_ACCOUNT (path to service account JSON).
    """
    if firebase_admin._apps:
        return True

    database_url = _get_database_url()

    # 1) Explicit path in config
    service_account_path = getattr(Config, "FIREBASE_SERVICE_ACCOUNT", None)
    if service_account_path and os.path.exists(service_account_path):
        cred_obj = credentials.Certificate(service_account_path)
    else:
        # 2) GOOGLE_APPLICATION_CREDENTIALS path present?
        adc_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if adc_path and os.path.exists(adc_path):
            try:
                cred_obj = credentials.Certificate(adc_path)
            except Exception:
                cred_obj = None
        else:
            cred_obj = None

    if cred_obj is None:
        logger.info("ℹ️ firebase_admin credentials not found, will use REST fallback")
        return False

    firebase_admin.initialize_app(cred_obj, {"databaseURL": database_url})
    logger.info("✅ firebase_admin initialized")
    return True


class _SnapshotChild:
    def __init__(self, key: str, value: Any):
        self._key = key
        self._value = value

    def key(self) -> str:
        return self._key

    def val(self) -> Any:
        return self._value


class _SnapshotCompat:
    """Pyrebase-like snapshot wrapper providing .val() and .each()."""

    def __init__(self, value: Any):
        self._value = value

    def val(self) -> Any:
        return self._value

    def each(self) -> List[_SnapshotChild] | None:
        if isinstance(self._value, dict):
            return [_SnapshotChild(k, v) for k, v in self._value.items()]
        return None


class FirebaseDBAdapter:
    """Adapter to mimic Pyrebase's chained .child().get().set() API on top of firebase_admin."""

    def __init__(self, path: str = "/"):
        self._path = path if path.startswith("/") else f"/{path}"

    def child(self, *path_parts: str) -> "FirebaseDBAdapter":
        path = self._path.rstrip("/")
        for part in path_parts:
            part = str(part).strip("/")
            if not part:
                continue
            path = f"{path}/{part}"
        return FirebaseDBAdapter(path)

    def _ref(self):
        return admin_db.reference(self._path)

    def set(self, data: Any) -> None:
        return self._ref().set(data)

    def get(self) -> _SnapshotCompat:
        value = self._ref().get()
        return _SnapshotCompat(value)

    def push(self, data: Any):
        # firebase_admin Reference has push in RTDB; return child key-compatible object
        ref = self._ref().push(data)
        return ref

    def update(self, data: Dict[str, Any]) -> None:
        return self._ref().update(data)

    def remove(self) -> None:
        return self._ref().delete()


class RestDBAdapter:
    """Pyrebase-like adapter using Firebase Realtime Database REST API with idToken."""

    def __init__(self, database_url: str, id_token: str, refresh_token: Optional[str], api_key: str, path: str = "/"):
        self._database_url = database_url.rstrip("/")
        self._id_token = id_token
        self._refresh_token = refresh_token
        self._api_key = api_key
        self._path = path if path.startswith("/") else f"/{path}"
        self._lock = threading.Lock()
        # Start background refresher if possible
        if self._refresh_token:
            thread = threading.Thread(target=self._token_refresher, daemon=True)
            thread.start()

    def _token_refresher(self):
        # Refresh every 50 minutes similar to old logic
        while True:
            time.sleep(3000)
            try:
                url = f"https://securetoken.googleapis.com/v1/token?key={self._api_key}"
                resp = requests.post(url, data={
                    "grant_type": "refresh_token",
                    "refresh_token": self._refresh_token,
                }, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                with self._lock:
                    self._id_token = data.get("id_token", self._id_token)
                    self._refresh_token = data.get("refresh_token", self._refresh_token)
                logger.info("🔁 REST idToken refreshed")
            except Exception as e:
                logger.error(f"❌ REST token refresh error: {e}")

    def _auth_params(self) -> Dict[str, str]:
        with self._lock:
            token = self._id_token
        return {"auth": token}

    def child(self, *path_parts: str) -> "RestDBAdapter":
        path = self._path.rstrip("/")
        for part in path_parts:
            part = str(part).strip("/")
            if not part:
                continue
            path = f"{path}/{part}"
        return RestDBAdapter(self._database_url, self._id_token, self._refresh_token, self._api_key, path)

    def _url(self) -> str:
        return f"{self._database_url}{self._path}.json"

    def set(self, data: Any) -> None:
        r = requests.put(self._url(), params=self._auth_params(), json=data, timeout=60)
        r.raise_for_status()

    def update(self, data: Dict[str, Any]) -> None:
        r = requests.patch(self._url(), params=self._auth_params(), json=data, timeout=60)
        r.raise_for_status()

    def remove(self) -> None:
        r = requests.delete(self._url(), params=self._auth_params(), timeout=60)
        r.raise_for_status()

    def push(self, data: Any):
        # POST to parent path to create unique key
        parent_url = f"{self._database_url}{self._path}.json"
        r = requests.post(parent_url, params=self._auth_params(), json=data, timeout=60)
        r.raise_for_status()
        return r.json()

    def get(self) -> _SnapshotCompat:
        r = requests.get(self._url(), params=self._auth_params(), timeout=60)
        r.raise_for_status()
        return _SnapshotCompat(r.json())


# Initialize db adapter (admin or REST fallback)
use_admin = _init_firebase_admin_if_needed()
if use_admin:
    db = FirebaseDBAdapter("/")
else:
    database_url = _get_database_url()
    api_key = getattr(Config, "FIREBASE_CONF", {}).get("apiKey")
    if not api_key:
        raise RuntimeError("FIREBASE_CONF.apiKey отсутствует — нужен для REST аутентификации")
    # Sign in via REST
    auth_url = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={api_key}"
    resp = requests.post(auth_url, json={
        "email": getattr(Config, "FIREBASE_USER", None),
        "password": getattr(Config, "FIREBASE_PASSWORD", None),
        "returnSecureToken": True,
    }, timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    id_token = payload.get("idToken")
    refresh_token = payload.get("refreshToken")
    if not id_token:
        raise RuntimeError("Не удалось получить idToken через REST аутентификацию")
    logger.info("✅ REST Firebase auth successful")
    db = RestDBAdapter(database_url, id_token, refresh_token, api_key, "/")


def db_child_by_path(db_adapter: FirebaseDBAdapter, path: str) -> FirebaseDBAdapter:
    for part in path.strip("/").split("/"):
        db_adapter = db_adapter.child(part)
    return db_adapter


# Cheking Users are in Main User Directory in DB
def check_user(message):
    user_id_str = str(message.chat.id)

    # Create The User Folder Inside The "Users" Directory
    user_dir = os.path.join("users", user_id_str)
    create_directory(user_dir)

    # Updated path for cookie.txt
    cookie_src = os.path.join(os.getcwd(), "cookies", "cookie.txt")
    cookie_dest = os.path.join(user_dir, os.path.basename(Config.COOKIE_FILE_PATH))

    # Copy Cookie.txt to the User's Folder if Not Already Present
    if os.path.exists(cookie_src) and not os.path.exists(cookie_dest):
        import shutil
        shutil.copy(cookie_src, cookie_dest)

    # Register the User in the Database if Not Already Registered
    user_db = db.child("bot").child("tgytdlp_bot").child("users").get().each()
    users = [user.key() for user in user_db] if user_db else []
    if user_id_str not in users:
        data = {"ID": message.chat.id, "timestamp": math.floor(time.time())}
        db.child("bot").child("tgytdlp_bot").child("users").child(user_id_str).set(data)


# Checking user is Blocked or not
def is_user_blocked(message):
    blocked = db.child("bot").child("tgytdlp_bot").child("blocked_users").get().each()
    blocked_users = [int(b_user.key()) for b_user in blocked] if blocked else []
    if int(message.chat.id) in blocked_users:
        send_to_all(message, "🚫 You are banned from the bot!")
        return True
    else:
        return False


def write_logs(message, video_url, video_title):
    ts = str(math.floor(time.time()))
    data = {"ID": str(message.chat.id), "timestamp": ts,
            "name": message.chat.first_name, "urls": str(video_url), "title": video_title}
    db.child("bot").child("tgytdlp_bot").child("logs").child(str(message.chat.id)).child(str(ts)).set(data)
    logger.info("Log for user added")


# ####################################################################################
# Initialize minimal structure
_format = {"ID": '0', "timestamp": math.floor(time.time())}
try:
    db.child("bot").child("tgytdlp_bot").child("users").child("0").set(_format)
    db.child("bot").child("tgytdlp_bot").child("blocked_users").child("0").set(_format)
    db.child("bot").child("tgytdlp_bot").child("unblocked_users").child("0").set(_format)
    logger.info("db created")
except Exception as e:
    logger.error(f"❌ Error initializing base db structure: {e}")

starting_point.append(time.time())
logger.info("Bot started")
