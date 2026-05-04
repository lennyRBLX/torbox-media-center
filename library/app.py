import os
from dotenv import load_dotenv
from enum import Enum

load_dotenv()

SCAN_METADATA = os.getenv("ENABLE_METADATA", "false").lower() == "true"
RAW_MODE = os.getenv("RAW_MODE", "false").lower() == "true"
TMDB_API_KEY = os.getenv("TMDB_API_KEY", None)
TMDB_DIAG_ENABLED = os.getenv("TMDB_DIAG_ENABLED", "false").lower() == "true"

class MountRefreshTimes(Enum):
    # times are shown in hours
    slowest = 24 # 24 hours
    very_slow = 12 # 12 hours
    slow = 6 # 6 hours
    normal = 3 # 3 hours
    fast = 2 # 2 hours
    ultra_fast = 1 # 1 hour
    instant = 0.1 # 6 minutes

MOUNT_REFRESH_TIME = os.getenv("MOUNT_REFRESH_TIME", MountRefreshTimes.normal.name)
MOUNT_REFRESH_TIME = MOUNT_REFRESH_TIME.lower()
assert MOUNT_REFRESH_TIME in [e.name for e in MountRefreshTimes], f"Invalid mount refresh time: {MOUNT_REFRESH_TIME}. Valid options are: {[e.name for e in MountRefreshTimes]}"

if MOUNT_REFRESH_TIME == "instant":
    print("!!! Instant mount refresh time may cause rate limiting issues with the API. Use with caution. !!!")

if SCAN_METADATA and RAW_MODE:
    SCAN_METADATA = False
    print("!!! RAW_MODE IS NOT COMPATIBLE WITH METADATA SCANNING. Disabling metadata scanning. !!!")
elif SCAN_METADATA:
    print("!!! Metadata scanning is enabled. This may slow down the processing of files. !!!")

if MOUNT_REFRESH_TIME == "instant" and SCAN_METADATA:
    print("!!! Using instant mount refresh time with metadata scanning may lead to excessive API calls. Falling back to 'fast' refresh time. !!!")
    MOUNT_REFRESH_TIME = MountRefreshTimes.fast.value
else:
    MOUNT_REFRESH_TIME = MountRefreshTimes[MOUNT_REFRESH_TIME].value

ENABLE_MEDIA_FETCH = os.getenv("ENABLE_MEDIA_FETCH", "false").lower() == "true"
ENABLE_WANT_API = os.getenv("ENABLE_WANT_API", "false").lower() == "true"
MEDIA_FETCH_DEBUG = os.getenv("MEDIA_FETCH_DEBUG", "false").lower() == "true"
TBM_TOOLS_URL = os.getenv("TBM_TOOLS_URL", "https://tbm.tools")
AIOSTREAMS_URLS = [u.strip() for u in os.getenv("AIOSTREAMS_URLS", "").split(",") if u.strip()]
TMDB_DISCOVER_INTERVAL = int(os.getenv("TMDB_DISCOVER_INTERVAL", "1"))
DISCOVER_MOVIES_PER_RUN = int(os.getenv("DISCOVER_MOVIES_PER_RUN", "6"))
DISCOVER_SERIES_EPISODES_PER_RUN = int(os.getenv("DISCOVER_SERIES_EPISODES_PER_RUN", "20"))
DISCOVER_ANIME_EPISODES_PER_RUN = int(os.getenv("DISCOVER_ANIME_EPISODES_PER_RUN", "10"))
ENABLE_ANIME_DISCOVER = os.getenv("ENABLE_ANIME_DISCOVER", "false").lower() == "true"
WANT_API_PORT = int(os.getenv("WANT_API_PORT", "9876"))
ACQUISITION_INTERVAL = int(os.getenv("ACQUISITION_INTERVAL", "5"))
ACQUISITION_HOURLY_BUDGET = int(os.getenv("ACQUISITION_HOURLY_BUDGET", "36"))
MIN_DOWNLOAD_SPEED_MBS = float(os.getenv("MIN_DOWNLOAD_SPEED_MBS", "1"))
EXCLUDE_RESOLUTIONS = {
    r.strip().lower()
    for r in os.getenv("EXCLUDE_RESOLUTIONS", "").split(",")
    if r.strip()
}

def getCurrentVersion():
    return "v2.0.0"