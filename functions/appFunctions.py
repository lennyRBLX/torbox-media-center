from library.app import RAW_MODE
from functions.torboxFunctions import getUserDownloads, DownloadType, _reset_diag_log
from library.filesystem import MOUNT_METHOD, MOUNT_PATH
from library.app import MOUNT_REFRESH_TIME
from library.torbox import TORBOX_API_KEY
from library.profiling import timer
from functions.databaseFunctions import getAllData, removeStaleData, getDatabase, getDatabaseLock, upsertData
import logging
import os
import threading
from datetime import datetime, timezone
from library.app import getCurrentVersion
import git

log_boot = logging.getLogger("boot")
log_refresh = logging.getLogger("refresh")

refresh_lock = threading.Lock()
REFRESH_STATE_DB = "refresh_state"


def _saveRefreshTimestamp():
    upsertData(
        {"key": "last_refresh", "timestamp": datetime.now(timezone.utc).isoformat()},
        REFRESH_STATE_DB,
        ["key"],
    )


def getSecondsSinceLastRefresh() -> float | None:
    db = getDatabase(REFRESH_STATE_DB)
    db_lock = getDatabaseLock(REFRESH_STATE_DB)
    if db is None or db_lock is None:
        return None
    from tinydb import Query
    with db_lock:
        q = Query()
        results = db.search(q.key == "last_refresh")
    if not results:
        return None
    try:
        ts = datetime.fromisoformat(results[0]["timestamp"])
        return (datetime.now(timezone.utc) - ts).total_seconds()
    except (ValueError, TypeError, KeyError):
        return None

def initializeFolders():
    folders = [MOUNT_PATH]
    if not RAW_MODE:
        folders.extend([
            os.path.join(MOUNT_PATH, "movies"),
            os.path.join(MOUNT_PATH, "series"),
        ])
    for folder in folders:
        os.makedirs(folder, exist_ok=True)

def getAllUserDownloadsFresh():
    _reset_diag_log()
    all_downloads = []
    evicted_records: list[dict] = []
    log_refresh.info("Fetching all user downloads...")
    for download_type in DownloadType:
        log_refresh.debug(f"Fetching {download_type.value} downloads...")
        with timer("getAllUserDownloadsFresh.type", type=download_type.value) as fields:
            downloads, success, detail = getUserDownloads(download_type)
            fields["count"] = len(downloads) if downloads else 0
        if not success:
            log_refresh.error(f"Error fetching {download_type.value}: {detail}")
            continue
        if not downloads:
            log_refresh.info(f"No {download_type.value} downloads found.")
            continue
        valid_keys = {d["stable_key"] for d in downloads if d and "stable_key" in d}
        existing_records, _, _ = getAllData(download_type.value)
        existing_count = len(existing_records) if existing_records else 0
        if existing_count > 0 and len(valid_keys) < existing_count * 0.5:
            log_refresh.warning(
                f"API returned {len(valid_keys)} keys vs {existing_count} DB records "
                f"for {download_type.value} — skipping stale removal (possible partial response)"
            )
            all_downloads.extend(downloads)
            continue
        stale, stale_success, stale_detail = removeStaleData(download_type.value, valid_keys, "stable_key")
        if not stale_success:
            log_refresh.error(f"Error removing stale {download_type.value} data: {stale_detail}")
        elif stale:
            evicted_records.extend(stale)
            log_refresh.info(f"Evicted {len(stale)} stale {download_type.value} records.")
        all_downloads.extend(downloads)
        log_refresh.debug(f"Fetched {len(downloads)} {download_type.value} downloads.")
    return all_downloads, evicted_records

def runRefreshCycle(mount_method: str | None = None, include_mount_sync: bool = False, trigger: str = "scheduled"):
    if mount_method is None:
        mount_method = MOUNT_METHOD

    if not refresh_lock.acquire(blocking=False):
        log_refresh.info(f"Skipping {trigger} refresh — another refresh already running.")
        return False, "Refresh is already running."

    try:
        log_refresh.info(f"Starting {trigger} refresh cycle...")
        with timer("runRefreshCycle", trigger=trigger) as outer:
            with timer("runRefreshCycle.fetchAll") as fetch_fields:
                result = getAllUserDownloadsFresh()
                if result is None:
                    all_downloads, evicted_records = [], []
                else:
                    all_downloads, evicted_records = result
                fetch_fields["count"] = len(all_downloads)
                fetch_fields["evicted"] = len(evicted_records)

            if include_mount_sync:
                with timer("runRefreshCycle.mountSync", method=mount_method):
                    if mount_method == "strm":
                        from functions.stremFilesystemFunctions import runStrm
                        runStrm(evicted_records=evicted_records)
                    elif mount_method == "fuse":
                        from functions.fuseFilesystemFunctions import requestFuseRefresh
                        requestFuseRefresh()

            _saveRefreshTimestamp()
            outer["count"] = len(all_downloads)
        log_refresh.info(f"Completed {trigger} refresh cycle.")
        return True, f"Completed refresh cycle for {len(all_downloads)} downloads."
    except Exception as e:
        log_refresh.exception(f"Error during {trigger} refresh cycle: {e}")
        return False, f"Error during refresh cycle: {e}"
    finally:
        refresh_lock.release()

def getAllUserDownloads():
    all_downloads = []
    for download_type in DownloadType:
        log_refresh.debug(f"Fetching {download_type.value} downloads...")
        downloads, success, detail = getAllData(download_type.value)
        if not success:
            log_refresh.error(f"Error fetching {download_type.value}: {detail}")
            continue
        if not downloads:
            continue
        all_downloads.extend(downloads)
        log_refresh.debug(f"Fetched {len(downloads)} {download_type.value} downloads.")
    return all_downloads

def _checkVersionAsync():
    try:
        latest_version = getLatestVersion()
        current_version = getCurrentVersion()
        if latest_version and latest_version != current_version:
            log_boot.warning(f"New version available: {latest_version}. Running: {current_version}. Consider updating.")
    except Exception as e:
        log_boot.debug(f"Version check failed: {e}")


def bootUp():
    log_boot.debug("Booting up...")
    log_boot.info("Mount method: %s", MOUNT_METHOD)
    log_boot.info("Mount path: %s", MOUNT_PATH)
    log_boot.info("TorBox API Key: %s", TORBOX_API_KEY)
    log_boot.info("Mount refresh time: %s hours", MOUNT_REFRESH_TIME)

    threading.Thread(target=_checkVersionAsync, daemon=True).start()

    initializeFolders()

    return True

def getMountMethod():
    return MOUNT_METHOD

def getMountPath():
    return MOUNT_PATH

def getMountRefreshTime():
    return MOUNT_REFRESH_TIME

def getLatestVersion():
    try:
        url = "https://github.com/torbox-app/torbox-media-center.git"
        g = git.cmd.Git()
        tags_output = g.ls_remote("--tags", url)
        tags = [line.split("refs/tags/")[1] for line in tags_output.splitlines() if "refs/tags/" in line]
        tags = [tag for tag in tags if not tag.endswith("^{}")]
        tags.sort(key=lambda s: list(map(int, s.lstrip('v').split('.'))))
        latest_tag = tags[-1] if tags else None
        return latest_tag
    except Exception as e:
        log_boot.error(f"Error fetching latest version: {e}")
        return None
