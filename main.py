from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from functions.appFunctions import bootUp, getMountMethod, getMountRefreshTime, runRefreshCycle, getSecondsSinceLastRefresh
from functions.databaseFunctions import closeAllDatabases
from library.app import ENABLE_MEDIA_FETCH, ENABLE_WANT_API, TMDB_DISCOVER_INTERVAL, ACQUISITION_INTERVAL
from library.profiling import _reset_profile_log, _close_profile_log
from datetime import timedelta
import atexit
import logging
import os
from sys import platform

PID_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".torbox-media-center.pid")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s,%(msecs)03d [%(name)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logging.getLogger("httpx").setLevel(logging.WARNING)

log = logging.getLogger("boot")

def writePidFile():
    try:
        with open(PID_FILE, "w") as pid_file:
            pid_file.write(str(os.getpid()))
    except OSError as e:
        log.warning(f"Unable to write PID file: {e}")

def removePidFile():
    try:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
    except OSError as e:
        log.warning(f"Unable to remove PID file: {e}")

if __name__ == "__main__":
    bootUp()
    mount_method = getMountMethod()

    if mount_method == "strm":
        scheduler = BlockingScheduler()
    elif mount_method == "fuse":
        if platform == "win32":
            log.error("FUSE mount method not supported on Windows. Use STRM or run on Linux.")
            exit(1)
        scheduler = BackgroundScheduler()
    else:
        log.error("Invalid mount method specified.")
        exit(1)

    writePidFile()
    atexit.register(removePidFile)

    if ENABLE_WANT_API:
        from functions.apiServer import startApiServer
        startApiServer()

    _reset_profile_log()
    atexit.register(_close_profile_log)

    refresh_interval_hours = getMountRefreshTime()
    refresh_interval_secs = refresh_interval_hours * 3600
    elapsed = getSecondsSinceLastRefresh()

    if elapsed is None:
        log.info("No previous refresh recorded, running now.")
        runRefreshCycle(mount_method=mount_method, include_mount_sync=True, trigger="startup")
        initial_delay_secs = refresh_interval_secs
    elif elapsed >= refresh_interval_secs:
        log.info(f"Last refresh {elapsed / 3600:.1f}h ago (>= {refresh_interval_hours}h), running now.")
        runRefreshCycle(mount_method=mount_method, include_mount_sync=True, trigger="startup")
        initial_delay_secs = refresh_interval_secs
    else:
        remaining = refresh_interval_secs - elapsed
        log.info(f"Last refresh {elapsed / 60:.0f}m ago, next in {remaining / 60:.0f}m.")
        initial_delay_secs = remaining

    if ENABLE_MEDIA_FETCH:
        from functions.discoveryFunctions import runDiscovery
        from functions.acquisitionFunctions import runAcquisition

        runDiscovery()

        scheduler.add_job(
            runDiscovery,
            "interval",
            hours=TMDB_DISCOVER_INTERVAL,
            id="run_discovery",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
        )
        scheduler.add_job(
            runAcquisition,
            "interval",
            minutes=ACQUISITION_INTERVAL,
            id="run_acquisition",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )

    from datetime import datetime, timezone
    next_refresh = datetime.now(timezone.utc) + timedelta(seconds=initial_delay_secs)
    scheduler.add_job(
        runRefreshCycle,
        "interval",
        hours=refresh_interval_hours,
        next_run_time=next_refresh,
        kwargs={
            "mount_method": mount_method,
            "include_mount_sync": True,
            "trigger": "scheduled",
        },
        id="get_all_user_downloads_fresh",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30,
    )

    try:
        log.info("Starting scheduler and mounting...")
        if mount_method == "strm":
            from functions.stremFilesystemFunctions import runStrm
            runStrm()
            scheduler.start()
        elif mount_method == "fuse":
            from functions.fuseFilesystemFunctions import runFuse
            scheduler.start()
            runFuse()
    except (KeyboardInterrupt, SystemExit):
        if mount_method == "fuse":
            from functions.fuseFilesystemFunctions import unmountFuse
            unmountFuse()
        closeAllDatabases()
        exit(0)
