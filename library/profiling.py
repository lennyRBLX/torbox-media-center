import logging
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone

from library.app import PROFILE_ENABLED

PROFILE_LOG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "profile.log",
)

log = logging.getLogger("profile")
_profile_lock = threading.Lock()
_file_handler: logging.FileHandler | None = None


def _reset_profile_log():
    global _file_handler
    if _file_handler is not None:
        log.removeHandler(_file_handler)
        _file_handler.close()
        _file_handler = None
    if not PROFILE_ENABLED:
        return
    _file_handler = logging.FileHandler(PROFILE_LOG_PATH, mode="w", encoding="utf-8")
    _file_handler.setFormatter(logging.Formatter("%(message)s"))
    log.addHandler(_file_handler)
    log.setLevel(logging.INFO)
    log.info(
        f"=== Profile Log — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} ==="
    )


def _close_profile_log():
    global _file_handler
    if _file_handler is not None:
        log.removeHandler(_file_handler)
        _file_handler.close()
        _file_handler = None


def profile(message: str):
    if not PROFILE_ENABLED:
        return
    log.info(message)


def _format_fields(fields: dict) -> str:
    if not fields:
        return ""
    parts = []
    for k, v in fields.items():
        if v is None:
            continue
        parts.append(f"{k}={v}")
    return (" " + " ".join(parts)) if parts else ""


@contextmanager
def timer(name: str, **fields):
    if not PROFILE_ENABLED:
        yield {}
        return
    extra: dict = {}
    start = time.perf_counter()
    try:
        yield extra
    finally:
        duration_ms = int((time.perf_counter() - start) * 1000)
        merged = {**fields, **extra}
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        line = f"{ts} step={name}{_format_fields(merged)} duration_ms={duration_ms}"
        with _profile_lock:
            log.info(line)
