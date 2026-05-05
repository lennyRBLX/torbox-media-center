import os
import re
import glob
import shutil
import hashlib
import logging
import threading
from library.app import RAW_MODE
from library.filesystem import MOUNT_PATH
from functions.appFunctions import getAllUserDownloads
from functions.wantFunctions import forceRequeueWanted

log = logging.getLogger("strm")

_runStrm_lock = threading.Lock()
_known_strm_urls: dict[str, str] = {}
_strm_initialized = False

_PART_RE = re.compile(r"(?:cd|part|disc|disk|dvd|pt)\s*0*(\d+)", re.IGNORECASE)
_TMDB_LINK_RE = re.compile(r"themoviedb\.org/(movie|tv)/(\d+)")


def getMountCategory(media_type: str | None):
    if media_type == "movie":
        return "movies"
    if media_type == "series" or media_type == "anime":
        return "series"
    return None


def generateFolderPath(data: dict) -> str | None:
    """
    Takes in a user download and returns the folder path for the download.
    """

    if RAW_MODE:
        original_path = data.get("path")
        if original_path:
            return os.path.dirname(original_path)
        return None
    else:
        root_folder: str | None = data.get("metadata_rootfoldername", None)
        metadata_foldername: str | None = data.get("metadata_foldername", None)
        media_type = data.get("metadata_mediatype")

        if not root_folder:
            return None

        if media_type == "series" or media_type == "anime":
            if not metadata_foldername or data.get("metadata_episode") is None:
                return None
            return os.path.join(
                root_folder,
                metadata_foldername,
            )

        if media_type == "movie":
            return os.path.join(root_folder)

        return None


def writeStremFile(strm_path: str, url: str) -> bool:
    if not url:
        return False
    if _known_strm_urls.get(strm_path) == url:
        return True
    try:
        os.makedirs(os.path.dirname(strm_path), exist_ok=True)
        if os.path.exists(strm_path):
            try:
                with open(strm_path, "r") as existing:
                    if existing.read() == url:
                        _known_strm_urls[strm_path] = url
                        return True
            except OSError:
                pass
        with open(strm_path, "w") as file:
            file.write(url)
        _known_strm_urls[strm_path] = url
        log.debug(f"Wrote strm file: {strm_path}")
        return True
    except FileNotFoundError as e:
        log.error(
            f"Error creating strm file (likely bad naming scheme of file): {e}"
        )
        return False
    except OSError as e:
        log.error(
            f"Error creating strm file (likely bad or missing permissions): {e}"
        )
        return False
    except Exception as e:
        log.error(f"Error creating strm file: {e}")
        return False


def _quality_token(d: dict) -> str | None:
    res = d.get("ptn_resolution")
    if not res:
        return None
    res = str(res)
    return "4K" if res == "2160p" else res


def _quality_source_token(d: dict) -> str | None:
    parts = []
    q = _quality_token(d)
    src = d.get("ptn_quality")
    codec = d.get("ptn_codec")
    if q:
        parts.append(q)
    if src:
        parts.append(str(src))
    if codec:
        parts.append(str(codec))
    return " ".join(parts) if parts else None


def _group_token(d: dict) -> str | None:
    g = d.get("ptn_group")
    return str(g) if g else None


def _part_token(d: dict) -> str | None:
    p = d.get("ptn_part")
    if p:
        return f"cd{p}"
    m = _PART_RE.search(d.get("file_name") or "")
    if m:
        return f"cd{int(m.group(1))}"
    return None


def _short_hash(d: dict) -> str:
    sk = str(d.get("stable_key") or d.get("download_link") or "")
    return hashlib.sha1(sk.encode()).hexdigest()[:6]


def _disambiguate_group(members: list[dict]) -> dict[str, tuple[str, str]]:
    """Return stable_key -> (mode, suffix). mode is 'stack' or 'version'."""
    n = len(members)
    keys = [str(d.get("stable_key")) for d in members]

    parts = [_part_token(d) for d in members]
    if all(parts) and len(set(parts)) == n:
        return {k: ("stack", t) for k, t in zip(keys, parts)}

    for strategy in (_quality_token, _quality_source_token, _group_token):
        toks = [strategy(d) for d in members]
        if all(toks) and len(set(toks)) == n:
            return {k: ("version", str(t)) for k, t in zip(keys, toks)}

    return {k: ("version", _short_hash(d)) for k, d in zip(keys, members)}


def _applySuffix(file_name: str, mode: str, suffix: str) -> str:
    stem, ext = os.path.splitext(file_name)
    if mode == "stack":
        return f"{stem}-{suffix}{ext}"
    return f"{stem} - {suffix}{ext}"


def runStrm(evicted_records: list[dict] | None = None):
    global _strm_initialized
    if not _runStrm_lock.acquire(blocking=False):
        log.info("Skipping runStrm because another run is already in progress.")
        return
    try:
        all_downloads = getAllUserDownloads()

        if not all_downloads:
            log.info(
                "No downloads found in database. Skipping strm sync to avoid deleting existing files."
            )
            return

        if _strm_initialized:
            existing_strm_files = set(_known_strm_urls.keys())
        else:
            existing_strm_files = set(
                glob.glob(os.path.join(MOUNT_PATH, "**", "*.strm"), recursive=True)
            )
            _strm_initialized = True

        # Pass 1: group downloads by base strm_path
        groups: dict[str, list[dict]] = {}
        for download in all_downloads:
            url = download.get("download_link")
            file_name = download.get("metadata_filename")
            if not url or not file_name:
                continue
            folder = generateFolderPath(download)
            if folder is None:
                continue
            if RAW_MODE:
                base_strm = os.path.join(MOUNT_PATH, folder, f"{file_name}.strm")
            else:
                cat = getMountCategory(download.get("metadata_mediatype"))
                if cat is None:
                    continue
                base_strm = os.path.join(
                    MOUNT_PATH, cat, folder, f"{file_name}.strm"
                )
            groups.setdefault(base_strm, []).append(download)

        # Pass 2: resolve collisions, one URL per .strm
        path_to_url: dict[str, str] = {}
        for base_strm, members in groups.items():
            base_dir = os.path.dirname(base_strm)
            if len(members) == 1:
                path_to_url[base_strm] = members[0]["download_link"]
                continue

            if RAW_MODE:
                # RAW_MODE paths should already be unique; if not, fall back to hash.
                for d in members:
                    file_name = d.get("metadata_filename")
                    new_name = _applySuffix(file_name, "version", _short_hash(d))
                    path_to_url[os.path.join(base_dir, f"{new_name}.strm")] = d["download_link"]
                continue

            suffixes = _disambiguate_group(members)
            for d in members:
                sk = str(d.get("stable_key"))
                mode, suffix = suffixes[sk]
                file_name = d.get("metadata_filename")
                new_name = _applySuffix(file_name, mode, suffix)
                path_to_url[os.path.join(base_dir, f"{new_name}.strm")] = d["download_link"]

        new_strm_files = set(path_to_url.keys())
        for strm_path, url in path_to_url.items():
            writeStremFile(strm_path, url)

        # Build set of dirs that any surviving .strm leads through; any dir
        # outside this set is dead and gets rmtree'd (wipes .nfo, posters,
        # subtitles, etc.).
        live_dirs: set[str] = set()
        for p in new_strm_files:
            d = os.path.dirname(p)
            while d and d != MOUNT_PATH:
                if d in live_dirs:
                    break
                live_dirs.add(d)
                d = os.path.dirname(d)

        for strm_file in existing_strm_files - new_strm_files:
            try:
                if os.path.exists(strm_file):
                    os.remove(strm_file)
                _known_strm_urls.pop(strm_file, None)
                log.debug(f"Removed stale .strm file: {strm_file}")
            except OSError as e:
                log.error(f"Error removing .strm file {strm_file}: {e}")
                continue
            d = os.path.dirname(strm_file)
            while d and d != MOUNT_PATH and d not in live_dirs:
                if not os.path.isdir(d):
                    d = os.path.dirname(d)
                    continue
                try:
                    shutil.rmtree(d)
                    log.debug(f"Removed stale folder: {d}")
                except PermissionError as e:
                    log.warning(f"Permission denied removing {d}: {e}")
                    break
                except OSError as e:
                    log.error(f"Error removing folder {d}: {e}")
                    break
                d = os.path.dirname(d)

        _requeueEvicted(evicted_records)

        log.debug(f"Updated {len(path_to_url)} strm files.")
    finally:
        _runStrm_lock.release()


def _requeueEvicted(evicted_records: list[dict] | None):
    if not evicted_records:
        return
    seen: set[tuple[int, int | None]] = set()
    requeued = 0
    for rec in evicted_records:
        link = rec.get("metadata_link") or ""
        m = _TMDB_LINK_RE.search(link)
        if not m:
            log.debug(f"Re-want skip (no TMDB link): {rec.get('metadata_title')}")
            continue
        media_type = "movie" if m.group(1) == "movie" else "series"
        tmdb_id = int(m.group(2))
        season = rec.get("metadata_season")
        if isinstance(season, list):
            season = season[0] if season else None
        season = season if isinstance(season, int) else None
        key = (tmdb_id, season)
        if key in seen:
            continue
        seen.add(key)
        seasons_needed = [season] if season is not None else None
        title = rec.get("metadata_title")
        year_raw = rec.get("metadata_years")
        year = year_raw if isinstance(year_raw, int) else None
        ok, _ = forceRequeueWanted(
            tmdb_id=tmdb_id,
            media_type=media_type,
            title=title,
            year=year,
            seasons_needed=seasons_needed,
        )
        if ok:
            requeued += 1
            log.info(
                f"Re-wanted '{title}' S{season} (TMDB {tmdb_id}) after eviction."
            )
    if requeued:
        log.info(f"Re-queued {requeued} evicted titles for re-acquisition.")
