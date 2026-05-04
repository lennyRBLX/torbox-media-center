from library.http import api_http_client, search_api_http_client, general_http_client, tmdb_http_client, requestWrapper
import httpx
from enum import Enum
import PTN
from library.torbox import TORBOX_API_KEY
from library.app import SCAN_METADATA, TMDB_API_KEY, TMDB_DIAG_ENABLED, EXCLUDE_RESOLUTIONS
from functions.mediaFunctions import constructSeriesTitle, cleanTitle, cleanYear, normaliseTitle, scoreTmdbResult, TMDB_SCORE_THRESHOLD
from rapidfuzz import fuzz
from functions.databaseFunctions import batchUpsertData, getDatabase, getDatabaseLock
import os
import re
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
from tinydb import Query
import time
import threading
from datetime import datetime, timezone

# --- Pre-compiled regexes for searchTMDB ---
_AUTH_SE_PATTERNS = (
    re.compile(r"\bS(\d{1,2})\s*-\s*(\d{1,4})\b", re.IGNORECASE),
    re.compile(r"\bSeason\s+(\d+)\s*-\s*(\d{1,4})\b", re.IGNORECASE),
    re.compile(r"\b(\d+)(?:st|nd|rd|th)\s+Season\s*-\s*(\d{1,4})\b", re.IGNORECASE),
)
_SEP = r"[\s\-#._:\(\)\[\]]{0,5}"
_RE_SEASON = re.compile(rf"\bSeason{_SEP}(\d+)", re.IGNORECASE)
_RE_ORDINAL_SEASON = re.compile(rf"\b(\d+)(?:st|nd|rd|th){_SEP}Season\b", re.IGNORECASE)
_RE_EPISODE = re.compile(rf"\bEpisode{_SEP}(\d+)", re.IGNORECASE)
_RE_S_EP = re.compile(r"\bS(\d+)E", re.IGNORECASE)
_RE_S_STANDALONE = re.compile(r"\bS(\d+)\b", re.IGNORECASE)
_RE_E_FROM_SEP = re.compile(r"\bS\d+E(\d+)", re.IGNORECASE)
_RE_BARE_LEADING_NUM = re.compile(r"^(\d{1,3})\s*[-.\s_]\s*\D")
_RE_TV_INDICATOR = re.compile(
    r"\bS\d+E"
    r"|\bS\d+\b"
    rf"|\bSeason{_SEP}\d+"
    rf"|\b\d+(?:st|nd|rd|th){_SEP}Season\b"
    rf"|\bEpisode{_SEP}\d+"
    rf"|\bPart{_SEP}\d+",
    re.IGNORECASE,
)
_RE_TRAILING_ROMAN = re.compile(r"\s+(?:i{1,3}|iv|v(?:i{0,3})|ix|x(?:i{0,3}))$", re.IGNORECASE)

# --- Pre-compiled regexes for process_file / searchMetadata ---
_RE_EPISODE_ONLY = re.compile(r"^(\d+\s*[-.]|S\d+E)", re.IGNORECASE)
_RE_STARTS_WITH_SE = re.compile(r"^S\d+E\d+", re.IGNORECASE)
_RE_NUMERIC_ONLY = re.compile(r"^\d+$")
SAMPLE_FILE_RE = re.compile(r"(^|[\W_])sample([\W_]|$)", re.IGNORECASE)
RESOLUTION_RE = re.compile(r"(2160|1080|720|480)p", re.IGNORECASE)
FANCUT_OVERRIDES = {
    "4K77": ("Star Wars", 1977),
    "4K80": ("The Empire Strikes Back", 1980),
    "4K83": ("Return of the Jedi", 1983),
}
FANCUT_RE = re.compile(r"\b(4K77|4K80|4K83)\b")
SKIP_SUBFOLDERS = frozenset({"extras", "specials", "op", "ed", "ncop", "nced", "featurettes"})

# --- Diagnostics log file ---
DIAG_LOG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tmdb_diagnostics.log")
_diag_lock = threading.Lock()
_diag_file = None

_cache_hits = 0
_cache_misses = 0
_cache_counter_lock = threading.Lock()

_batch_mode = False
_batch_cache: dict[str, dict] = {}


def _loadKnownStableKeys(download_type: str) -> set[str]:
    db = getDatabase(download_type)
    db_lock = getDatabaseLock(download_type)
    if db is None or db_lock is None:
        return set()
    with db_lock:
        return {r.get("stable_key") for r in db.all() if r.get("stable_key")}


def _enterBatchMode():
    global _batch_mode, _batch_cache
    _batch_cache = {}

    db = getDatabase(METADATA_CACHE_DB_NAME)
    db_lock = getDatabaseLock(METADATA_CACHE_DB_NAME)
    if db and db_lock:
        now = int(time.time())
        with db_lock:
            for record in db.all():
                if (record.get("schema_version") == METADATA_CACHE_SCHEMA_VERSION
                        and record.get("expires_at", 0) > now):
                    _batch_cache[record["cache_key"]] = record
        logging.info(f"Preloaded {len(_batch_cache)} metadata cache entries into memory.")

    _batch_mode = True


def _exitBatchMode():
    global _batch_mode, _batch_cache
    _batch_mode = False

    if _batch_cache:
        db = getDatabase(METADATA_CACHE_DB_NAME)
        db_lock = getDatabaseLock(METADATA_CACHE_DB_NAME)
        if db and db_lock:
            with db_lock:
                db.truncate()
                db.insert_multiple(_batch_cache.values())
            logging.info(f"Flushed {len(_batch_cache)} metadata cache entries to DB.")

    _batch_cache = {}


def _reset_diag_log():
    global _cache_hits, _cache_misses, _diag_file
    with _cache_counter_lock:
        _cache_hits = 0
        _cache_misses = 0
    if _diag_file is not None:
        _diag_file.close()
        _diag_file = None
    if not TMDB_DIAG_ENABLED:
        return
    _diag_file = open(DIAG_LOG_PATH, "w", encoding="utf-8")
    _diag_file.write(f"=== TMDB Diagnostics Log — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} ===\n\n")
    _diag_file.flush()


def _close_diag_log():
    global _diag_file
    if _diag_file is not None:
        _diag_file.close()
        _diag_file = None


def diag(message: str):
    if not TMDB_DIAG_ENABLED:
        return
    print(message)
    with _diag_lock:
        if _diag_file is not None:
            _diag_file.write(message + "\n")


def _bump_cache_hit():
    global _cache_hits
    with _cache_counter_lock:
        _cache_hits += 1


def _bump_cache_miss():
    global _cache_misses
    with _cache_counter_lock:
        _cache_misses += 1

class DownloadType(Enum):
    torrent = "torrents"
    usenet = "usenet"
    webdl = "webdl"

class IDType(Enum):
    torrents = "torrent_id"
    usenet = "usenet_id"
    webdl = "web_id"

ACCEPTABLE_MIME_TYPES = [
    "video/x-matroska",
    "video/mp4",
]

METADATA_CACHE_DB_NAME = "metadata_cache"
METADATA_CACHE_SCHEMA_VERSION = 2
METADATA_CACHE_TTL_SECONDS = 60 * 60 * 24 * 30
METADATA_TRANSIENT_FAILURE_TTL_SECONDS = 60 * 60 * 6
METADATA_PERMANENT_FAILURE_TTL_SECONDS = 60 * 60 * 24 * 7
METADATA_MAX_WORKERS = 15

PREFETCH_WORKERS = 6


def getMetadataCacheKey(download_type: DownloadType, item: dict, file: dict):
    return f"v{METADATA_CACHE_SCHEMA_VERSION}:{download_type.value}:{item.get('id')}:{item.get('hash')}:{file.get('id')}:{file.get('short_name') or file.get('name')}:{file.get('mimetype')}"


def getCachedMetadata(cache_key: str):
    if _batch_mode and cache_key in _batch_cache:
        r = _batch_cache[cache_key]
        return r.get("metadata"), r.get("success", False), r.get("detail", "")

    db = getDatabase(METADATA_CACHE_DB_NAME)
    db_lock = getDatabaseLock(METADATA_CACHE_DB_NAME)

    if db is None or db_lock is None:
        return None

    query = Query()
    with db_lock:
        record = db.get(query.cache_key == cache_key)
        if record is None:
            return None

        now = int(time.time())
        if record.get("schema_version") != METADATA_CACHE_SCHEMA_VERSION or record.get("expires_at", 0) <= now:
            db.remove(query.cache_key == cache_key)
            return None

        return record.get("metadata"), record.get("success", False), record.get("detail", "")

def setCachedMetadata(cache_key: str, metadata: dict, success: bool, detail: str, failure_kind: str | None = None):
    now = int(time.time())
    if success:
        ttl_seconds = METADATA_CACHE_TTL_SECONDS
    elif failure_kind == "permanent":
        ttl_seconds = METADATA_PERMANENT_FAILURE_TTL_SECONDS
    else:
        ttl_seconds = METADATA_TRANSIENT_FAILURE_TTL_SECONDS

    record = {
        "cache_key": cache_key,
        "schema_version": METADATA_CACHE_SCHEMA_VERSION,
        "success": success,
        "failure_kind": failure_kind if not success else None,
        "detail": detail,
        "metadata": metadata,
        "cached_at": now,
        "expires_at": now + ttl_seconds,
    }

    if _batch_mode:
        _batch_cache[cache_key] = record
        return

    db = getDatabase(METADATA_CACHE_DB_NAME)
    db_lock = getDatabaseLock(METADATA_CACHE_DB_NAME)

    if db is None or db_lock is None:
        return

    query = Query()
    with db_lock:
        db.upsert(record, query.cache_key == cache_key)

def pruneExpiredMetadataCache():
    db = getDatabase(METADATA_CACHE_DB_NAME)
    db_lock = getDatabaseLock(METADATA_CACHE_DB_NAME)

    if db is None or db_lock is None:
        return

    now = int(time.time())
    query = Query()

    with db_lock:
        removed = db.remove((query.schema_version != METADATA_CACHE_SCHEMA_VERSION) | (query.expires_at <= now))
        if removed:
            logging.info(f"Pruned {len(removed)} expired metadata cache entries.")


def process_file(item, file, type):
    short_name = file.get("short_name") or file.get("name") or str(file.get("id"))
    mimetype = file.get("mimetype")
    item_name = item.get("name")
    torrent_id = item.get("id")

    if not mimetype or not mimetype.startswith("video/") or mimetype not in ACCEPTABLE_MIME_TYPES:
        logging.debug(f"Skipping file {short_name} with mimetype {mimetype}")
        return None

    file_stem_basename = os.path.splitext(os.path.basename(short_name))[0]
    if SAMPLE_FILE_RE.search(file_stem_basename):
        diag(f"  SKIPPED (sample file, torrent_id={torrent_id}): {short_name}")
        return None

    if EXCLUDE_RESOLUTIONS:
        haystack = f"{short_name} {item_name or ''}"
        m = RESOLUTION_RE.search(haystack)
        if m and f"{m.group(1)}p".lower() in EXCLUDE_RESOLUTIONS:
            diag(f"  SKIPPED (excluded resolution {m.group(1)}p, torrent_id={torrent_id}): {short_name}")
            return None

    data = {
        "item_id": item.get("id"),
        "type": type.value,
        "folder_name": item_name,
        "DEBUG_name": item_name,
        "DEBUG_hash": item.get("hash"),
        "DEBUG_file_name": short_name,
        "folder_hash": item.get("hash"),
        "file_id": file.get("id"),
        "stable_key": f"{item.get('hash')}:{file.get('id')}",
        "file_name": short_name,
        "file_size": file.get("size"),
        "file_mimetype": mimetype,
        "path": file.get("name"),
        "download_link": f"https://api.torbox.app/v1/api/{type.value}/requestdl?token={TORBOX_API_KEY}&{IDType[type.value].value}={item.get('id')}&file_id={file.get('id')}&redirect=true",
        "extension": os.path.splitext(short_name)[-1],
    }

    title_data = PTN.parse(short_name)

    fancut_match = FANCUT_RE.search(short_name)
    if fancut_match:
        fancut_title, fancut_year = FANCUT_OVERRIDES[fancut_match.group(1)]
        title_data["title"] = fancut_title
        title_data["year"] = fancut_year
        if not title_data.get("resolution"):
            title_data["resolution"] = "2160p"

    data["ptn_resolution"] = title_data.get("resolution")
    data["ptn_quality"] = title_data.get("quality")
    data["ptn_codec"] = title_data.get("codec")
    data["ptn_group"] = title_data.get("group")
    data["ptn_part"] = title_data.get("part")

    if item_name == item.get("hash"):
        item_name = title_data.get("title", short_name)
        data["folder_name"] = item_name

    file_stem = os.path.splitext(short_name)[0]
    is_episode_only = bool(_RE_EPISODE_ONLY.match(file_stem))
    has_no_folder_context = item_name == file_stem or item_name == short_name or item_name == item.get("hash")
    if is_episode_only and has_no_folder_context:
        diag(f"  SKIPPED (episode-only, no folder context, torrent_id={torrent_id}): {short_name}")
        return None

    file_path = file.get("name") or ""
    folder_name = os.path.dirname(file_path) if "/" in file_path or "\\" in file_path else ""

    if folder_name:
        path_parts = folder_name.replace("\\", "/").split("/")
        if any(part.strip().lower() in SKIP_SUBFOLDERS for part in path_parts):
            diag(f"  SKIPPED (non-episode subfolder '{folder_name}', torrent_id={torrent_id}): {short_name}")
            return None

    cache_key = getMetadataCacheKey(type, item, file) if SCAN_METADATA else None
    metadata, _, _ = searchMetadata(
        title_data.get("title", short_name),
        title_data,
        short_name,
        f"{item_name} {short_name}",
        item.get("hash"),
        item_name,
        folder_name=folder_name,
        cache_key=cache_key,
        torrent_id=torrent_id,
    )
    data.update(metadata)
    logging.debug(data)
    return data

def getUserDownloads(type: DownloadType):
    offset = 0
    limit = 1000

    file_data = []

    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "bypass_cache": True,
        }
        try:
            response = requestWrapper(api_http_client, "GET", f"/{type.value}/mylist", params=params)
        except Exception as e:
            logging.error(f"Error fetching {type.value} at offset {offset}: {e}")
            return None, False, f"Error fetching {type.value} at offset {offset}: {e}"
        if response.status_code != 200:
            return None, False, f"Error fetching {type.value} at offset {offset}. {response.status_code}"
        try:
            data = response.json().get("data", [])
        except Exception as e:
            logging.error(f"Error parsing {type.value} at offset {offset}: {e}")
            logging.error(f"Response: {response.text}")
            return None, False, f"Error parsing {type.value} at offset {offset}. {e}"
        if not data:
            break
        file_data.extend(data)
        offset += limit
        if len(data) < limit:
            break

    if not file_data:
        return None, True, f"No {type.value} found."

    logging.debug(f"Fetched {len(file_data)} {type.value} items from API.")

    if SCAN_METADATA:
        pruneExpiredMetadataCache()

    known_keys = _loadKnownStableKeys(type.value)
    files_to_process = []
    skipped_items = 0
    skipped_files = 0

    for item in file_data:
        if not item.get("cached", False):
            continue
        item_hash = item.get("hash", "")
        item_files = item.get("files", [])

        def _is_processable(f):
            if f.get("mimetype") not in ACCEPTABLE_MIME_TYPES:
                return False
            sname = f.get("short_name") or f.get("name") or ""
            stem = os.path.splitext(os.path.basename(sname))[0]
            if SAMPLE_FILE_RE.search(stem):
                return False
            if EXCLUDE_RESOLUTIONS:
                m = RESOLUTION_RE.search(f"{sname} {item.get('name') or ''}")
                if m and f"{m.group(1)}p".lower() in EXCLUDE_RESOLUTIONS:
                    return False
            return True

        processable = [f for f in item_files if _is_processable(f)]

        all_known = bool(processable) and all(
            f"{item_hash}:{f.get('id')}" in known_keys for f in processable
        )

        if all_known:
            skipped_items += 1
            skipped_files += len(item_files)
            continue

        for file in item_files:
            files_to_process.append((item, file))

    total_items = sum(1 for i in file_data if i.get("cached", False))
    changed_items = total_items - skipped_items
    logging.info(
        f"Incremental: {skipped_items}/{total_items} items unchanged "
        f"({skipped_files} files skipped), {changed_items} items changed "
        f"({len(files_to_process)} files to process)"
    )

    if not files_to_process:
        cache_empty = False
        if SCAN_METADATA and known_keys:
            cache_db = getDatabase(METADATA_CACHE_DB_NAME)
            cache_lock = getDatabaseLock(METADATA_CACHE_DB_NAME)
            if cache_db and cache_lock:
                with cache_lock:
                    cache_empty = len(cache_db.all()) == 0
        if cache_empty:
            logging.warning(f"All {type.value} items unchanged but metadata cache is empty — forcing full reprocessing.")
            for item in file_data:
                if not item.get("cached", False):
                    continue
                for file in item.get("files", []):
                    files_to_process.append((item, file))
        else:
            logging.info(f"All {type.value} items unchanged, skipping processing.")
            db = getDatabase(type.value)
            db_lock = getDatabaseLock(type.value)
            if db and db_lock:
                with db_lock:
                    existing = db.all()
                return existing, True, f"{type.value.capitalize()} unchanged."
            return [], True, f"{type.value.capitalize()} unchanged."

    files = []

    max_workers = int(multiprocessing.cpu_count() * 2 - 1)
    if SCAN_METADATA:
        max_workers = min(max_workers, METADATA_MAX_WORKERS)

    if SCAN_METADATA:
        _enterBatchMode()

    try:
        if SCAN_METADATA and TMDB_API_KEY:
            logging.info(f"Extracting unique titles from {len(files_to_process)} files...")
            unique_queries = set()
            for item, file in files_to_process:
                item_name = item.get("name", "")
                if item_name == item.get("hash"):
                    short_name = file.get("short_name") or file.get("name") or ""
                    item_name = PTN.parse(short_name).get("title", short_name)
                parsed_title = PTN.parse(item_name).get("title", item_name)
                normalised = normaliseTitle(parsed_title)
                if normalised:
                    unique_queries.add(normalised)

            total_queries = len(unique_queries)
            logging.info(f"Prefetching TMDB searches for {total_queries} unique titles with {PREFETCH_WORKERS} threads...")
            prefetch_ok = 0
            prefetch_fail = 0
            prefetch_start = time.time()
            prefetch_done = 0

            def _prefetch_one(query_with_idx):
                idx, query = query_with_idx
                ok = True
                for search_type in ("tv", "movie"):
                    try:
                        resp = requestWrapper(
                            tmdb_http_client, "GET", f"/search/{search_type}",
                            params={"api_key": TMDB_API_KEY, "query": query},
                        )
                        result_count = len(resp.json().get("results", []))
                        diag(f"  Prefetch [{idx}/{total_queries}] {search_type} '{query}' → {result_count} results")
                    except Exception as e:
                        diag(f"  Prefetch [{idx}/{total_queries}] {search_type} '{query}' → FAILED: {e}")
                        ok = False
                return ok

            with ThreadPoolExecutor(max_workers=PREFETCH_WORKERS) as pf_executor:
                futures = {
                    pf_executor.submit(_prefetch_one, (i, q)): i
                    for i, q in enumerate(unique_queries, 1)
                }
                for future in as_completed(futures):
                    prefetch_done += 1
                    try:
                        if future.result():
                            prefetch_ok += 1
                        else:
                            prefetch_fail += 1
                    except Exception:
                        prefetch_fail += 1

                    if prefetch_done % 25 == 0 or prefetch_done == total_queries:
                        elapsed = time.time() - prefetch_start
                        logging.info(f"Prefetch progress: {prefetch_done}/{total_queries} ({elapsed:.1f}s elapsed)")

            elapsed = time.time() - prefetch_start
            logging.info(
                f"Prefetch complete: {prefetch_ok} ok, {prefetch_fail} failed, "
                f"{total_queries} total in {elapsed:.1f}s"
            )

        total_files = len(files_to_process)
        logging.info(f"Processing {total_files} files with {max_workers} threads...")
        process_start = time.time()
        completed = 0
        skipped = 0
        errors = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(process_file, item, file, type): (item, file)
                for item, file in files_to_process
            }

            for future in as_completed(future_to_file):
                completed += 1
                try:
                    data = future.result()
                    if data:
                        files.append(data)
                    else:
                        skipped += 1
                except Exception as e:
                    errors += 1
                    item, file = future_to_file[future]
                    logging.error(f"Error processing file {file.get('short_name', 'unknown')}: {e}")
                    logging.error(traceback.format_exc())

                if completed % 50 == 0 or completed == total_files:
                    elapsed = time.time() - process_start
                    rate = completed / elapsed if elapsed > 0 else 0
                    logging.info(
                        f"Processing: {completed}/{total_files} "
                        f"({len(files)} matched, {skipped} skipped, {errors} errors) "
                        f"[{elapsed:.1f}s, {rate:.1f} files/s]"
                    )

        if files:
            logging.info(f"Writing {len(files)} records to {type.value} DB...")
            write_start = time.time()
            batchUpsertData(files, type.value, ["stable_key"])
            logging.info(f"Batch wrote {len(files)} records to {type.value} DB in {time.time() - write_start:.1f}s")
    finally:
        if SCAN_METADATA:
            logging.info("Flushing metadata cache...")
            _exitBatchMode()
            _close_diag_log()
            with _cache_counter_lock:
                hits, misses = _cache_hits, _cache_misses
            total = hits + misses
            hit_pct = (hits * 100 // total) if total else 0
            logging.info(f"Metadata cache stats after {type.value}: hits={hits} misses={misses} ({hit_pct}% hit rate)")

    return files, True, f"{type.value.capitalize()} fetched successfully."

def searchTMDB(title: str, title_data: dict, file_name: str, item_name: str = "", folder_name: str = "", torrent_id=None):
    if not TMDB_API_KEY:
        return None

    parsed_year = cleanYear(title_data.get("year"))
    parsed_season = title_data.get("season")
    parsed_episode = title_data.get("episode")
    if isinstance(parsed_season, list):
        parsed_season = parsed_season[0] if parsed_season else None
    if isinstance(parsed_episode, list):
        parsed_episode = parsed_episode[0] if parsed_episode else None
    ptn_season = parsed_season
    ptn_episode = parsed_episode

    override_match_text = None
    for _pattern in _AUTH_SE_PATTERNS:
        _m = _pattern.search(file_name or "")
        if _m:
            override_season = int(_m.group(1))
            override_episode = int(_m.group(2))
            if override_season != parsed_season or override_episode != parsed_episode:
                override_match_text = _m.group(0)
            parsed_season = override_season
            parsed_episode = override_episode
            break

    sources = (file_name, title, folder_name, item_name)

    if parsed_season is None:
        for source in sources:
            if not source:
                continue
            m = (
                _RE_SEASON.search(source)
                or _RE_ORDINAL_SEASON.search(source)
                or _RE_S_EP.search(source)
                or _RE_S_STANDALONE.search(source)
            )
            if m:
                parsed_season = int(m.group(1))
                break

    if parsed_episode is None:
        for source in sources:
            if not source:
                continue
            m = _RE_EPISODE.search(source) or _RE_E_FROM_SEP.search(source)
            if m:
                parsed_episode = int(m.group(1))
                break

    if parsed_episode is None and parsed_season is not None and file_name:
        m = _RE_BARE_LEADING_NUM.match(file_name)
        if m:
            parsed_episode = int(m.group(1))

    has_season_tag = any(_RE_TV_INDICATOR.search(s) for s in sources if s)
    is_tv = parsed_season is not None or parsed_episode is not None or has_season_tag
    extension = os.path.splitext(file_name)[-1]

    search_order = [("tv", "tv"), ("movie", "movie")] if is_tv else [("movie", "movie"), ("tv", "tv")]

    best_result = None
    best_score = -1
    best_media_type = None
    best_breakdown = None
    all_scored: list[dict] = []

    normalised_input = normaliseTitle(title)

    search_query = normalised_input
    if not search_query:
        return None

    search_queries = [search_query]
    stripped = _RE_TRAILING_ROMAN.sub("", search_query)
    if stripped and stripped != search_query:
        search_queries.append(stripped)

    TITLE_DETAIL_THRESHOLD = 90

    for current_query in search_queries:
        if best_score >= TMDB_SCORE_THRESHOLD:
            break

        for search_type, media_type in search_order:
            base_params = {"api_key": TMDB_API_KEY, "query": current_query}
            try:
                response = requestWrapper(tmdb_http_client, "GET", f"/search/{search_type}", params=base_params)
            except Exception as e:
                logging.warning(f"TMDB search error for '{current_query}' ({search_type}): {e}")
                continue

            if response.status_code != 200:
                logging.warning(f"TMDB search returned {response.status_code} for '{current_query}' ({search_type})")
                continue

            candidates = response.json().get("results", [])[:5]

            if parsed_year and candidates:
                for candidate in candidates:
                    candidate_title = candidate.get("title") or candidate.get("name") or ""
                    partial = fuzz.partial_ratio(normalised_input, normaliseTitle(candidate_title))
                    if partial >= TITLE_DETAIL_THRESHOLD and media_type == "tv":
                        tmdb_id = candidate.get("id")
                        try:
                            detail_resp = requestWrapper(tmdb_http_client, "GET", f"/tv/{tmdb_id}", params={"api_key": TMDB_API_KEY})
                            if detail_resp.status_code == 200:
                                detail = detail_resp.json()
                                candidate["last_air_date"] = detail.get("last_air_date")
                                candidate["seasons"] = detail.get("seasons", [])
                        except Exception as e:
                            logging.debug(f"TMDB detail fetch failed for tv/{tmdb_id}: {e}")

            for result in candidates:
                score, breakdown = scoreTmdbResult(title, parsed_year, parsed_season, parsed_episode, result, media_type)
                all_scored.append(breakdown)
                if score > best_score:
                    best_score = score
                    best_result = result
                    best_media_type = media_type
                    best_breakdown = breakdown

            if best_score >= TMDB_SCORE_THRESHOLD:
                break

    # --- Diagnostic output ---
    diag_lines = [
        "",
        f"  TMDB DIAGNOSTICS for file: {file_name}",
        f"  Torrent ID: {torrent_id if torrent_id is not None else '(none)'}",
        f"  Folder name: {folder_name or '(none)'}",
        f"  Torrent name: {item_name or '(none)'}",
        f"  Search title: {title}",
        f"  Normalised input: '{normalised_input}'",
        f"  PTN parsed -> year={parsed_year}  season={parsed_season}  episode={parsed_episode}  is_tv={is_tv}"
        + (f"  [override: PTN had season={ptn_season} episode={ptn_episode}, matched '{override_match_text}' in file name]" if override_match_text else ""),
        f"  Threshold: {TMDB_SCORE_THRESHOLD}   Candidates scored: {len(all_scored)}",
        "  " + "-" * 130,
        f"  {'#':<3} {'Score':<7} {'Title':<7} {'Part':<6} {'Exact':<6} {'Year':<6} {'Type':<6} {'Seas':<6} {'TMDB Title':<35} {'Normalised TMDB':<30} {'Airing':<12} {'Type':<6} {'ID':<10}",
        "  " + "-" * 130,
    ]
    for i, bd in enumerate(sorted(all_scored, key=lambda x: x["total"], reverse=True)):
        marker = " <-- BEST" if bd is best_breakdown else ""
        tied = " [TIED]" if bd is not best_breakdown and bd["total"] == best_score else ""
        yr_start = bd.get("tmdb_year") or "-"
        yr_end = bd.get("tmdb_year_end")
        airing = f"{yr_start}-{yr_end}" if yr_end and yr_end != yr_start else str(yr_start)
        diag_lines.append(
            f"  {i+1:<3} {bd['total']:<7} {bd['title_score']:<7} {bd.get('title_partial', '-'):<6} {bd.get('title_exact', '-'):<6} "
            f"{bd['year_score']:<6} {bd['type_score']:<6} {bd['season_score']:<6} "
            f"{bd['tmdb_title'][:35]:<35} {bd['normalised_tmdb'][:30]:<30} {airing:<12} {bd['media_type']:<6} {bd['tmdb_id']:<10}{marker}{tied}"
        )
    diag_lines.append("  " + "-" * 130)

    SINGLE_RESULT_THRESHOLD = 50
    is_single_result = len(all_scored) == 1
    accepted = best_result is not None and (
        best_score >= TMDB_SCORE_THRESHOLD
        or (is_single_result and best_score >= SINGLE_RESULT_THRESHOLD)
    )

    if not accepted:
        diag_lines.append(f"  RESULT: NO MATCH (best score {best_score} < threshold {TMDB_SCORE_THRESHOLD})")
        diag("\n".join(diag_lines))
        return None

    if is_single_result and best_score < TMDB_SCORE_THRESHOLD:
        diag_lines.append(f"  RESULT: ACCEPTED (single result) '{best_breakdown['tmdb_title']}' (score {best_score} >= single-result threshold {SINGLE_RESULT_THRESHOLD})")
    else:
        diag_lines.append(f"  RESULT: ACCEPTED '{best_breakdown['tmdb_title']}' (score {best_score} >= threshold {TMDB_SCORE_THRESHOLD})")
    diag("\n".join(diag_lines))

    tmdb_title = cleanTitle(best_result.get("title") or best_result.get("name") or title)
    date_str = best_result.get("release_date") or best_result.get("first_air_date") or ""
    tmdb_year = None
    if date_str:
        try:
            tmdb_year = int(date_str[:4])
        except (ValueError, IndexError):
            pass

    poster = best_result.get("poster_path")
    backdrop = best_result.get("backdrop_path")

    metadata = {
        "metadata_title": tmdb_title,
        "metadata_link": f"https://www.themoviedb.org/{best_media_type}/{best_result.get('id')}",
        "metadata_mediatype": "series" if best_media_type == "tv" else "movie",
        "metadata_image": f"https://image.tmdb.org/t/p/w500{poster}" if poster else None,
        "metadata_backdrop": f"https://image.tmdb.org/t/p/w1280{backdrop}" if backdrop else None,
        "metadata_years": tmdb_year,
        "metadata_season": parsed_season,
        "metadata_episode": parsed_episode,
        "metadata_filename": file_name,
        "metadata_rootfoldername": f"{tmdb_title} ({tmdb_year})" if tmdb_year else tmdb_title,
        "metadata_tmdb_score": best_score,
    }

    if best_media_type == "tv":
        series_season_episode = constructSeriesTitle(season=parsed_season, episode=parsed_episode)
        if series_season_episode:
            metadata["metadata_filename"] = f"{tmdb_title} {series_season_episode}{extension}"
        metadata["metadata_foldername"] = constructSeriesTitle(season=parsed_season, folder=True)
    elif best_media_type == "movie" and tmdb_year:
        metadata["metadata_filename"] = f"{tmdb_title} ({tmdb_year}){extension}"

    return metadata

def searchMetadata(query: str, title_data: dict, file_name: str, full_title: str, hash: str, item_name: str, folder_name: str = "", cache_key: str | None = None, torrent_id=None):
    base_metadata = {
        "metadata_title": cleanTitle(query),
        "metadata_link": None,
        "metadata_mediatype": "movie",
        "metadata_image": None,
        "metadata_backdrop": None,
        "metadata_years": None,
        "metadata_season": None,
        "metadata_episode": None,
        "metadata_filename": file_name,
        "metadata_rootfoldername": title_data.get("item_name", None),
    }

    def cacheAndReturn(metadata: dict, success: bool, detail: str, failure_kind: str | None = None):
        if cache_key is not None:
            setCachedMetadata(cache_key, metadata, success, detail, failure_kind=failure_kind)
        return metadata, success, detail

    if not SCAN_METADATA:
        base_metadata["metadata_rootfoldername"] = item_name
        return base_metadata, False, "Metadata scanning is disabled."

    if cache_key is not None:
        cached_result = getCachedMetadata(cache_key)
        if cached_result is not None:
            cached_metadata, cached_success, cached_detail = cached_result
            logging.debug(f"Metadata cache hit for key {cache_key}")
            _bump_cache_hit()
            return cached_metadata, cached_success, f"Metadata cache hit. {cached_detail}"
        _bump_cache_miss()

    folder_title_data = PTN.parse(item_name) if item_name else {}
    folder_query = folder_title_data.get("title", item_name) or query
    file_query = title_data.get("title", "")

    folder_merged_data = {**title_data}
    if folder_title_data.get("year") is not None:
        folder_merged_data["year"] = folder_title_data["year"]

    tmdb_result = searchTMDB(folder_query, folder_merged_data, file_name, item_name=item_name, folder_name=folder_name, torrent_id=torrent_id)
    if tmdb_result is not None:
        base_metadata.update(tmdb_result)
        return cacheAndReturn(base_metadata, True, f"TMDB match via torrent name (score: {tmdb_result.get('metadata_tmdb_score')}). Searching for {folder_query}, item hash: {hash}")

    subfolder_query = ""
    if folder_name:
        last_part = folder_name.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]
        subfolder_title_data = PTN.parse(last_part) if last_part else {}
        subfolder_query = subfolder_title_data.get("title", last_part) or ""
    normalised_subfolder = normaliseTitle(subfolder_query) if subfolder_query else ""
    normalised_folder = normaliseTitle(folder_query)

    if (subfolder_query and normalised_subfolder
            and normalised_subfolder != normalised_folder):
        diag(f"  >> Retrying TMDB with subfolder name: '{subfolder_query}' (normalised: '{normalised_subfolder}', torrent_id={torrent_id})")
        subfolder_merged_data = {**title_data}
        if subfolder_title_data.get("year") is not None:
            subfolder_merged_data["year"] = subfolder_title_data["year"]
        tmdb_result = searchTMDB(subfolder_query, subfolder_merged_data, file_name, item_name=item_name, folder_name=folder_name, torrent_id=torrent_id)
        if tmdb_result is not None:
            base_metadata.update(tmdb_result)
            return cacheAndReturn(base_metadata, True, f"TMDB match via subfolder name (score: {tmdb_result.get('metadata_tmdb_score')}). Searching for {subfolder_query}, item hash: {hash}")

    normalised_file_query = normaliseTitle(file_query) if file_query else ""
    starts_with_episode_tag = bool(_RE_STARTS_WITH_SE.match(file_name))
    is_numeric_only = bool(normalised_file_query and _RE_NUMERIC_ONLY.match(normalised_file_query))
    already_tried = normalised_file_query in (normalised_folder, normalised_subfolder)
    if (file_query and not starts_with_episode_tag and not is_numeric_only
            and not already_tried):
        diag(f"  >> Retrying TMDB with file name: '{file_query}' (normalised: '{normalised_file_query}', torrent_id={torrent_id})")
        tmdb_result = searchTMDB(file_query, title_data, file_name, item_name=item_name, folder_name=folder_name, torrent_id=torrent_id)
        if tmdb_result is not None:
            base_metadata.update(tmdb_result)
            return cacheAndReturn(base_metadata, True, f"TMDB match via file name (score: {tmdb_result.get('metadata_tmdb_score')}). Searching for {file_query}, item hash: {hash}")
    else:
        skip_reasons = []
        if not file_query:
            skip_reasons.append("no file title")
        if starts_with_episode_tag:
            skip_reasons.append("filename starts with S##E##")
        if is_numeric_only:
            skip_reasons.append("normalised file title is numeric-only")
        if already_tried:
            skip_reasons.append(f"same as already-tried query ('{normalised_file_query}')")
        diag(f"  >> File fallback SKIPPED (torrent_id={torrent_id}): {', '.join(skip_reasons) or 'unknown'}")

    diag(f"  >> Falling back to TorBox Search API for: {file_name} (torrent_id={torrent_id})")
    extension = os.path.splitext(file_name)[-1]
    try:
        response = requestWrapper(search_api_http_client, "GET", f"/meta/search/{full_title}", params={"type": "file"})
    except Exception as e:
        logging.error(f"Error searching metadata: {e}")
        return cacheAndReturn(base_metadata, False, f"Error searching metadata: {e}. Searching for {query}, item hash: {hash}", failure_kind="transient")
    if response.status_code != 200:
        logging.error(f"Error searching metadata: {response.status_code}. {response.text}")
        kind = "transient" if response.status_code >= 500 or response.status_code == 429 else "permanent"
        return cacheAndReturn(base_metadata, False, f"Error searching metadata. {response.status_code}. Searching for {query}, item hash: {hash}", failure_kind=kind)
    try:
        data = response.json().get("data", [])[0]

        title = cleanTitle(data.get("title"))
        base_metadata["metadata_title"] = title
        base_metadata["metadata_years"] = cleanYear(title_data.get("year", None) or data.get("releaseYears", None))

        if data.get("type") == "anime" or data.get("type") == "series":
            series_season_episode = constructSeriesTitle(season=title_data.get("season", None), episode=title_data.get("episode", None))
            file_name = f"{title} {series_season_episode}{extension}"
            base_metadata["metadata_foldername"] = constructSeriesTitle(season=title_data.get("season"), folder=True)
            _s = title_data.get("season")
            _e = title_data.get("episode")
            base_metadata["metadata_season"] = _s[0] if isinstance(_s, list) else _s
            base_metadata["metadata_episode"] = _e[0] if isinstance(_e, list) else _e
        elif data.get("type") == "movie":
            file_name = f"{title} ({base_metadata['metadata_years']}){extension}"
        else:
            return cacheAndReturn(base_metadata, False, f"No metadata found. Searching for {query}, item hash: {hash}", failure_kind="permanent")

        base_metadata["metadata_filename"] = file_name
        base_metadata["metadata_mediatype"] = data.get("type")
        base_metadata["metadata_link"] = data.get("link")
        base_metadata["metadata_image"] = data.get("image")
        base_metadata["metadata_backdrop"] = data.get("backdrop")
        base_metadata["metadata_rootfoldername"] = f"{title} ({base_metadata['metadata_years']})"

        return cacheAndReturn(base_metadata, True, f"Metadata found. Searching for {query}, item hash: {hash}")
    except IndexError:
        return cacheAndReturn(base_metadata, False, f"No metadata found. Searching for {query}, item hash: {hash}", failure_kind="permanent")
    except httpx.TimeoutException:
        return cacheAndReturn(base_metadata, False, f"Timeout searching metadata. Searching for {query}, item hash: {hash}")
    except Exception as e:
        logging.error(f"Error searching metadata: {e}")
        logging.error(f"Error searching metadata: {traceback.format_exc()}")
        return cacheAndReturn(base_metadata, False, f"Error searching metadata: {e}. Searching for {query}, item hash: {hash}")

def getDownloadLink(url: str):
    response = requestWrapper(general_http_client, "GET", url)
    if response.status_code == httpx.codes.TEMPORARY_REDIRECT or response.status_code == httpx.codes.PERMANENT_REDIRECT or response.status_code == httpx.codes.FOUND:
        return response.headers.get('Location')
    return url

def downloadFile(url: str, size: int, offset: int = 0):
    headers = {
        "Range": f"bytes={offset}-{offset + size - 1}",
        **general_http_client.headers,
    }
    response = requestWrapper(general_http_client, "GET", url, headers=headers)
    if response.status_code == httpx.codes.OK:
        return response.content
    elif response.status_code == httpx.codes.PARTIAL_CONTENT:
        return response.content
    else:
        logging.error(f"Error downloading file: {response.status_code}")
        raise Exception(f"Error downloading file: {response.status_code}")
