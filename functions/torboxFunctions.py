from library.http import api_http_client, search_api_http_client, general_http_client, tmdb_http_client, requestWrapper
import httpx
from enum import Enum
import PTN
from library.torbox import TORBOX_API_KEY
from library.app import SCAN_METADATA, TMDB_API_KEY, TMDB_DIAG_ENABLED
from functions.mediaFunctions import constructSeriesTitle, cleanTitle, cleanYear, normaliseTitle, scoreTmdbResult, TMDB_SCORE_THRESHOLD
from rapidfuzz import fuzz
from functions.databaseFunctions import upsertData, getDatabase, getDatabaseLock
import os
import re
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
from tinydb import Query
import hashlib
import json
import time
import threading
from datetime import datetime, timezone

# --- Diagnostics log file ---
DIAG_LOG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tmdb_diagnostics.log")
_diag_lock = threading.Lock()

# Per-refresh cache hit/miss counters (reset by _reset_diag_log)
_cache_hits = 0
_cache_misses = 0
_cache_counter_lock = threading.Lock()


def _reset_diag_log():
    """Clear the diagnostics log at the start of each refresh cycle."""
    global _cache_hits, _cache_misses
    with _cache_counter_lock:
        _cache_hits = 0
        _cache_misses = 0
    if not TMDB_DIAG_ENABLED:
        return
    with _diag_lock:
        with open(DIAG_LOG_PATH, "w", encoding="utf-8") as f:
            f.write(f"=== TMDB Diagnostics Log — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} ===\n\n")


def diag(message: str):
    """Write a line to stdout and the diagnostics log file. No-op when TMDB_DIAG_ENABLED is false."""
    if not TMDB_DIAG_ENABLED:
        return
    print(message)
    with _diag_lock:
        with open(DIAG_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(message + "\n")


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
METADATA_CACHE_TTL_SECONDS = 60 * 60 * 24 * 30  # 30 days
METADATA_FAILURE_CACHE_TTL_SECONDS = 60 * 60 * 6  # 6 hours
METADATA_MAX_WORKERS = 15

def getMetadataCacheKey(download_type: DownloadType, item: dict, file: dict):
    cache_key_data = {
        "schema_version": METADATA_CACHE_SCHEMA_VERSION,
        "download_type": download_type.value,
        "item_id": item.get("id"),
        "item_hash": item.get("hash"),
        "file_id": file.get("id"),
        "file_name": file.get("short_name") or file.get("name"),
        "file_path": file.get("name"),
        "file_mimetype": file.get("mimetype"),
    }

    return hashlib.sha256(json.dumps(cache_key_data, sort_keys=True, default=str).encode()).hexdigest()

def getCachedMetadata(cache_key: str):
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

def setCachedMetadata(cache_key: str, metadata: dict, success: bool, detail: str):
    db = getDatabase(METADATA_CACHE_DB_NAME)
    db_lock = getDatabaseLock(METADATA_CACHE_DB_NAME)

    if db is None or db_lock is None:
        return

    now = int(time.time())
    ttl_seconds = METADATA_CACHE_TTL_SECONDS if success else METADATA_FAILURE_CACHE_TTL_SECONDS

    record = {
        "cache_key": cache_key,
        "schema_version": METADATA_CACHE_SCHEMA_VERSION,
        "success": success,
        "detail": detail,
        "metadata": metadata,
        "cached_at": now,
        "expires_at": now + ttl_seconds,
    }

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

SAMPLE_FILE_RE = re.compile(r"(^|[\W_])sample([\W_]|$)", re.IGNORECASE)


def process_file(item, file, type):
    """Process a single file and return the processed data"""
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

    if item_name == item.get("hash"):
        item_name = title_data.get("title", short_name)
        data["folder_name"] = item_name

    # Skip episode-only files with no folder/torrent context to identify the show.
    # e.g. "226 - Wizard of Odd.mkv" or "S01E04. Golem.mkv" uploaded as single files.
    file_stem = os.path.splitext(short_name)[0]
    is_episode_only = bool(re.match(r"^(\d+\s*[-.]|S\d+E)", file_stem, re.IGNORECASE))
    has_no_folder_context = item_name == file_stem or item_name == short_name or item_name == item.get("hash")
    if is_episode_only and has_no_folder_context:
        diag(f"  SKIPPED (episode-only, no folder context, torrent_id={torrent_id}): {short_name}")
        return None

    # Extract the folder name from the file's path within the torrent.
    # e.g. "Season 1/Episode 01.mkv" -> "Season 1"
    file_path = file.get("name") or ""
    folder_name = os.path.dirname(file_path) if "/" in file_path or "\\" in file_path else ""

    # Skip files inside non-episode subfolders (extras, specials, OPs, EDs, etc.)
    SKIP_SUBFOLDERS = {"extras", "specials", "op", "ed", "ncop", "nced", "featurettes"}
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
    upsertData(data, type.value, ["stable_key"])
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
            response = api_http_client.get(f"/{type.value}/mylist", params=params)
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

    files = []

    # Get the number of CPU cores for parallel processing
    max_workers = int(multiprocessing.cpu_count() * 2 - 1)
    if SCAN_METADATA:
        max_workers = min(max_workers, METADATA_MAX_WORKERS)
    logging.info(f"Processing files with {max_workers} parallel threads")

    # Collect all files to process
    files_to_process = []
    for item in file_data:
        if not item.get("cached", False):
            continue
        for file in item.get("files", []):
            files_to_process.append((item, file))

    # Process files in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_file = {
            executor.submit(process_file, item, file, type): (item, file)
            for item, file in files_to_process
        }

        # Collect results as they complete
        for future in as_completed(future_to_file):
            try:
                data = future.result()
                if data:
                    files.append(data)
            except Exception as e:
                item, file = future_to_file[future]
                logging.error(f"Error processing file {file.get('short_name', 'unknown')}: {e}")
                logging.error(traceback.format_exc())

    if SCAN_METADATA:
        with _cache_counter_lock:
            hits, misses = _cache_hits, _cache_misses
        total = hits + misses
        hit_pct = (hits * 100 // total) if total else 0
        logging.info(f"Metadata cache stats after {type.value}: hits={hits} misses={misses} ({hit_pct}% hit rate)")

    return files, True, f"{type.value.capitalize()} fetched successfully."

def searchTMDB(title: str, title_data: dict, file_name: str, item_name: str = "", folder_name: str = "", torrent_id=None):
    """
    Searches TMDB for metadata, scores results, and returns the best match
    if it exceeds the confidence threshold. Returns None if no good match.
    """
    if not TMDB_API_KEY:
        return None

    parsed_year = cleanYear(title_data.get("year"))
    parsed_season = title_data.get("season")
    parsed_episode = title_data.get("episode")
    ptn_season = parsed_season
    ptn_episode = parsed_episode

    # Authoritative season+episode patterns that OVERRIDE PTN's parsing when
    # matched against the file name. PTN can misidentify episode numbers from
    # CRC32 hashes in brackets (e.g. "[E896C4BE]" → episode 896) or fail to
    # recognize anime-style "2nd Season - 05" / "S3 - 08" numbering entirely.
    auth_se_patterns = (
        # "S3 - 08" / "S03 - 08"
        re.compile(r"\bS(\d{1,2})\s*-\s*(\d{1,4})\b", re.IGNORECASE),
        # "Season 3 - 08"
        re.compile(r"\bSeason\s+(\d+)\s*-\s*(\d{1,4})\b", re.IGNORECASE),
        # "2nd Season - 05" / "3rd Season - 12"
        re.compile(r"\b(\d+)(?:st|nd|rd|th)\s+Season\s*-\s*(\d{1,4})\b", re.IGNORECASE),
    )
    override_match_text = None
    for _pattern in auth_se_patterns:
        _m = _pattern.search(file_name or "")
        if _m:
            override_season = int(_m.group(1))
            override_episode = int(_m.group(2))
            if override_season != parsed_season or override_episode != parsed_episode:
                override_match_text = _m.group(0)
            parsed_season = override_season
            parsed_episode = override_episode
            break

    # Extract season/episode from full-word patterns when PTN missed them.
    # Priority: file name > folder/search title > torrent name
    # Separator class covers torrent-naming delimiters like "Season-25",
    # "Season #25", "Season.25" — not just whitespace.
    _SEP = r"[\s\-#._:\(\)\[\]]{0,5}"
    season_re = re.compile(rf"\bSeason{_SEP}(\d+)", re.IGNORECASE)
    # Ordinal-prefixed season, e.g. "2nd Season", "1st.Season", "3rd-Season"
    ordinal_season_re = re.compile(rf"\b(\d+)(?:st|nd|rd|th){_SEP}Season\b", re.IGNORECASE)
    episode_re = re.compile(rf"\bEpisode{_SEP}(\d+)", re.IGNORECASE)
    s_ep_re = re.compile(r"\bS(\d+)E", re.IGNORECASE)
    s_standalone_re = re.compile(r"\bS(\d+)\b", re.IGNORECASE)  # [S01], S02, etc.

    sources = (file_name, title, folder_name, item_name)

    if parsed_season is None:
        for source in sources:
            if not source:
                continue
            m = (
                season_re.search(source)
                or ordinal_season_re.search(source)
                or s_ep_re.search(source)
                or s_standalone_re.search(source)
            )
            if m:
                parsed_season = int(m.group(1))
                break

    e_from_s_ep_re = re.compile(r"\bS\d+E(\d+)", re.IGNORECASE)

    if parsed_episode is None:
        for source in sources:
            if not source:
                continue
            m = episode_re.search(source) or e_from_s_ep_re.search(source)
            if m:
                parsed_episode = int(m.group(1))
                break

    # Bare-leading-number fallback: "12 - The Message.mkv" style.
    # Only on file_name, and only when a season was detected elsewhere, to
    # avoid false positives on ranked movie files like "01 - The Godfather.mkv".
    if parsed_episode is None and parsed_season is not None and file_name:
        m = re.match(r"^(\d{1,3})\s*[-.\s_]\s*\D", file_name)
        if m:
            parsed_episode = int(m.group(1))

    # Detect TV indicator from any source
    tv_indicator_re = re.compile(
        r"\bS\d+E"                            # S01E04, S01EXA
        r"|\bS\d+\b"                          # S01, [S01] (standalone season)
        rf"|\bSeason{_SEP}\d+"                # Season 03, Season-25, Season #25
        rf"|\b\d+(?:st|nd|rd|th){_SEP}Season\b"  # 2nd Season, 1st.Season
        rf"|\bEpisode{_SEP}\d+"               # Episode 039, Episode-7
        rf"|\bPart{_SEP}\d+",                 # Part 04, Part-2
        re.IGNORECASE,
    )
    has_season_tag = any(tv_indicator_re.search(s) for s in sources if s)
    is_tv = parsed_season is not None or parsed_episode is not None or has_season_tag
    extension = os.path.splitext(file_name)[-1]

    # Search order: TV first if season/episode detected, else movie first
    search_order = [("tv", "tv"), ("movie", "movie")] if is_tv else [("movie", "movie"), ("tv", "tv")]

    best_result = None
    best_score = -1
    best_media_type = None
    best_breakdown = None
    all_scored: list[dict] = []

    normalised_input = normaliseTitle(title)

    # Use normalised title as the TMDB query — strips Cyrillic, diacritics,
    # season/episode tags, etc. so the API gets a clean English search term.
    search_query = normalised_input
    if not search_query:
        return None

    # Build a list of queries to try: original, then without trailing roman numerals
    search_queries = [search_query]
    stripped = re.sub(r"\s+(?:i{1,3}|iv|v(?:i{0,3})|ix|x(?:i{0,3}))$", "", search_query, flags=re.IGNORECASE)
    if stripped and stripped != search_query:
        search_queries.append(stripped)

    TITLE_DETAIL_THRESHOLD = 90  # partial_ratio >= this triggers a detail fetch

    for current_query in search_queries:
        if best_score >= TMDB_SCORE_THRESHOLD:
            break

        for search_type, media_type in search_order:
            # Yearless search to get the broadest candidate pool
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

            # For candidates with strong title matches, fetch detail to get
            # the full airing range (first_air_date → last_air_date for TV).
            # This lets the scorer check if the parsed year falls within the
            # show's run rather than only matching the first air date.
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

            # If we already found a confident match, skip the other type
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

    # Accept if score meets threshold, or if there's only 1 result and score >= 50
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

    # Build metadata dict
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

    def cacheAndReturn(metadata: dict, success: bool, detail: str):
        if cache_key is not None:
            setCachedMetadata(cache_key, metadata, success, detail)
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

    # Try TMDB — torrent/folder name first, file name only as fallback.
    folder_title_data = PTN.parse(item_name) if item_name else {}
    folder_query = folder_title_data.get("title", item_name) or query
    file_query = title_data.get("title", "")

    # Merge: file is authoritative for season/episode (specific to the file),
    # folder is authoritative for year (often more accurate in torrent names).
    folder_merged_data = {**title_data}
    if folder_title_data.get("year") is not None:
        folder_merged_data["year"] = folder_title_data["year"]
    # Keep season/episode from file parse (title_data) — folder often has
    # a range like [1,2,3,...8] which is the whole series, not the specific episode.

    # 1. Search with torrent/folder name first
    tmdb_result = searchTMDB(folder_query, folder_merged_data, file_name, item_name=item_name, folder_name=folder_name, torrent_id=torrent_id)
    if tmdb_result is not None:
        base_metadata.update(tmdb_result)
        return cacheAndReturn(base_metadata, True, f"TMDB match via torrent name (score: {tmdb_result.get('metadata_tmdb_score')}). Searching for {folder_query}, item hash: {hash}")

    # 2. Try subfolder name (last path component of folder_name).
    #    e.g. "Total Drama Complete (...)/Total Drama Presents The Ridonculous Race" -> last part
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

    # 3. Fall back to file name only if torrent and subfolder searches failed, and:
    #    - file title differs from folder title (and subfolder title)
    #    - filename doesn't start with S##E## (PTN "title" would be episode title)
    #    - normalised file title isn't purely numeric (e.g. "304" from "3x04")
    normalised_file_query = normaliseTitle(file_query) if file_query else ""
    starts_with_episode_tag = bool(re.match(r"^S\d+E\d+", file_name, re.IGNORECASE))
    is_numeric_only = bool(normalised_file_query and re.match(r"^\d+$", normalised_file_query))
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

    # Fall back to TorBox Search API
    diag(f"  >> Falling back to TorBox Search API for: {file_name} (torrent_id={torrent_id})")
    extension = os.path.splitext(file_name)[-1]
    try:
        response = requestWrapper(search_api_http_client, "GET", f"/meta/search/{full_title}", params={"type": "file"})
    except Exception as e:
        logging.error(f"Error searching metadata: {e}")
        return cacheAndReturn(base_metadata, False, f"Error searching metadata: {e}. Searching for {query}, item hash: {hash}")
    if response.status_code != 200:
        logging.error(f"Error searching metadata: {response.status_code}. {response.text}")
        return cacheAndReturn(base_metadata, False, f"Error searching metadata. {response.status_code}. Searching for {query}, item hash: {hash}")
    try:
        data = response.json().get("data", [])[0]

        title = cleanTitle(data.get("title"))
        base_metadata["metadata_title"] = title
        base_metadata["metadata_years"] = cleanYear(title_data.get("year", None) or data.get("releaseYears", None))

        if data.get("type") == "anime" or data.get("type") == "series":
            series_season_episode = constructSeriesTitle(season=title_data.get("season", None), episode=title_data.get("episode", None))
            file_name = f"{title} {series_season_episode}{extension}"
            base_metadata["metadata_foldername"] = constructSeriesTitle(season=title_data.get("season"), folder=True)
            base_metadata["metadata_season"] = title_data.get("season")
            base_metadata["metadata_episode"] = title_data.get("episode")
        elif data.get("type") == "movie":
            file_name = f"{title} ({base_metadata['metadata_years']}){extension}"
        else:
            return cacheAndReturn(base_metadata, False, f"No metadata found. Searching for {query}, item hash: {hash}")

        base_metadata["metadata_filename"] = file_name
        base_metadata["metadata_mediatype"] = data.get("type")
        base_metadata["metadata_link"] = data.get("link")
        base_metadata["metadata_image"] = data.get("image")
        base_metadata["metadata_backdrop"] = data.get("backdrop")
        base_metadata["metadata_rootfoldername"] = f"{title} ({base_metadata['metadata_years']})"

        return cacheAndReturn(base_metadata, True, f"Metadata found. Searching for {query}, item hash: {hash}")
    except IndexError:
        return cacheAndReturn(base_metadata, False, f"No metadata found. Searching for {query}, item hash: {hash}")
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
