import re
import time
import logging
import threading
import httpx
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import NamedTuple
from library.http import (
    api_http_client,
    tbm_http_client,
    general_http_client,
    requestWrapper,
    tmdb_http_client,
)
from library.app import (
    ENABLE_MEDIA_FETCH,
    AIOSTREAMS_URLS,
    MEDIA_FETCH_DEBUG,
    MIN_DOWNLOAD_SPEED_MBS,
    TMDB_API_KEY,
    DISCOVER_MOVIES_PER_RUN,
    DISCOVER_SERIES_EPISODES_PER_RUN,
    DISCOVER_ANIME_EPISODES_PER_RUN,
    DISCOVER_TRENDING_ITEMS_PER_RUN,
    EXCLUDE_RESOLUTIONS,
    MOUNT_REFRESH_TIME,
)
from tinydb import Query
from functions.databaseFunctions import getDatabase, getDatabaseLock
from functions.wantFunctions import (
    getPendingWanted,
    updateWantedStatus,
    updateWantedField,
    LibraryIndex,
    SnapshotIndex,
    fetchTorboxSnapshot,
    loadAllLocalRecords,
    getAllWanted,
    retryFailedWanted,
    resolveImdbId,
    fetchAbsoluteEpisodes,
    pruneInvalidSeasons,
)
from functions.mediaFunctions import normaliseTitle, cleanTitle
import PTN
from library.profiling import timer

logger = logging.getLogger("acquire")

CATALOG_BUDGETS: dict[str, int] = {
    "trending": DISCOVER_TRENDING_ITEMS_PER_RUN,
    "nowPlaying": DISCOVER_MOVIES_PER_RUN,
    "popular": DISCOVER_SERIES_EPISODES_PER_RUN,
    "anime": DISCOVER_ANIME_EPISODES_PER_RUN,
}

_catalog_create_count: dict[str, int] = {k: 0 for k in CATALOG_BUDGETS}
_catalog_reset_time: float = 0.0
_active_uncached_count = 0

RESOLUTION_PRIORITY = {"2160p": 4, "1080p": 3, "720p": 2, "480p": 1}
QUALITY_PRIORITY = {"BluRay": 4, "Blu-ray": 4, "WEB-DL": 3, "WEBRip": 2, "HDTV": 1}
_RESOLUTION_REGEX = re.compile(r"(2160|1080|720|480)p", re.IGNORECASE)

CATALOG_RESET_INTERVAL_SECONDS = 3600
STALLED_TORRENT_TIMEOUT_SECONDS = 120
MAX_ACTIVE_UNCACHED_TORRENTS = 10
DOWNLOAD_VERIFICATION_DELAY_SECONDS = 10


class DownloadResult(NamedTuple):
    success: bool
    download_id: int | None
    is_cached: bool


class PendingDownload(NamedTuple):
    download_id: int
    download_type: str
    stream_name: str


class StreamAttempt(NamedTuple):
    confirmed: bool
    pending: PendingDownload | None


_aiostreams_pool: ThreadPoolExecutor | None = None
_aiostreams_pool_lock = threading.Lock()

_acquisitionSnapshotIndex: SnapshotIndex | None = None
_acquisitionSnapshotInitLock = threading.Lock()


def _getAcquisitionSnapshotIndex() -> SnapshotIndex:
    global _acquisitionSnapshotIndex
    with _acquisitionSnapshotInitLock:
        if _acquisitionSnapshotIndex is None:
            _acquisitionSnapshotIndex = SnapshotIndex()
        return _acquisitionSnapshotIndex


def _getAiostreamsPool(workers: int) -> ThreadPoolExecutor:
    global _aiostreams_pool
    with _aiostreams_pool_lock:
        if _aiostreams_pool is None:
            _aiostreams_pool = ThreadPoolExecutor(
                max_workers=max(1, workers),
                thread_name_prefix="aiostreams",
            )
        return _aiostreams_pool


def _resetCatalogIfNeeded():
    global _catalog_create_count, _catalog_reset_time
    now = time.time()
    if now >= _catalog_reset_time:
        _catalog_create_count = {k: 0 for k in CATALOG_BUDGETS}
        _catalog_reset_time = now + CATALOG_RESET_INTERVAL_SECONDS


def _getCatalogRemaining(catalog: str) -> int:
    _resetCatalogIfNeeded()
    budget = CATALOG_BUDGETS.get(catalog, 0)
    return budget - _catalog_create_count.get(catalog, 0)


def _getTotalRemaining() -> int:
    _resetCatalogIfNeeded()
    return sum(
        CATALOG_BUDGETS[k] - _catalog_create_count.get(k, 0) for k in CATALOG_BUDGETS
    )


def _decrementCatalog(catalog: str):
    _catalog_create_count[catalog] = _catalog_create_count.get(catalog, 0) + 1


def _cleanupDownloads() -> tuple[int, list[str]]:
    global _active_uncached_count
    removed_names: list[str] = []
    try:
        resp = requestWrapper(
            api_http_client,
            "GET",
            "/torrents/mylist",
            params={"limit": 1000, "offset": 0},
        )
        data = resp.json()
        if not data.get("success"):
            _active_uncached_count = 0
            return 0, removed_names
        items = data.get("data", [])
    except Exception as e:
        logger.error(f"failed to fetch torrents: {e}")
        _active_uncached_count = 0
        return 0, removed_names

    now = datetime.now(timezone.utc)
    removed = 0

    for torrent in items:
        if torrent.get("cached", False):
            continue

        torrent_id = torrent.get("id")
        name = torrent.get("name", "unknown")
        status = (torrent.get("download_state") or "").lower()
        speed = torrent.get("download_speed", 0) or 0
        speed_mbs = speed / (1024 * 1024)
        updated_at = torrent.get("updated_at", "")

        should_remove = False
        reason = ""

        if status in ("stalled", "stalleddl", "getting info", "metadl", "error"):
            if updated_at:
                try:
                    updated = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
                    age_seconds = (now - updated).total_seconds()
                    if age_seconds > STALLED_TORRENT_TIMEOUT_SECONDS:
                        should_remove = True
                        reason = f"status '{status}' for {age_seconds:.0f}s"
                except (ValueError, TypeError):
                    should_remove = True
                    reason = f"status '{status}' (unparseable timestamp)"
            else:
                should_remove = True
                reason = f"status '{status}'"

        if (
            not should_remove
            and speed_mbs < MIN_DOWNLOAD_SPEED_MBS
            and status not in ("", "downloading")
        ):
            should_remove = True
            reason = f"speed {speed_mbs:.2f} MB/s < {MIN_DOWNLOAD_SPEED_MBS} MB/s"

        if should_remove and torrent_id:
            try:
                requestWrapper(
                    api_http_client,
                    "POST",
                    "/torrents/controltorrent",
                    json={"torrent_id": torrent_id, "operation": "delete"},
                    use_cache=False,
                )
                removed += 1
                removed_names.append(name)
                logger.info(f"removed torrent '{name}' — {reason}")
            except Exception as e:
                logger.warning(f"failed to remove torrent {torrent_id}: {e}")

    active = sum(1 for t in items if not t.get("cached", False)) - removed
    _active_uncached_count = max(0, active)

    if removed:
        logger.info(
            f"removed {removed} unhealthy torrents. Active uncached: {_active_uncached_count}"
        )
    elif MEDIA_FETCH_DEBUG:
        logger.debug(
            f"no unhealthy torrents found. Active uncached: {_active_uncached_count}"
        )

    return _active_uncached_count, removed_names


def _requeueOrphanedAcquiring(removed_names: list[str]) -> int:
    if not removed_names:
        return 0
    removed_norm: set[str] = set()
    for raw in removed_names:
        if not raw:
            continue
        parsed = PTN.parse(raw)
        ptn_t = normaliseTitle(parsed.get("title", "") or "")
        if ptn_t:
            removed_norm.add(ptn_t)
        fb = normaliseTitle(cleanTitle(raw) or "")
        if fb:
            removed_norm.add(fb)
    if not removed_norm:
        return 0

    all_items = getAllWanted()
    requeued = 0
    for item in all_items:
        if item.get("status") != "acquiring":
            continue
        title = item.get("title") or ""
        n = normaliseTitle(title)
        if n and n in removed_norm:
            updateWantedStatus(
                item["tmdb_id"],
                "pending",
                failure_reason="cleaned: stalled torrent removed",
            )
            requeued += 1
    if requeued:
        logger.info(f"requeued {requeued} orphaned acquiring items.")
    return requeued


def _scoreNzb(nzb: dict) -> int:
    parsed = nzb.get("title_parsed_data", {})
    score = 0
    score += RESOLUTION_PRIORITY.get(parsed.get("resolution", ""), 0) * 10
    score += QUALITY_PRIORITY.get(parsed.get("quality", ""), 0) * 5
    if nzb.get("cached", False):
        score += 100
    return score


def _searchUsenet(imdb_id: str) -> tuple[list[dict], bool]:
    """Returns (nzbs, success). success=False means API error (not just empty results)."""
    try:
        resp = requestWrapper(
            tbm_http_client,
            "GET",
            f"/api/usenet/search?query=imdb%3A{imdb_id}&search_user_engines=false",
        )
        data = resp.json()
        if not data.get("data"):
            return [], True
        nzbs = data["data"].get("nzbs", [])
        filtered = [
            n for n in nzbs if n.get("cached", False) and n.get("type") == "usenet"
        ]
        if EXCLUDE_RESOLUTIONS:
            before = len(filtered)
            filtered = [n for n in filtered if _nzbResolutionAllowed(n)]
            if before - len(filtered):
                logger.info(
                    f"Usenet: filtered {before - len(filtered)} nzbs matching EXCLUDE_RESOLUTIONS (fail-closed)"
                )
        return filtered, True
    except Exception as e:
        logger.warning(f"Usenet search failed for {imdb_id}: {e}")
        return [], False


def _createUsenetDownload(
    nzb_url: str, name: str, catalog: str = "nowPlaying"
) -> DownloadResult:
    if _getCatalogRemaining(catalog) <= 0:
        logger.warning(f"Rate limit exhausted for {catalog}, skipping usenet create.")
        return DownloadResult(False, None, False)
    try:
        resp = api_http_client.post(
            "/usenet/createusenetdownload",
            data={"link": nzb_url, "name": name},
        )
        result = resp.json()
        _decrementCatalog(catalog)
        if result.get("success"):
            dl_data = result.get("data", {})
            dl_id = dl_data.get("usenetdownload_id") or dl_data.get("id")
            cached = dl_data.get("cached", False)
            logger.info(
                f"Created usenet download: {name} (id={dl_id}, cached={cached})"
            )
            return DownloadResult(True, dl_id, cached)
        logger.warning(f"Usenet create failed: {result.get('detail', result)}")
        return DownloadResult(False, None, False)
    except Exception as e:
        logger.exception(f"Usenet create error: {e}")
        return DownloadResult(False, None, False)


def _parseAiostreamsBaseUrls() -> list[str]:
    bases = []
    for url in AIOSTREAMS_URLS:
        base = url.replace("/manifest.json", "")
        bases.append(base)
    return bases


def _streamResolution(stream: dict) -> str | None:
    hints = stream.get("behaviorHints", {})
    parts = [
        hints.get("filename", "") or "",
        stream.get("name", "") or "",
        stream.get("description", "") or "",
    ]
    text = " ".join(parts)
    m = _RESOLUTION_REGEX.search(text)
    return f"{m.group(1)}p".lower() if m else None


def _passesResolutionFilter(stream: dict) -> bool:
    if not EXCLUDE_RESOLUTIONS:
        return True
    res = _streamResolution(stream)
    if res is None:
        return False
    return res not in EXCLUDE_RESOLUTIONS


def _nzbResolutionAllowed(nzb: dict) -> bool:
    if not EXCLUDE_RESOLUTIONS:
        return True
    raw = (nzb.get("title_parsed_data", {}).get("resolution") or "").lower()
    if raw:
        return raw not in EXCLUDE_RESOLUTIONS
    title = nzb.get("raw_title") or ""
    m = _RESOLUTION_REGEX.search(title)
    if not m:
        return False
    return f"{m.group(1)}p".lower() not in EXCLUDE_RESOLUTIONS


def _normKey(
    title: str | None, season: int | None = None, episode: int | None = None
) -> tuple:
    return (normaliseTitle(title or ""), season, episode)


def _markAcquired(
    snapshot_index: SnapshotIndex | None,
    in_flight: set[tuple] | None,
    title: str | None,
    season: int | None = None,
    episode: int | None = None,
) -> None:
    if not title:
        return
    if snapshot_index is not None:
        snapshot_index.markPresent(title, season)
    if in_flight is not None:
        in_flight.add(_normKey(title, season, episode))
        if isinstance(season, int):
            in_flight.add(_normKey(title, season, None))
        in_flight.add(_normKey(title, None, None))


def _scoreAiostream(stream: dict) -> tuple[int, int]:
    res = _streamResolution(stream)
    res_score = RESOLUTION_PRIORITY.get(res, 0) if res else 0
    size = stream.get("behaviorHints", {}).get("videoSize") or 0
    return (res_score, size)


def _searchAiostreams(
    media_type: str, tmdb_id: int, season: int | None = None, episode: int | None = None
) -> list[dict]:
    bases = _parseAiostreamsBaseUrls()
    if not bases:
        logger.info("AIOStreams: no URLs configured, skipping.")
        return []

    all_streams = []

    stremio_type = "movie" if media_type == "movie" else "series"
    if media_type == "movie":
        id_str = f"tmdb:{tmdb_id}"
    else:
        id_str = f"tmdb:{tmdb_id}:{season}:{episode}"

    def _fetch(base):
        url = f"{base}/stream/{stremio_type}/{id_str}.json"
        try:
            resp = general_http_client.get(url, timeout=15)
            if resp.status_code == 200:
                streams = resp.json().get("streams", [])
                logger.info(
                    f"AIOStreams: {len(streams)} streams from {base} for {id_str}"
                )
                return streams
            logger.info(f"AIOStreams: HTTP {resp.status_code} from {base} for {id_str}")
        except Exception as e:
            logger.warning(f"AIOStreams query failed ({base}): {e}")
        return []

    pool = _getAiostreamsPool(len(bases))
    for streams in pool.map(_fetch, bases):
        all_streams.extend(streams)

    result = [
        s for s in all_streams if s.get("infoHash") or s.get("nzbUrl") or s.get("url")
    ]

    if EXCLUDE_RESOLUTIONS:
        before = len(result)
        result = [s for s in result if _passesResolutionFilter(s)]
        excluded = before - len(result)
        if excluded:
            logger.info(
                f"AIOStreams: filtered {excluded} streams matching EXCLUDE_RESOLUTIONS={sorted(EXCLUDE_RESOLUTIONS)} (fail-closed)"
            )

    if not result:
        logger.info(
            f"AIOStreams: 0 usable streams for {stremio_type}/{id_str} (raw: {len(all_streams)})"
        )
        return result

    result.sort(key=_scoreAiostream, reverse=True)

    if MEDIA_FETCH_DEBUG:
        types = {"hash": 0, "nzb": 0, "url": 0}
        for s in result:
            if s.get("infoHash"):
                types["hash"] += 1
            elif s.get("nzbUrl"):
                types["nzb"] += 1
            elif s.get("url"):
                types["url"] += 1
        best = result[0]
        best_name = best.get("behaviorHints", {}).get("filename", "?")
        best_size = best.get("behaviorHints", {}).get("videoSize", 0)
        logger.info(
            f"AIOStreams: {len(result)} usable for {id_str} — "
            f"{types['hash']} torrent, {types['nzb']} nzb, {types['url']} debrid/url | "
            f"best: {best_name} ({best_size / (1024**3):.1f} GB)"
        )
    return result


def _createTorrentDownload(
    info_hash: str, name: str, catalog: str = "nowPlaying"
) -> DownloadResult:
    global _active_uncached_count
    if _getCatalogRemaining(catalog) <= 0:
        logger.warning(f"Rate limit exhausted for {catalog}, skipping torrent create.")
        return DownloadResult(False, None, False)

    if _active_uncached_count >= MAX_ACTIVE_UNCACHED_TORRENTS:
        logger.warning(
            f"Active torrent cap reached ({_active_uncached_count}/{MAX_ACTIVE_UNCACHED_TORRENTS}), skipping."
        )
        return DownloadResult(False, None, False)

    magnet = f"magnet:?xt=urn:btih:{info_hash}"
    try:
        resp = api_http_client.post(
            "/torrents/createtorrent",
            data={"magnet": magnet, "name": name, "seed": "1", "allow_zip": "false"},
        )
        result = resp.json()
        _decrementCatalog(catalog)
        if result.get("success"):
            dl_data = result.get("data", {})
            dl_id = dl_data.get("torrent_id") or dl_data.get("id")
            cached = dl_data.get("cached", False)
            if not cached:
                _active_uncached_count += 1
            logger.info(
                f"Created torrent download: {name} (id={dl_id}, cached={cached})"
            )
            return DownloadResult(True, dl_id, cached)
        logger.warning(f"Torrent create failed: {result.get('detail', result)}")
        return DownloadResult(False, None, False)
    except Exception as e:
        logger.exception(f"Torrent create error: {e}")
        return DownloadResult(False, None, False)


def _removeDownload(download_id: int, download_type: str):
    if download_type == "torrents":
        endpoint = "/torrents/controltorrent"
        payload = {"torrent_id": download_id, "operation": "delete"}
    else:
        endpoint = "/usenet/controlusenetdownload"
        payload = {"usenet_id": download_id, "operation": "delete"}
    try:
        requestWrapper(api_http_client, "POST", endpoint, json=payload, use_cache=False)
        logger.info(f"Removed {download_type} download {download_id}")
    except Exception as e:
        logger.warning(f"Failed to remove {download_type} {download_id}: {e}")


def _batchVerifyDownloads(pending: list[tuple[int, str]]) -> dict[int, bool | None]:
    if not pending:
        return {}

    time.sleep(DOWNLOAD_VERIFICATION_DELAY_SECONDS)

    by_type: dict[str, list[int]] = {}
    for dl_id, dl_type in pending:
        by_type.setdefault(dl_type, []).append(dl_id)

    results: dict[int, bool | None] = {}
    for dl_type, ids in by_type.items():
        id_set = set(ids)
        try:
            resp = requestWrapper(
                api_http_client,
                "GET",
                f"/{dl_type}/mylist",
                params={"limit": 1000, "offset": 0},
            )
            data = resp.json()
            if not data.get("success"):
                for dl_id in ids:
                    results[dl_id] = None
                continue
            for dl in data.get("data", []):
                dl_id = dl.get("id")
                if dl_id not in id_set:
                    continue
                id_set.discard(dl_id)
                if dl.get("cached", False):
                    results[dl_id] = True
                else:
                    speed = (dl.get("download_speed") or 0) / (1024 * 1024)
                    if speed >= MIN_DOWNLOAD_SPEED_MBS:
                        logger.info(
                            f"Download {dl_id} uncached but speed {speed:.1f} MB/s — keeping."
                        )
                    results[dl_id] = speed >= MIN_DOWNLOAD_SPEED_MBS
            for dl_id in id_set:
                results[dl_id] = None
        except Exception as e:
            logger.warning(f"Batch verify failed for {dl_type}: {e}")
            for dl_id in ids:
                results[dl_id] = None

    return results


def _tryAiostreamUrl(url: str, name: str, catalog: str) -> bool:
    if _getCatalogRemaining(catalog) <= 0:
        return False
    try:
        resp = general_http_client.get(url, timeout=30, follow_redirects=False)
        _decrementCatalog(catalog)
        location = resp.headers.get("location", "")
        if resp.status_code in (301, 302, 307, 308) and "tb-cdn.io" in location:
            logger.info(f"AIOStreams URL cached for '{name}' → {location[:80]}")
            return True
        logger.info(
            f"AIOStreams URL not cached for '{name}' (status {resp.status_code}, location: {location[:80]})"
        )
        return False
    except Exception as e:
        logger.warning(f"AIOStreams URL error for '{name}': {e}")
        return False


def _tryAiostreams(
    streams: list[dict],
    name: str,
    catalog: str,
) -> StreamAttempt:
    for stream in streams:
        stream_name = stream.get("behaviorHints", {}).get("filename", name)
        info_hash = stream.get("infoHash")
        nzb_url = stream.get("nzbUrl")
        playback_url = stream.get("url")

        if playback_url and not info_hash and not nzb_url:
            if _tryAiostreamUrl(playback_url, stream_name, catalog):
                return StreamAttempt(True, None)
            continue

        if info_hash:
            result = _createTorrentDownload(info_hash, stream_name, catalog=catalog)
            dl_type = "torrents"
        elif nzb_url:
            result = _createUsenetDownload(nzb_url, stream_name, catalog=catalog)
            dl_type = "usenet"
        else:
            continue

        if not result.success:
            continue
        if result.is_cached:
            return StreamAttempt(True, None)
        if result.download_id is not None:
            return StreamAttempt(
                False, PendingDownload(result.download_id, dl_type, stream_name)
            )
        return StreamAttempt(True, None)

    return StreamAttempt(False, None)


def _resolveVerifications(
    pending: list[PendingDownload],
) -> set[int]:
    """Batch verify pending downloads. Returns set of confirmed dl_ids.
    Removes uncached downloads."""
    global _active_uncached_count
    if not pending:
        return set()

    results = _batchVerifyDownloads([(dl_id, dl_type) for dl_id, dl_type, _ in pending])
    confirmed: set[int] = set()

    for dl_id, dl_type, dl_name in pending:
        if results.get(dl_id) is True:
            confirmed.add(dl_id)
        else:
            _removeDownload(dl_id, dl_type)
            if dl_type == "torrents":
                _active_uncached_count = max(0, _active_uncached_count - 1)
            logger.info(
                f"Batch verify: uncached/slow {dl_type} {dl_id} for '{dl_name}', removed."
            )

    return confirmed


def _acquireMovie(
    item: dict,
    catalog: str,
    library_index: LibraryIndex | None = None,
    snapshot_index: SnapshotIndex | None = None,
    in_flight: set[tuple] | None = None,
) -> bool:
    tmdb_id = item["tmdb_id"]
    imdb_id = item.get("imdb_id")
    title = item.get("title", f"TMDB-{tmdb_id}")

    if not imdb_id:
        imdb_id = resolveImdbId(tmdb_id, "movie")
        if imdb_id:
            updateWantedField(tmdb_id, "imdb_id", imdb_id)

    if library_index is not None and library_index.contains(title, "movie"):
        updateWantedStatus(tmdb_id, "acquired")
        return True
    if snapshot_index is not None and snapshot_index.contains(title):
        logger.info(f"Cloud already has '{title}' (snapshot dedup); marking acquiring.")
        updateWantedStatus(tmdb_id, "acquiring")
        return True
    if in_flight is not None and _normKey(title) in in_flight:
        logger.info(f"In-flight dedup hit for '{title}'; skipping create.")
        updateWantedStatus(tmdb_id, "acquiring")
        return True

    with timer("acquireMovie.aiostreams", tmdb_id=tmdb_id) as f:
        streams = _searchAiostreams("movie", tmdb_id)
        confirmed, pending = _tryAiostreams(streams, title, catalog)
        f["streams"] = len(streams) if streams else 0
        f["confirmed"] = bool(confirmed)
    if confirmed:
        _markAcquired(snapshot_index, in_flight, title)
        updateWantedStatus(tmdb_id, "acquiring")
        return True

    if pending:
        with timer("acquireMovie.verify", tmdb_id=tmdb_id):
            confirmed_ids = _resolveVerifications([pending])
        if pending.download_id in confirmed_ids:
            _markAcquired(snapshot_index, in_flight, title)
            updateWantedStatus(tmdb_id, "acquiring")
            return True

    if imdb_id:
        with timer("acquireMovie.usenet", tmdb_id=tmdb_id) as f:
            nzbs, _ = _searchUsenet(imdb_id)
            f["nzbs"] = len(nzbs) if nzbs else 0
        if nzbs:
            nzbs.sort(key=_scoreNzb, reverse=True)
            best = nzbs[0]
            nzb_url = best.get("nzb")
            raw_title = best.get("raw_title", title)
            if nzb_url:
                if _createUsenetDownload(nzb_url, raw_title, catalog=catalog).success:
                    _markAcquired(snapshot_index, in_flight, title)
                    updateWantedStatus(tmdb_id, "acquiring")
                    return True

    if not imdb_id:
        updateWantedStatus(
            tmdb_id, "deferred", failure_reason="IMDB ID resolve failed; will retry"
        )
    else:
        updateWantedStatus(
            tmdb_id, "failed", failure_reason="No cached stream or usenet found"
        )
    return False


def _fetchSeasonEpisodes(
    tmdb_id: int, seasons_needed: list[int], title: str
) -> dict[int, list[dict]]:
    today = datetime.now(timezone.utc).date().isoformat()
    season_episodes: dict[int, list[dict]] = {}
    invalid_seasons: list[int] = []
    for season_num in seasons_needed:
        try:
            season_data = _tmdbGetSeason(tmdb_id, season_num)
        except Exception as e:
            logger.warning(f"Failed to get season {season_num} for {title}: {e}")
            continue
        if season_data is None:
            invalid_seasons.append(season_num)
            continue
        aired = [
            ep
            for ep in (season_data.get("episodes") or [])
            if (ep.get("air_date") or "") and ep["air_date"] <= today
        ]
        if aired:
            season_episodes[season_num] = aired
    if invalid_seasons:
        logger.info(
            f"{title} (tmdb={tmdb_id}): seasons {invalid_seasons} returned 404 — pruning"
        )
        pruneInvalidSeasons(tmdb_id, invalid_seasons)
    return season_episodes


def _fetchAbsoluteSeasonEpisodes(group_id: str) -> dict[int, list[dict]]:
    today = datetime.now(timezone.utc).date().isoformat()
    eps = fetchAbsoluteEpisodes(group_id)
    aired: list[dict] = []
    for i, ep in enumerate(eps, start=1):
        if (ep.get("air_date") or "") and ep["air_date"] <= today:
            aired.append({**ep, "episode_number": i})
    return {1: aired} if aired else {}


def _episodeAlreadyHandled(
    title: str,
    season_num: int,
    ep_num: int,
    library_index: LibraryIndex | None,
    snapshot_index: SnapshotIndex | None,
    in_flight: set[tuple] | None,
    is_absolute: bool = False,
) -> bool:
    # Absolute-numbered anime stores files under TMDB-standard (S5E1 etc.) but is
    # acquired under a single virtual S1 with absolute episode numbers. Per-episode
    # dedup against the standard-layout library would always miss; rely on title
    # presence only and accept rare-duplicate AIOStreams hits.
    if is_absolute:
        if library_index is not None and library_index.contains(title, "series"):
            return True
        if snapshot_index is not None and snapshot_index.contains(title):
            return True
        return False
    if library_index is not None and library_index.contains(
        title, "series", season=season_num, episode=ep_num
    ):
        return True
    if snapshot_index is not None and snapshot_index.contains(title, season=season_num):
        return True
    if in_flight is not None and (
        _normKey(title, season_num, ep_num) in in_flight
        or _normKey(title, season_num, None) in in_flight
    ):
        return True
    return False


def _identifyUnmetEpisodes(
    title: str,
    season_episodes: dict[int, list[dict]],
    library_index: LibraryIndex | None,
    snapshot_index: SnapshotIndex | None,
    in_flight: set[tuple] | None,
    is_absolute: bool = False,
) -> set[tuple[int, int]]:
    unmet: set[tuple[int, int]] = set()
    for season_num, episodes in season_episodes.items():
        for ep in episodes:
            ep_num = ep.get("episode_number")
            if ep_num is None:
                continue
            if _episodeAlreadyHandled(
                title,
                season_num,
                ep_num,
                library_index,
                snapshot_index,
                in_flight,
                is_absolute=is_absolute,
            ):
                continue
            unmet.add((season_num, ep_num))
    return unmet


def _attemptAiostreamsForEpisodes(
    title: str,
    tmdb_id: int,
    catalog: str,
    season_episodes: dict[int, list[dict]],
    unmet_eps: set[tuple[int, int]],
    snapshot_index: SnapshotIndex | None,
    in_flight: set[tuple] | None,
    confirmed_eps: set[tuple[int, int]],
    pending_verify: list[tuple[int, str, str, int, int]],
    is_absolute: bool = False,
) -> bool:
    """Returns budget_exhausted flag. Mutates unmet_eps, confirmed_eps, pending_verify."""
    with timer("acquireSeries.aiostreams", tmdb_id=tmdb_id) as f:
        ep_jobs: list[tuple[int, int, str]] = [
            (season_num, ep_num, f"{title} S{season_num:02d}E{ep_num:02d}")
            for season_num, episodes in season_episodes.items()
            for ep in episodes
            if (ep_num := ep.get("episode_number")) is not None
            and (season_num, ep_num) in unmet_eps
        ]

        def _do_search(job):
            season_num, ep_num, ep_label = job
            streams = _searchAiostreams(
                "series", tmdb_id, season=season_num, episode=ep_num
            )
            return season_num, ep_num, ep_label, streams

        search_results: list[tuple[int, int, str, list[dict]]] = []
        if ep_jobs:
            with ThreadPoolExecutor(max_workers=min(4, len(ep_jobs))) as ex:
                search_results = list(ex.map(_do_search, ep_jobs))

        ep_attempts = 0
        budget_exhausted = False
        for season_num, ep_num, ep_label, streams in search_results:
            if _getCatalogRemaining(catalog) <= 0:
                logger.warning(
                    f"Rate limit exhausted for {catalog} during series acquisition."
                )
                budget_exhausted = True
                break
            if _episodeAlreadyHandled(
                title,
                season_num,
                ep_num,
                None,
                snapshot_index,
                in_flight,
                is_absolute=is_absolute,
            ):
                unmet_eps.discard((season_num, ep_num))
                continue
            confirmed, pending = _tryAiostreams(streams, ep_label, catalog)
            ep_attempts += 1
            if confirmed:
                confirmed_eps.add((season_num, ep_num))
                unmet_eps.discard((season_num, ep_num))
                _markAcquired(
                    snapshot_index, in_flight, title, season=season_num, episode=ep_num
                )
            elif pending:
                pending_verify.append(
                    (
                        pending.download_id,
                        pending.download_type,
                        pending.stream_name,
                        season_num,
                        ep_num,
                    )
                )

        f["ep_jobs"] = len(ep_jobs)
        f["ep_attempts"] = ep_attempts
        f["confirmed"] = len(confirmed_eps)
        f["pending_verify"] = len(pending_verify)
        return budget_exhausted


def _resolvePendingEpisodeVerifications(
    title: str,
    tmdb_id: int,
    pending_verify: list[tuple[int, str, str, int, int]],
    confirmed_eps: set[tuple[int, int]],
    unmet_eps: set[tuple[int, int]],
    snapshot_index: SnapshotIndex | None,
    in_flight: set[tuple] | None,
) -> None:
    if not pending_verify:
        return
    with timer("acquireSeries.verify", tmdb_id=tmdb_id, count=len(pending_verify)):
        confirmed_ids = _resolveVerifications(
            [
                PendingDownload(dl_id, dl_type, dl_name)
                for dl_id, dl_type, dl_name, _, _ in pending_verify
            ],
        )
    for dl_id, _, _, s_num, e_num in pending_verify:
        if dl_id in confirmed_ids:
            confirmed_eps.add((s_num, e_num))
            unmet_eps.discard((s_num, e_num))
            _markAcquired(snapshot_index, in_flight, title, season=s_num, episode=e_num)


def _attemptUsenetForEpisodes(
    title: str,
    tmdb_id: int,
    imdb_id: str,
    catalog: str,
    season_episodes: dict[int, list[dict]],
    unmet_eps: set[tuple[int, int]],
    confirmed_eps: set[tuple[int, int]],
    snapshot_index: SnapshotIndex | None,
    in_flight: set[tuple] | None,
    is_absolute: bool = False,
) -> bool:
    """Returns budget_exhausted flag. Mutates unmet_eps, confirmed_eps."""
    budget_exhausted = False
    with timer("acquireSeries.usenet", tmdb_id=tmdb_id) as f:
        nzb_results: list[dict] = []
        nzb_fetched = False
        usenet_created = 0
        for season_num, episodes in season_episodes.items():
            if budget_exhausted:
                break
            for ep in episodes:
                ep_num = ep.get("episode_number")
                if ep_num is None or (season_num, ep_num) not in unmet_eps:
                    continue
                if _getCatalogRemaining(catalog) <= 0:
                    budget_exhausted = True
                    break
                if _episodeAlreadyHandled(
                    title,
                    season_num,
                    ep_num,
                    None,
                    snapshot_index,
                    in_flight,
                    is_absolute=is_absolute,
                ):
                    unmet_eps.discard((season_num, ep_num))
                    continue

                if not nzb_fetched:
                    nzb_results, _ = _searchUsenet(imdb_id)
                    nzb_fetched = True

                matched_nzbs = [
                    n
                    for n in nzb_results
                    if n.get("title_parsed_data", {}).get("season") == season_num
                    and n.get("title_parsed_data", {}).get("episode") == ep_num
                ]
                if not matched_nzbs:
                    continue
                matched_nzbs.sort(key=_scoreNzb, reverse=True)
                best = matched_nzbs[0]
                nzb_url = best.get("nzb")
                if not nzb_url:
                    continue
                raw_title = best.get(
                    "raw_title", f"{title} S{season_num:02d}E{ep_num:02d}"
                )
                if _createUsenetDownload(nzb_url, raw_title, catalog=catalog).success:
                    confirmed_eps.add((season_num, ep_num))
                    unmet_eps.discard((season_num, ep_num))
                    usenet_created += 1
                    _markAcquired(
                        snapshot_index,
                        in_flight,
                        title,
                        season=season_num,
                        episode=ep_num,
                    )
        f["nzbs"] = len(nzb_results)
        f["created"] = usenet_created
    return budget_exhausted


def _finalizeSeriesStatus(
    tmdb_id: int, imdb_id: str | None, any_acquired: bool, budget_exhausted: bool
) -> None:
    if budget_exhausted:
        updateWantedStatus(tmdb_id, "pending")
    elif any_acquired:
        updateWantedStatus(tmdb_id, "acquiring")
    elif not imdb_id:
        updateWantedStatus(
            tmdb_id, "deferred", failure_reason="IMDB ID resolve failed; will retry"
        )
    else:
        updateWantedStatus(
            tmdb_id, "failed", failure_reason="No episodes could be acquired"
        )


def _acquireSeries(
    item: dict,
    catalog: str,
    library_index: LibraryIndex | None = None,
    snapshot_index: SnapshotIndex | None = None,
    in_flight: set[tuple] | None = None,
) -> bool:
    tmdb_id = item["tmdb_id"]
    imdb_id = item.get("imdb_id")
    title = item.get("title", f"TMDB-{tmdb_id}")
    seasons_needed = item.get("seasons_needed", [])
    absolute_group_id = item.get("absolute_group_id")
    is_absolute = bool(absolute_group_id)

    if not imdb_id:
        imdb_id = resolveImdbId(tmdb_id, "series")
        if imdb_id:
            updateWantedField(tmdb_id, "imdb_id", imdb_id)

    if not seasons_needed:
        updateWantedStatus(tmdb_id, "failed", failure_reason="No seasons specified")
        return False

    if is_absolute:
        season_episodes = _fetchAbsoluteSeasonEpisodes(absolute_group_id)
    else:
        season_episodes = _fetchSeasonEpisodes(tmdb_id, seasons_needed, title)
    unmet_eps = _identifyUnmetEpisodes(
        title,
        season_episodes,
        library_index,
        snapshot_index,
        in_flight,
        is_absolute=is_absolute,
    )

    confirmed_eps: set[tuple[int, int]] = set()
    pending_verify: list[tuple[int, str, str, int, int]] = []

    budget_exhausted = _attemptAiostreamsForEpisodes(
        title,
        tmdb_id,
        catalog,
        season_episodes,
        unmet_eps,
        snapshot_index,
        in_flight,
        confirmed_eps,
        pending_verify,
        is_absolute=is_absolute,
    )

    _resolvePendingEpisodeVerifications(
        title,
        tmdb_id,
        pending_verify,
        confirmed_eps,
        unmet_eps,
        snapshot_index,
        in_flight,
    )

    # tbm.tools usenet fallback matches by TMDB-standard season+episode parsed from
    # NZB titles; absolute-anime acquisition uses a virtual S1 with absolute episode
    # numbers that won't match. AIOStreams already aggregates usenet for these.
    if imdb_id and unmet_eps and not budget_exhausted and not is_absolute:
        budget_exhausted = _attemptUsenetForEpisodes(
            title,
            tmdb_id,
            imdb_id,
            catalog,
            season_episodes,
            unmet_eps,
            confirmed_eps,
            snapshot_index,
            in_flight,
        )

    any_acquired = bool(confirmed_eps)
    _finalizeSeriesStatus(tmdb_id, imdb_id, any_acquired, budget_exhausted)
    return any_acquired


def _tmdbGetSeason(tmdb_id: int, season_num: int) -> dict | None:
    try:
        resp = requestWrapper(
            tmdb_http_client,
            "GET",
            f"/tv/{tmdb_id}/season/{season_num}",
            params={"api_key": TMDB_API_KEY, "language": "en-US"},
        )
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return None
        raise
    return resp.json()


def _assignOrphanedCatalogs(all_items: list[dict]):
    movie_ids: list[int] = []
    other_ids: list[int] = []
    for item in all_items:
        if item.get("catalog") is not None:
            continue
        if item.get("media_type") == "movie":
            movie_ids.append(item["tmdb_id"])
        else:
            other_ids.append(item["tmdb_id"])
    fixed = len(movie_ids) + len(other_ids)
    if not fixed:
        return
    db = getDatabase("wanted")
    db_lock = getDatabaseLock("wanted")
    if db is None or db_lock is None:
        return
    q = Query()
    with db_lock:
        if movie_ids:
            db.update({"catalog": "nowPlaying"}, q.tmdb_id.one_of(movie_ids))
        if other_ids:
            db.update({"catalog": "popular"}, q.tmdb_id.one_of(other_ids))
    logger.info(f"Lifecycle: assigned catalog to {fixed} orphaned items.")


def _verifyAcquiringItems(
    library_index: LibraryIndex,
    snapshot_index: SnapshotIndex | None = None,
):
    all_items = getAllWanted()
    _assignOrphanedCatalogs(all_items)

    acquiring = [i for i in all_items if i.get("status") == "acquiring"]
    if not acquiring:
        retried = retryFailedWanted()
        if retried:
            logger.info(f"Lifecycle: retried {retried} previously failed items.")
        return

    now = datetime.now(timezone.utc)
    promoted = 0
    held = 0
    requeued = 0

    for item in acquiring:
        title = item.get("title", "")
        media_type = item.get("media_type", "movie")
        tmdb_id = item["tmdb_id"]

        if library_index.contains(title, media_type):
            updateWantedStatus(tmdb_id, "acquired")
            promoted += 1
            continue

        if snapshot_index is not None and snapshot_index.contains(title):
            held += 1
            continue

        stale_threshold = MOUNT_REFRESH_TIME * 3600 * 2
        last = item.get("last_attempt")
        if last:
            try:
                elapsed = (now - datetime.fromisoformat(last)).total_seconds()
                if elapsed < stale_threshold:
                    continue
            except (ValueError, TypeError):
                pass
        updateWantedStatus(tmdb_id, "pending")
        requeued += 1

    if promoted or requeued or held:
        logger.info(
            f"Lifecycle: {promoted} promoted, {held} held (cloud has it, awaiting metadata), {requeued} re-queued."
        )

    retried = retryFailedWanted()
    if retried:
        logger.info(f"Lifecycle: retried {retried} previously failed items.")


class AcquisitionContext(NamedTuple):
    library_index: LibraryIndex
    snapshot_index: SnapshotIndex
    in_flight: set[tuple]


def _setupAcquisitionCycle() -> AcquisitionContext:
    with timer("runAcquisition.cleanup") as f:
        _, removed_names = _cleanupDownloads()
        f["active_uncached"] = _active_uncached_count
        f["removed"] = len(removed_names)
    _resetCatalogIfNeeded()

    with timer("runAcquisition.requeueOrphans") as f:
        f["requeued"] = _requeueOrphanedAcquiring(removed_names)

    with timer("runAcquisition.snapshot") as f:
        local_records = loadAllLocalRecords()
        cloud_snapshot: list[dict] | None = None
        try:
            cloud_snapshot = fetchTorboxSnapshot()
        except Exception as e:
            logger.warning(f"Snapshot fetch failed; reusing existing index: {e}")

        snapshot_index = _getAcquisitionSnapshotIndex()
        if cloud_snapshot is not None:
            snapshot_index.applyCloudSnapshot(cloud_snapshot)
        snapshot_index.applyLocalRecords(local_records)
        f["cloud_items"] = len(cloud_snapshot) if cloud_snapshot is not None else 0
        f["titles"] = len(snapshot_index)

    with timer("runAcquisition.libraryIndex") as f:
        library_index = LibraryIndex(local_records=local_records)
        f["titles"] = len(getattr(library_index, "_titles", set()))
        f["episodes"] = len(getattr(library_index, "_episodes", set()))

    with timer("runAcquisition.verifyAcquiring"):
        _verifyAcquiringItems(library_index, snapshot_index=snapshot_index)

    return AcquisitionContext(library_index, snapshot_index, set())


def _processCatalog(catalog: str, budget: int, ctx: AcquisitionContext) -> int:
    remaining = _getCatalogRemaining(catalog)
    if remaining <= 0:
        return 0

    pending = getPendingWanted(catalog=catalog, limit=remaining)
    if not pending:
        return 0

    acquired = 0
    with timer("runAcquisition.catalog", catalog=catalog) as cat_fields:
        for item in pending:
            if _getCatalogRemaining(catalog) <= 0:
                break

            tmdb_id = item["tmdb_id"]
            media_type = item["media_type"]
            title = item.get("title", f"TMDB-{tmdb_id}")

            if MEDIA_FETCH_DEBUG:
                logger.debug(
                    f"acquiring catalog={catalog} title={title!r} media_type={media_type} tmdb_id={tmdb_id}"
                )

            try:
                if media_type == "movie":
                    success = _acquireMovie(
                        item,
                        catalog=catalog,
                        library_index=ctx.library_index,
                        snapshot_index=ctx.snapshot_index,
                        in_flight=ctx.in_flight,
                    )
                else:
                    success = _acquireSeries(
                        item,
                        catalog=catalog,
                        library_index=ctx.library_index,
                        snapshot_index=ctx.snapshot_index,
                        in_flight=ctx.in_flight,
                    )
                if success:
                    acquired += 1
            except Exception as e:
                logger.exception(f"Acquisition failed for {title}: {e}")
                updateWantedStatus(tmdb_id, "failed", failure_reason=str(e))

        cat_fields["pending"] = len(pending)
        cat_fields["acquired"] = acquired
        cat_fields["created"] = _catalog_create_count.get(catalog, 0)
        cat_fields["budget"] = budget

    if acquired and MEDIA_FETCH_DEBUG:
        logger.debug(
            f"catalog={catalog} acquired={acquired}/{len(pending)} budget_remaining={_getCatalogRemaining(catalog)}/{budget}"
        )
    return acquired


def runAcquisition():
    if not ENABLE_MEDIA_FETCH:
        return

    logger.info("Starting acquisition run...")

    with timer("runAcquisition") as outer:
        ctx = _setupAcquisitionCycle()

        if _getTotalRemaining() <= 0:
            outer["skipped"] = "budget_exhausted"
            logger.info("All catalog rate limits exhausted, skipping acquisition run.")
            return

        total_acquired = sum(
            _processCatalog(catalog, budget, ctx)
            for catalog, budget in CATALOG_BUDGETS.items()
        )

        outer["acquired"] = total_acquired
        logger.info(f"Acquisition run complete: {total_acquired} items acquired.")
