import re
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
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
    EXCLUDE_RESOLUTIONS,
    MOUNT_REFRESH_TIME,
)
from tinydb import Query
from functions.databaseFunctions import getDatabase, getDatabaseLock
from functions.wantFunctions import (
    getPendingWanted,
    updateWantedStatus,
    updateWantedField,
    isAlreadyInLibrary,
    LibraryIndex,
    getAllWanted,
    retryFailedWanted,
    _resolveImdbId,
)

logger = logging.getLogger(__name__)

CATALOG_BUDGETS: dict[str, int] = {
    "nowPlaying": DISCOVER_MOVIES_PER_RUN,
    "popular": DISCOVER_SERIES_EPISODES_PER_RUN,
    "anime": DISCOVER_ANIME_EPISODES_PER_RUN,
}

_catalog_creates: dict[str, int] = {k: 0 for k in CATALOG_BUDGETS}
_catalog_reset_time: float = 0.0
_active_uncached_count = 0

RESOLUTION_PRIORITY = {"2160p": 4, "1080p": 3, "720p": 2, "480p": 1}
QUALITY_PRIORITY = {"BluRay": 4, "Blu-ray": 4, "WEB-DL": 3, "WEBRip": 2, "HDTV": 1}
_RES_RE = re.compile(r"(2160|1080|720|480)p", re.IGNORECASE)


def _resetCatalogIfNeeded():
    global _catalog_creates, _catalog_reset_time
    now = time.time()
    if now >= _catalog_reset_time:
        _catalog_creates = {k: 0 for k in CATALOG_BUDGETS}
        _catalog_reset_time = now + 3600


def _getCatalogRemaining(catalog: str) -> int:
    _resetCatalogIfNeeded()
    budget = CATALOG_BUDGETS.get(catalog, 0)
    return budget - _catalog_creates.get(catalog, 0)


def _getTotalRemaining() -> int:
    _resetCatalogIfNeeded()
    return sum(
        CATALOG_BUDGETS[k] - _catalog_creates.get(k, 0)
        for k in CATALOG_BUDGETS
    )


def _decrementCatalog(catalog: str):
    _catalog_creates[catalog] = _catalog_creates.get(catalog, 0) + 1


def _cleanupDownloads() -> int:
    global _active_uncached_count
    try:
        resp = requestWrapper(
            api_http_client, "GET", "/torrents/mylist",
            params={"limit": 1000, "offset": 0},
        )
        data = resp.json()
        if not data.get("success"):
            _active_uncached_count = 0
            return 0
        items = data.get("data", [])
    except Exception as e:
        logger.error(f"Cleanup: failed to fetch torrents: {e}")
        _active_uncached_count = 0
        return 0

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
                    if age_seconds > 120:
                        should_remove = True
                        reason = f"status '{status}' for {age_seconds:.0f}s"
                except (ValueError, TypeError):
                    should_remove = True
                    reason = f"status '{status}' (unparseable timestamp)"
            else:
                should_remove = True
                reason = f"status '{status}'"

        if not should_remove and speed_mbs < MIN_DOWNLOAD_SPEED_MBS and status not in ("", "downloading"):
            should_remove = True
            reason = f"speed {speed_mbs:.2f} MB/s < {MIN_DOWNLOAD_SPEED_MBS} MB/s"

        if should_remove and torrent_id:
            try:
                requestWrapper(
                    api_http_client, "POST", "/torrents/controltorrent",
                    json={"torrent_id": torrent_id, "operation": "delete"},
                    use_cache=False,
                )
                removed += 1
                logger.info(f"Cleanup: removed torrent '{name}' — {reason}")
            except Exception as e:
                logger.warning(f"Cleanup: failed to remove torrent {torrent_id}: {e}")

    active = sum(1 for t in items if not t.get("cached", False)) - removed
    _active_uncached_count = max(0, active)

    if removed:
        logger.info(f"Cleanup: removed {removed} unhealthy torrents. Active uncached: {_active_uncached_count}")
    elif MEDIA_FETCH_DEBUG:
        logger.debug(f"Cleanup: no unhealthy torrents found. Active uncached: {_active_uncached_count}")

    return _active_uncached_count


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
            tbm_http_client, "GET",
            f"/api/usenet/search?query=imdb%3A{imdb_id}&search_user_engines=false",
        )
        data = resp.json()
        if not data.get("data"):
            return [], True
        nzbs = data["data"].get("nzbs", [])
        filtered = [n for n in nzbs if n.get("cached", False) and n.get("type") == "usenet"]
        if EXCLUDE_RESOLUTIONS:
            before = len(filtered)
            filtered = [
                n for n in filtered
                if (n.get("title_parsed_data", {}).get("resolution") or "").lower() not in EXCLUDE_RESOLUTIONS
            ]
            if before - len(filtered):
                logger.info(f"Usenet: filtered {before - len(filtered)} nzbs matching EXCLUDE_RESOLUTIONS")
        return filtered, True
    except Exception as e:
        logger.warning(f"Usenet search failed for {imdb_id}: {e}")
        return [], False


def _createUsenetDownload(nzb_url: str, name: str, catalog: str = "nowPlaying") -> tuple[bool, int | None, bool]:
    if _getCatalogRemaining(catalog) <= 0:
        logger.warning(f"Rate limit exhausted for {catalog}, skipping usenet create.")
        return False, None, False
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
            logger.info(f"Created usenet download: {name} (id={dl_id}, cached={cached})")
            return True, dl_id, cached
        logger.warning(f"Usenet create failed: {result.get('detail', result)}")
        return False, None, False
    except Exception as e:
        logger.error(f"Usenet create error: {e}")
        return False, None, False


def _parseAiostreamsBaseUrls() -> list[str]:
    bases = []
    for url in AIOSTREAMS_URLS:
        base = url.replace("/manifest.json", "")
        bases.append(base)
    return bases


def _streamResolution(stream: dict) -> str | None:
    hints = stream.get("behaviorHints", {})
    text = hints.get("filename", "") or stream.get("name", "") or stream.get("description", "")
    m = _RES_RE.search(text)
    return f"{m.group(1)}p".lower() if m else None


def _scoreAiostream(stream: dict) -> tuple[int, int]:
    res = _streamResolution(stream)
    res_score = RESOLUTION_PRIORITY.get(res, 0) if res else 0
    size = stream.get("behaviorHints", {}).get("videoSize") or 0
    return (res_score, size)


def _searchAiostreams(media_type: str, tmdb_id: int, season: int | None = None, episode: int | None = None) -> list[dict]:
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
                logger.info(f"AIOStreams: {len(streams)} streams from {base} for {id_str}")
                return streams
            logger.info(f"AIOStreams: HTTP {resp.status_code} from {base} for {id_str}")
        except Exception as e:
            logger.warning(f"AIOStreams query failed ({base}): {e}")
        return []

    with ThreadPoolExecutor(max_workers=len(bases)) as pool:
        for streams in pool.map(_fetch, bases):
            all_streams.extend(streams)

    result = [s for s in all_streams if s.get("infoHash") or s.get("nzbUrl") or s.get("url")]

    if EXCLUDE_RESOLUTIONS:
        before = len(result)
        result = [s for s in result if (_streamResolution(s) or "") not in EXCLUDE_RESOLUTIONS]
        excluded = before - len(result)
        if excluded:
            logger.info(f"AIOStreams: filtered {excluded} streams matching EXCLUDE_RESOLUTIONS={sorted(EXCLUDE_RESOLUTIONS)}")

    if not result:
        logger.info(f"AIOStreams: 0 usable streams for {stremio_type}/{id_str} (raw: {len(all_streams)})")
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


def _createTorrentDownload(info_hash: str, name: str, catalog: str = "nowPlaying") -> tuple[bool, int | None, bool]:
    global _active_uncached_count
    if _getCatalogRemaining(catalog) <= 0:
        logger.warning(f"Rate limit exhausted for {catalog}, skipping torrent create.")
        return False, None, False

    if _active_uncached_count >= 10:
        logger.warning(f"Active torrent cap reached ({_active_uncached_count}/10), skipping.")
        return False, None, False

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
            logger.info(f"Created torrent download: {name} (id={dl_id}, cached={cached})")
            return True, dl_id, cached
        logger.warning(f"Torrent create failed: {result.get('detail', result)}")
        return False, None, False
    except Exception as e:
        logger.error(f"Torrent create error: {e}")
        return False, None, False


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

    time.sleep(10)

    by_type: dict[str, list[int]] = {}
    for dl_id, dl_type in pending:
        by_type.setdefault(dl_type, []).append(dl_id)

    results: dict[int, bool | None] = {}
    for dl_type, ids in by_type.items():
        id_set = set(ids)
        try:
            resp = requestWrapper(
                api_http_client, "GET", f"/{dl_type}/mylist",
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
                        logger.info(f"Download {dl_id} uncached but speed {speed:.1f} MB/s — keeping.")
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
        logger.info(f"AIOStreams URL not cached for '{name}' (status {resp.status_code}, location: {location[:80]})")
        return False
    except Exception as e:
        logger.warning(f"AIOStreams URL error for '{name}': {e}")
        return False


def _tryAiostreams(
    streams: list[dict], name: str, catalog: str,
) -> tuple[bool, tuple[int, str, str] | None]:
    """Try AIOStreams. URL streams verified inline; torrent/usenet deferred.

    Returns (confirmed, pending). pending = (dl_id, dl_type, stream_name) if
    a download was created but needs batch verification.
    """
    for stream in streams:
        stream_name = stream.get("behaviorHints", {}).get("filename", name)
        info_hash = stream.get("infoHash")
        nzb_url = stream.get("nzbUrl")
        playback_url = stream.get("url")

        if playback_url and not info_hash and not nzb_url:
            if _tryAiostreamUrl(playback_url, stream_name, catalog):
                return True, None
            continue

        if info_hash:
            success, dl_id, cached = _createTorrentDownload(info_hash, stream_name, catalog=catalog)
            dl_type = "torrents"
        elif nzb_url:
            success, dl_id, cached = _createUsenetDownload(nzb_url, stream_name, catalog=catalog)
            dl_type = "usenet"
        else:
            continue

        if not success:
            continue
        if cached:
            return True, None
        if dl_id is not None:
            return False, (dl_id, dl_type, stream_name)
        return True, None

    return False, None


def _resolveVerifications(
    pending: list[tuple[int, str, str]],
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
            logger.info(f"Batch verify: uncached/slow {dl_type} {dl_id} for '{dl_name}', removed.")

    return confirmed


def _acquireMovie(item: dict, catalog: str, library_index: LibraryIndex | None = None) -> bool:
    tmdb_id = item["tmdb_id"]
    imdb_id = item.get("imdb_id")
    title = item.get("title", f"TMDB-{tmdb_id}")

    if not imdb_id:
        imdb_id = _resolveImdbId(tmdb_id, "movie")
        if imdb_id:
            updateWantedField(tmdb_id, "imdb_id", imdb_id)

    if isAlreadyInLibrary(imdb_id, title, "movie", library_index=library_index):
        updateWantedStatus(tmdb_id, "acquired")
        return True

    streams = _searchAiostreams("movie", tmdb_id)
    confirmed, pending = _tryAiostreams(streams, title, catalog)
    if confirmed:
        updateWantedStatus(tmdb_id, "acquiring")
        return True

    if pending:
        confirmed_ids = _resolveVerifications([pending])
        if pending[0] in confirmed_ids:
            updateWantedStatus(tmdb_id, "acquiring")
            return True

    if imdb_id:
        nzbs, _ = _searchUsenet(imdb_id)
        if nzbs:
            nzbs.sort(key=_scoreNzb, reverse=True)
            best = nzbs[0]
            nzb_url = best.get("nzb")
            raw_title = best.get("raw_title", title)
            if nzb_url:
                success, _, _ = _createUsenetDownload(nzb_url, raw_title, catalog=catalog)
                if success:
                    updateWantedStatus(tmdb_id, "acquiring")
                    return True

    if not imdb_id:
        updateWantedStatus(tmdb_id, "deferred", failure_reason="IMDB ID resolve failed; will retry")
    else:
        updateWantedStatus(tmdb_id, "failed", failure_reason="No cached stream or usenet found")
    return False


def _acquireSeries(item: dict, catalog: str, library_index: LibraryIndex | None = None) -> bool:
    tmdb_id = item["tmdb_id"]
    imdb_id = item.get("imdb_id")
    title = item.get("title", f"TMDB-{tmdb_id}")
    seasons_needed = item.get("seasons_needed", [])

    if not imdb_id:
        imdb_id = _resolveImdbId(tmdb_id, "series")
        if imdb_id:
            updateWantedField(tmdb_id, "imdb_id", imdb_id)

    if not seasons_needed:
        updateWantedStatus(tmdb_id, "failed", failure_reason="No seasons specified")
        return False

    season_episodes: dict[int, list[dict]] = {}
    for season_num in seasons_needed:
        try:
            season_data = _tmdbGetSeason(tmdb_id, season_num)
            season_episodes[season_num] = season_data.get("episodes", [])
        except Exception as e:
            logger.warning(f"Failed to get season {season_num} for {title}: {e}")

    confirmed_eps: set[tuple[int, int]] = set()
    pending_verify: list[tuple[int, str, str, int, int]] = []
    budget_exhausted = False

    for season_num, episodes in season_episodes.items():
        if budget_exhausted:
            break
        for ep in episodes:
            ep_num = ep.get("episode_number")
            if ep_num is None:
                continue
            if isAlreadyInLibrary(imdb_id, title, "series", season=season_num, episode=ep_num, library_index=library_index):
                continue
            if _getCatalogRemaining(catalog) <= 0:
                logger.warning(f"Rate limit exhausted for {catalog} during series acquisition.")
                budget_exhausted = True
                break

            ep_label = f"{title} S{season_num:02d}E{ep_num:02d}"
            streams = _searchAiostreams("series", tmdb_id, season=season_num, episode=ep_num)
            confirmed, pending = _tryAiostreams(streams, ep_label, catalog)
            if confirmed:
                confirmed_eps.add((season_num, ep_num))
            elif pending:
                pending_verify.append((pending[0], pending[1], pending[2], season_num, ep_num))

    if pending_verify:
        confirmed_ids = _resolveVerifications(
            [(dl_id, dl_type, dl_name) for dl_id, dl_type, dl_name, _, _ in pending_verify],
        )
        for dl_id, _, _, s_num, e_num in pending_verify:
            if dl_id in confirmed_ids:
                confirmed_eps.add((s_num, e_num))

    if imdb_id:
        nzb_results: list[dict] = []
        nzb_fetched = False
        for season_num, episodes in season_episodes.items():
            if budget_exhausted:
                break
            for ep in episodes:
                ep_num = ep.get("episode_number")
                if ep_num is None:
                    continue
                if (season_num, ep_num) in confirmed_eps:
                    continue
                if isAlreadyInLibrary(imdb_id, title, "series", season=season_num, episode=ep_num, library_index=library_index):
                    continue
                if _getCatalogRemaining(catalog) <= 0:
                    budget_exhausted = True
                    break

                if not nzb_fetched:
                    nzb_results, _ = _searchUsenet(imdb_id)
                    nzb_fetched = True

                matched_nzbs = [
                    n for n in nzb_results
                    if n.get("title_parsed_data", {}).get("season") == season_num
                    and n.get("title_parsed_data", {}).get("episode") == ep_num
                ]
                if matched_nzbs:
                    matched_nzbs.sort(key=_scoreNzb, reverse=True)
                    best = matched_nzbs[0]
                    nzb_url = best.get("nzb")
                    raw_title = best.get("raw_title", f"{title} S{season_num:02d}E{ep_num:02d}")
                    if nzb_url:
                        success, _, _ = _createUsenetDownload(nzb_url, raw_title, catalog=catalog)
                        if success:
                            confirmed_eps.add((season_num, ep_num))

    any_acquired = bool(confirmed_eps)

    if budget_exhausted:
        updateWantedStatus(tmdb_id, "pending")
    elif any_acquired:
        updateWantedStatus(tmdb_id, "acquiring")
    elif not imdb_id:
        updateWantedStatus(tmdb_id, "deferred", failure_reason="IMDB ID resolve failed; will retry")
    else:
        updateWantedStatus(tmdb_id, "failed", failure_reason="No episodes could be acquired")

    return any_acquired


def _tmdbGetSeason(tmdb_id: int, season_num: int) -> dict:
    resp = requestWrapper(
        tmdb_http_client, "GET",
        f"/tv/{tmdb_id}/season/{season_num}",
        params={"api_key": TMDB_API_KEY, "language": "en-US"},
    )
    return resp.json()


def _assignOrphanedCatalogs(all_items: list[dict]):
    fixed = 0
    db = getDatabase("wanted")
    db_lock = getDatabaseLock("wanted")
    if db is None or db_lock is None:
        return
    with db_lock:
        q = Query()
        for item in all_items:
            if item.get("catalog") is not None:
                continue
            catalog = "nowPlaying" if item.get("media_type") == "movie" else "popular"
            db.update({"catalog": catalog}, q.tmdb_id == item["tmdb_id"])
            fixed += 1
    if fixed:
        logger.info(f"Lifecycle: assigned catalog to {fixed} orphaned items.")


def _verifyAcquiringItems(library_index: LibraryIndex):
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
    requeued = 0

    for item in acquiring:
        title = item.get("title", "")
        media_type = item.get("media_type", "movie")
        tmdb_id = item["tmdb_id"]

        if library_index.contains(title, media_type):
            updateWantedStatus(tmdb_id, "acquired")
            promoted += 1
        else:
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

    if promoted or requeued:
        logger.info(f"Lifecycle: {promoted} promoted to acquired, {requeued} re-queued to pending.")

    retried = retryFailedWanted()
    if retried:
        logger.info(f"Lifecycle: retried {retried} previously failed items.")


def runAcquisition():
    if not ENABLE_MEDIA_FETCH:
        return

    logger.info("Starting acquisition run...")

    _cleanupDownloads()
    _resetCatalogIfNeeded()

    library_index = LibraryIndex()

    _verifyAcquiringItems(library_index)

    if _getTotalRemaining() <= 0:
        logger.info("All catalog rate limits exhausted, skipping acquisition run.")
        return

    total_acquired = 0

    for catalog, budget in CATALOG_BUDGETS.items():
        remaining = _getCatalogRemaining(catalog)
        if remaining <= 0:
            continue

        pending = getPendingWanted(catalog=catalog, limit=remaining)
        if not pending:
            continue

        acquired = 0
        for item in pending:
            if _getCatalogRemaining(catalog) <= 0:
                break

            tmdb_id = item["tmdb_id"]
            media_type = item["media_type"]
            title = item.get("title", f"TMDB-{tmdb_id}")

            if MEDIA_FETCH_DEBUG:
                logger.debug(f"Acquiring [{catalog}]: {title} ({media_type}, TMDB {tmdb_id})")

            try:
                if media_type == "movie":
                    success = _acquireMovie(item, catalog=catalog, library_index=library_index)
                else:
                    success = _acquireSeries(item, catalog=catalog, library_index=library_index)
                if success:
                    acquired += 1
            except Exception as e:
                logger.error(f"Acquisition failed for {title}: {e}")
                updateWantedStatus(tmdb_id, "failed", failure_reason=str(e))

        total_acquired += acquired
        if acquired and MEDIA_FETCH_DEBUG:
            logger.debug(f"Catalog {catalog}: {acquired}/{len(pending)} acquired, {_getCatalogRemaining(catalog)}/{budget} budget remaining.")

    logger.info(f"Acquisition run complete: {total_acquired} items acquired.")
