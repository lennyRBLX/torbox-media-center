import PTN
import threading
from collections import OrderedDict
from typing import NamedTuple
from functions.databaseFunctions import (
    getDatabase,
    getDatabaseLock,
    getAllData,
    upsertData,
)
from library.http import tmdb_http_client, api_http_client, requestWrapper
from library.app import TMDB_API_KEY, MEDIA_FETCH_DEBUG
from library.profiling import timer
from functions.mediaFunctions import normaliseTitle, cleanTitle
from functions.torboxFunctions import NON_EPISODE_TAG_RE
from tinydb import Query
from datetime import datetime, timezone
import logging

WANTED_DB = "wanted"
_LOCAL_DB_TYPES = ("torrents", "usenet", "webdl")

logger = logging.getLogger("want")

_IMDB_CACHE_CAPACITY = 10_000
_imdb_cache: "OrderedDict[tuple[int, str], str | None]" = OrderedDict()
_imdb_cache_lock = threading.Lock()


def _imdbCachePut(key: tuple[int, str], value: str | None) -> None:
    with _imdb_cache_lock:
        if key in _imdb_cache:
            _imdb_cache.move_to_end(key)
        _imdb_cache[key] = value
        while len(_imdb_cache) > _IMDB_CACHE_CAPACITY:
            _imdb_cache.popitem(last=False)


def _imdbCacheGet(key: tuple[int, str]) -> tuple[bool, str | None]:
    with _imdb_cache_lock:
        if key not in _imdb_cache:
            return False, None
        _imdb_cache.move_to_end(key)
        return True, _imdb_cache[key]


def primeImdbCache(tmdb_id: int, media_type: str, imdb_id: str | None) -> None:
    _imdbCachePut((tmdb_id, media_type), imdb_id)


def loadAllLocalRecords() -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for db_type in _LOCAL_DB_TYPES:
        data, ok, _ = getAllData(db_type)
        out[db_type] = data if ok and data else []
    return out


OwnerContribution = tuple[frozenset[str], frozenset[tuple[str, int]]]


def _parseSnapshotItem(item: dict) -> OwnerContribution:
    titles: set[str] = set()
    seasons: set[tuple[str, int]] = set()
    for field in ("name", "file_name"):
        raw = item.get(field, "")
        if not raw:
            continue
        if field == "file_name" and NON_EPISODE_TAG_RE.search(raw):
            continue
        parsed = PTN.parse(raw)
        ptn_title = parsed.get("title", "")
        t = normaliseTitle(ptn_title) or normaliseTitle(cleanTitle(raw))
        if not t:
            continue
        titles.add(t)
        season = parsed.get("season")
        if isinstance(season, int):
            seasons.add((t, season))
        elif isinstance(season, list):
            for s in season:
                if isinstance(s, int):
                    seasons.add((t, s))
    return frozenset(titles), frozenset(seasons)


def _parseLocalRecord(record: dict) -> OwnerContribution:
    m = normaliseTitle(record.get("metadata_title") or "")
    if not m:
        return frozenset(), frozenset()
    s = record.get("metadata_season")
    if isinstance(s, list):
        s = s[0] if s else None
    seasons = frozenset({(m, s)}) if isinstance(s, int) else frozenset()
    return frozenset({m}), seasons


class SnapshotDelta(NamedTuple):
    added: int
    removed: int


class SnapshotIndex:
    """Long-lived dedup index for cloud snapshot + local DB records.

    Mutated incrementally via applyCloudSnapshot / applyLocalRecords;
    rebuilt-from-scratch construction is no longer required.

    Each cloud item or local record is an *owner* contributing parsed titles.
    Refcounting ensures a title only drops when its last owner does.
    markPresent() adds ephemeral in-flight markings cleared on each cloud apply.
    """

    __slots__ = (
        "_titles",
        "_title_seasons",
        "_title_refs",
        "_title_season_refs",
        "_cloud_owners",
        "_local_owners",
        "_inflight_titles",
        "_inflight_seasons",
        "_lock",
    )

    def __init__(
        self,
        snapshot: list[dict] | None = None,
        local_records: dict[str, list[dict]] | None = None,
    ):
        self._titles: set[str] = set()
        self._title_seasons: set[tuple[str, int]] = set()
        self._title_refs: dict[str, int] = {}
        self._title_season_refs: dict[tuple[str, int], int] = {}
        self._cloud_owners: dict[object, OwnerContribution] = {}
        self._local_owners: dict[tuple[str, str], OwnerContribution] = {}
        self._inflight_titles: set[str] = set()
        self._inflight_seasons: set[tuple[str, int]] = set()
        self._lock = threading.Lock()
        if snapshot:
            self.applyCloudSnapshot(snapshot)
        if local_records is not None:
            self.applyLocalRecords(local_records)

    def __len__(self) -> int:
        with self._lock:
            return len(self._titles) + len(self._inflight_titles - self._titles)

    def _addTitle(self, t: str) -> None:
        self._title_refs[t] = self._title_refs.get(t, 0) + 1
        self._titles.add(t)

    def _dropTitle(self, t: str) -> None:
        c = self._title_refs.get(t, 0)
        if c <= 1:
            self._title_refs.pop(t, None)
            self._titles.discard(t)
        else:
            self._title_refs[t] = c - 1

    def _addSeason(self, key: tuple[str, int]) -> None:
        self._title_season_refs[key] = self._title_season_refs.get(key, 0) + 1
        self._title_seasons.add(key)

    def _dropSeason(self, key: tuple[str, int]) -> None:
        c = self._title_season_refs.get(key, 0)
        if c <= 1:
            self._title_season_refs.pop(key, None)
            self._title_seasons.discard(key)
        else:
            self._title_season_refs[key] = c - 1

    def applyCloudSnapshot(self, snapshot: list[dict]) -> SnapshotDelta:
        with timer("snapshot.applyCloud", items=len(snapshot)) as t:
            new_keys: set[object] = set()
            parsed_new: dict[object, OwnerContribution] = {}
            for item in snapshot:
                key = item.get("id")
                if key is None:
                    continue
                new_keys.add(key)
                if key not in self._cloud_owners and key not in parsed_new:
                    parsed_new[key] = _parseSnapshotItem(item)

            with self._lock:
                cur_keys = set(self._cloud_owners.keys())
                removed_keys = cur_keys - new_keys
                for key in removed_keys:
                    titles, seasons = self._cloud_owners.pop(key)
                    for title in titles:
                        self._dropTitle(title)
                    for season in seasons:
                        self._dropSeason(season)
                for key, contribution in parsed_new.items():
                    self._cloud_owners[key] = contribution
                    titles, seasons = contribution
                    for title in titles:
                        self._addTitle(title)
                    for season in seasons:
                        self._addSeason(season)
                self._inflight_titles.clear()
                self._inflight_seasons.clear()
                t["added"] = len(parsed_new)
                t["removed"] = len(removed_keys)
                t["titles"] = len(self._titles)
                return SnapshotDelta(added=len(parsed_new), removed=len(removed_keys))

    def applyLocalRecords(
        self, local_records: dict[str, list[dict]]
    ) -> SnapshotDelta:
        with timer("snapshot.applyLocal") as t:
            new_keys: set[tuple[str, str]] = set()
            parsed_new: dict[tuple[str, str], OwnerContribution] = {}
            for db_type in _LOCAL_DB_TYPES:
                for record in local_records.get(db_type, ()):
                    stable_key = record.get("stable_key")
                    if not stable_key:
                        continue
                    key = (db_type, stable_key)
                    new_keys.add(key)
                    if key not in self._local_owners:
                        parsed_new[key] = _parseLocalRecord(record)

            with self._lock:
                cur_keys = set(self._local_owners.keys())
                removed_keys = cur_keys - new_keys
                for key in removed_keys:
                    titles, seasons = self._local_owners.pop(key)
                    for title in titles:
                        self._dropTitle(title)
                    for season in seasons:
                        self._dropSeason(season)
                for key, contribution in parsed_new.items():
                    self._local_owners[key] = contribution
                    titles, seasons = contribution
                    for title in titles:
                        self._addTitle(title)
                    for season in seasons:
                        self._addSeason(season)
                t["added"] = len(parsed_new)
                t["removed"] = len(removed_keys)
                return SnapshotDelta(added=len(parsed_new), removed=len(removed_keys))

    def contains(self, title: str, season: int | None = None) -> bool:
        if not title:
            return False
        n = normaliseTitle(title)
        with self._lock:
            if season is not None and (self._title_seasons or self._inflight_seasons):
                return (n, season) in self._title_seasons or (
                    n,
                    season,
                ) in self._inflight_seasons
            return n in self._titles or n in self._inflight_titles

    def markPresent(self, title: str, season: int | None = None) -> None:
        n = normaliseTitle(title)
        if not n:
            return
        with self._lock:
            self._inflight_titles.add(n)
            if isinstance(season, int):
                self._inflight_seasons.add((n, season))


class LibraryIndex:
    __slots__ = ("_titles", "_seasons", "_episodes", "_season_packs")

    def __init__(self, local_records: dict[str, list[dict]] | None = None):
        with timer("LibraryIndex.init") as f:
            self._titles: set[str] = set()
            self._seasons: set[tuple[str, int]] = set()
            self._episodes: set[tuple[str, int, int]] = set()
            self._season_packs: set[tuple[str, int]] = set()

            records = (
                local_records if local_records is not None else loadAllLocalRecords()
            )
            for db_type in _LOCAL_DB_TYPES:
                data = records.get(db_type, ())
                if not data:
                    continue
                for item in data:
                    title = normaliseTitle(item.get("metadata_title") or "")
                    if not title:
                        continue
                    self._titles.add(title)
                    season = item.get("metadata_season")
                    episode = item.get("metadata_episode")
                    if isinstance(season, list):
                        season = season[0] if season else None
                    if not isinstance(season, int):
                        continue
                    self._seasons.add((title, season))
                    if isinstance(episode, list):
                        eps = [e for e in episode if isinstance(e, int)]
                    elif isinstance(episode, int):
                        eps = [episode]
                    else:
                        eps = []
                    if eps:
                        for e in eps:
                            self._episodes.add((title, season, e))
                    else:
                        self._season_packs.add((title, season))
            f["titles"] = len(self._titles)
            f["episodes"] = len(self._episodes)

    def contains(
        self,
        title: str,
        media_type: str = "movie",
        season: int | None = None,
        episode: int | None = None,
    ) -> bool:
        if not title:
            return False
        t = normaliseTitle(title)
        if media_type == "movie":
            return t in self._titles
        if episode is not None and season is not None:
            if (t, season) in self._season_packs:
                return True
            return (t, season, episode) in self._episodes
        if season is not None:
            return (t, season) in self._seasons
        return t in self._titles


def resolveImdbId(tmdb_id: int, media_type: str) -> str | None:
    cache_key = (tmdb_id, media_type)
    hit, value = _imdbCacheGet(cache_key)
    if hit:
        return value
    endpoint = "movie" if media_type == "movie" else "tv"
    try:
        resp = requestWrapper(
            tmdb_http_client,
            "GET",
            f"/{endpoint}/{tmdb_id}/external_ids",
            params={"api_key": TMDB_API_KEY},
        )
        data = resp.json()
        imdb_id = data.get("imdb_id")
    except Exception as e:
        logger.warning(f"Failed to resolve IMDB ID for TMDB {tmdb_id}: {e}")
        imdb_id = None
    _imdbCachePut(cache_key, imdb_id)
    return imdb_id


def isAnime(detail: dict) -> bool:
    if "JP" not in (detail.get("origin_country") or []):
        return False
    return any(g.get("id") == 16 for g in (detail.get("genres") or []))


def findAbsoluteGroupId(tmdb_id: int) -> str | None:
    try:
        resp = requestWrapper(
            tmdb_http_client,
            "GET",
            f"/tv/{tmdb_id}/episode_groups",
            params={"api_key": TMDB_API_KEY},
        )
        results = resp.json().get("results") or []
    except Exception as e:
        logger.debug(f"episode_groups lookup failed for tmdb={tmdb_id}: {e}")
        return None
    preferred = next(
        (
            g
            for g in results
            if g.get("type") == 2 and "no specials" in (g.get("name") or "").lower()
        ),
        None,
    )
    if preferred:
        return preferred.get("id")
    fallback = next((g for g in results if g.get("type") == 2), None)
    return fallback.get("id") if fallback else None


def fetchAbsoluteEpisodes(group_id: str) -> list[dict]:
    try:
        resp = requestWrapper(
            tmdb_http_client,
            "GET",
            f"/tv/episode_group/{group_id}",
            params={"api_key": TMDB_API_KEY},
        )
        groups = resp.json().get("groups") or []
    except Exception as e:
        logger.debug(f"episode_group fetch failed for group={group_id}: {e}")
        return []
    out: list[dict] = []
    for g in sorted(groups, key=lambda x: x.get("order", 0)):
        out.extend(g.get("episodes") or [])
    return out


def pruneInvalidSeasons(tmdb_id: int, invalid_seasons: list[int]) -> None:
    if not invalid_seasons:
        return
    db = getDatabase(WANTED_DB)
    db_lock = getDatabaseLock(WANTED_DB)
    if db is None or db_lock is None:
        return
    invalid_set = set(invalid_seasons)
    q = Query()
    with db_lock:
        rec = db.get(q.tmdb_id == tmdb_id)
        if not rec:
            return
        remaining = [
            s for s in (rec.get("seasons_needed") or []) if s not in invalid_set
        ]
        if remaining:
            db.update({"seasons_needed": remaining}, q.tmdb_id == tmdb_id)
        else:
            db.update(
                {"status": "acquired", "seasons_needed": []}, q.tmdb_id == tmdb_id
            )
    logger.info(
        f"Pruned non-existent seasons {sorted(invalid_set)} from want record tmdb={tmdb_id}"
    )


def fetchTorboxSnapshot() -> list[dict]:
    items = []
    for endpoint in ("/torrents/mylist", "/usenet/mylist"):
        offset = 0
        limit = 1000
        while True:
            try:
                resp = requestWrapper(
                    api_http_client,
                    "GET",
                    endpoint,
                    params={"limit": limit, "offset": offset},
                )
                data = resp.json()
                if not data.get("success") or not data.get("data"):
                    break
                page = data["data"]
                for dl in page:
                    if not dl.get("cached", False):
                        continue
                    for f in dl.get("files", []):
                        items.append(
                            {
                                "id": dl.get("id"),
                                "name": dl.get("name", ""),
                                "hash": dl.get("hash", ""),
                                "file_name": f.get("name", ""),
                                "type": "torrents"
                                if "torrent" in endpoint
                                else "usenet",
                            }
                        )
                if len(page) < limit:
                    break
                offset += limit
            except Exception as e:
                logger.warning(f"Failed to fetch TorBox snapshot from {endpoint}: {e}")
                break
    return items


def isAlreadyInLibrary(
    imdb_id: str | None,
    title: str | None,
    media_type: str,
    season: int | None = None,
    episode: int | None = None,
    snapshot_index: SnapshotIndex | None = None,
    library_index: LibraryIndex | None = None,
) -> bool:
    if snapshot_index is not None:
        return snapshot_index.contains(title, season=season)

    if library_index is not None:
        return library_index.contains(title, media_type, season, episode)

    for db_type in ("torrents", "usenet", "webdl"):
        data, success, _ = getAllData(db_type)
        if not success or not data:
            continue
        for item in data:
            matched = False
            if not matched and title:
                item_title = item.get("metadata_title", "")
                if item_title and title.lower() == item_title.lower():
                    matched = True
            if not matched:
                continue

            if media_type == "movie":
                return True
            if season is not None and item.get("metadata_season") != season:
                continue
            if episode is not None and item.get("metadata_episode") != episode:
                continue
            return True
    return False


def addWanted(
    tmdb_id: int,
    media_type: str,
    title: str | None = None,
    year: int | None = None,
    priority: int = 10,
    seasons_needed: list[int] | None = None,
    catalog: str | None = None,
    imdb_id: str | None = None,
    absolute_group_id: str | None = None,
) -> tuple[bool, str]:
    db = getDatabase(WANTED_DB)
    db_lock = getDatabaseLock(WANTED_DB)
    if db is None or db_lock is None:
        return False, "Database connection failed."

    with db_lock:
        q = Query()
        existing = db.search(q.tmdb_id == tmdb_id)
        if existing and existing[0].get("status") in (
            "acquired",
            "acquiring",
            "pending",
        ):
            return False, f"Already {existing[0]['status']}."

    if imdb_id is None:
        imdb_id = resolveImdbId(tmdb_id, media_type)

    record = {
        "tmdb_id": tmdb_id,
        "imdb_id": imdb_id,
        "media_type": media_type,
        "title": title,
        "year": year,
        "status": "pending",
        "priority": priority,
        "seasons_needed": seasons_needed,
        "catalog": catalog,
        "added_at": datetime.now(timezone.utc).isoformat(),
        "last_attempt": None,
        "failure_reason": None,
        "absolute_group_id": absolute_group_id,
    }

    if existing:
        existing_record = existing[0]
        record["added_at"] = existing_record.get("added_at", record["added_at"])
        if existing_record.get("priority", 0) > priority:
            record["priority"] = existing_record["priority"]
        if seasons_needed and existing_record.get("seasons_needed"):
            merged = sorted(
                set(existing_record["seasons_needed"]) | set(seasons_needed)
            )
            record["seasons_needed"] = merged

    success, detail = upsertData(record, WANTED_DB, ["tmdb_id"])
    if success and MEDIA_FETCH_DEBUG:
        logger.debug(f"Added to want queue: {title} (TMDB {tmdb_id}, {media_type})")
    return success, detail


def getPendingWanted(limit: int = 5, catalog: str | None = None) -> list[dict]:
    db = getDatabase(WANTED_DB)
    db_lock = getDatabaseLock(WANTED_DB)
    if db is None or db_lock is None:
        return []

    with db_lock:
        q = Query()
        if catalog is not None:
            pending = db.search((q.status == "pending") & (q.catalog == catalog))
        else:
            pending = db.search(q.status == "pending")

    pending.sort(key=lambda x: (-x.get("priority", 0), x.get("last_attempt") or ""))
    return pending[:limit]


def updateWantedStatus(
    tmdb_id: int,
    status: str,
    failure_reason: str | None = None,
) -> tuple[bool, str]:
    db = getDatabase(WANTED_DB)
    db_lock = getDatabaseLock(WANTED_DB)
    if db is None or db_lock is None:
        return False, "Database connection failed."

    with db_lock:
        q = Query()
        updates = {
            "status": status,
            "last_attempt": datetime.now(timezone.utc).isoformat(),
        }
        if failure_reason is not None:
            updates["failure_reason"] = failure_reason
        try:
            db.update(updates, q.tmdb_id == tmdb_id)
            return True, f"Updated {tmdb_id} to {status}."
        except Exception as e:
            return False, f"Error updating status: {e}"


def updateWantedField(tmdb_id: int, field: str, value) -> tuple[bool, str]:
    db = getDatabase(WANTED_DB)
    db_lock = getDatabaseLock(WANTED_DB)
    if db is None or db_lock is None:
        return False, "Database connection failed."

    with db_lock:
        q = Query()
        try:
            db.update({field: value}, q.tmdb_id == tmdb_id)
            return True, f"Updated {tmdb_id}.{field}."
        except Exception as e:
            return False, f"Error updating {field}: {e}"


def getAcquiredWanted() -> list[dict]:
    db = getDatabase(WANTED_DB)
    db_lock = getDatabaseLock(WANTED_DB)
    if db is None or db_lock is None:
        return []

    with db_lock:
        q = Query()
        return db.search((q.status == "acquired") | (q.status == "acquiring"))


def retryFailedWanted(cooldown_hours: int = 6, deferred_cooldown_hours: int = 1) -> int:
    db = getDatabase(WANTED_DB)
    db_lock = getDatabaseLock(WANTED_DB)
    if db is None or db_lock is None:
        return 0

    now = datetime.now(timezone.utc)
    retried = 0

    with db_lock:
        q = Query()
        candidates = db.search((q.status == "failed") | (q.status == "deferred"))

    for item in candidates:
        cd = (
            deferred_cooldown_hours
            if item.get("status") == "deferred"
            else cooldown_hours
        )
        last_attempt = item.get("last_attempt")
        if last_attempt:
            try:
                elapsed = (now - datetime.fromisoformat(last_attempt)).total_seconds()
                if elapsed < cd * 3600:
                    continue
            except (ValueError, TypeError):
                pass
        updateWantedStatus(item["tmdb_id"], "pending", failure_reason=None)
        retried += 1

    return retried


def forceRequeueWanted(
    tmdb_id: int,
    media_type: str,
    title: str | None = None,
    year: int | None = None,
    seasons_needed: list[int] | None = None,
) -> tuple[bool, str]:
    db = getDatabase(WANTED_DB)
    db_lock = getDatabaseLock(WANTED_DB)
    if db is None or db_lock is None:
        return False, "Database connection failed."

    with db_lock:
        q = Query()
        existing = db.search(q.tmdb_id == tmdb_id)

    if existing:
        e = existing[0]
        merged = seasons_needed
        if seasons_needed and e.get("seasons_needed"):
            merged = sorted(set(e["seasons_needed"]) | set(seasons_needed))
        elif e.get("seasons_needed") and not seasons_needed:
            merged = e["seasons_needed"]
        with db_lock:
            try:
                db.update(
                    {
                        "status": "pending",
                        "last_attempt": None,
                        "failure_reason": None,
                        "seasons_needed": merged,
                    },
                    q.tmdb_id == tmdb_id,
                )
                return True, f"Force-requeued {tmdb_id}."
            except Exception as ex:
                return False, f"Error force-requeueing: {ex}"

    return addWanted(
        tmdb_id=tmdb_id,
        media_type=media_type,
        title=title,
        year=year,
        seasons_needed=seasons_needed,
        catalog=None,
    )


def removeWanted(tmdb_id: int) -> tuple[bool, str]:
    db = getDatabase(WANTED_DB)
    db_lock = getDatabaseLock(WANTED_DB)
    if db is None or db_lock is None:
        return False, "Database connection failed."

    with db_lock:
        q = Query()
        try:
            db.remove(q.tmdb_id == tmdb_id)
            return True, f"Removed {tmdb_id} from want queue."
        except Exception as e:
            return False, f"Error removing: {e}"


def getAllWanted() -> list[dict]:
    data, success, _ = getAllData(WANTED_DB)
    if not success or not data:
        return []
    return data
