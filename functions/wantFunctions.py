import PTN
from functions.databaseFunctions import getDatabase, getDatabaseLock, getAllData, upsertData
from library.http import tmdb_http_client, api_http_client, requestWrapper
from library.app import TMDB_API_KEY, MEDIA_FETCH_DEBUG
from functions.mediaFunctions import normaliseTitle, cleanTitle
from tinydb import Query
from datetime import datetime, timezone
import logging

WANTED_DB = "wanted"

logger = logging.getLogger(__name__)


class SnapshotIndex:
    __slots__ = ("_titles", "_title_seasons")

    def __init__(self, snapshot: list[dict]):
        self._titles: set[str] = set()
        self._title_seasons: set[tuple[str, int]] = set()
        for item in snapshot:
            for field in ("name", "file_name"):
                raw = item.get(field, "")
                if not raw:
                    continue
                parsed = PTN.parse(raw)
                ptn_title = parsed.get("title", "")
                t = normaliseTitle(ptn_title) or normaliseTitle(cleanTitle(raw))
                if not t:
                    continue
                self._titles.add(t)
                season = parsed.get("season")
                if isinstance(season, int):
                    self._title_seasons.add((t, season))
                elif isinstance(season, list):
                    for s in season:
                        self._title_seasons.add((t, s))

        for db_type in ("torrents", "usenet", "webdl"):
            data, ok, _ = getAllData(db_type)
            if not ok or not data:
                continue
            for it in data:
                m = normaliseTitle(it.get("metadata_title") or "")
                if not m:
                    continue
                self._titles.add(m)
                s = it.get("metadata_season")
                if isinstance(s, list):
                    s = s[0] if s else None
                if isinstance(s, int):
                    self._title_seasons.add((m, s))

    def contains(self, title: str, season: int | None = None) -> bool:
        if not title:
            return False
        n = normaliseTitle(title)
        if season is not None and self._title_seasons:
            return (n, season) in self._title_seasons
        return n in self._titles


class LibraryIndex:
    __slots__ = ("_titles", "_seasons", "_episodes", "_season_packs")

    def __init__(self):
        self._titles: set[str] = set()
        self._seasons: set[tuple[str, int]] = set()
        self._episodes: set[tuple[str, int, int]] = set()
        self._season_packs: set[tuple[str, int]] = set()

        for db_type in ("torrents", "usenet", "webdl"):
            data, success, _ = getAllData(db_type)
            if not success or not data:
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

    def contains(self, title: str, media_type: str = "movie", season: int | None = None, episode: int | None = None) -> bool:
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


def _resolveImdbId(tmdb_id: int, media_type: str) -> str | None:
    endpoint = "movie" if media_type == "movie" else "tv"
    try:
        resp = requestWrapper(
            tmdb_http_client,
            "GET",
            f"/{endpoint}/{tmdb_id}/external_ids",
            params={"api_key": TMDB_API_KEY},
        )
        data = resp.json()
        return data.get("imdb_id")
    except Exception as e:
        logger.warning(f"Failed to resolve IMDB ID for TMDB {tmdb_id}: {e}")
        return None


def fetchTorboxSnapshot() -> list[dict]:
    items = []
    for endpoint in ("/torrents/mylist", "/usenet/mylist"):
        offset = 0
        limit = 1000
        while True:
            try:
                resp = requestWrapper(
                    api_http_client, "GET", endpoint,
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
                        items.append({
                            "id": dl.get("id"),
                            "name": dl.get("name", ""),
                            "hash": dl.get("hash", ""),
                            "file_name": f.get("name", ""),
                            "type": "torrents" if "torrent" in endpoint else "usenet",
                        })
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
) -> tuple[bool, str]:
    db = getDatabase(WANTED_DB)
    db_lock = getDatabaseLock(WANTED_DB)
    if db is None or db_lock is None:
        return False, "Database connection failed."

    with db_lock:
        q = Query()
        existing = db.search(q.tmdb_id == tmdb_id)
        if existing and existing[0].get("status") in ("acquired", "acquiring", "pending"):
            return False, f"Already {existing[0]['status']}."

    imdb_id = _resolveImdbId(tmdb_id, media_type)

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
    }

    if existing:
        existing_record = existing[0]
        record["added_at"] = existing_record.get("added_at", record["added_at"])
        if existing_record.get("priority", 0) > priority:
            record["priority"] = existing_record["priority"]
        if seasons_needed and existing_record.get("seasons_needed"):
            merged = sorted(set(existing_record["seasons_needed"]) | set(seasons_needed))
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
        cd = deferred_cooldown_hours if item.get("status") == "deferred" else cooldown_hours
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
