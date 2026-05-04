import re
import logging
from datetime import date, datetime, timezone
from library.http import tmdb_http_client, requestWrapper
from library.app import (
    TMDB_API_KEY,
    MEDIA_FETCH_DEBUG,
    ENABLE_ANIME_DISCOVER,
)
from functions.databaseFunctions import getDatabase, getDatabaseLock, upsertData
from functions.wantFunctions import (
    addWanted,
    isAlreadyInLibrary,
    getAllWanted,
    SnapshotIndex,
)
from functions.databaseFunctions import getAllData

logger = logging.getLogger(__name__)

DISCOVER_STATE_DB = "discover_state"

EXCLUDED_GENRE_IDS = "10764,10766,10767,10763,10762"
ALLOWED_LANGUAGES = "en|ko|ja|de|sv|fr"

SPORTS_AWARDS_RE = re.compile(
    r"WWE|WWF|WCW|AEW|TNA|NFL|NBA|MLB|NHL|MLS|WNBA|UFC|MMA|Boxing|"
    r"NASCAR|F1|Formula 1|Formula One|PGA|LPGA|Olympics|Olympic|"
    r"FIFA|World Cup|UEFA|Premier League|Super Bowl|Stanley Cup|"
    r"Wimbledon|US Open|Australian Open|French Open|NCAA|"
    r"Tour de France|Giro|Cricket World Cup|IPL|Rugby World Cup|Six Nations|"
    r"The Oscars|Academy Awards|SAG Awards|Screen Actors Guild|"
    r"Emmy Awards|Primetime Emmy|Daytime Emmy|Grammy Awards|The Grammys|"
    r"Tony Awards|The Tonys|Golden Globe|Golden Globes|BAFTA|British Academy|"
    r"Critics' Choice|Critics Choice|People's Choice|"
    r"MTV Movie Awards|MTV Video Music Awards|VMA|Billboard Music Awards|"
    r"Teen Choice Awards|NAACP Image Awards|Directors Guild|DGA Awards|"
    r"Writers Guild|WGA Awards|Producers Guild|PGA Awards|"
    r"Annie Awards|VES Awards|ACE Eddie Awards|BRIT Awards|"
    r"CMT Music Awards|CMA Awards|ACM Awards|American Music Awards|"
    r"iHeartRadio Music Awards",
    re.IGNORECASE,
)

EXCLUDED_GENRE_NAMES = {"Talk", "Talk Show", "News", "Reality", "Soap"}

GENRE_MAP = {
    10759: "Action & Adventure", 16: "Animation", 35: "Comedy", 80: "Crime",
    99: "Documentary", 18: "Drama", 10751: "Family", 10762: "Kids",
    9648: "Mystery", 10763: "News", 10764: "Reality", 10765: "Sci-Fi & Fantasy",
    10766: "Soap", 10767: "Talk", 10768: "War & Politics", 37: "Western",
    28: "Action", 12: "Adventure", 14: "Fantasy", 36: "History", 27: "Horror",
    10402: "Music", 10749: "Romance", 878: "Science Fiction", 10770: "TV Movie",
    53: "Thriller", 10752: "War",
}


def _defaultState() -> dict:
    return {
        "key": "state",
        "page_movies": 1,
        "page_series": 1,
        "page_anime": 1,
        "last_full_reset": datetime.now(timezone.utc).isoformat(),
    }


def _loadState() -> dict:
    db = getDatabase(DISCOVER_STATE_DB)
    db_lock = getDatabaseLock(DISCOVER_STATE_DB)
    if db is None or db_lock is None:
        return _defaultState()
    from tinydb import Query
    with db_lock:
        q = Query()
        results = db.search(q.key == "state")
        if results:
            return results[0]
    state = _defaultState()
    _saveState(state)
    return state


def _saveState(state: dict):
    upsertData(state, DISCOVER_STATE_DB, ["key"])


def _needsDailyReset(state: dict) -> bool:
    last_reset = state.get("last_full_reset")
    if not last_reset:
        return True
    try:
        reset_time = datetime.fromisoformat(last_reset)
        elapsed = (datetime.now(timezone.utc) - reset_time).total_seconds()
        return elapsed >= 24 * 3600
    except (ValueError, TypeError):
        return True


def _todayStr() -> str:
    return date.today().isoformat()


def _yearMonthStr(years_ago: int = 0) -> str:
    d = date.today()
    y = d.year - years_ago
    return f"{y}-{d.month:02d}"


def _genreTitle(genre_ids: list[int] | None) -> str:
    if not genre_ids:
        return ""
    return ", ".join(
        GENRE_MAP[gid] for gid in genre_ids[:2] if gid in GENRE_MAP
    )


def _tmdbGet(endpoint: str, params: dict) -> dict:
    params["api_key"] = TMDB_API_KEY
    resp = requestWrapper(tmdb_http_client, "GET", f"/{endpoint}", params=params)
    return resp.json()


def _getSeasonInfo(tmdb_id: int) -> list[dict]:
    data = _tmdbGet(f"tv/{tmdb_id}", {"language": "en-US"})
    seasons = data.get("seasons", [])
    return [
        {"season_number": s["season_number"], "episode_count": s.get("episode_count", 0)}
        for s in seasons
        if s.get("season_number", 0) > 0 and s.get("episode_count", 0) > 0
    ]


def _shouldExclude(item: dict, exclude_anime: bool = True) -> bool:
    title = item.get("title") or item.get("name") or ""
    genre_ids = item.get("genre_ids", [])
    genre_title = _genreTitle(genre_ids)

    if any(g in EXCLUDED_GENRE_NAMES for g in genre_title.split(", ") if g):
        return True
    if SPORTS_AWARDS_RE.search(title):
        return True
    if exclude_anime and 16 in genre_ids and item.get("original_language") == "ja":
        return True

    release = item.get("release_date") or item.get("first_air_date") or ""
    if release and release > _todayStr():
        return True

    return False



def _discoverNowPlayingMovies(snapshot_index: SnapshotIndex, start_page: int) -> tuple[int, int]:
    logger.info(f"Discovery: fetching nowPlaying movies (page {start_page})...")
    added = 0
    today = _todayStr()
    ym_last_year = _yearMonthStr(1)

    params = {
        "language": "en-US",
        "region": "US",
        "with_release_type": "4|5",
        "release_date.gte": f"{ym_last_year}-01",
        "release_date.lte": today,
        "primary_release_date.gte": f"{ym_last_year}-01",
        "sort_by": "release_date.desc",
        "vote_count.gte": 50,
        "with_original_language": ALLOWED_LANGUAGES,
        "without_genres": EXCLUDED_GENRE_IDS,
        "include_adult": "false",
        "include_video": "false",
        "page": start_page,
    }

    try:
        data = _tmdbGet("discover/movie", params)
    except Exception as e:
        logger.error(f"Discovery: nowPlaying page {start_page} failed: {e}")
        return 0, start_page

    results = data.get("results", [])
    if not results:
        logger.info("Discovery: nowPlaying movies — no results, resetting to page 1.")
        return 0, 1

    for item in results:
        if _shouldExclude(item):
            continue

        tmdb_id = item["id"]
        title = item.get("title", "")
        year = int(item["release_date"][:4]) if item.get("release_date") else None

        if isAlreadyInLibrary(None, title, "movie", snapshot_index=snapshot_index):
            continue

        success, _ = addWanted(tmdb_id, "movie", title=title, year=year, priority=5, catalog="nowPlaying")
        if success:
            added += 1
            if MEDIA_FETCH_DEBUG:
                logger.debug(f"Discovery: queued movie {title} ({tmdb_id})")

    logger.info(f"Discovery: nowPlaying movies — {added} queued from page {start_page}.")
    return added, start_page + 1


def _discoverPopularSeries(snapshot_index: SnapshotIndex, start_page: int) -> tuple[int, int]:
    logger.info(f"Discovery: fetching popular series (page {start_page})...")
    episodes_queued = 0
    shows_added = 0
    today = _todayStr()

    params = {
        "language": "en-US",
        "sort_by": "popularity.desc",
        "with_networks": "6783|8304|213|2739|2552|3186|453|1024|4330|49",
        "first_air_date.lte": today,
        "air_date.lte": today,
        "with_original_language": ALLOWED_LANGUAGES,
        "without_genres": EXCLUDED_GENRE_IDS,
        "include_adult": "false",
        "page": start_page,
    }

    try:
        data = _tmdbGet("discover/tv", params)
    except Exception as e:
        logger.error(f"Discovery: popular series page {start_page} failed: {e}")
        return 0, start_page

    results = data.get("results", [])
    if not results:
        logger.info("Discovery: popular series — no results, resetting to page 1.")
        return 0, 1

    for item in results:
        if _shouldExclude(item, exclude_anime=True):
            continue

        tmdb_id = item["id"]
        title = item.get("name", "")

        try:
            season_info = _getSeasonInfo(tmdb_id)
        except Exception:
            continue

        new_seasons = []
        ep_count = 0
        for s in season_info:
            sn = s["season_number"]
            if not isAlreadyInLibrary(None, title, "series", season=sn, snapshot_index=snapshot_index):
                new_seasons.append(sn)
                ep_count += s["episode_count"]

        if not new_seasons:
            continue

        year_str = item.get("first_air_date", "")
        year = int(year_str[:4]) if year_str else None

        success, _ = addWanted(
            tmdb_id, "series", title=title, year=year,
            priority=3, seasons_needed=new_seasons, catalog="popular",
        )
        if success:
            shows_added += 1
            episodes_queued += ep_count
            if MEDIA_FETCH_DEBUG:
                logger.debug(
                    f"Discovery: queued series {title} ({tmdb_id}) "
                    f"seasons {new_seasons} (~{ep_count} eps)"
                )

    logger.info(
        f"Discovery: popular series — {shows_added} shows queued, "
        f"~{episodes_queued} episodes from page {start_page}."
    )
    return episodes_queued, start_page + 1


def _discoverAnime(snapshot_index: SnapshotIndex, start_page: int) -> tuple[int, int]:
    if not ENABLE_ANIME_DISCOVER:
        return 0, start_page

    logger.info(f"Discovery: fetching trending anime (page {start_page})...")
    episodes_queued = 0
    shows_added = 0
    ym_last_year = _yearMonthStr(1)
    today = _todayStr()

    params = {
        "language": "en-US",
        "first_air_date.gte": "2010-01-01",
        "air_date.gte": f"{ym_last_year}-01",
        "air_date.lte": today,
        "with_genres": "16",
        "without_genres": "10751,10762",
        "with_origin_country": "JP",
        "sort_by": "popularity.desc",
        "include_adult": "false",
        "page": start_page,
    }

    try:
        data = _tmdbGet("discover/tv", params)
    except Exception as e:
        logger.error(f"Discovery: anime page {start_page} failed: {e}")
        return 0, start_page

    results = data.get("results", [])
    if not results:
        logger.info("Discovery: anime — no results, resetting to page 1.")
        return 0, 1

    for item in results:
        genre_title = _genreTitle(item.get("genre_ids", []))
        if genre_title == "Animation":
            continue
        title = item.get("name", "")
        if SPORTS_AWARDS_RE.search(title):
            continue

        release = item.get("first_air_date", "")
        if release and release > today:
            continue

        tmdb_id = item["id"]

        try:
            season_info = _getSeasonInfo(tmdb_id)
        except Exception:
            continue

        new_seasons = []
        ep_count = 0
        for s in season_info:
            sn = s["season_number"]
            if not isAlreadyInLibrary(None, title, "series", season=sn, snapshot_index=snapshot_index):
                new_seasons.append(sn)
                ep_count += s["episode_count"]

        if not new_seasons:
            continue

        year = int(release[:4]) if release else None
        success, _ = addWanted(
            tmdb_id, "series", title=title, year=year,
            priority=3, seasons_needed=new_seasons, catalog="anime",
        )
        if success:
            shows_added += 1
            episodes_queued += ep_count
            if MEDIA_FETCH_DEBUG:
                logger.debug(
                    f"Discovery: queued anime {title} ({tmdb_id}) "
                    f"seasons {new_seasons} (~{ep_count} eps)"
                )

    logger.info(
        f"Discovery: anime — {shows_added} shows queued, "
        f"~{episodes_queued} episodes from page {start_page}."
    )
    return episodes_queued, start_page + 1


def runDiscovery():
    logger.info("Starting TMDB discovery run...")

    try:
        all_wanted = getAllWanted()
        pending_catalogs = {
            item.get("catalog")
            for item in all_wanted
            if item.get("status") == "pending" and item.get("catalog") in ("nowPlaying", "popular", "anime")
        }
        if pending_catalogs:
            logger.info(f"Discovery: skipping — {len(pending_catalogs)} catalogs still have pending items.")
            return

        state = _loadState()
        snapshot = []
        for db_type in ("torrents", "usenet", "webdl"):
            data, success, _ = getAllData(db_type)
            if success and data:
                snapshot.extend(data)
        snapshot_index = SnapshotIndex(snapshot)

        if _needsDailyReset(state):
            logger.info("Discovery: 24h elapsed — resetting pagination.")
            state["page_movies"] = 1
            state["page_series"] = 1
            state["page_anime"] = 1
            state["last_full_reset"] = datetime.now(timezone.utc).isoformat()

        movies, next_movie_page = _discoverNowPlayingMovies(
            snapshot_index, state["page_movies"],
        )
        series_eps, next_series_page = _discoverPopularSeries(
            snapshot_index, state["page_series"],
        )
        anime_eps, next_anime_page = _discoverAnime(
            snapshot_index, state["page_anime"],
        )

        state["page_movies"] = next_movie_page
        state["page_series"] = next_series_page
        state["page_anime"] = next_anime_page
        _saveState(state)

        logger.info(
            f"Discovery complete: {movies} movies, ~{series_eps} series eps, "
            f"~{anime_eps} anime eps. "
            f"Next pages: movies={next_movie_page}, series={next_series_page}, "
            f"anime={next_anime_page}."
        )
    except Exception as e:
        logger.error(f"Discovery run failed: {e}")
