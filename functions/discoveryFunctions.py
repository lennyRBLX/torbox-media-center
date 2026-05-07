import re
import logging
from datetime import date, datetime, timezone
from typing import Callable
from library.http import tmdb_http_client, requestWrapper
from library.app import (
    TMDB_API_KEY,
    MEDIA_FETCH_DEBUG,
    ENABLE_ANIME_DISCOVER,
    ENABLE_TRENDING_DISCOVER,
    DISCOVER_TRENDING_ITEMS_PER_RUN,
)
from functions.databaseFunctions import getDatabase, getDatabaseLock, upsertData
from functions.wantFunctions import (
    addWanted,
    isAlreadyInLibrary,
    getAllWanted,
    SnapshotIndex,
    primeImdbCache,
    isAnime,
    findAbsoluteGroupId,
    fetchAbsoluteEpisodes,
)
from library.profiling import timer

logger = logging.getLogger("discover")

DISCOVER_STATE_DB = "discover_state"

EXCLUDED_GENRE_IDS_SET = {10764, 10766, 10767, 10763, 10762}
EXCLUDED_GENRE_IDS = ",".join(str(g) for g in EXCLUDED_GENRE_IDS_SET)
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

# Threshold for treating an anime as "long-runner that uses absolute episode numbering".
# Below this, releases reliably tag standard SxxExx (Demon Slayer 63 eps, AoT 87,
# MHA 171). Above it, TMDB's multi-season layout drifts from release numbering
# (One Piece 1168, Detective Conan 1260+, Doraemon 1445).
ABSOLUTE_REMAP_MIN_EPISODES = 200

GENRE_MAP = {
    10759: "Action & Adventure",
    16: "Animation",
    35: "Comedy",
    80: "Crime",
    99: "Documentary",
    18: "Drama",
    10751: "Family",
    10762: "Kids",
    9648: "Mystery",
    10763: "News",
    10764: "Reality",
    10765: "Sci-Fi & Fantasy",
    10766: "Soap",
    10767: "Talk",
    10768: "War & Politics",
    37: "Western",
    28: "Action",
    12: "Adventure",
    14: "Fantasy",
    36: "History",
    27: "Horror",
    10402: "Music",
    10749: "Romance",
    878: "Science Fiction",
    10770: "TV Movie",
    53: "Thriller",
    10752: "War",
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
    return ", ".join(GENRE_MAP[gid] for gid in genre_ids[:2] if gid in GENRE_MAP)


def _tmdbGet(endpoint: str, params: dict) -> dict:
    params["api_key"] = TMDB_API_KEY
    resp = requestWrapper(tmdb_http_client, "GET", f"/{endpoint}", params=params)
    return resp.json()


def _tmdbGetTvDetail(tmdb_id: int) -> dict:
    return _tmdbGet(
        f"tv/{tmdb_id}",
        {"language": "en-US", "append_to_response": "external_ids"},
    )


def _getSeasonInfoFromDetail(detail: dict) -> list[dict]:
    today = _todayStr()
    return [
        {
            "season_number": s["season_number"],
            "episode_count": s.get("episode_count", 0),
        }
        for s in (detail.get("seasons") or [])
        if s.get("season_number", 0) > 0
        and s.get("episode_count", 0) > 0
        and s.get("air_date")
        and s["air_date"] <= today
    ]


def _getSeasonInfo(tmdb_id: int) -> list[dict]:
    with timer("getSeasonInfo", tmdb_id=tmdb_id):
        detail = _tmdbGetTvDetail(tmdb_id)
        ext = detail.get("external_ids") or {}
        primeImdbCache(tmdb_id, "series", ext.get("imdb_id"))
        return _getSeasonInfoFromDetail(detail)


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


def _verifyMovieReleased(tmdb_id: int, today: str) -> tuple[bool, str | None]:
    try:
        data = _tmdbGet(
            f"movie/{tmdb_id}",
            {"append_to_response": "release_dates,external_ids"},
        )
    except Exception:
        return False, None
    imdb_id = data.get("imdb_id") or (data.get("external_ids") or {}).get("imdb_id")
    primeImdbCache(tmdb_id, "movie", imdb_id)
    results = (data.get("release_dates") or {}).get("results", []) if data else []
    us = next((c for c in results if c.get("iso_3166_1") == "US"), None)
    ordered = ([us] if us else []) + [c for c in results if c is not us]
    for c in ordered:
        for r in c.get("release_dates", []):
            if r.get("type") in (4, 5) and (r.get("release_date", "")[:10] <= today):
                return True, imdb_id
    return False, imdb_id


def _discoverTrendingWeek(snapshot_index: SnapshotIndex) -> tuple[int, int]:
    if not ENABLE_TRENDING_DISCOVER:
        return 0, 0

    logger.info(f"fetching trending (page 1, top {DISCOVER_TRENDING_ITEMS_PER_RUN})...")
    today = _todayStr()

    try:
        data = _tmdbGet("trending/all/week", {"language": "en-US", "page": 1})
    except Exception as e:
        logger.error(f"trending failed: {e}")
        return 0, 0

    items = data.get("results", [])[:DISCOVER_TRENDING_ITEMS_PER_RUN]
    movies_added = series_added = 0

    for item in items:
        media_type = item.get("media_type")
        if media_type not in ("movie", "tv"):
            continue
        if _shouldExclude(item, exclude_anime=True):
            continue

        tmdb_id = item["id"]

        if media_type == "movie":
            released, imdb_id = _verifyMovieReleased(tmdb_id, today)
            if not released:
                continue
            title = item.get("title", "")
            rdate = item.get("release_date") or ""
            year = int(rdate[:4]) if rdate else None
            if isAlreadyInLibrary(None, title, "movie", snapshot_index=snapshot_index):
                continue
            success, _ = addWanted(
                tmdb_id,
                "movie",
                title=title,
                year=year,
                priority=10,
                catalog="trending",
                imdb_id=imdb_id,
            )
            if success:
                movies_added += 1
                if MEDIA_FETCH_DEBUG:
                    logger.debug(f"queued trending movie {title} ({tmdb_id})")
        else:
            title = item.get("name", "")
            try:
                detail = _tmdbGetTvDetail(tmdb_id)
            except Exception:
                continue
            primeImdbCache(
                tmdb_id, "series", (detail.get("external_ids") or {}).get("imdb_id")
            )

            air = item.get("first_air_date") or ""
            year = int(air[:4]) if air else None

            absolute_group_id = (
                findAbsoluteGroupId(tmdb_id)
                if isAnime(detail)
                and (detail.get("number_of_episodes") or 0)
                >= ABSOLUTE_REMAP_MIN_EPISODES
                else None
            )
            if absolute_group_id:
                if isAlreadyInLibrary(
                    None, title, "series", snapshot_index=snapshot_index
                ):
                    continue
                abs_eps = fetchAbsoluteEpisodes(absolute_group_id)
                if abs_eps:
                    success, _ = addWanted(
                        tmdb_id,
                        "series",
                        title=title,
                        year=year,
                        priority=10,
                        seasons_needed=[1],
                        catalog="trending",
                        absolute_group_id=absolute_group_id,
                    )
                    if success:
                        series_added += 1
                        if MEDIA_FETCH_DEBUG:
                            logger.debug(
                                f"queued trending series {title} ({tmdb_id}) "
                                f"absolute (~{len(abs_eps)} eps)"
                            )
                    continue

            season_info = _getSeasonInfoFromDetail(detail)
            new_seasons = []
            for s in season_info:
                sn = s["season_number"]
                if not isAlreadyInLibrary(
                    None, title, "series", season=sn, snapshot_index=snapshot_index
                ):
                    new_seasons.append(sn)

            if not new_seasons:
                continue

            success, _ = addWanted(
                tmdb_id,
                "series",
                title=title,
                year=year,
                priority=10,
                seasons_needed=new_seasons,
                catalog="trending",
            )
            if success:
                series_added += 1
                if MEDIA_FETCH_DEBUG:
                    logger.debug(
                        f"queued trending series {title} ({tmdb_id}) "
                        f"seasons {new_seasons}"
                    )

    logger.info(f"trending — {movies_added} movies, {series_added} series queued.")
    return movies_added, series_added


def _discoverNowPlayingMovies(
    snapshot_index: SnapshotIndex, start_page: int
) -> tuple[int, int]:
    logger.info(f"fetching nowPlaying movies (page {start_page})...")
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
        logger.error(f"nowPlaying page {start_page} failed: {e}")
        return 0, start_page

    results = data.get("results", [])
    if not results:
        logger.info("nowPlaying movies — no results, resetting to page 1.")
        return 0, 1

    for item in results:
        if _shouldExclude(item):
            continue

        tmdb_id = item["id"]
        title = item.get("title", "")
        year = int(item["release_date"][:4]) if item.get("release_date") else None

        if isAlreadyInLibrary(None, title, "movie", snapshot_index=snapshot_index):
            continue

        success, _ = addWanted(
            tmdb_id, "movie", title=title, year=year, priority=5, catalog="nowPlaying"
        )
        if success:
            added += 1
            if MEDIA_FETCH_DEBUG:
                logger.debug(f"queued movie {title} ({tmdb_id})")

    logger.info(f"nowPlaying movies — {added} queued from page {start_page}.")
    return added, start_page + 1


def _processSeriesResults(
    results: list[dict],
    catalog: str,
    snapshot_index: SnapshotIndex,
    pre_filter: Callable[[dict], bool],
    priority: int,
) -> tuple[int, int]:
    with timer("processSeriesResults", catalog=catalog, items=len(results)) as f:
        episodes_queued = 0
        shows_added = 0

        for item in results:
            if pre_filter(item):
                continue

            tmdb_id = item["id"]
            title = item.get("name", "")

            try:
                detail = _tmdbGetTvDetail(tmdb_id)
            except Exception:
                continue
            primeImdbCache(
                tmdb_id, "series", (detail.get("external_ids") or {}).get("imdb_id")
            )

            air_date = item.get("first_air_date", "")
            year = int(air_date[:4]) if air_date else None

            absolute_group_id = (
                findAbsoluteGroupId(tmdb_id)
                if isAnime(detail)
                and (detail.get("number_of_episodes") or 0)
                >= ABSOLUTE_REMAP_MIN_EPISODES
                else None
            )
            if absolute_group_id:
                if isAlreadyInLibrary(
                    None, title, "series", snapshot_index=snapshot_index
                ):
                    continue
                abs_eps = fetchAbsoluteEpisodes(absolute_group_id)
                if abs_eps:
                    success, _ = addWanted(
                        tmdb_id,
                        "series",
                        title=title,
                        year=year,
                        priority=priority,
                        seasons_needed=[1],
                        catalog=catalog,
                        absolute_group_id=absolute_group_id,
                    )
                    if success:
                        shows_added += 1
                        episodes_queued += len(abs_eps)
                        if MEDIA_FETCH_DEBUG:
                            logger.debug(
                                f"queued {catalog} {title} ({tmdb_id}) absolute (~{len(abs_eps)} eps)"
                            )
                    continue

            season_info = _getSeasonInfoFromDetail(detail)
            new_seasons: list[int] = []
            ep_count = 0
            for s in season_info:
                sn = s["season_number"]
                if not isAlreadyInLibrary(
                    None, title, "series", season=sn, snapshot_index=snapshot_index
                ):
                    new_seasons.append(sn)
                    ep_count += s["episode_count"]

            if not new_seasons:
                continue

            success, _ = addWanted(
                tmdb_id,
                "series",
                title=title,
                year=year,
                priority=priority,
                seasons_needed=new_seasons,
                catalog=catalog,
            )
            if success:
                shows_added += 1
                episodes_queued += ep_count
                if MEDIA_FETCH_DEBUG:
                    logger.debug(
                        f"queued {catalog} {title} ({tmdb_id}) seasons {new_seasons} (~{ep_count} eps)"
                    )

        f["shows_added"] = shows_added
        f["episodes_queued"] = episodes_queued
        return shows_added, episodes_queued


def _discoverPopularSeries(
    snapshot_index: SnapshotIndex, start_page: int
) -> tuple[int, int]:
    logger.info(f"fetching popular series (page {start_page})...")
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
        logger.error(f"popular series page {start_page} failed: {e}")
        return 0, start_page

    results = data.get("results", [])
    if not results:
        logger.info("popular series — no results, resetting to page 1.")
        return 0, 1

    shows_added, episodes_queued = _processSeriesResults(
        results,
        "popular",
        snapshot_index,
        pre_filter=lambda item: _shouldExclude(item, exclude_anime=True),
        priority=3,
    )

    logger.info(
        f"popular series — {shows_added} shows queued, "
        f"~{episodes_queued} episodes from page {start_page}."
    )
    return episodes_queued, start_page + 1


def _animePreFilter(item: dict, today: str) -> bool:
    if _genreTitle(item.get("genre_ids", [])) == "Animation":
        return True
    if SPORTS_AWARDS_RE.search(item.get("name", "")):
        return True
    release = item.get("first_air_date", "")
    if release and release > today:
        return True
    return False


def _discoverAnime(snapshot_index: SnapshotIndex, start_page: int) -> tuple[int, int]:
    if not ENABLE_ANIME_DISCOVER:
        return 0, start_page

    logger.info(f"fetching trending anime (page {start_page})...")
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
        logger.error(f"anime page {start_page} failed: {e}")
        return 0, start_page

    results = data.get("results", [])
    if not results:
        logger.info("anime — no results, resetting to page 1.")
        return 0, 1

    shows_added, episodes_queued = _processSeriesResults(
        results,
        "anime",
        snapshot_index,
        pre_filter=lambda item: _animePreFilter(item, today),
        priority=3,
    )

    logger.info(
        f"anime — {shows_added} shows queued, "
        f"~{episodes_queued} episodes from page {start_page}."
    )
    return episodes_queued, start_page + 1


def runDiscovery():
    logger.info("Starting TMDB discovery run...")

    try:
        with timer("runDiscovery") as outer:
            all_wanted = getAllWanted()
            pending_catalogs = {
                item.get("catalog")
                for item in all_wanted
                if item.get("status") == "pending"
                and item.get("catalog")
                in ("trending", "nowPlaying", "popular", "anime")
            }
            if pending_catalogs:
                outer["skipped_catalogs"] = sorted(pending_catalogs)
                logger.info(
                    f"Skipping catalogs with pending items: {sorted(pending_catalogs)}"
                )

            state = _loadState()
            snapshot_index = SnapshotIndex([])

            if _needsDailyReset(state):
                logger.info("24h elapsed — resetting pagination.")
                state["page_movies"] = 1
                state["page_series"] = 1
                state["page_anime"] = 1
                state["last_full_reset"] = datetime.now(timezone.utc).isoformat()

            trend_m = trend_s = 0
            movies = 0
            series_eps = 0
            anime_eps = 0
            next_movie_page = state["page_movies"]
            next_series_page = state["page_series"]
            next_anime_page = state["page_anime"]

            if "trending" not in pending_catalogs:
                with timer("runDiscovery.trending") as f:
                    trend_m, trend_s = _discoverTrendingWeek(snapshot_index)
                    f["added_movies"] = trend_m
                    f["added_series"] = trend_s

            if "nowPlaying" not in pending_catalogs:
                with timer(
                    "runDiscovery.nowPlayingMovies", page=state["page_movies"]
                ) as f:
                    movies, next_movie_page = _discoverNowPlayingMovies(
                        snapshot_index,
                        state["page_movies"],
                    )
                    f["added"] = movies

            if "popular" not in pending_catalogs:
                with timer(
                    "runDiscovery.popularSeries", page=state["page_series"]
                ) as f:
                    series_eps, next_series_page = _discoverPopularSeries(
                        snapshot_index,
                        state["page_series"],
                    )
                    f["added_episodes"] = series_eps

            if "anime" not in pending_catalogs:
                with timer("runDiscovery.anime", page=state["page_anime"]) as f:
                    anime_eps, next_anime_page = _discoverAnime(
                        snapshot_index,
                        state["page_anime"],
                    )
                    f["added_episodes"] = anime_eps

            state["page_movies"] = next_movie_page
            state["page_series"] = next_series_page
            state["page_anime"] = next_anime_page
            _saveState(state)

            outer["trending_movies"] = trend_m
            outer["trending_series"] = trend_s
            outer["movies"] = movies
            outer["series_eps"] = series_eps
            outer["anime_eps"] = anime_eps

            logger.info(
                f"complete: {trend_m} trending movies, {trend_s} trending series, "
                f"{movies} movies, ~{series_eps} series eps, ~{anime_eps} anime eps. "
                f"Next pages: movies={next_movie_page}, series={next_series_page}, "
                f"anime={next_anime_page}."
            )
    except Exception as e:
        logger.exception(f"Discovery run failed: {e}")
