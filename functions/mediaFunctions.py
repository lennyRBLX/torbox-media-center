import re
import logging
import unicodedata
from rapidfuzz import fuzz

def constructSeriesTitle(season = None, episode = None, folder: bool = False):
    """
    Constructs a proper title for a series based on the season and episode.

    :param season: The season number or a list of season numbers.
    :param episode: The episode number or a list of episode numbers.
    :param folder: If True, the title will be formatted for a folder name.
    """


    title_season = None
    title_episode = None

    if isinstance(season, list):
        if len(season) == 1:
            season_val = season[0]
            if folder:
                title_season = f"Season {season_val}"
            else:
                title_season = f"S{season_val:02}"
        elif len(season) > 1:
            title_season = f"S{season[0]:02}-S{season[-1]:02}"
    elif isinstance(season, int) or season is not None:
        if folder:
            title_season = f"Season {season}"
        else:
            title_season = f"S{season:02}"

    if isinstance(episode, list):
        if len(episode) == 1:
            title_episode = f"E{episode[0]:02}"
        elif len(episode) > 1:
            title_episode = f"E{episode[0]:02}-E{episode[-1]:02}"
    elif isinstance(episode, int) or episode is not None:
        title_episode = f"E{episode:02}"

    if title_season and title_episode:
        return f"{title_season}{title_episode}"
    elif title_season:
        return title_season
    elif title_episode:
        return title_episode
    else:
        return None

def cleanTitle(title: str):
    """
    Removes invalid characters from the title.
    """
    title = re.sub(r"[\/\\\:\*\?\"\<\>\|]", "", title)
    return title

def cleanYear(year: str | int | None):
    """
    Cleans the year listing which can be a string (2023-2024) or an int (2023).
    """
    try:
        if not year:
            return None
        if isinstance(year, str):
            year = re.sub(r"[–—−‐‑]", "-", year)
            year = year.split("-")[0]
            year = year.strip()
            return int(year)
        if type(year) is int:
            return year
        if year and year != "None":
            return int(year)
        else:
            return None
    except Exception as e:
        logging.error(f"Error cleaning year: {e}")
        return None

UMLAUT_MAP = {
    "Ä": "Ae", "ä": "ae",
    "Ö": "Oe", "ö": "oe",
    "Ü": "Ue", "ü": "ue",
    "ß": "ss",
}

def normaliseTitle(title: str) -> str:
    """
    Normalises a title for fuzzy comparison.
    Handles umlauts, diacritics, special chars — modeled after AIOStreams.
    """
    if not title:
        return ""
    for char, replacement in UMLAUT_MAP.items():
        title = title.replace(char, replacement)
    title = title.replace("&", "and")
    # Strip season/episode tags and everything after them.
    # Handles S01E02, S01EXB, S01, E08, etc. — everything after is
    # episode-specific info (episode title, scene number) not the show title.
    title = re.sub(r"\bS\d+E\S*.*", "", title, flags=re.IGNORECASE)
    # "Season01", "Season 1", "Season.03" — and everything after (episode info)
    title = re.sub(r"\bSeason\s*\d+\b.*", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\bS\d+\b", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\bE\d+\b", "", title, flags=re.IGNORECASE)
    # Strip #x## season×episode tags (Latin x and Cyrillic х) and everything after
    title = re.sub(r"\b\d+[xх]\d+\b.*", "", title, flags=re.IGNORECASE)
    # Strip "Part #" / "Chapter #" and everything after (season/arc indicators)
    title = re.sub(r"\bPart\s+\d+\b.*", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\bChapter\s+\d+\b.*", "", title, flags=re.IGNORECASE)
    # Strip OP/ED/NCOP/NCED tags (with optional number) and everything after
    title = re.sub(r"\b(?:NC)?(?:OP|ED)\d*\b.*", "", title, flags=re.IGNORECASE)
    title = unicodedata.normalize("NFD", title)
    title = "".join(c for c in title if unicodedata.category(c) != "Mn")
    title = re.sub(r"[^a-zA-Z0-9\s]", "", title)
    # Strip standalone 4-digit years (e.g. "Firefly 2002 Serenity" -> "Firefly Serenity")
    title = re.sub(r"\b(?:19|20)\d{2}\b", "", title)
    # Strip leading episode number prefixes (e.g. "226 - Wizard of Odd" -> "Wizard of Odd")
    title = re.sub(r"^\d+\s+", "", title)
    title = re.sub(r"\s+", " ", title).strip().lower()
    return title

TMDB_SCORE_THRESHOLD = 100

def scoreTmdbResult(parsed_title: str, parsed_year: int | None, parsed_season: int | None, parsed_episode: int | None, tmdb_result: dict, media_type: str) -> tuple[int, dict]:
    """
    Scores a TMDB search result against parsed file data.
    Max score: 225. Threshold for acceptance: 100.

    Returns (total_score, breakdown_dict) where breakdown_dict contains
    per-component scores and the values used to compute them.
    """
    breakdown = {
        "tmdb_title": tmdb_result.get("title") or tmdb_result.get("name") or "",
        "tmdb_id": tmdb_result.get("id"),
        "media_type": media_type,
        "title_score": 0,
        "year_score": 0,
        "type_score": 0,
        "season_score": 0,
        "normalised_parsed": normaliseTitle(parsed_title),
        "normalised_tmdb": "",
        "tmdb_year": None,
        "parsed_year": parsed_year,
        "parsed_season": parsed_season,
        "parsed_episode": parsed_episode,
    }

    score = 0

    # Title score (0-115)
    # partial_ratio catches substring matches (0-100), then an exact-match
    # bonus (0-15) breaks ties when partial_ratio returns 100 for both a
    # perfect match and a longer title that merely contains the query.
    breakdown["normalised_tmdb"] = normaliseTitle(breakdown["tmdb_title"])
    partial = int(fuzz.partial_ratio(breakdown["normalised_parsed"], breakdown["normalised_tmdb"]))
    exact = int(fuzz.ratio(breakdown["normalised_parsed"], breakdown["normalised_tmdb"]))
    exact_bonus = round(exact * 15 / 100)  # scale 0-100 into 0-15
    title_score = partial + exact_bonus
    breakdown["title_score"] = title_score
    breakdown["title_partial"] = partial
    breakdown["title_exact"] = exact
    score += title_score

    # Year score (0-50)
    # Uses airing range if available (from detail fetch), otherwise falls
    # back to first air/release date from search results.
    date_str = tmdb_result.get("release_date") or tmdb_result.get("first_air_date") or ""
    if date_str:
        try:
            breakdown["tmdb_year"] = int(date_str[:4])
        except (ValueError, IndexError):
            pass
    end_date_str = tmdb_result.get("last_air_date") or ""
    tmdb_year_end = None
    if end_date_str:
        try:
            tmdb_year_end = int(end_date_str[:4])
        except (ValueError, IndexError):
            pass
    breakdown["tmdb_year_end"] = tmdb_year_end

    year_score = 0
    if parsed_year and breakdown["tmdb_year"]:
        start = breakdown["tmdb_year"]
        end = tmdb_year_end or start
        if start <= parsed_year <= end:
            # Parsed year falls within the show's airing range
            year_score = 50
        else:
            diff = min(abs(parsed_year - start), abs(parsed_year - end))
            if diff == 1:
                year_score = 25
    breakdown["year_score"] = year_score
    score += year_score

    # Media type score (0-30)
    is_tv_parsed = parsed_season is not None or parsed_episode is not None
    is_tv_result = media_type == "tv"
    type_score = 30 if is_tv_parsed == is_tv_result else 0
    breakdown["type_score"] = type_score
    score += type_score

    # Season/episode validation score (-20 to 30)
    season_score = 0
    if is_tv_result and parsed_season is not None:
        seasons = tmdb_result.get("seasons") or []
        if seasons:
            season_numbers = [s.get("season_number") for s in seasons]
            if parsed_season in season_numbers:
                season_score = 30
            elif parsed_season > max(season_numbers, default=0):
                season_score = -20
    breakdown["season_score"] = season_score
    score += season_score

    breakdown["total"] = score
    return score, breakdown
