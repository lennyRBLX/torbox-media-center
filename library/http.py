import httpx
from library.torbox import TORBOX_API_KEY
from library.app import getCurrentVersion, TBM_TOOLS_URL
import time
import logging
import hashlib
import json
import random
import threading

log = logging.getLogger("http")

TORBOX_API_URL = "https://api.torbox.app/v1/api"
TORBOX_SEARCH_API_URL = "https://search-api.torbox.app"
USER_AGENT = f"TorBox-Media-Center/{getCurrentVersion()} TorBox/1.0"
CACHE_TTL = 300
MAX_CACHE_SIZE = 1024
MAX_CACHED_BODY_BYTES = 256 * 1024
_cache: dict[str, tuple[float, httpx.Response]] = {}
_cache_request_count = 0

_collapse_locks: dict[str, threading.Lock] = {}
_collapse_meta_lock = threading.Lock()

def makeCacheKey(method: str, url: str, base_url: str, **kwargs) -> str:
    key_data = {
        "method": method,
        "url": url,
        "base_url": base_url,
        "params": kwargs.get("params"),
        "json": kwargs.get("json"),
        "data": kwargs.get("data"),
    }
    key_str = json.dumps(key_data, sort_keys=True, default=str)
    return hashlib.sha256(key_str.encode()).hexdigest()

def _getCollapseLock(cache_key: str) -> threading.Lock:
    with _collapse_meta_lock:
        if cache_key not in _collapse_locks:
            _collapse_locks[cache_key] = threading.Lock()
        return _collapse_locks[cache_key]

transport = httpx.HTTPTransport(
    retries=10
)

api_http_client = httpx.Client(
    base_url=TORBOX_API_URL,
    headers={
        "Authorization": f"Bearer {TORBOX_API_KEY}",
        "User-Agent": USER_AGENT,
    },
    timeout=httpx.Timeout(60),
    follow_redirects=True,
    transport=transport
)

search_api_http_client = httpx.Client(
    base_url=TORBOX_SEARCH_API_URL,
    headers={
        "Authorization": f"Bearer {TORBOX_API_KEY}",
        "User-Agent": USER_AGENT,
    },
    timeout=httpx.Timeout(60),
    follow_redirects=True,
    transport=transport,
)

tmdb_http_client = httpx.Client(
    base_url="https://api.themoviedb.org/3",
    headers={
        "User-Agent": USER_AGENT,
    },
    timeout=httpx.Timeout(10),
    follow_redirects=True,
    http2=True,
)

general_http_client = httpx.Client(
    headers={
        "Authorization": f"Bearer {TORBOX_API_KEY}",
        "User-Agent": USER_AGENT,
    },
    timeout=httpx.Timeout(60),
    follow_redirects=False,
    transport=transport,
)

tbm_http_client = httpx.Client(
    base_url=TBM_TOOLS_URL,
    headers={
        "x-api-key": TORBOX_API_KEY,
        "User-Agent": USER_AGENT,
    },
    timeout=httpx.Timeout(30),
    follow_redirects=True,
    transport=transport,
)


def _checkCache(cache_key: str):
    if cache_key in _cache:
        cached_time, cached_response = _cache[cache_key]
        if time.time() - cached_time < CACHE_TTL:
            return cached_response
        else:
            del _cache[cache_key]
            with _collapse_meta_lock:
                _collapse_locks.pop(cache_key, None)
    return None


def _pruneCache():
    now = time.time()
    expired = [k for k, (ts, _) in _cache.items() if now - ts >= CACHE_TTL]
    for k in expired:
        del _cache[k]
    if len(_cache) > MAX_CACHE_SIZE:
        sorted_keys = sorted(_cache, key=lambda k: _cache[k][0])
        for k in sorted_keys[:len(_cache) - MAX_CACHE_SIZE]:
            del _cache[k]
    with _collapse_meta_lock:
        stale = [k for k in _collapse_locks if k not in _cache]
        for k in stale:
            del _collapse_locks[k]


def _executeRequest(client: httpx.Client, method: str, url: str, cache_key: str | None, **kwargs) -> httpx.Response:
    global _cache_request_count
    max_retries = 5
    backoff_factor = 1.5

    for attempt in range(max_retries):
        try:
            response = client.request(method, url, **kwargs)
            response.raise_for_status()

            if cache_key is not None:
                try:
                    body_size = len(response.content)
                except Exception:
                    body_size = 0
                if body_size <= MAX_CACHED_BODY_BYTES:
                    _cache[cache_key] = (time.time(), response)
                    _cache_request_count += 1
                    if _cache_request_count % 200 == 0:
                        _pruneCache()

            return response
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                wait_time = backoff_factor * (2 ** attempt) * (0.5 + random.random())
                retry_after_header = e.response.headers.get("Retry-After")
                if retry_after_header is not None:
                    try:
                        retry_after_seconds = float(retry_after_header)
                        if retry_after_seconds > wait_time:
                            wait_time = retry_after_seconds
                    except ValueError:
                        pass
                log.warning(f"429 for {url}. Retrying in {wait_time:.2f}s...")
                time.sleep(wait_time)
            else:
                log.error(f"HTTP error for {url}: {e}")
                raise
        except httpx.RequestError as e:
            wait_time = backoff_factor * (2 ** attempt) * (0.5 + random.random())
            log.warning(f"Request error on {url}: {e}. Retrying in {wait_time:.2f}s...")
            time.sleep(wait_time)
    raise httpx.RequestError(f"Failed to complete request to {url} after {max_retries} attempts.")


def requestWrapper(client: httpx.Client, method: str, url: str, use_cache: bool = True, **kwargs) -> httpx.Response:
    cacheable = use_cache and method.upper() == "GET"

    if not cacheable:
        return _executeRequest(client, method, url, None, **kwargs)

    cache_key = makeCacheKey(method, url, str(client.base_url), **kwargs)

    cached = _checkCache(cache_key)
    if cached is not None:
        log.debug(f"Cache hit for {url}")
        return cached

    collapse_lock = _getCollapseLock(cache_key)
    with collapse_lock:
        cached = _checkCache(cache_key)
        if cached is not None:
            log.debug(f"Cache hit (collapsed) for {url}")
            return cached

        return _executeRequest(client, method, url, cache_key, **kwargs)
