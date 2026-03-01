"""
NFL API Connector
=================
Connects to the BallDontLie NFL API to pull player stats, standings, and team data.

Confirmed working endpoints:
    - /teams             : All 32 NFL teams
    - /standings         : Team standings by season
    - /season_stats      : Player season stats (all positions, 60+ fields)

Usage:
    from nfl.nfl_api import NFLApiClient
    client = NFLApiClient()
    stats  = client.get_season_stats(season=2024)
"""

import time
import json
import logging
import hashlib
from pathlib import Path
from typing import Optional, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

API_KEY              = "ebb921c1-e920-458c-9892-f1e537bf2a8a"
BASE_URL             = "https://api.balldontlie.io/nfl/v1"
DEFAULT_CACHE_DIR    = Path(__file__).resolve().parent.parent / "data" / "nfl_cache"
DEFAULT_CACHE_TTL    = 3600
HISTORICAL_CACHE_TTL = 86400 * 7
RATE_LIMIT_DELAY     = 1.1   # ALL-STAR = 60 req/min


class NFLApiClient:

    def __init__(self, api_key=API_KEY, cache_dir=None, use_cache=True):
        self.api_key   = api_key
        self.cache_dir = Path(cache_dir) if cache_dir else DEFAULT_CACHE_DIR
        self.use_cache = use_cache
        self._last_request_time = 0.0
        if self.use_cache:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.session = self._build_session()

    def _build_session(self):
        session = requests.Session()
        retry   = Retry(total=3, backoff_factor=2,
                        status_forcelist=[429, 500, 502, 503, 504],
                        allowed_methods=["GET"])
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers.update({"Authorization": self.api_key, "Accept": "application/json"})
        return session

    def _rate_limit(self):
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)

    def _cache_key(self, endpoint, params):
        raw    = endpoint + json.dumps(params, sort_keys=True)
        digest = hashlib.md5(raw.encode()).hexdigest()[:12]
        slug   = endpoint.replace("/", "_")[:50]
        return f"nfl__{slug}__{digest}.json"

    def _read_cache(self, key, ttl):
        path = self.cache_dir / key
        if not path.exists():
            return None
        if time.time() - path.stat().st_mtime > ttl:
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def _write_cache(self, key, data):
        (self.cache_dir / key).write_text(
            json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    def _get_all_pages(self, endpoint, params=None, ttl=None) -> list:
        """Fetch all pages from a paginated endpoint and return combined data list."""
        params  = dict(params or {})
        ttl     = ttl or DEFAULT_CACHE_TTL
        all_data = []
        params.setdefault("per_page", 100)
        cursor  = None
        page    = 1

        while True:
            page_params = dict(params)
            if cursor:
                page_params["cursor"] = cursor

            cache_key = self._cache_key(endpoint, page_params)
            cached    = self._read_cache(cache_key, ttl) if self.use_cache else None

            if cached is not None:
                page_data = cached
            else:
                self._rate_limit()
                url  = f"{BASE_URL}/{endpoint}"
                logger.info(f"GET /{endpoint} page={page} params={params.get('season','')}")
                try:
                    resp = self.session.get(url, params=page_params, timeout=20)
                    resp.raise_for_status()
                except requests.HTTPError as e:
                    logger.error(f"HTTP {e.response.status_code} for /{endpoint}")
                    raise
                finally:
                    self._last_request_time = time.monotonic()
                page_data = resp.json()
                if self.use_cache:
                    self._write_cache(cache_key, page_data)

            records = page_data.get("data", [])
            all_data.extend(records)

            meta        = page_data.get("meta", {})
            next_cursor = meta.get("next_cursor")
            if not next_cursor or not records:
                break

            cursor = next_cursor
            page  += 1

        return all_data

    # ------------------------------------------------------------------
    # Teams
    # ------------------------------------------------------------------

    def get_teams(self) -> list[dict]:
        """Get all 32 NFL teams."""
        return self._get_all_pages("teams", ttl=HISTORICAL_CACHE_TTL)

    # ------------------------------------------------------------------
    # Standings
    # ------------------------------------------------------------------

    def get_standings(self, season: int) -> list[dict]:
        """
        Get team standings for a season.
        Fields: team, wins, losses, ties, points_for, points_against,
                playoff_seed, point_differential, overall_record,
                conference_record, division_record, home_record, road_record
        """
        return self._get_all_pages("standings", {"season": season}, ttl=HISTORICAL_CACHE_TTL)

    # ------------------------------------------------------------------
    # Player Season Stats
    # ------------------------------------------------------------------

    def get_season_stats(self, season: int, postseason: bool = False) -> list[dict]:
        """
        Get all player season stats for a given year.
        Returns 60+ fields covering QB/RB/WR/TE/DEF/K stats.
        postseason=True returns playoff stats.
        """
        params = {"season": season, "postseason": str(postseason).lower()}
        return self._get_all_pages("season_stats", params, ttl=HISTORICAL_CACHE_TTL)

    # ------------------------------------------------------------------
    # Players
    # ------------------------------------------------------------------

    def get_players(self) -> list[dict]:
        """Get all NFL players."""
        return self._get_all_pages("players", ttl=HISTORICAL_CACHE_TTL)

    def search_player(self, name: str) -> list[dict]:
        """Search players by name."""
        return self._get_all_pages("players", {"search": name}, ttl=HISTORICAL_CACHE_TTL)

    # ------------------------------------------------------------------
    # Games
    # ------------------------------------------------------------------

    def get_games(self, season: int, week: Optional[int] = None) -> list[dict]:
        """Get games for a season, optionally filtered by week."""
        params = {"season": season}
        if week:
            params["week"] = week
        return self._get_all_pages("games", params, ttl=HISTORICAL_CACHE_TTL)

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def get_seasons(self) -> list[int]:
        return [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
