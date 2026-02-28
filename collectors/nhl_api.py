"""
NHL Stats API Connector
=======================
A Python module for interfacing with the NHL Stats API (api-web.nhle.com).

Features:
- Automatic retries with exponential backoff
- Local JSON caching to avoid redundant API calls
- Rate limiting to be respectful to the API
- Clean, typed methods for every major endpoint

Usage:
    from collectors.nhl_api import NHLApiClient

    client = NHLApiClient()

    # Get all teams
    teams = client.get_teams()

    # Get a player's stats
    stats = client.get_player_stats(player_id=8478402)  # Connor McDavid

    # Get standings
    standings = client.get_standings(season="20232024")
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

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://api-web.nhle.com/v1"
STATS_URL = "https://api.nhle.com/stats/rest/en"   # legacy stats endpoint (still active)

DEFAULT_CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "cache"
DEFAULT_CACHE_TTL = 3600          # seconds — 1 hour for most endpoints
STANDINGS_CACHE_TTL = 300         # 5 minutes for live-ish data
HISTORICAL_CACHE_TTL = 86400 * 7  # 7 days for past seasons

RATE_LIMIT_DELAY = 0.4            # seconds between requests (≈150 req/min)


# ---------------------------------------------------------------------------
# NHLApiClient
# ---------------------------------------------------------------------------
class NHLApiClient:
    """
    Client for the NHL Stats API.

    Parameters
    ----------
    cache_dir : Path or str, optional
        Directory to store cached JSON responses. Defaults to ../data/cache
        relative to this file.
    use_cache : bool
        Whether to cache responses locally. Default True.
    cache_ttl : int
        Default cache time-to-live in seconds. Default 3600 (1 hour).
    """

    def __init__(
        self,
        cache_dir: Optional[Path] = None,
        use_cache: bool = True,
        cache_ttl: int = DEFAULT_CACHE_TTL,
    ):
        self.cache_dir = Path(cache_dir) if cache_dir else DEFAULT_CACHE_DIR
        self.use_cache = use_cache
        self.cache_ttl = cache_ttl
        self._last_request_time: float = 0.0

        if self.use_cache:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Cache directory: {self.cache_dir}")

        # Session with automatic retry on transient errors
        self.session = self._build_session()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_session(self) -> requests.Session:
        """Create a requests Session with retry logic."""
        session = requests.Session()
        retry_strategy = Retry(
            total=4,
            backoff_factor=1.5,          # waits 1.5s, 3s, 4.5s, 6s
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": "NHL-Stats-Project/1.0 (educational use)",
            "Accept": "application/json",
        })
        return session

    def _rate_limit(self) -> None:
        """Enforce a minimum delay between outbound requests."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)

    def _cache_key(self, url: str, params: dict) -> str:
        """Generate a filesystem-safe cache filename from the URL + params."""
        raw = url + json.dumps(params, sort_keys=True)
        digest = hashlib.md5(raw.encode()).hexdigest()[:12]
        # Also embed a human-readable slug for easier debugging
        slug = url.replace(BASE_URL, "").replace(STATS_URL, "").strip("/").replace("/", "_")[:60]
        return f"{slug}__{digest}.json"

    def _read_cache(self, key: str, ttl: int) -> Optional[Any]:
        """Return cached data if it exists and is fresh, else None."""
        path = self.cache_dir / key
        if not path.exists():
            return None
        age = time.time() - path.stat().st_mtime
        if age > ttl:
            logger.debug(f"Cache expired for {key} (age={age:.0f}s)")
            return None
        logger.debug(f"Cache hit: {key}")
        return json.loads(path.read_text(encoding="utf-8"))

    def _write_cache(self, key: str, data: Any) -> None:
        """Persist data to the cache directory."""
        path = self.cache_dir / key
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def _get(
        self,
        url: str,
        params: Optional[dict] = None,
        ttl: Optional[int] = None,
    ) -> Any:
        """
        Core GET request with caching and rate limiting.

        Parameters
        ----------
        url    : Full URL to request.
        params : Query string parameters.
        ttl    : Cache TTL in seconds. Falls back to self.cache_ttl.

        Returns
        -------
        Parsed JSON response (dict or list).
        """
        params = params or {}
        ttl = ttl if ttl is not None else self.cache_ttl

        if self.use_cache:
            key = self._cache_key(url, params)
            cached = self._read_cache(key, ttl)
            if cached is not None:
                return cached

        self._rate_limit()
        logger.info(f"GET {url}  params={params or ''}")
        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
        except requests.HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code} for {url}: {e}")
            raise
        except requests.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            raise
        finally:
            self._last_request_time = time.monotonic()

        data = response.json()

        if self.use_cache:
            self._write_cache(key, data)

        return data

    # ------------------------------------------------------------------
    # Teams
    # ------------------------------------------------------------------

    def get_teams(self) -> list[dict]:
        """
        Return all active NHL franchises/teams.

        Returns a list of team dicts with keys including:
            id, fullName, triCode, conference, division, venue, ...
        """
        url = f"{BASE_URL}/standings/now"
        raw = self._get(url, ttl=HISTORICAL_CACHE_TTL)
        # Standings payload contains full team info for every club
        return raw.get("standings", [])

    def get_team_roster(self, team_abbrev: str, season: Optional[str] = None) -> dict:
        """
        Fetch the full roster for a team.

        Parameters
        ----------
        team_abbrev : Three-letter team code, e.g. "EDM", "TOR", "NYR".
        season      : Season string like "20232024". Defaults to current season.

        Returns
        -------
        Dict with keys 'forwards', 'defensemen', 'goalies', each a list of players.
        """
        if season:
            url = f"{BASE_URL}/roster/{team_abbrev}/{season}"
        else:
            url = f"{BASE_URL}/roster/{team_abbrev}/current"
        return self._get(url, ttl=HISTORICAL_CACHE_TTL)

    # ------------------------------------------------------------------
    # Players
    # ------------------------------------------------------------------

    def get_player_info(self, player_id: int) -> dict:
        """
        Fetch biographical and career info for a player.

        Parameters
        ----------
        player_id : NHL player ID (e.g. 8478402 for Connor McDavid).
        """
        url = f"{BASE_URL}/player/{player_id}/landing"
        return self._get(url, ttl=HISTORICAL_CACHE_TTL)

    def get_player_stats(
        self,
        player_id: int,
        season: Optional[str] = None,
        game_type: str = "2",  # 2 = regular season, 3 = playoffs
    ) -> dict:
        """
        Fetch season-level stats for a player.

        Parameters
        ----------
        player_id  : NHL player ID.
        season     : e.g. "20232024". Defaults to current season.
        game_type  : "2" regular season, "3" playoffs.

        Returns
        -------
        Full player landing page including featuredStats and seasonTotals.
        """
        data = self.get_player_info(player_id)
        return data

    def get_player_game_log(
        self,
        player_id: int,
        season: str,
        game_type: str = "2",
    ) -> list[dict]:
        """
        Fetch a game-by-game log for a player in a given season.

        Parameters
        ----------
        player_id  : NHL player ID.
        season     : e.g. "20232024".
        game_type  : "2" regular season, "3" playoffs.
        """
        url = f"{BASE_URL}/player/{player_id}/game-log/{season}/{game_type}"
        raw = self._get(url, ttl=HISTORICAL_CACHE_TTL)
        return raw.get("gameLog", [])

    def search_players(self, name: str) -> list[dict]:
        """
        Search for players by name.

        Parameters
        ----------
        name : Partial or full player name, e.g. "McDavid" or "Nathan MacKinnon".
        """
        url = f"{BASE_URL}/player-search"
        raw = self._get(url, params={"name": name, "active": "true"}, ttl=HISTORICAL_CACHE_TTL)
        # Fallback — the suggest endpoint is more reliable for search
        if not raw or not isinstance(raw, list):
            url2 = f"https://suggest.svc.nhl.com/svc/suggest/v1/minactiveplayers/{name}/99"
            raw = self._get(url2, ttl=HISTORICAL_CACHE_TTL)
            suggestions = raw.get("suggestions", [])
            # Format: "8478402|McDavid|Connor|..."
            players = []
            for s in suggestions:
                parts = s.split("|")
                if len(parts) >= 3:
                    players.append({
                        "id": int(parts[0]),
                        "lastName": parts[1],
                        "firstName": parts[2],
                    })
            return players
        return raw

    # ------------------------------------------------------------------
    # Skater & Goalie Leaderboards
    # ------------------------------------------------------------------

    def get_skater_stats_leaders(
        self,
        season: str,
        category: str = "points",
        game_type: int = 2,
        limit: int = 100,
    ) -> list[dict]:
        """
        Fetch the top skaters for a given statistical category.

        Parameters
        ----------
        season    : e.g. "20232024".
        category  : Stat category — "points", "goals", "assists", "plusMinus",
                    "penaltyMinutes", "powerPlayGoals", "shorthandedGoals",
                    "gameWinningGoals", "shots", "hits", "blockedShots", "timeOnIce"
        game_type : 2 = regular season, 3 = playoffs.
        limit     : Number of players to return (max 100).
        """
        url = f"{BASE_URL}/skater-stats-leaders/{season}/{game_type}"
        params = {"categories": category, "limit": limit}
        raw = self._get(url, params=params, ttl=HISTORICAL_CACHE_TTL)
        return raw.get(category, [])

    def get_skater_stats_summary(
        self,
        season: str,
        game_type: int = 2,
        start: int = 0,
        limit: int = 100,
    ) -> dict:
        """
        Fetch comprehensive skater stats (ALL stats per player) using the
        NHL stats REST API summary report. This is the correct endpoint to
        use when you need shots, hits, blocks, PP goals etc. all in one record.

        Parameters
        ----------
        season    : e.g. "20232024".
        game_type : 2 = regular season, 3 = playoffs.
        start     : Pagination offset.
        limit     : Records per page (max 100).

        Returns
        -------
        Dict with keys 'data' (list of player stat dicts) and 'total' (int).
        """
        url = f"{STATS_URL}/skater/summary"
        params = {
            "cayenneExp": f"seasonId={season} and gameTypeId={game_type}",
            "sort":       '[{"property":"points","direction":"DESC"},{"property":"goals","direction":"DESC"}]',
            "start":      start,
            "limit":      limit,
        }
        return self._get(url, params=params, ttl=HISTORICAL_CACHE_TTL)

    def get_goalie_stats_summary(
        self,
        season: str,
        game_type: int = 2,
        start: int = 0,
        limit: int = 100,
    ) -> dict:
        """
        Fetch comprehensive goalie stats (ALL stats per goalie) using the
        NHL stats REST API summary report.

        Parameters
        ----------
        season    : e.g. "20232024".
        game_type : 2 = regular season, 3 = playoffs.
        start     : Pagination offset.
        limit     : Records per page (max 100).
        """
        url = f"{STATS_URL}/goalie/summary"
        params = {
            "cayenneExp": f"seasonId={season} and gameTypeId={game_type}",
            "sort":       '[{"property":"wins","direction":"DESC"}]',
            "start":      start,
            "limit":      limit,
        }
        return self._get(url, params=params, ttl=HISTORICAL_CACHE_TTL)

    def get_goalie_stats_leaders(
        self,
        season: str,
        category: str = "wins",
        game_type: int = 2,
        limit: int = 50,
    ) -> list[dict]:
        """
        Fetch the top goalies for a given statistical category.

        Parameters
        ----------
        season   : e.g. "20232024".
        category : "wins", "savePctg", "goalsAgainstAverage", "shutouts".
        """
        url = f"{BASE_URL}/goalie-stats-leaders/{season}/{game_type}"
        params = {"categories": category, "limit": limit}
        raw = self._get(url, params=params, ttl=HISTORICAL_CACHE_TTL)
        return raw.get(category, [])

    # ------------------------------------------------------------------
    # Standings
    # ------------------------------------------------------------------

    def get_standings(self, date: Optional[str] = None) -> list[dict]:
        """
        Fetch NHL standings.

        Parameters
        ----------
        date : ISO date string "YYYY-MM-DD". Defaults to current standings.

        Returns
        -------
        List of team standing dicts with wins, losses, OTL, points, etc.
        """
        if date:
            url = f"{BASE_URL}/standings/{date}"
            ttl = HISTORICAL_CACHE_TTL
        else:
            url = f"{BASE_URL}/standings/now"
            ttl = STANDINGS_CACHE_TTL
        raw = self._get(url, ttl=ttl)
        return raw.get("standings", [])

    # ------------------------------------------------------------------
    # Schedule & Games
    # ------------------------------------------------------------------

    def get_schedule(self, date: Optional[str] = None) -> dict:
        """
        Fetch the NHL schedule for a given date.

        Parameters
        ----------
        date : "YYYY-MM-DD". Defaults to today.
        """
        if date:
            url = f"{BASE_URL}/schedule/{date}"
        else:
            url = f"{BASE_URL}/schedule/now"
        return self._get(url, ttl=STANDINGS_CACHE_TTL)

    def get_team_schedule(self, team_abbrev: str, season: str) -> dict:
        """
        Fetch the full season schedule for a specific team.

        Parameters
        ----------
        team_abbrev : e.g. "EDM".
        season      : e.g. "20232024".
        """
        url = f"{BASE_URL}/club-schedule-season/{team_abbrev}/{season}"
        return self._get(url, ttl=HISTORICAL_CACHE_TTL)

    def get_boxscore(self, game_id: int) -> dict:
        """
        Fetch the full boxscore for a completed or in-progress game.

        Parameters
        ----------
        game_id : NHL game ID (e.g. 2023020001).
                  Game IDs follow the pattern: YYYY0T#### where:
                    YYYY = season start year
                    T    = 02 (regular season) or 03 (playoffs)
                    #### = game number
        """
        url = f"{BASE_URL}/gamecenter/{game_id}/boxscore"
        return self._get(url, ttl=HISTORICAL_CACHE_TTL)

    def get_play_by_play(self, game_id: int) -> dict:
        """
        Fetch play-by-play data for a game (useful for advanced metrics).

        Parameters
        ----------
        game_id : NHL game ID.
        """
        url = f"{BASE_URL}/gamecenter/{game_id}/play-by-play"
        return self._get(url, ttl=HISTORICAL_CACHE_TTL)

    # ------------------------------------------------------------------
    # Convenience: Season helpers
    # ------------------------------------------------------------------

    def current_season(self) -> str:
        """
        Return the current season string (e.g. '20242025') by inspecting
        today's standings.
        """
        standings = self.get_standings()
        if standings:
            return standings[0].get("seasonId", "20242025")
        return "20242025"

    def all_team_abbreviations(self) -> list[str]:
        """Return a sorted list of all active team tri-codes (e.g. ['ANA','ARI',...])."""
        standings = self.get_standings()
        return sorted(t["teamAbbrev"]["default"] for t in standings if "teamAbbrev" in t)
