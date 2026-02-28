"""
PGA Tour API Connector
======================
Connects to the Live Golf Data API via RapidAPI to pull PGA Tour statistics.

Confirmed working endpoints (tested 2026-02-28):
    - /schedule        : Full season schedule with tournId, name, date, purse etc.
    - /players         : Player search by firstName/lastName
    - /leaderboard     : Tournament leaderboard by tournId + year
    - /tournament      : Tournament details
    - /earnings        : Player earnings per tournament
    - /points          : FedExCup points per tournament

Usage:
    from golf.golf_api import PGAApiClient

    client   = PGAApiClient()
    schedule = client.get_schedule(year=2025)
    board    = client.get_leaderboard(tourn_id="006", year=2025)
"""

import time
import json
import logging
import hashlib
import datetime
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

RAPIDAPI_HOST        = "live-golf-data.p.rapidapi.com"
BASE_URL             = "https://live-golf-data.p.rapidapi.com"
API_KEY              = "f863c76c8emshe363a6e7c16a37fp1c5117jsn2dd5bc8af0f1"

DEFAULT_CACHE_DIR    = Path(__file__).resolve().parent.parent / "data" / "golf_cache"
DEFAULT_CACHE_TTL    = 3600
HISTORICAL_CACHE_TTL = 86400 * 7
LIVE_CACHE_TTL       = 300
RATE_LIMIT_DELAY     = 0.6


class PGAApiClient:
    """Client for the Live Golf Data API (RapidAPI / slashgolf)."""

    def __init__(self, api_key=API_KEY, cache_dir=None, use_cache=True, cache_ttl=DEFAULT_CACHE_TTL):
        self.api_key   = api_key
        self.cache_dir = Path(cache_dir) if cache_dir else DEFAULT_CACHE_DIR
        self.use_cache = use_cache
        self.cache_ttl = cache_ttl
        self._last_request_time = 0.0
        if self.use_cache:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.session = self._build_session()

    def _build_session(self):
        session = requests.Session()
        retry   = Retry(total=3, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers.update({"x-rapidapi-host": RAPIDAPI_HOST, "x-rapidapi-key": self.api_key, "Accept": "application/json"})
        return session

    def _rate_limit(self):
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)

    def _cache_key(self, url, params):
        raw    = url + json.dumps(params, sort_keys=True)
        digest = hashlib.md5(raw.encode()).hexdigest()[:12]
        slug   = url.replace(BASE_URL, "").strip("/").replace("/", "_")[:60]
        return f"golf__{slug}__{digest}.json"

    def _read_cache(self, key, ttl):
        path = self.cache_dir / key
        if not path.exists():
            return None
        if time.time() - path.stat().st_mtime > ttl:
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def _write_cache(self, key, data):
        (self.cache_dir / key).write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    def _get(self, endpoint, params=None, ttl=None):
        params = params or {}
        ttl    = ttl if ttl is not None else self.cache_ttl
        url    = f"{BASE_URL}/{endpoint}"
        if self.use_cache:
            key    = self._cache_key(url, params)
            cached = self._read_cache(key, ttl)
            if cached is not None:
                return cached
        self._rate_limit()
        logger.info(f"GET /{endpoint}  params={params or ''}")
        try:
            resp = self.session.get(url, params=params, timeout=20)
            resp.raise_for_status()
        except requests.HTTPError as e:
            logger.error(f"HTTP {e.response.status_code} for /{endpoint}")
            raise
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        finally:
            self._last_request_time = time.monotonic()
        data = resp.json()
        if self.use_cache:
            self._write_cache(key, data)
        return data

    # --- Schedule ---
    def get_schedule(self, year=2025, org_id="1"):
        """Fetch PGA Tour schedule. Fields: tournId, name, date, purse, winnersShare, fedexCupPoints"""
        params = {"year": str(year), "orgId": org_id}
        data   = self._get("schedule", params=params, ttl=HISTORICAL_CACHE_TTL)
        return data if isinstance(data, list) else data.get("schedule", data.get("tournaments", []))

    def get_completed_tournaments(self, year=2025):
        """Return only completed tournaments based on end date."""
        today    = datetime.date.today().isoformat()
        schedule = self.get_schedule(year=year)
        return [t for t in schedule if (t.get("date") or {}).get("end", "")[:10] < today]

    # --- Leaderboard ---
    def get_leaderboard(self, tourn_id, year, round_id=None):
        """Fetch tournament leaderboard. tournId from schedule."""
        params = {"tournId": tourn_id, "year": str(year)}
        if round_id:
            params["roundId"] = str(round_id)
        return self._get("leaderboard", params=params, ttl=HISTORICAL_CACHE_TTL)

    # --- Tournament details ---
    def get_tournament(self, tourn_id, year):
        """Fetch tournament metadata — course, field, location."""
        return self._get("tournament", params={"tournId": tourn_id, "year": str(year)}, ttl=HISTORICAL_CACHE_TTL)

    # --- Players ---
    def search_player(self, last_name=None, first_name=None, player_id=None):
        """Search players. Fields returned: playerId, firstName, lastName"""
        params = {}
        if last_name:  params["lastName"]  = last_name
        if first_name: params["firstName"] = first_name
        if player_id:  params["playerId"]  = player_id
        if not params: raise ValueError("Provide at least one parameter.")
        data = self._get("players", params=params, ttl=HISTORICAL_CACHE_TTL)
        return data if isinstance(data, list) else data.get("players", [])

    # --- Earnings & Points ---
    def get_tournament_earnings(self, tourn_id, year):
        """Player earnings for a tournament."""
        data = self._get("earnings", params={"tournId": tourn_id, "year": str(year)}, ttl=HISTORICAL_CACHE_TTL)
        return data if isinstance(data, list) else data.get("earnings", [])

    def get_tournament_points(self, tourn_id, year):
        """FedExCup points per player for a tournament."""
        data = self._get("points", params={"tournId": tourn_id, "year": str(year)}, ttl=HISTORICAL_CACHE_TTL)
        return data if isinstance(data, list) else data.get("points", [])

    # --- Probe helper ---
    def probe_leaderboard_fields(self, year=2025):
        """Fetch one leaderboard and print sample fields — useful for debugging."""
        completed = self.get_completed_tournaments(year=year)
        if not completed:
            logger.warning("No completed tournaments found")
            return {}
        t     = completed[0]
        tid   = t.get("tournId", "")
        name  = t.get("name", "")
        logger.info(f"Probing: {name} ({tid})")
        board = self.get_leaderboard(tourn_id=tid, year=year)
        rows  = board.get("leaderboard") or board.get("players") or board.get("results") or []
        if rows:
            logger.info(f"Sample leaderboard keys: {list(rows[0].keys())}")
            return {"tournament": name, "sample_row": rows[0], "total": len(rows)}
        return {"tournament": name, "raw_keys": list(board.keys()), "raw_preview": str(board)[:500]}

    def get_seasons(self):
        return [2023, 2024, 2025]
