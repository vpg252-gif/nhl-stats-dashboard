# NHL Stats Project

A Python toolkit for collecting, storing, and analysing NHL statistics.

## Project Structure

```
nhl_stats/
├── collectors/
│   └── nhl_api.py       ← API connector (Phase 1 — DONE)
├── processors/          ← Coming next: parse & clean data
├── db/                  ← Coming next: SQLite schema & loader
├── analysis/            ← Coming later: stats & modelling
├── outputs/             ← Excel exports, charts
├── data/
│   └── cache/           ← Auto-created: cached API responses
├── test_connector.py    ← Quick smoke test
└── requirements.txt
```

## Setup

```bash
# 1. Create a virtual environment (recommended)
python -m venv venv
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the smoke test
python test_connector.py
```

## NHLApiClient — Quick Reference

```python
from collectors.nhl_api import NHLApiClient

client = NHLApiClient()

# --- Season info ---
season = client.current_season()                      # e.g. "20242025"
teams  = client.all_team_abbreviations()              # ['ANA', 'BOS', ...]

# --- Standings ---
standings = client.get_standings()                    # current
standings = client.get_standings(date="2024-12-01")   # on a specific date

# --- Teams & Rosters ---
roster = client.get_team_roster("EDM")                # current season
roster = client.get_team_roster("TOR", "20232024")    # specific season

# --- Players ---
info     = client.get_player_info(8478402)                           # McDavid bio
game_log = client.get_player_game_log(8478402, "20242025")           # game-by-game
results  = client.search_players("Crosby")                           # search by name

# --- Leaderboards ---
pts_leaders = client.get_skater_stats_leaders("20242025", "points")
goal_leaders = client.get_skater_stats_leaders("20242025", "goals")
save_leaders = client.get_goalie_stats_leaders("20242025", "savePctg")

# Skater categories:
#   "points", "goals", "assists", "plusMinus", "penaltyMinutes",
#   "powerPlayGoals", "shorthandedGoals", "gameWinningGoals",
#   "shots", "hits", "blockedShots", "timeOnIce"

# Goalie categories:
#   "wins", "savePctg", "goalsAgainstAverage", "shutouts"

# --- Schedule & Games ---
today    = client.get_schedule()
specific = client.get_schedule("2025-01-15")
team_sched = client.get_team_schedule("EDM", "20242025")
boxscore   = client.get_boxscore(2024020001)
pbp        = client.get_play_by_play(2024020001)
```

## Caching

All responses are cached in `data/cache/` as JSON files. Default TTL:
- Live data (standings, today's schedule): **5 minutes**
- Current season data: **1 hour**
- Historical data: **7 days**

To force a fresh fetch, either delete the cache file or set `use_cache=False`:

```python
client = NHLApiClient(use_cache=False)
```

## Next Steps

- **Phase 2** — Data Collector: scripts to bulk-pull all teams, rosters, and season stats
- **Phase 3** — Database: SQLite schema + loader to persist structured data
- **Phase 4** — Analysis: Pandas-based stats functions and Excel export
