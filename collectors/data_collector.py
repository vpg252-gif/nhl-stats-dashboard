"""
NHL Data Collector
==================
Bulk-pulls NHL data using the API connector and saves everything locally
as clean JSON files ready for loading into the database.

What it collects:
    - All team info & standings
    - Full rosters for every team
    - Skater season stats (top 500 players)
    - Goalie season stats (top 100 goalies)
    - Player game logs for every rostered player
    - Team schedules for the full season

Usage:
    python collectors/data_collector.py

    # Or with options:
    python collectors/data_collector.py --season 20232024
    python collectors/data_collector.py --skip-game-logs   # faster, skips per-game data
"""

import json
import time
import logging
import argparse
from pathlib import Path
from typing import Optional

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from collectors.nhl_api import NHLApiClient

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Output directories
# ---------------------------------------------------------------------------
BASE_DIR     = Path(__file__).resolve().parent.parent
DATA_DIR     = BASE_DIR / "data"
RAW_DIR      = DATA_DIR / "raw"

TEAMS_DIR    = RAW_DIR / "teams"
ROSTERS_DIR  = RAW_DIR / "rosters"
SKATERS_DIR  = RAW_DIR / "skaters"
GOALIES_DIR  = RAW_DIR / "goalies"
GAMELOGS_DIR = RAW_DIR / "game_logs"
SCHEDULE_DIR = RAW_DIR / "schedules"
STANDINGS_DIR = RAW_DIR / "standings"

ALL_DIRS = [
    TEAMS_DIR, ROSTERS_DIR, SKATERS_DIR, GOALIES_DIR,
    GAMELOGS_DIR, SCHEDULE_DIR, STANDINGS_DIR
]


def setup_directories():
    """Create all output directories if they don't exist."""
    for d in ALL_DIRS:
        d.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {RAW_DIR}")


def save_json(path: Path, data) -> None:
    """Save data as a formatted JSON file."""
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def load_json(path: Path):
    """Load a JSON file."""
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Collection functions
# ---------------------------------------------------------------------------

def collect_standings(client: NHLApiClient, season: str) -> list[dict]:
    """Pull and save current standings."""
    logger.info("── Collecting standings...")
    standings = client.get_standings()
    save_json(STANDINGS_DIR / f"standings_{season}.json", standings)
    logger.info(f"   Saved {len(standings)} teams in standings")
    return standings


def collect_teams(standings: list[dict]) -> list[dict]:
    """
    Extract clean team records from standings data and save.
    Returns a list of simplified team dicts.
    """
    logger.info("── Collecting team info...")
    teams = []
    for t in standings:
        teams.append({
            "id":           t.get("teamId"),
            "name":         t.get("teamName", {}).get("default", ""),
            "abbrev":       t.get("teamAbbrev", {}).get("default", ""),
            "conference":   t.get("conferenceName", ""),
            "division":     t.get("divisionName", ""),
            "logo_url":     t.get("teamLogo", ""),
            # Current season record
            "wins":         t.get("wins", 0),
            "losses":       t.get("losses", 0),
            "ot_losses":    t.get("otLosses", 0),
            "points":       t.get("points", 0),
            "games_played": t.get("gamesPlayed", 0),
            "goals_for":    t.get("goalFor", 0),
            "goals_against":t.get("goalAgainst", 0),
            "pp_pct":       t.get("powerPlayPct", 0.0),
            "pk_pct":       t.get("penaltyKillPct", 0.0),
            "streak":       t.get("streakCode", ""),
        })

    save_json(TEAMS_DIR / "teams.json", teams)
    logger.info(f"   Saved {len(teams)} teams")
    return teams


def collect_rosters(client: NHLApiClient, teams: list[dict], season: str) -> dict:
    """
    Pull the full roster for every team.
    Returns a dict of {abbrev: roster_data}.
    """
    logger.info("── Collecting rosters for all teams...")
    all_players = {}   # player_id -> player info (deduplicated)
    rosters_by_team = {}

    for team in teams:
        abbrev = team["abbrev"]
        if not abbrev:
            continue
        try:
            roster = client.get_team_roster(abbrev, season)
            save_json(ROSTERS_DIR / f"roster_{abbrev}_{season}.json", roster)
            rosters_by_team[abbrev] = roster

            # Flatten all players into a single lookup
            for position_group in ["forwards", "defensemen", "goalies"]:
                for player in roster.get(position_group, []):
                    pid = player.get("id")
                    if pid:
                        all_players[pid] = {
                            "id":            pid,
                            "first_name":    player.get("firstName", {}).get("default", ""),
                            "last_name":     player.get("lastName", {}).get("default", ""),
                            "number":        player.get("sweaterNumber"),
                            "position":      player.get("positionCode"),
                            "team_abbrev":   abbrev,
                            "shoots_catches":player.get("shootsCatches", ""),
                            "height_inches": player.get("heightInInches"),
                            "weight_pounds": player.get("weightInPounds"),
                            "birth_date":    player.get("birthDate", ""),
                            "birth_country": player.get("birthCountry", ""),
                            "headshot_url":  player.get("headshot", ""),
                        }

            logger.info(f"   {abbrev}: {sum(len(roster.get(g,[])) for g in ['forwards','defensemen','goalies'])} players")
            time.sleep(0.2)  # gentle rate limiting

        except Exception as e:
            logger.warning(f"   Failed to get roster for {abbrev}: {e}")

    # Save master player list
    player_list = list(all_players.values())
    save_json(ROSTERS_DIR / "all_players.json", player_list)
    logger.info(f"   Total unique players across all rosters: {len(player_list)}")
    return rosters_by_team


def collect_skater_stats(client: NHLApiClient, season: str) -> list[dict]:
    """
    Pull comprehensive season stats for ALL skaters using the NHL stats
    summary endpoint. This returns every stat (goals, assists, shots, hits,
    blocks, PP goals, etc.) in a single record per player — no merging needed.
    Paginates automatically until all players are collected.
    """
    logger.info("── Collecting skater season stats (comprehensive)...")

    all_skaters = []
    start = 0
    page_size = 100

    while True:
        try:
            raw = client.get_skater_stats_summary(
                season=season, start=start, limit=page_size
            )
            page = raw.get("data", [])
            total = raw.get("total", 0)

            if not page:
                break

            for p in page:
                goals  = p.get("goals", 0) or 0
                points = p.get("points", 0) or 0
                shots  = p.get("shots", 0) or 0
                gp     = p.get("gamesPlayed", 0) or 0

                all_skaters.append({
                    "player_id":       p.get("playerId"),
                    "first_name":      p.get("skaterFullName", "").split(" ")[0] if p.get("skaterFullName") else "",
                    "last_name":       " ".join(p.get("skaterFullName", "").split(" ")[1:]) if p.get("skaterFullName") else "",
                    "full_name":       p.get("skaterFullName", ""),
                    "team_abbrev":     p.get("teamAbbrevs", ""),
                    "position":        p.get("positionCode", ""),
                    "season":          season,
                    "games_played":    gp,
                    "goals":           goals,
                    "assists":         p.get("assists", 0),
                    "points":          points,
                    "plus_minus":      p.get("plusMinus", 0),
                    "penalty_minutes": p.get("penaltyMinutes", 0),
                    "pp_goals":        p.get("ppGoals", 0),
                    "pp_points":       p.get("ppPoints", 0),
                    "sh_goals":        p.get("shGoals", 0),
                    "gw_goals":        p.get("gameWinningGoals", 0),
                    "shots":           shots,
                    "hits":            p.get("hits", 0),
                    "blocked_shots":   p.get("blockedShots", 0),
                    "toi_per_game":    p.get("timeOnIcePerGame", ""),
                    "shooting_pct":    round(goals / shots * 100, 1) if shots > 0 else 0.0,
                    "points_per_game": round(points / gp, 3) if gp > 0 else 0.0,
                    "goals_per_game":  round(goals / gp, 3) if gp > 0 else 0.0,
                })

            logger.info(f"   Page {start//page_size + 1}: {len(page)} players "
                        f"(total so far: {len(all_skaters)}/{total})")
            start += page_size
            time.sleep(0.3)

            if start >= total:
                break

        except Exception as e:
            logger.warning(f"   Failed at offset {start}: {e}")
            break

    save_json(SKATERS_DIR / f"skater_stats_{season}.json", all_skaters)
    logger.info(f"   ✅ Saved comprehensive stats for {len(all_skaters)} skaters")
    return all_skaters


def collect_goalie_stats(client: NHLApiClient, season: str) -> list[dict]:
    """
    Pull comprehensive season stats for ALL goalies using the NHL stats
    summary endpoint. Returns wins, losses, GAA, save%, shutouts, etc.
    in a single record per goalie.
    """
    logger.info("── Collecting goalie season stats (comprehensive)...")

    all_goalies = []
    start = 0
    page_size = 100

    while True:
        try:
            raw = client.get_goalie_stats_summary(
                season=season, start=start, limit=page_size
            )
            page = raw.get("data", [])
            total = raw.get("total", 0)

            if not page:
                break

            for g in page:
                all_goalies.append({
                    "player_id":    g.get("goalieFullName", "").replace(" ", "_") or g.get("playerId"),
                    "player_id":    g.get("playerId"),
                    "first_name":   g.get("goalieFullName", "").split(" ")[0] if g.get("goalieFullName") else "",
                    "last_name":    " ".join(g.get("goalieFullName", "").split(" ")[1:]) if g.get("goalieFullName") else "",
                    "full_name":    g.get("goalieFullName", ""),
                    "team_abbrev":  g.get("teamAbbrevs", ""),
                    "season":       season,
                    "games_played": g.get("gamesPlayed", 0),
                    "games_started":g.get("gamesStarted", 0),
                    "wins":         g.get("wins", 0),
                    "losses":       g.get("losses", 0),
                    "ot_losses":    g.get("otLosses", 0),
                    "save_pct":     g.get("savePct", 0.0),
                    "gaa":          g.get("goalsAgainstAverage", 0.0),
                    "shutouts":     g.get("shutouts", 0),
                    "saves":        g.get("saves", 0),
                    "shots_against":g.get("shotsAgainst", 0),
                    "goals_against":g.get("goalsAgainst", 0),
                    "toi":          g.get("timeOnIce", ""),
                })

            logger.info(f"   Page {start//page_size + 1}: {len(page)} goalies "
                        f"(total so far: {len(all_goalies)}/{total})")
            start += page_size
            time.sleep(0.3)

            if start >= total:
                break

        except Exception as e:
            logger.warning(f"   Failed at offset {start}: {e}")
            break

    save_json(GOALIES_DIR / f"goalie_stats_{season}.json", all_goalies)
    logger.info(f"   ✅ Saved comprehensive stats for {len(all_goalies)} goalies")
    return all_goalies


def collect_game_logs(
    client: NHLApiClient,
    season: str,
    rosters_by_team: dict,
    limit_per_team: Optional[int] = None,
) -> None:
    """
    Pull game-by-game logs for every rostered player.
    These are saved individually as data/raw/game_logs/{player_id}_{season}.json

    Parameters
    ----------
    limit_per_team : If set, only pull logs for this many players per team
                     (useful for testing without pulling 700+ players).
    """
    logger.info("── Collecting player game logs (this takes a few minutes)...")

    total_players = 0
    total_games   = 0
    errors        = 0

    for abbrev, roster in rosters_by_team.items():
        all_positions = (
            roster.get("forwards", []) +
            roster.get("defensemen", []) +
            roster.get("goalies", [])
        )

        if limit_per_team:
            all_positions = all_positions[:limit_per_team]

        for player in all_positions:
            pid = player.get("id")
            if not pid:
                continue

            out_path = GAMELOGS_DIR / f"{pid}_{season}.json"
            if out_path.exists():
                continue  # already collected

            try:
                game_log = client.get_player_game_log(pid, season)
                save_json(out_path, game_log)
                total_players += 1
                total_games   += len(game_log)
                time.sleep(0.2)

            except Exception as e:
                errors += 1
                logger.warning(f"   Failed game log for player {pid}: {e}")

    logger.info(f"   Game logs: {total_players} players, {total_games} game entries, {errors} errors")


def collect_schedules(client: NHLApiClient, teams: list[dict], season: str) -> None:
    """Pull and save the full season schedule for every team."""
    logger.info("── Collecting team schedules...")

    for team in teams:
        abbrev = team["abbrev"]
        if not abbrev:
            continue
        try:
            schedule = client.get_team_schedule(abbrev, season)
            save_json(SCHEDULE_DIR / f"schedule_{abbrev}_{season}.json", schedule)
            time.sleep(0.2)
        except Exception as e:
            logger.warning(f"   Failed schedule for {abbrev}: {e}")

    logger.info(f"   Saved schedules for {len(teams)} teams")


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_collector(season: Optional[str] = None, skip_game_logs: bool = False):
    """
    Run the full data collection pipeline.

    Parameters
    ----------
    season        : e.g. "20242025". Defaults to current season.
    skip_game_logs: If True, skips the slow per-player game log collection.
    """
    start = time.time()
    logger.info("="*55)
    logger.info("NHL DATA COLLECTOR — Starting")
    logger.info("="*55)

    setup_directories()
    client = NHLApiClient()

    # Resolve season
    if not season:
        season = client.current_season()
    logger.info(f"Season: {season}")

    # --- Run each collection step ---
    standings       = collect_standings(client, season)
    teams           = collect_teams(standings)
    rosters_by_team = collect_rosters(client, teams, season)
    skaters         = collect_skater_stats(client, season)
    goalies         = collect_goalie_stats(client, season)
    collect_schedules(client, teams, season)

    if not skip_game_logs:
        collect_game_logs(client, season, rosters_by_team)
    else:
        logger.info("── Skipping game logs (--skip-game-logs flag set)")

    elapsed = time.time() - start
    logger.info("="*55)
    logger.info(f"✅ Collection complete in {elapsed:.1f}s")
    logger.info(f"   Teams:    {len(teams)}")
    logger.info(f"   Skaters:  {len(skaters)}")
    logger.info(f"   Goalies:  {len(goalies)}")
    logger.info(f"   Data saved to: {RAW_DIR}")
    logger.info("="*55)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NHL Data Collector")
    parser.add_argument(
        "--season",
        type=str,
        default=None,
        help="Season to collect, e.g. 20232024. Defaults to current season."
    )
    parser.add_argument(
        "--skip-game-logs",
        action="store_true",
        help="Skip per-player game log collection (faster for testing)."
    )
    args = parser.parse_args()
    run_collector(season=args.season, skip_game_logs=args.skip_game_logs)
