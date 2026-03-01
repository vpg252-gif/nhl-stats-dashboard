"""
NFL Data Collector
==================
Pulls player stats and standings for 2018-2025 from BallDontLie NFL API
and loads everything into a local SQLite database.

Usage:
    python nfl/nfl_collector.py                    # all seasons
    python nfl/nfl_collector.py --season 2024      # single season
    python nfl/nfl_collector.py --season 2024 --rebuild  # wipe + reload
"""

import sys
import os
import json
import sqlite3
import logging
import argparse
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nfl.nfl_api import NFLApiClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH  = DATA_DIR / "nfl_stats.db"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS nfl_teams (
    id              INTEGER PRIMARY KEY,
    abbreviation    TEXT,
    full_name       TEXT,
    location        TEXT,
    name            TEXT,
    conference      TEXT,
    division        TEXT
);

CREATE TABLE IF NOT EXISTS nfl_standings (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    season              INTEGER,
    team_id             INTEGER,
    team_abbrev         TEXT,
    team_name           TEXT,
    conference          TEXT,
    division            TEXT,
    wins                INTEGER,
    losses              INTEGER,
    ties                INTEGER,
    points_for          INTEGER,
    points_against      INTEGER,
    point_differential  INTEGER,
    playoff_seed        INTEGER,
    overall_record      TEXT,
    home_record         TEXT,
    road_record         TEXT,
    division_record     TEXT,
    conference_record   TEXT,
    win_streak          INTEGER,
    UNIQUE(season, team_id)
);

CREATE TABLE IF NOT EXISTS nfl_player_stats (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    season                      INTEGER,
    postseason                  INTEGER DEFAULT 0,
    player_id                   INTEGER,
    first_name                  TEXT,
    last_name                   TEXT,
    full_name                   TEXT,
    position                    TEXT,
    position_abbrev             TEXT,
    team                        TEXT,
    games_played                INTEGER,
    -- Passing
    passing_completions         INTEGER,
    passing_attempts            INTEGER,
    passing_yards               INTEGER,
    passing_touchdowns          INTEGER,
    passing_interceptions       INTEGER,
    passing_yards_per_game      REAL,
    passing_completion_pct      REAL,
    yards_per_pass_attempt      REAL,
    qbr                         REAL,
    -- Rushing
    rushing_attempts            INTEGER,
    rushing_yards               INTEGER,
    rushing_touchdowns          INTEGER,
    rushing_yards_per_game      REAL,
    yards_per_rush_attempt      REAL,
    rushing_fumbles             INTEGER,
    rushing_fumbles_lost        INTEGER,
    rushing_first_downs         INTEGER,
    -- Receiving
    receptions                  INTEGER,
    receiving_targets           INTEGER,
    receiving_yards             INTEGER,
    receiving_touchdowns        INTEGER,
    receiving_yards_per_game    REAL,
    yards_per_reception         REAL,
    receiving_fumbles           INTEGER,
    receiving_first_downs       INTEGER,
    -- Defense
    total_tackles               INTEGER,
    solo_tackles                INTEGER,
    assist_tackles              INTEGER,
    defensive_sacks             REAL,
    defensive_sack_yards        REAL,
    defensive_interceptions     INTEGER,
    interception_touchdowns     INTEGER,
    fumbles_forced              INTEGER,
    fumbles_recovered           INTEGER,
    fumbles_touchdowns          INTEGER,
    -- Kicking
    field_goal_attempts         INTEGER,
    field_goals_made            INTEGER,
    field_goal_pct              REAL,
    field_goals_made_1_19       INTEGER,
    field_goals_made_20_29      INTEGER,
    field_goals_made_30_39      INTEGER,
    field_goals_made_40_49      INTEGER,
    field_goals_made_50         INTEGER,
    punts                       INTEGER,
    punt_yards                  INTEGER,
    UNIQUE(season, postseason, player_id)
);
"""


def get_db(rebuild=False) -> sqlite3.Connection:
    if rebuild and DB_PATH.exists():
        DB_PATH.unlink()
        logger.info("Database wiped for rebuild")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_teams(conn, teams):
    rows = [{
        "id":           t.get("id"),
        "abbreviation": t.get("abbreviation",""),
        "full_name":    t.get("full_name",""),
        "location":     t.get("location",""),
        "name":         t.get("name",""),
        "conference":   t.get("conference",""),
        "division":     t.get("division",""),
    } for t in teams]
    conn.executemany("""
        INSERT OR REPLACE INTO nfl_teams (id, abbreviation, full_name, location, name, conference, division)
        VALUES (:id, :abbreviation, :full_name, :location, :name, :conference, :division)
    """, rows)
    conn.commit()
    logger.info(f"   Loaded {len(rows)} teams")


def load_standings(conn, standings, season):
    rows = []
    for s in standings:
        team = s.get("team", {})
        rows.append({
            "season":             season,
            "team_id":            team.get("id"),
            "team_abbrev":        team.get("abbreviation",""),
            "team_name":          team.get("full_name",""),
            "conference":         team.get("conference",""),
            "division":           team.get("division",""),
            "wins":               s.get("wins"),
            "losses":             s.get("losses"),
            "ties":               s.get("ties"),
            "points_for":         s.get("points_for"),
            "points_against":     s.get("points_against"),
            "point_differential": s.get("point_differential"),
            "playoff_seed":       s.get("playoff_seed"),
            "overall_record":     s.get("overall_record",""),
            "home_record":        s.get("home_record",""),
            "road_record":        s.get("road_record",""),
            "division_record":    s.get("division_record",""),
            "conference_record":  s.get("conference_record",""),
            "win_streak":         s.get("win_streak"),
        })
    conn.executemany("""
        INSERT OR REPLACE INTO nfl_standings
            (season, team_id, team_abbrev, team_name, conference, division,
             wins, losses, ties, points_for, points_against, point_differential,
             playoff_seed, overall_record, home_record, road_record,
             division_record, conference_record, win_streak)
        VALUES
            (:season, :team_id, :team_abbrev, :team_name, :conference, :division,
             :wins, :losses, :ties, :points_for, :points_against, :point_differential,
             :playoff_seed, :overall_record, :home_record, :road_record,
             :division_record, :conference_record, :win_streak)
    """, rows)
    conn.commit()
    logger.info(f"   Loaded {len(rows)} standings rows")


def load_player_stats(conn, stats, season, postseason=False):
    rows = []
    for s in stats:
        p = s.get("player", {})
        rows.append({
            "season":                   season,
            "postseason":               1 if postseason else 0,
            "player_id":                p.get("id"),
            "first_name":               p.get("first_name",""),
            "last_name":                p.get("last_name",""),
            "full_name":                f"{p.get('first_name','')} {p.get('last_name','')}".strip(),
            "position":                 p.get("position",""),
            "position_abbrev":          p.get("position_abbreviation",""),
            "team":                     p.get("team","") if isinstance(p.get("team"), str) else "",
            "games_played":             s.get("games_played"),
            "passing_completions":      s.get("passing_completions"),
            "passing_attempts":         s.get("passing_attempts"),
            "passing_yards":            s.get("passing_yards"),
            "passing_touchdowns":       s.get("passing_touchdowns"),
            "passing_interceptions":    s.get("passing_interceptions"),
            "passing_yards_per_game":   s.get("passing_yards_per_game"),
            "passing_completion_pct":   s.get("passing_completion_pct"),
            "yards_per_pass_attempt":   s.get("yards_per_pass_attempt"),
            "qbr":                      s.get("qbr"),
            "rushing_attempts":         s.get("rushing_attempts"),
            "rushing_yards":            s.get("rushing_yards"),
            "rushing_touchdowns":       s.get("rushing_touchdowns"),
            "rushing_yards_per_game":   s.get("rushing_yards_per_game"),
            "yards_per_rush_attempt":   s.get("yards_per_rush_attempt"),
            "rushing_fumbles":          s.get("rushing_fumbles"),
            "rushing_fumbles_lost":     s.get("rushing_fumbles_lost"),
            "rushing_first_downs":      s.get("rushing_first_downs"),
            "receptions":               s.get("receptions"),
            "receiving_targets":        s.get("receiving_targets"),
            "receiving_yards":          s.get("receiving_yards"),
            "receiving_touchdowns":     s.get("receiving_touchdowns"),
            "receiving_yards_per_game": s.get("receiving_yards_per_game"),
            "yards_per_reception":      s.get("yards_per_reception"),
            "receiving_fumbles":        s.get("receiving_fumbles"),
            "receiving_first_downs":    s.get("receiving_first_downs"),
            "total_tackles":            s.get("total_tackles"),
            "solo_tackles":             s.get("solo_tackles"),
            "assist_tackles":           s.get("assist_tackles"),
            "defensive_sacks":          s.get("defensive_sacks"),
            "defensive_sack_yards":     s.get("defensive_sack_yards"),
            "defensive_interceptions":  s.get("defensive_interceptions"),
            "interception_touchdowns":  s.get("interception_touchdowns"),
            "fumbles_forced":           s.get("fumbles_forced"),
            "fumbles_recovered":        s.get("fumbles_recovered"),
            "fumbles_touchdowns":       s.get("fumbles_touchdowns"),
            "field_goal_attempts":      s.get("field_goal_attempts"),
            "field_goals_made":         s.get("field_goals_made"),
            "field_goal_pct":           s.get("field_goal_pct"),
            "field_goals_made_1_19":    s.get("field_goals_made_1_19"),
            "field_goals_made_20_29":   s.get("field_goals_made_20_29"),
            "field_goals_made_30_39":   s.get("field_goals_made_30_39"),
            "field_goals_made_40_49":   s.get("field_goals_made_40_49"),
            "field_goals_made_50":      s.get("field_goals_made_50"),
            "punts":                    s.get("punts"),
            "punt_yards":               s.get("punt_yards"),
        })
    conn.executemany("""
        INSERT OR REPLACE INTO nfl_player_stats
            (season, postseason, player_id, first_name, last_name, full_name,
             position, position_abbrev, team, games_played,
             passing_completions, passing_attempts, passing_yards, passing_touchdowns,
             passing_interceptions, passing_yards_per_game, passing_completion_pct,
             yards_per_pass_attempt, qbr,
             rushing_attempts, rushing_yards, rushing_touchdowns, rushing_yards_per_game,
             yards_per_rush_attempt, rushing_fumbles, rushing_fumbles_lost, rushing_first_downs,
             receptions, receiving_targets, receiving_yards, receiving_touchdowns,
             receiving_yards_per_game, yards_per_reception, receiving_fumbles, receiving_first_downs,
             total_tackles, solo_tackles, assist_tackles, defensive_sacks, defensive_sack_yards,
             defensive_interceptions, interception_touchdowns, fumbles_forced, fumbles_recovered,
             fumbles_touchdowns, field_goal_attempts, field_goals_made, field_goal_pct,
             field_goals_made_1_19, field_goals_made_20_29, field_goals_made_30_39,
             field_goals_made_40_49, field_goals_made_50, punts, punt_yards)
        VALUES
            (:season, :postseason, :player_id, :first_name, :last_name, :full_name,
             :position, :position_abbrev, :team, :games_played,
             :passing_completions, :passing_attempts, :passing_yards, :passing_touchdowns,
             :passing_interceptions, :passing_yards_per_game, :passing_completion_pct,
             :yards_per_pass_attempt, :qbr,
             :rushing_attempts, :rushing_yards, :rushing_touchdowns, :rushing_yards_per_game,
             :yards_per_rush_attempt, :rushing_fumbles, :rushing_fumbles_lost, :rushing_first_downs,
             :receptions, :receiving_targets, :receiving_yards, :receiving_touchdowns,
             :receiving_yards_per_game, :yards_per_reception, :receiving_fumbles, :receiving_first_downs,
             :total_tackles, :solo_tackles, :assist_tackles, :defensive_sacks, :defensive_sack_yards,
             :defensive_interceptions, :interception_touchdowns, :fumbles_forced, :fumbles_recovered,
             :fumbles_touchdowns, :field_goal_attempts, :field_goals_made, :field_goal_pct,
             :field_goals_made_1_19, :field_goals_made_20_29, :field_goals_made_30_39,
             :field_goals_made_40_49, :field_goals_made_50, :punts, :punt_yards)
    """, rows)
    conn.commit()
    logger.info(f"   Loaded {len(rows)} player stat rows ({'postseason' if postseason else 'regular'})")


def print_summary(conn):
    logger.info("=" * 55)
    logger.info("NFL DATABASE SUMMARY")
    logger.info("=" * 55)
    for table in ["nfl_teams", "nfl_standings", "nfl_player_stats"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        logger.info(f"   {table:<35} {count:>6} rows")

    logger.info("=" * 55)
    logger.info("TOP QB PASSING YARDS 2024 (regular season)")
    logger.info("=" * 55)
    rows = conn.execute("""
        SELECT full_name, passing_yards, passing_touchdowns, passing_interceptions,
               ROUND(qbr,1) as qbr, games_played
        FROM nfl_player_stats
        WHERE season=2024 AND postseason=0
          AND passing_yards IS NOT NULL AND passing_yards > 0
        ORDER BY passing_yards DESC LIMIT 10
    """).fetchall()
    for r in rows:
        logger.info(f"   {r['full_name']:<25} {r['passing_yards']} yds  "
                    f"{r['passing_touchdowns']} TD  {r['passing_interceptions']} INT  "
                    f"QBR:{r['qbr']}")

    logger.info("=" * 55)
    logger.info("TOP RB RUSHING YARDS 2024")
    logger.info("=" * 55)
    rows = conn.execute("""
        SELECT full_name, rushing_yards, rushing_touchdowns, rushing_attempts,
               ROUND(yards_per_rush_attempt,2) as ypc
        FROM nfl_player_stats
        WHERE season=2024 AND postseason=0
          AND rushing_yards IS NOT NULL AND rushing_yards > 200
          AND position_abbrev = 'RB'
        ORDER BY rushing_yards DESC LIMIT 10
    """).fetchall()
    for r in rows:
        logger.info(f"   {r['full_name']:<25} {r['rushing_yards']} yds  "
                    f"{r['rushing_touchdowns']} TD  {r['ypc']} YPC")
    logger.info("=" * 55)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_collector(seasons=None, rebuild=False):
    import time as t
    start = t.time()

    logger.info("=" * 55)
    logger.info("NFL DATA COLLECTOR — Starting")
    logger.info("=" * 55)

    client  = NFLApiClient()
    conn    = get_db(rebuild=rebuild)
    seasons = seasons or [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]

    # Teams (once)
    logger.info("── Collecting teams...")
    teams = client.get_teams()
    load_teams(conn, teams)

    for season in seasons:
        logger.info(f"\n{'─'*55}")
        logger.info(f"Season: {season}")
        logger.info(f"{'─'*55}")

        # Standings
        logger.info(f"── Standings {season}...")
        try:
            standings = client.get_standings(season=season)
            load_standings(conn, standings, season)
        except Exception as e:
            logger.warning(f"   Standings failed: {e}")

        # Regular season stats
        logger.info(f"── Player stats {season} (regular season)...")
        try:
            stats = client.get_season_stats(season=season, postseason=False)
            load_player_stats(conn, stats, season, postseason=False)
        except Exception as e:
            logger.warning(f"   Regular season stats failed: {e}")

        # Postseason stats
        logger.info(f"── Player stats {season} (postseason)...")
        try:
            stats = client.get_season_stats(season=season, postseason=True)
            load_player_stats(conn, stats, season, postseason=True)
        except Exception as e:
            logger.warning(f"   Postseason stats failed: {e}")

    print_summary(conn)
    elapsed = t.time() - start
    logger.info(f"✅ Collection complete in {elapsed:.1f}s")
    logger.info(f"   Database: {DB_PATH}")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NFL Data Collector")
    parser.add_argument("--season", type=int, default=None)
    parser.add_argument("--rebuild", action="store_true")
    args = parser.parse_args()

    seasons = [args.season] if args.season else None
    run_collector(seasons=seasons, rebuild=args.rebuild)
