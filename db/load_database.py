"""
NHL Database — Schema & Loader
===============================
Creates a SQLite database and loads all raw JSON data into clean tables.

Tables created:
    - teams         : All 32 NHL teams with current season records
    - players       : Every rostered player with bio info
    - skater_stats  : Season stats for all skaters
    - goalie_stats  : Season stats for all goalies
    - standings     : Full standings with rankings

Usage:
    python db/load_database.py

    # Force a fresh rebuild (drops and recreates all tables):
    python db/load_database.py --rebuild
"""

import sys
import os
import json
import sqlite3
import logging
import argparse
from pathlib import Path

# Path fix so this script can be run from the project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
# Paths
# ---------------------------------------------------------------------------
BASE_DIR      = Path(__file__).resolve().parent.parent
DATA_DIR      = BASE_DIR / "data"
RAW_DIR       = DATA_DIR / "raw"
DB_PATH       = DATA_DIR / "nhl_stats.db"

TEAMS_FILE    = RAW_DIR / "teams"    / "teams.json"
PLAYERS_FILE  = RAW_DIR / "rosters" / "all_players.json"
SKATERS_DIR   = RAW_DIR / "skaters"
GOALIES_DIR   = RAW_DIR / "goalies"
STANDINGS_DIR = RAW_DIR / "standings"


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
-- Teams table
CREATE TABLE IF NOT EXISTS teams (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    abbrev          TEXT NOT NULL UNIQUE,
    conference      TEXT,
    division        TEXT,
    wins            INTEGER DEFAULT 0,
    losses          INTEGER DEFAULT 0,
    ot_losses       INTEGER DEFAULT 0,
    points          INTEGER DEFAULT 0,
    games_played    INTEGER DEFAULT 0,
    goals_for       INTEGER DEFAULT 0,
    goals_against   INTEGER DEFAULT 0,
    pp_pct          REAL DEFAULT 0.0,
    pk_pct          REAL DEFAULT 0.0,
    streak          TEXT,
    logo_url        TEXT
);

-- Players table (bio info for all rostered players)
CREATE TABLE IF NOT EXISTS players (
    id              INTEGER PRIMARY KEY,
    first_name      TEXT,
    last_name       TEXT,
    full_name       TEXT,
    number          INTEGER,
    position        TEXT,
    team_abbrev     TEXT,
    shoots_catches  TEXT,
    height_inches   INTEGER,
    weight_pounds   INTEGER,
    birth_date      TEXT,
    birth_country   TEXT,
    headshot_url    TEXT,
    FOREIGN KEY (team_abbrev) REFERENCES teams(abbrev)
);

-- Skater season stats
CREATE TABLE IF NOT EXISTS skater_stats (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id       INTEGER NOT NULL,
    first_name      TEXT,
    last_name       TEXT,
    full_name       TEXT,
    team_abbrev     TEXT,
    position        TEXT,
    season          TEXT,
    games_played    INTEGER,
    goals           INTEGER,
    assists         INTEGER,
    points          INTEGER,
    plus_minus      INTEGER,
    penalty_minutes INTEGER,
    pp_goals        INTEGER,
    sh_goals        INTEGER,
    gw_goals        INTEGER,
    shots           INTEGER,
    hits            INTEGER,
    blocked_shots   INTEGER,
    toi             TEXT,
    -- Calculated fields (populated after load)
    points_per_game REAL,
    goals_per_game  REAL,
    shooting_pct    REAL,
    FOREIGN KEY (player_id) REFERENCES players(id)
);

-- Goalie season stats
CREATE TABLE IF NOT EXISTS goalie_stats (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id       INTEGER NOT NULL,
    first_name      TEXT,
    last_name       TEXT,
    full_name       TEXT,
    team_abbrev     TEXT,
    season          TEXT,
    wins            INTEGER,
    losses          INTEGER,
    ot_losses       INTEGER,
    save_pct        REAL,
    gaa             REAL,
    shutouts        INTEGER,
    FOREIGN KEY (player_id) REFERENCES players(id)
);

-- Full standings snapshot
CREATE TABLE IF NOT EXISTS standings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    season          TEXT,
    team_abbrev     TEXT,
    team_name       TEXT,
    conference      TEXT,
    division        TEXT,
    wins            INTEGER,
    losses          INTEGER,
    ot_losses       INTEGER,
    points          INTEGER,
    games_played    INTEGER,
    goals_for       INTEGER,
    goals_against   INTEGER,
    goal_diff       INTEGER,
    home_wins       INTEGER,
    home_losses     INTEGER,
    away_wins       INTEGER,
    away_losses     INTEGER,
    l10_wins        INTEGER,
    l10_losses      INTEGER,
    pp_pct          REAL,
    pk_pct          REAL,
    FOREIGN KEY (team_abbrev) REFERENCES teams(abbrev)
);
"""

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_connection() -> sqlite3.Connection:
    """Open and return a database connection with foreign keys enabled."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row      # lets you access columns by name
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute("PRAGMA journal_mode = WAL")   # faster writes
    return conn


def create_schema(conn: sqlite3.Connection, rebuild: bool = False) -> None:
    """Create tables. If rebuild=True, drop existing tables first."""
    if rebuild:
        logger.info("Rebuilding database — dropping existing tables...")
        tables = ["skater_stats", "goalie_stats", "standings", "players", "teams"]
        for t in tables:
            conn.execute(f"DROP TABLE IF EXISTS {t}")
        conn.commit()

    conn.executescript(SCHEMA)
    conn.commit()
    logger.info(f"Schema ready: {DB_PATH}")


def load_json(path: Path):
    if not path.exists():
        logger.warning(f"File not found: {path}")
        return None
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_teams(conn: sqlite3.Connection) -> int:
    """Load teams from teams.json."""
    logger.info("── Loading teams...")
    data = load_json(TEAMS_FILE)
    if not data:
        return 0

    sql = """
        INSERT OR REPLACE INTO teams
            (id, name, abbrev, conference, division,
             wins, losses, ot_losses, points, games_played,
             goals_for, goals_against, pp_pct, pk_pct, streak, logo_url)
        VALUES
            (:id, :name, :abbrev, :conference, :division,
             :wins, :losses, :ot_losses, :points, :games_played,
             :goals_for, :goals_against, :pp_pct, :pk_pct, :streak, :logo_url)
    """
    conn.executemany(sql, data)
    conn.commit()
    logger.info(f"   Loaded {len(data)} teams")
    return len(data)


def load_players(conn: sqlite3.Connection) -> int:
    """Load all rostered players from all_players.json."""
    logger.info("── Loading players...")
    data = load_json(PLAYERS_FILE)
    if not data:
        return 0

    # Add computed full_name field
    for p in data:
        p["full_name"] = f"{p.get('first_name','')} {p.get('last_name','')}".strip()

    sql = """
        INSERT OR REPLACE INTO players
            (id, first_name, last_name, full_name, number, position,
             team_abbrev, shoots_catches, height_inches, weight_pounds,
             birth_date, birth_country, headshot_url)
        VALUES
            (:id, :first_name, :last_name, :full_name, :number, :position,
             :team_abbrev, :shoots_catches, :height_inches, :weight_pounds,
             :birth_date, :birth_country, :headshot_url)
    """
    conn.executemany(sql, data)
    conn.commit()
    logger.info(f"   Loaded {len(data)} players")
    return len(data)


def load_skater_stats(conn: sqlite3.Connection) -> int:
    """Load skater season stats from all skater_stats_*.json files."""
    logger.info("── Loading skater stats...")
    total = 0

    skater_files = list(SKATERS_DIR.glob("skater_stats_*.json"))
    if not skater_files:
        logger.warning("   No skater stats files found")
        return 0

    for path in skater_files:
        data = load_json(path)
        if not data:
            continue

        season = path.stem.replace("skater_stats_", "")
        rows = []
        for p in data:
            goals   = p.get("goals") or 0
            assists = p.get("assists") or 0
            points  = p.get("points") or 0
            shots   = p.get("shots") or 0
            gp      = p.get("games_played") or 0

            rows.append({
                "player_id":       p.get("player_id"),
                "first_name":      p.get("first_name", ""),
                "last_name":       p.get("last_name", ""),
                "full_name":       p.get("full_name") or f"{p.get('first_name','')} {p.get('last_name','')}".strip(),
                "team_abbrev":     p.get("team_abbrev", ""),
                "position":        p.get("position", ""),
                "season":          season,
                "games_played":    gp,
                "goals":           goals,
                "assists":         assists,
                "points":          points,
                "plus_minus":      p.get("plus_minus") or 0,
                "penalty_minutes": p.get("penalty_minutes") or 0,
                "pp_goals":        p.get("pp_goals") or 0,
                "sh_goals":        p.get("sh_goals") or 0,
                "gw_goals":        p.get("gw_goals") or 0,
                "shots":           shots,
                "hits":            p.get("hits") or 0,
                "blocked_shots":   p.get("blocked_shots") or 0,
                "toi":             p.get("toi_per_game") or p.get("toi") or "",
                "points_per_game": p.get("points_per_game") or (round(points / gp, 3) if gp > 0 else None),
                "goals_per_game":  p.get("goals_per_game") or (round(goals / gp, 3) if gp > 0 else None),
                "shooting_pct":    p.get("shooting_pct") or (round(goals / shots * 100, 1) if shots > 0 else None),
            })

        sql = """
            INSERT OR REPLACE INTO skater_stats
                (player_id, first_name, last_name, full_name, team_abbrev, position, season,
                 games_played, goals, assists, points, plus_minus, penalty_minutes,
                 pp_goals, sh_goals, gw_goals, shots, hits, blocked_shots, toi,
                 points_per_game, goals_per_game, shooting_pct)
            VALUES
                (:player_id, :first_name, :last_name, :full_name, :team_abbrev, :position, :season,
                 :games_played, :goals, :assists, :points, :plus_minus, :penalty_minutes,
                 :pp_goals, :sh_goals, :gw_goals, :shots, :hits, :blocked_shots, :toi,
                 :points_per_game, :goals_per_game, :shooting_pct)
        """
        conn.executemany(sql, rows)
        conn.commit()
        total += len(rows)
        logger.info(f"   {path.name}: {len(rows)} skaters loaded")

    return total


def load_goalie_stats(conn: sqlite3.Connection) -> int:
    """Load goalie season stats from all goalie_stats_*.json files."""
    logger.info("── Loading goalie stats...")
    total = 0

    goalie_files = list(GOALIES_DIR.glob("goalie_stats_*.json"))
    if not goalie_files:
        logger.warning("   No goalie stats files found")
        return 0

    for path in goalie_files:
        data = load_json(path)
        if not data:
            continue

        rows = []
        season = path.stem.replace("goalie_stats_", "")
        for g in data:
            rows.append({
                "player_id":   g.get("player_id"),
                "first_name":  g.get("first_name", ""),
                "last_name":   g.get("last_name", ""),
                "full_name":   f"{g.get('first_name','')} {g.get('last_name','')}".strip(),
                "team_abbrev": g.get("team_abbrev", ""),
                "season":      season,
                "wins":        g.get("wins"),
                "losses":      None,
                "ot_losses":   None,
                "save_pct":    g.get("save_pct"),
                "gaa":         g.get("gaa"),
                "shutouts":    g.get("shutouts"),
            })

        sql = """
            INSERT OR REPLACE INTO goalie_stats
                (player_id, first_name, last_name, full_name, team_abbrev, season,
                 wins, losses, ot_losses, save_pct, gaa, shutouts)
            VALUES
                (:player_id, :first_name, :last_name, :full_name, :team_abbrev, :season,
                 :wins, :losses, :ot_losses, :save_pct, :gaa, :shutouts)
        """
        conn.executemany(sql, rows)
        conn.commit()
        total += len(rows)
        logger.info(f"   {path.name}: {len(rows)} goalies loaded")

    return total


def load_standings(conn: sqlite3.Connection) -> int:
    """Load standings from all standings_*.json files."""
    logger.info("── Loading standings...")
    total = 0

    standings_files = list(STANDINGS_DIR.glob("standings_*.json"))
    if not standings_files:
        logger.warning("   No standings files found")
        return 0

    for path in standings_files:
        data = load_json(path)
        if not data:
            continue

        season = path.stem.replace("standings_", "")
        rows = []
        for t in data:
            home_wins   = t.get("homeWins", 0)   or 0
            home_losses = t.get("homeLosses", 0) or 0
            away_wins   = t.get("roadWins", 0)   or 0
            away_losses = t.get("roadLosses", 0) or 0
            l10_wins    = t.get("l10Wins", 0)    or 0
            l10_losses  = t.get("l10Losses", 0)  or 0
            gf          = t.get("goalFor", 0)    or 0
            ga          = t.get("goalAgainst", 0)or 0

            rows.append({
                "season":       season,
                "team_abbrev":  t.get("teamAbbrev", {}).get("default", ""),
                "team_name":    t.get("teamName", {}).get("default", ""),
                "conference":   t.get("conferenceName", ""),
                "division":     t.get("divisionName", ""),
                "wins":         t.get("wins", 0),
                "losses":       t.get("losses", 0),
                "ot_losses":    t.get("otLosses", 0),
                "points":       t.get("points", 0),
                "games_played": t.get("gamesPlayed", 0),
                "goals_for":    gf,
                "goals_against":ga,
                "goal_diff":    gf - ga,
                "home_wins":    home_wins,
                "home_losses":  home_losses,
                "away_wins":    away_wins,
                "away_losses":  away_losses,
                "l10_wins":     l10_wins,
                "l10_losses":   l10_losses,
                "pp_pct":       t.get("powerPlayPct", 0.0),
                "pk_pct":       t.get("penaltyKillPct", 0.0),
            })

        sql = """
            INSERT INTO standings
                (season, team_abbrev, team_name, conference, division,
                 wins, losses, ot_losses, points, games_played,
                 goals_for, goals_against, goal_diff,
                 home_wins, home_losses, away_wins, away_losses,
                 l10_wins, l10_losses, pp_pct, pk_pct)
            VALUES
                (:season, :team_abbrev, :team_name, :conference, :division,
                 :wins, :losses, :ot_losses, :points, :games_played,
                 :goals_for, :goals_against, :goal_diff,
                 :home_wins, :home_losses, :away_wins, :away_losses,
                 :l10_wins, :l10_losses, :pp_pct, :pk_pct)
        """
        conn.executemany(sql, rows)
        conn.commit()
        total += len(rows)
        logger.info(f"   {path.name}: {len(rows)} teams loaded")

    return total


def print_summary(conn: sqlite3.Connection) -> None:
    """Print a quick summary of what's in the database."""
    logger.info("="*55)
    logger.info("DATABASE SUMMARY")
    logger.info("="*55)

    tables = ["teams", "players", "skater_stats", "goalie_stats", "standings"]
    for table in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logger.info(f"   {table:20} {count:>6} rows")
        except Exception:
            logger.info(f"   {table:20} (empty or not found)")

    # Bonus: quick top 5 scorers preview
    logger.info("="*55)
    logger.info("TOP 5 POINTS LEADERS (preview)")
    logger.info("="*55)
    try:
        rows = conn.execute("""
            SELECT full_name, team_abbrev, goals, assists, points
            FROM skater_stats
            WHERE points IS NOT NULL
            ORDER BY points DESC
            LIMIT 5
        """).fetchall()
        for i, r in enumerate(rows, 1):
            logger.info(f"   {i}. {r['full_name']:25} {r['team_abbrev']:5} "
                        f"{r['goals']}G {r['assists']}A {r['points']}P")
    except Exception as e:
        logger.warning(f"   Could not preview scorers: {e}")

    logger.info(f"="*55)
    logger.info(f"   Database saved to: {DB_PATH}")
    logger.info("="*55)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_loader(rebuild: bool = False):
    """Run the full database load pipeline."""
    logger.info("="*55)
    logger.info("NHL DATABASE LOADER — Starting")
    logger.info("="*55)

    conn = get_connection()

    try:
        create_schema(conn, rebuild=rebuild)
        load_teams(conn)
        load_players(conn)
        load_skater_stats(conn)
        load_goalie_stats(conn)
        load_standings(conn)
        print_summary(conn)
        logger.info("✅ Database load complete!")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NHL Database Loader")
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Drop and recreate all tables before loading."
    )
    args = parser.parse_args()
    run_loader(rebuild=args.rebuild)
