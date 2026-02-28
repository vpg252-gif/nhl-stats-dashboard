"""
PGA Tour Data Collector
=======================
Bulk-pulls golf data using the API connector and saves everything locally
as clean JSON files, then loads into SQLite for analysis.

What it collects:
    - Tournament schedules (2023, 2024, 2025)
    - Leaderboard results for every completed tournament
    - Player finish history aggregated across all tournaments

Usage:
    python golf/golf_collector.py
    python golf/golf_collector.py --year 2025      # single season
    python golf/golf_collector.py --limit 10       # first N tournaments only (testing)
"""

import sys
import os
import json
import time
import sqlite3
import logging
import argparse
from pathlib import Path
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from golf.golf_api import PGAApiClient

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
RAW_DIR       = DATA_DIR / "golf_raw"
DB_PATH       = DATA_DIR / "golf_stats.db"

SCHEDULE_DIR  = RAW_DIR / "schedules"
RESULTS_DIR   = RAW_DIR / "results"

for d in [SCHEDULE_DIR, RESULTS_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def save_json(path: Path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Database schema
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS golf_tournaments (
    tourn_id        TEXT,
    year            INTEGER,
    name            TEXT,
    start_date      TEXT,
    end_date        TEXT,
    purse           INTEGER,
    winners_share   INTEGER,
    fedex_points    INTEGER,
    format          TEXT,
    PRIMARY KEY (tourn_id, year)
);

CREATE TABLE IF NOT EXISTS golf_results (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    tourn_id            TEXT,
    year                INTEGER,
    tournament_name     TEXT,
    player_id           TEXT,
    first_name          TEXT,
    last_name           TEXT,
    full_name           TEXT,
    position            TEXT,
    position_num        INTEGER,
    total_score         TEXT,
    total_strokes       INTEGER,
    is_amateur          INTEGER,
    made_cut            INTEGER,
    top_5               INTEGER,
    top_10              INTEGER,
    top_20              INTEGER,
    top_25              INTEGER,
    win                 INTEGER,
    FOREIGN KEY (tourn_id, year) REFERENCES golf_tournaments(tourn_id, year)
);

CREATE TABLE IF NOT EXISTS golf_player_season_stats (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id           TEXT,
    first_name          TEXT,
    last_name           TEXT,
    full_name           TEXT,
    year                INTEGER,
    events              INTEGER DEFAULT 0,
    cuts_made           INTEGER DEFAULT 0,
    wins                INTEGER DEFAULT 0,
    top_5               INTEGER DEFAULT 0,
    top_10              INTEGER DEFAULT 0,
    top_20              INTEGER DEFAULT 0,
    top_25              INTEGER DEFAULT 0,
    total_strokes       INTEGER DEFAULT 0,
    avg_score           REAL,
    best_finish         INTEGER,
    worst_finish        INTEGER,
    total_fedex_points  INTEGER DEFAULT 0,
    cut_pct             REAL,
    win_pct             REAL,
    UNIQUE(player_id, year)
);
"""


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Collection
# ---------------------------------------------------------------------------

def collect_schedule(client: PGAApiClient, year: int) -> list[dict]:
    """Pull and save schedule for a season."""
    logger.info(f"── Schedule {year}...")
    schedule = client.get_schedule(year=year)
    save_json(SCHEDULE_DIR / f"schedule_{year}.json", schedule)
    logger.info(f"   {len(schedule)} tournaments")
    return schedule


def parse_position(pos_str: str) -> Optional[int]:
    """Convert position string like '1', 'T5', 'CUT', 'WD' to int."""
    if not pos_str:
        return None
    clean = str(pos_str).upper().replace("T", "").strip()
    try:
        return int(clean)
    except ValueError:
        return None


def collect_leaderboard(
    client: PGAApiClient,
    tourn_id: str,
    year: int,
    tourn_name: str,
) -> list[dict]:
    """
    Fetch leaderboard for one tournament and parse into clean result rows.
    Returns list of player result dicts.
    """
    out_path = RESULTS_DIR / f"leaderboard_{tourn_id}_{year}.json"
    if out_path.exists():
        raw = load_json(out_path)
    else:
        try:
            raw = client.get_leaderboard(tourn_id=tourn_id, year=year)
            save_json(out_path, raw)
            time.sleep(0.6)
        except Exception as e:
            logger.warning(f"   Failed leaderboard {tourn_name}: {e}")
            return []

    rows = raw.get("leaderboardRows", [])
    if not rows:
        logger.warning(f"   No leaderboardRows for {tourn_name}")
        return []

    results = []
    for p in rows:
        pos_str  = p.get("position", "")
        pos_num  = parse_position(pos_str)
        status   = str(p.get("status", "")).upper()
        made_cut = 0 if status in ["CUT", "WD", "DQ", "MDF"] else 1

        # Parse total strokes safely
        strokes_raw = p.get("totalStrokesFromCompletedRounds", "")
        try:
            total_strokes = int(strokes_raw)
        except (ValueError, TypeError):
            total_strokes = None

        results.append({
            "tourn_id":        tourn_id,
            "year":            year,
            "tournament_name": tourn_name,
            "player_id":       p.get("playerId", ""),
            "first_name":      p.get("firstName", ""),
            "last_name":       p.get("lastName", ""),
            "full_name":       f"{p.get('firstName','')} {p.get('lastName','')}".strip(),
            "position":        pos_str,
            "position_num":    pos_num,
            "total_score":     p.get("total", ""),
            "total_strokes":   total_strokes,
            "is_amateur":      1 if p.get("isAmateur") else 0,
            "made_cut":        made_cut,
            "win":             1 if pos_num == 1 else 0,
            "top_5":           1 if pos_num and pos_num <= 5 else 0,
            "top_10":          1 if pos_num and pos_num <= 10 else 0,
            "top_20":          1 if pos_num and pos_num <= 20 else 0,
            "top_25":          1 if pos_num and pos_num <= 25 else 0,
        })

    return results


# ---------------------------------------------------------------------------
# Database loaders
# ---------------------------------------------------------------------------

def load_tournaments(conn: sqlite3.Connection, schedule: list[dict], year: int):
    """Insert tournament records into golf_tournaments."""
    rows = []
    for t in schedule:
        date     = t.get("date", {})
        start    = date.get("start", "")[:10] if isinstance(date, dict) else ""
        end      = date.get("end", "")[:10] if isinstance(date, dict) else ""
        rows.append({
            "tourn_id":      t.get("tournId", ""),
            "year":          year,
            "name":          t.get("name", ""),
            "start_date":    start,
            "end_date":      end,
            "purse":         t.get("purse"),
            "winners_share": t.get("winnersShare"),
            "fedex_points":  t.get("fedexCupPoints"),
            "format":        t.get("format", ""),
        })
    conn.executemany("""
        INSERT OR REPLACE INTO golf_tournaments
            (tourn_id, year, name, start_date, end_date, purse, winners_share, fedex_points, format)
        VALUES
            (:tourn_id, :year, :name, :start_date, :end_date, :purse, :winners_share, :fedex_points, :format)
    """, rows)
    conn.commit()


def load_results(conn: sqlite3.Connection, results: list[dict]):
    """Insert leaderboard rows into golf_results."""
    if not results:
        return
    conn.executemany("""
        INSERT INTO golf_results
            (tourn_id, year, tournament_name, player_id, first_name, last_name, full_name,
             position, position_num, total_score, total_strokes, is_amateur,
             made_cut, win, top_5, top_10, top_20, top_25)
        VALUES
            (:tourn_id, :year, :tournament_name, :player_id, :first_name, :last_name, :full_name,
             :position, :position_num, :total_score, :total_strokes, :is_amateur,
             :made_cut, :win, :top_5, :top_10, :top_20, :top_25)
    """, results)
    conn.commit()


def build_season_stats(conn: sqlite3.Connection):
    """
    Aggregate golf_results into golf_player_season_stats.
    Runs after all results are loaded.
    """
    logger.info("── Building player season stats...")
    conn.execute("DELETE FROM golf_player_season_stats")
    conn.execute("""
        INSERT INTO golf_player_season_stats
            (player_id, first_name, last_name, full_name, year,
             events, cuts_made, wins, top_5, top_10, top_20, top_25,
             total_strokes, avg_score, best_finish, worst_finish,
             cut_pct, win_pct)
        SELECT
            player_id,
            MAX(first_name)                                 AS first_name,
            MAX(last_name)                                  AS last_name,
            MAX(full_name)                                  AS full_name,
            year,
            COUNT(*)                                        AS events,
            SUM(made_cut)                                   AS cuts_made,
            SUM(win)                                        AS wins,
            SUM(top_5)                                      AS top_5,
            SUM(top_10)                                     AS top_10,
            SUM(top_20)                                     AS top_20,
            SUM(top_25)                                     AS top_25,
            SUM(CASE WHEN total_strokes IS NOT NULL THEN total_strokes ELSE 0 END) AS total_strokes,
            ROUND(
                AVG(CASE WHEN total_strokes IS NOT NULL AND made_cut=1 THEN total_strokes END),
                1
            )                                               AS avg_score,
            MIN(CASE WHEN position_num IS NOT NULL THEN position_num END) AS best_finish,
            MAX(CASE WHEN position_num IS NOT NULL THEN position_num END) AS worst_finish,
            ROUND(CAST(SUM(made_cut) AS REAL) / COUNT(*) * 100, 1) AS cut_pct,
            ROUND(CAST(SUM(win) AS REAL) / COUNT(*) * 100, 1)      AS win_pct
        FROM golf_results
        WHERE player_id != ''
        GROUP BY player_id, year
    """)
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM golf_player_season_stats").fetchone()[0]
    logger.info(f"   Built stats for {count} player-seasons")


def print_summary(conn: sqlite3.Connection):
    logger.info("="*55)
    logger.info("GOLF DATABASE SUMMARY")
    logger.info("="*55)
    for table in ["golf_tournaments", "golf_results", "golf_player_season_stats"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        logger.info(f"   {table:<35} {count:>6} rows")

    logger.info("="*55)
    logger.info("TOP 10 WINNERS (2023-2025)")
    logger.info("="*55)
    rows = conn.execute("""
        SELECT full_name, year, events, wins, top_5, top_10, best_finish,
               ROUND(cut_pct,1) as cut_pct
        FROM golf_player_season_stats
        WHERE wins > 0
        ORDER BY wins DESC, top_10 DESC
        LIMIT 10
    """).fetchall()
    for r in rows:
        logger.info(f"   {r['full_name']:<25} {r['year']}  "
                    f"{r['wins']}W  {r['top_5']} top5  {r['top_10']} top10  "
                    f"cut%={r['cut_pct']}")
    logger.info("="*55)


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_collector(years: Optional[list] = None, limit: Optional[int] = None):
    """
    Run the full golf data collection pipeline.

    Parameters
    ----------
    years : List of season years to collect. Defaults to [2023, 2024, 2025].
    limit : Max tournaments per season (for testing).
    """
    import time as t
    start = t.time()

    logger.info("="*55)
    logger.info("PGA TOUR DATA COLLECTOR — Starting")
    logger.info("="*55)

    client = PGAApiClient()
    conn   = get_db()
    years  = years or [2023, 2024, 2025]

    total_tournaments = 0
    total_results     = 0

    for year in years:
        logger.info(f"\n{'─'*55}")
        logger.info(f"Season: {year}")
        logger.info(f"{'─'*55}")

        # Schedule
        schedule = collect_schedule(client, year)
        load_tournaments(conn, schedule, year)

        # Completed tournaments only
        completed = client.get_completed_tournaments(year=year)
        if limit:
            completed = completed[:limit]

        logger.info(f"── Collecting leaderboards: {len(completed)} completed tournaments")

        for i, tourn in enumerate(completed, 1):
            tid  = tourn.get("tournId", "")
            name = tourn.get("name", "")
            logger.info(f"   [{i}/{len(completed)}] {name} ({tid})")

            results = collect_leaderboard(client, tid, year, name)
            load_results(conn, results)

            total_results += len(results)
            total_tournaments += 1

    # Build aggregated season stats
    build_season_stats(conn)
    print_summary(conn)

    elapsed = t.time() - start
    logger.info(f"✅ Collection complete in {elapsed:.1f}s")
    logger.info(f"   Tournaments: {total_tournaments}")
    logger.info(f"   Result rows: {total_results}")
    logger.info(f"   Database:    {DB_PATH}")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PGA Tour Data Collector")
    parser.add_argument("--year", type=int, default=None, help="Single year to collect e.g. 2025")
    parser.add_argument("--limit", type=int, default=None, help="Max tournaments per season (for testing)")
    args = parser.parse_args()

    years = [args.year] if args.year else None
    run_collector(years=years, limit=args.limit)
