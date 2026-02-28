"""
NHL API Connector - Quick Test
==============================
Run this script to verify the connector is working.

    python test_connector.py

Requirements:
    pip install requests
"""

import sys
import json
from pathlib import Path

# Allow running from the project root
sys.path.insert(0, str(Path(__file__).resolve().parent))

from collectors.nhl_api import NHLApiClient


def pretty(data, max_items=3):
    """Print a trimmed JSON preview."""
    if isinstance(data, list):
        preview = data[:max_items]
        print(json.dumps(preview, indent=2))
        if len(data) > max_items:
            print(f"  ... and {len(data) - max_items} more items")
    else:
        print(json.dumps(data, indent=2)[:1500])


def main():
    client = NHLApiClient()

    print("\n" + "="*55)
    print("1. Current Season")
    print("="*55)
    season = client.current_season()
    print(f"  Season: {season}")

    print("\n" + "="*55)
    print("2. All Team Abbreviations")
    print("="*55)
    teams = client.all_team_abbreviations()
    print(f"  {teams}")
    print(f"  Total teams: {len(teams)}")

    print("\n" + "="*55)
    print("3. Current Standings (top 5)")
    print("="*55)
    standings = client.get_standings()
    for team in standings[:5]:
        name = team.get("teamName", {}).get("default", "Unknown")
        abbrev = team.get("teamAbbrev", {}).get("default", "?")
        pts = team.get("points", 0)
        wins = team.get("wins", 0)
        losses = team.get("losses", 0)
        otl = team.get("otLosses", 0)
        print(f"  {abbrev:4} {name:30} {wins}W-{losses}L-{otl}OTL  {pts}pts")

    print("\n" + "="*55)
    print("4. Points Leaders (top 5)")
    print("="*55)
    leaders = client.get_skater_stats_leaders(season=season, category="points", limit=5)
    for i, p in enumerate(leaders[:5], 1):
        name = f"{p.get('firstName', {}).get('default','')} {p.get('lastName', {}).get('default','')}"
        pts = p.get("value", "?")
        team = p.get("teamAbbrevs", "?")
        print(f"  {i}. {name:25} {team:5} {pts} pts")

    print("\n" + "="*55)
    print("5. Edmonton Oilers Roster (forwards only, top 5)")
    print("="*55)
    roster = client.get_team_roster("EDM")
    forwards = roster.get("forwards", [])
    for p in forwards[:5]:
        name = f"{p.get('firstName', {}).get('default','')} {p.get('lastName', {}).get('default','')}"
        pos = p.get("positionCode", "?")
        pid = p.get("id", "?")
        print(f"  #{p.get('sweaterNumber','?'):2}  {name:25} {pos}  (id: {pid})")

    print("\n" + "="*55)
    print("6. Connor McDavid Game Log (first 5 games)")
    print("="*55)
    # McDavid's player ID is 8478402
    game_log = client.get_player_game_log(player_id=8478402, season=season)
    for game in game_log[:5]:
        date = game.get("gameDate", "?")
        opp = game.get("opponentAbbrev", "?")
        g = game.get("goals", 0)
        a = game.get("assists", 0)
        pts = game.get("points", 0)
        toi = game.get("toi", "?")
        print(f"  {date}  vs {opp:4}  {g}G {a}A {pts}P  TOI: {toi}")

    print("\n✅ All tests passed! The API connector is working.\n")


if __name__ == "__main__":
    main()
