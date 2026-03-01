"""
BallDontLie NFL API - Quick Test
Run: python test_nfl_connector.py
"""

import sys
import json
import requests
from pathlib import Path

API_KEY  = "ebb921c1-e920-458c-9892-f1e537bf2a8a"
BASE_URL = "https://api.balldontlie.io/nfl/v1"
HEADERS  = {"Authorization": API_KEY}


def get(endpoint, params=None):
    url  = f"{BASE_URL}/{endpoint}"
    resp = requests.get(url, headers=HEADERS, params=params or {}, timeout=20)
    print(f"  Status: {resp.status_code}")
    resp.raise_for_status()
    return resp.json()


def pretty(data, limit=3):
    if isinstance(data, list):
        print(json.dumps(data[:limit], indent=2))
    elif isinstance(data, dict):
        print(json.dumps(data, indent=2)[:2000])


print("\n" + "="*55)
print("1. Teams")
print("="*55)
try:
    data = get("teams")
    teams = data.get("data", data) if isinstance(data, dict) else data
    for t in teams[:5]:
        print(f"  {t.get('abbreviation','?'):<6} {t.get('full_name','?'):<30} {t.get('division','?')}")
    print(f"  Fields: {list(teams[0].keys())}")
except Exception as e:
    print(f"  ERROR: {e}")

print("\n" + "="*55)
print("2. Standings 2024")
print("="*55)
try:
    data = get("standings", {"season": 2024})
    rows = data.get("data", [])
    for r in rows[:5]:
        team = r.get("team", {})
        print(f"  {team.get('abbreviation','?'):<6} W:{r.get('wins','?')} L:{r.get('losses','?')} {r.get('conference','?')} {r.get('division','?')}")
    print(f"  Fields: {list(rows[0].keys()) if rows else 'empty'}")
except Exception as e:
    print(f"  ERROR: {e}")

print("\n" + "="*55)
print("3. Season Stats 2024 (first 5 players)")
print("="*55)
try:
    data = get("season_stats", {"season": 2024, "per_page": 5})
    rows = data.get("data", [])
    for r in rows[:5]:
        p = r.get("player", {})
        name = f"{p.get('first_name','')} {p.get('last_name','')}"
        pos  = p.get("position", "?")
        print(f"  {name:<25} {pos:<5}")
    if rows:
        print(f"  All stat fields: {list(rows[0].keys())}")
        # Show non-null stats for first player
        print(f"  Sample row: {json.dumps(rows[0], indent=2)[:1000]}")
except Exception as e:
    print(f"  ERROR: {e}")

print("\n" + "="*55)
print("4. Season Stats 2024 — QB filter")
print("="*55)
try:
    data = get("season_stats", {"season": 2024, "per_page": 5, "position": "QB"})
    rows = data.get("data", [])
    for r in rows[:5]:
        p    = r.get("player", {})
        name = f"{p.get('first_name','')} {p.get('last_name','')}"
        print(f"  {name:<25} Pass Yds: {r.get('passing_yards','?')}  TDs: {r.get('passing_touchdowns','?')}  INTs: {r.get('passing_interceptions','?')}")
except Exception as e:
    print(f"  ERROR: {e}")

print("\n" + "="*55)
print("5. Team Season Stats 2024")
print("="*55)
try:
    data = get("team_season_stats", {"season": 2024, "per_page": 5})
    rows = data.get("data", [])
    for r in rows[:5]:
        t = r.get("team", {})
        print(f"  {t.get('abbreviation','?'):<6} Pts/G: {r.get('total_points_per_game','?')}  Off Yds/G: {r.get('total_offensive_yards_per_game','?')}")
    if rows:
        print(f"  Fields: {list(rows[0].keys())}")
except Exception as e:
    print(f"  ERROR: {e}")

print("\n✅ NFL API test complete!\n")
