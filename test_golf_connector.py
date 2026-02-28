"""
PGA Tour API Connector - Quick Test
Run: python test_golf_connector.py
"""

import sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from golf.golf_api import PGAApiClient

client = PGAApiClient()

print("\n" + "="*55)
print("1. PGA Tour Schedule 2025 (first 5 events)")
print("="*55)
schedule = client.get_schedule(year=2025)
for t in schedule[:5]:
    name = t.get("name", "?")
    date = t.get("date", {})
    purse = t.get("purse", "?")
    print(f"  {name:<45} Purse: ${purse:,}" if isinstance(purse, int) else f"  {name:<45} {date}")
print(f"  Total events: {len(schedule)}")
print(f"  Fields: {list(schedule[0].keys()) if schedule else 'empty'}")

print("\n" + "="*55)
print("2. Completed Tournaments 2025")
print("="*55)
completed = client.get_completed_tournaments(year=2025)
print(f"  Completed so far: {len(completed)}")
for t in completed[:5]:
    print(f"  {t.get('tournId'):<8} {t.get('name')}")

print("\n" + "="*55)
print("3. Leaderboard Field Probe (first completed event)")
print("="*55)
result = client.probe_leaderboard_fields(year=2025)
print(json.dumps(result, indent=2)[:1500])

print("\n" + "="*55)
print("4. Earnings for first completed tournament")
print("="*55)
if completed:
    tid  = completed[0].get("tournId")
    earn = client.get_tournament_earnings(tourn_id=tid, year=2025)
    print(f"  {len(earn)} players with earnings data")
    if earn:
        print(f"  Fields: {list(earn[0].keys())}")
        print(f"  Sample: {json.dumps(earn[0], indent=2)[:300]}")

print("\n" + "="*55)
print("5. FedExCup Points for first completed tournament")
print("="*55)
if completed:
    pts = client.get_tournament_points(tourn_id=tid, year=2025)
    print(f"  {len(pts)} players with points data")
    if pts:
        print(f"  Fields: {list(pts[0].keys())}")
        print(f"  Sample: {json.dumps(pts[0], indent=2)[:300]}")

print("\n✅ Golf connector test complete!\n")
