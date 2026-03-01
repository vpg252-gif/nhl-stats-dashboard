"""
Multi-Sport Stats Dashboard — NHL + PGA Tour + NFL
"""

import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

st.set_page_config(page_title="Sports Stats Dashboard", page_icon="🏆", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    .stApp { background-color: #0e1117; }
    .sport-header { padding: 20px 28px; border-radius: 12px; margin-bottom: 20px; text-align: center; }
    .nhl-header  { background: linear-gradient(135deg, #001F5B 0%, #C8102E 100%); }
    .golf-header { background: linear-gradient(135deg, #1a4731 0%, #2d7a4f 100%); }
    .nfl-header  { background: linear-gradient(135deg, #1a1a2e 0%, #825A2C 50%, #D4AF37 100%); }
    .sport-header h1 { color: white; font-size: 2.2rem; font-weight: 800; margin: 0; }
    .sport-header p  { color: rgba(255,255,255,0.75); font-size: 0.9rem; margin: 6px 0 0 0; }
    .metric-card { background: #1c2333; border: 1px solid #2d3748; border-radius: 10px; padding: 16px 20px; text-align: center; }
    .metric-card .label { color: #a0aec0; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.8px; }
    .metric-card .value { color: #ffffff; font-size: 1.7rem; font-weight: 800; margin: 4px 0; }
    .metric-card .sub   { font-size: 0.8rem; font-weight: 600; }
    .nhl-sub  { color: #C8102E; } .golf-sub { color: #2d7a4f; } .nfl-sub { color: #D4AF37; }
    .section-header { color: #ffffff; font-size: 1.1rem; font-weight: 700; padding: 8px 0 10px 0; margin-bottom: 14px; }
    .nhl-section  { border-bottom: 2px solid #C8102E; }
    .golf-section { border-bottom: 2px solid #2d7a4f; }
    .nfl-section  { border-bottom: 2px solid #D4AF37; }
    #MainMenu {visibility: hidden;} footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

NHL_DB  = Path(__file__).parent / "data" / "nhl_stats.db"
GOLF_DB = Path(__file__).parent / "data" / "golf_stats.db"
NFL_DB  = Path(__file__).parent / "data" / "nfl_stats.db"

@st.cache_resource
def get_nhl_conn():
    if not NHL_DB.exists(): return None
    conn = sqlite3.connect(str(NHL_DB), check_same_thread=False); conn.row_factory = sqlite3.Row; return conn

@st.cache_resource
def get_golf_conn():
    if not GOLF_DB.exists(): return None
    conn = sqlite3.connect(str(GOLF_DB), check_same_thread=False); conn.row_factory = sqlite3.Row; return conn

@st.cache_resource
def get_nfl_conn():
    if not NFL_DB.exists(): return None
    conn = sqlite3.connect(str(NFL_DB), check_same_thread=False); conn.row_factory = sqlite3.Row; return conn

# ── NHL loaders ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_skaters():
    conn = get_nhl_conn()
    if not conn: return pd.DataFrame()
    return pd.read_sql_query("""SELECT full_name AS "Player", team_abbrev AS "Team", position AS "Pos",
        games_played AS "GP", goals AS "G", assists AS "A", points AS "PTS", plus_minus AS "+/-",
        pp_goals AS "PPG", sh_goals AS "SHG", gw_goals AS "GWG", shots AS "SOG", penalty_minutes AS "PIM",
        ROUND(COALESCE(shooting_pct,0),1) AS "Sh%", ROUND(COALESCE(points_per_game,0),3) AS "P/GP",
        ROUND(COALESCE(goals_per_game,0),3) AS "G/GP"
        FROM skater_stats WHERE points IS NOT NULL ORDER BY points DESC, goals DESC""", conn)

@st.cache_data(ttl=300)
def load_goalies():
    conn = get_nhl_conn()
    if not conn: return pd.DataFrame()
    return pd.read_sql_query("""SELECT full_name AS "Goalie", team_abbrev AS "Team",
        wins AS "W", losses AS "L", ot_losses AS "OTL",
        ROUND(COALESCE(save_pct,0),3) AS "SV%", ROUND(COALESCE(gaa,0),2) AS "GAA", shutouts AS "SO"
        FROM goalie_stats WHERE wins IS NOT NULL ORDER BY save_pct DESC, wins DESC""", conn)

@st.cache_data(ttl=300)
def load_standings():
    conn = get_nhl_conn()
    if not conn: return pd.DataFrame()
    return pd.read_sql_query("""SELECT team_name AS "Team", team_abbrev AS "Abbrev", division AS "Division",
        conference AS "Conference", wins AS "W", losses AS "L", ot_losses AS "OTL", points AS "PTS",
        games_played AS "GP", goals_for AS "GF", goals_against AS "GA", goal_diff AS "GDiff",
        ROUND(CAST(goals_for AS REAL)/NULLIF(games_played,0),2) AS "GF/GP",
        ROUND(CAST(goals_against AS REAL)/NULLIF(games_played,0),2) AS "GA/GP",
        ROUND(pp_pct,1) AS "PP%", ROUND(pk_pct,1) AS "PK%",
        home_wins AS "HW", home_losses AS "HL", away_wins AS "AW", away_losses AS "AL",
        l10_wins AS "L10W", l10_losses AS "L10L"
        FROM standings ORDER BY points DESC, wins DESC""", conn)

@st.cache_data(ttl=300)
def load_advanced():
    conn = get_nhl_conn()
    if not conn: return pd.DataFrame()
    return pd.read_sql_query("""SELECT full_name AS "Player", team_abbrev AS "Team", position AS "Pos", points AS "PTS",
        ROUND(COALESCE(points_per_game,0),3) AS "P/GP", ROUND(COALESCE(goals_per_game,0),3) AS "G/GP",
        ROUND(CASE WHEN COALESCE(points,0)>0 THEN CAST(COALESCE(goals,0) AS REAL)/points*100 ELSE 0 END,1) AS "Goal%",
        ROUND(CASE WHEN COALESCE(points,0)>0 THEN CAST(COALESCE(assists,0) AS REAL)/points*100 ELSE 0 END,1) AS "Ast%",
        plus_minus AS "+/-", ROUND(COALESCE(shooting_pct,0),1) AS "Sh%", penalty_minutes AS "PIM"
        FROM skater_stats WHERE points >= 10 ORDER BY points_per_game DESC""", conn)

# ── Golf loaders ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_golf_season_stats(year=None):
    conn = get_golf_conn()
    if not conn: return pd.DataFrame()
    where = f"WHERE year = {year}" if year else "WHERE year IN (2023,2024,2025)"
    return pd.read_sql_query(f"""SELECT full_name AS "Player", year AS "Year", events AS "Events",
        cuts_made AS "Cuts", wins AS "Wins", top_5 AS "Top 5", top_10 AS "Top 10",
        top_20 AS "Top 20", top_25 AS "Top 25", best_finish AS "Best Finish",
        ROUND(COALESCE(avg_score,0),1) AS "Avg Score",
        ROUND(COALESCE(cut_pct,0),1) AS "Cut%", ROUND(COALESCE(win_pct,0),1) AS "Win%"
        FROM golf_player_season_stats {where} ORDER BY wins DESC, top_10 DESC""", conn)

@st.cache_data(ttl=300)
def load_golf_results(year=None):
    conn = get_golf_conn()
    if not conn: return pd.DataFrame()
    where = f"WHERE r.year = {year}" if year else "WHERE r.year IN (2023,2024,2025)"
    return pd.read_sql_query(f"""SELECT r.full_name AS "Player", r.year AS "Year",
        r.tournament_name AS "Tournament", r.position AS "Finish", r.position_num AS "Pos#",
        r.total_score AS "Score", r.total_strokes AS "Strokes", r.made_cut AS "Made Cut",
        r.win AS "Win", r.top_5 AS "Top 5", r.top_10 AS "Top 10", t.purse AS "Purse"
        FROM golf_results r LEFT JOIN golf_tournaments t ON r.tourn_id=t.tourn_id AND r.year=t.year
        {where} ORDER BY r.year DESC, r.position_num ASC""", conn)

@st.cache_data(ttl=300)
def load_golf_tournaments(year=None):
    conn = get_golf_conn()
    if not conn: return pd.DataFrame()
    where = f"WHERE year = {year}" if year else "WHERE year IN (2023,2024,2025)"
    return pd.read_sql_query(f"""SELECT name AS "Tournament", year AS "Year", start_date AS "Start",
        end_date AS "End", purse AS "Purse", winners_share AS "Winner's Share",
        fedex_points AS "FedEx Pts", format AS "Format"
        FROM golf_tournaments {where} ORDER BY year DESC, start_date DESC""", conn)

@st.cache_data(ttl=300)
def load_golf_winners():
    conn = get_golf_conn()
    if not conn: return pd.DataFrame()
    return pd.read_sql_query("""SELECT r.full_name AS "Player", r.year AS "Year",
        r.tournament_name AS "Tournament", r.total_score AS "Score", t.purse AS "Purse"
        FROM golf_results r LEFT JOIN golf_tournaments t ON r.tourn_id=t.tourn_id AND r.year=t.year
        WHERE r.win = 1 ORDER BY r.year DESC, t.purse DESC""", conn)

# ── NFL loaders ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_nfl_qbs(season=2024, postseason=0):
    conn = get_nfl_conn()
    if not conn: return pd.DataFrame()
    return pd.read_sql_query(f"""SELECT full_name AS "Player", season AS "Season",
        games_played AS "GP", passing_yards AS "Pass Yds", passing_touchdowns AS "Pass TD",
        passing_interceptions AS "INT", ROUND(passing_completion_pct,1) AS "Comp%",
        ROUND(passing_yards_per_game,1) AS "Yds/G", ROUND(yards_per_pass_attempt,2) AS "YPA",
        ROUND(qbr,1) AS "QBR", rushing_yards AS "Rush Yds", rushing_touchdowns AS "Rush TD"
        FROM nfl_player_stats
        WHERE season={season} AND postseason={postseason}
          AND passing_yards IS NOT NULL AND passing_yards > 0
        ORDER BY passing_yards DESC""", conn)

@st.cache_data(ttl=300)
def load_nfl_rbs(season=2024, postseason=0):
    conn = get_nfl_conn()
    if not conn: return pd.DataFrame()
    return pd.read_sql_query(f"""SELECT full_name AS "Player", season AS "Season",
        games_played AS "GP", rushing_yards AS "Rush Yds", rushing_touchdowns AS "Rush TD",
        rushing_attempts AS "Att", ROUND(yards_per_rush_attempt,2) AS "YPC",
        ROUND(rushing_yards_per_game,1) AS "Yds/G", rushing_first_downs AS "1st Downs",
        receptions AS "Rec", receiving_yards AS "Rec Yds", receiving_touchdowns AS "Rec TD"
        FROM nfl_player_stats
        WHERE season={season} AND postseason={postseason}
          AND rushing_yards IS NOT NULL AND rushing_yards > 0
          AND position_abbrev = 'RB'
        ORDER BY rushing_yards DESC""", conn)

@st.cache_data(ttl=300)
def load_nfl_receivers(season=2024, postseason=0):
    conn = get_nfl_conn()
    if not conn: return pd.DataFrame()
    return pd.read_sql_query(f"""SELECT full_name AS "Player", position AS "Pos", season AS "Season",
        games_played AS "GP", receptions AS "Rec", receiving_targets AS "Targets",
        receiving_yards AS "Rec Yds", receiving_touchdowns AS "Rec TD",
        ROUND(yards_per_reception,1) AS "YPR", ROUND(receiving_yards_per_game,1) AS "Yds/G",
        receiving_first_downs AS "1st Downs"
        FROM nfl_player_stats
        WHERE season={season} AND postseason={postseason}
          AND receiving_yards IS NOT NULL AND receiving_yards > 0
          AND position_abbrev IN ('WR','TE')
        ORDER BY receiving_yards DESC""", conn)

@st.cache_data(ttl=300)
def load_nfl_defense(season=2024, postseason=0):
    conn = get_nfl_conn()
    if not conn: return pd.DataFrame()
    return pd.read_sql_query(f"""SELECT full_name AS "Player", position AS "Pos", season AS "Season",
        games_played AS "GP", total_tackles AS "Tackles", solo_tackles AS "Solo",
        assist_tackles AS "Ast", ROUND(defensive_sacks,1) AS "Sacks",
        defensive_interceptions AS "INT", fumbles_forced AS "FF", fumbles_recovered AS "FR"
        FROM nfl_player_stats
        WHERE season={season} AND postseason={postseason}
          AND total_tackles IS NOT NULL AND total_tackles > 0
        ORDER BY total_tackles DESC""", conn)

@st.cache_data(ttl=300)
def load_nfl_standings(season=2024):
    conn = get_nfl_conn()
    if not conn: return pd.DataFrame()
    return pd.read_sql_query(f"""SELECT team_name AS "Team", team_abbrev AS "Abbrev",
        conference AS "Conference", division AS "Division",
        wins AS "W", losses AS "L", ties AS "T", overall_record AS "Record",
        points_for AS "PF", points_against AS "PA", point_differential AS "Diff",
        playoff_seed AS "Seed", home_record AS "Home", road_record AS "Away",
        division_record AS "Div", conference_record AS "Conf"
        FROM nfl_standings WHERE season={season}
        ORDER BY conference, playoff_seed ASC NULLS LAST, wins DESC""", conn)

@st.cache_data(ttl=300)
def load_nfl_career(position_abbrev="QB"):
    conn = get_nfl_conn()
    if not conn: return pd.DataFrame()
    if position_abbrev == "QB":
        return pd.read_sql_query("""SELECT full_name AS "Player",
            COUNT(DISTINCT season) AS "Seasons",
            SUM(games_played) AS "GP",
            SUM(COALESCE(passing_yards,0)) AS "Career Pass Yds",
            SUM(COALESCE(passing_touchdowns,0)) AS "Career Pass TD",
            SUM(COALESCE(passing_interceptions,0)) AS "Career INT",
            ROUND(AVG(COALESCE(qbr,0)),1) AS "Avg QBR",
            SUM(COALESCE(rushing_yards,0)) AS "Career Rush Yds",
            SUM(COALESCE(rushing_touchdowns,0)) AS "Career Rush TD"
            FROM nfl_player_stats
            WHERE postseason=0 AND passing_yards IS NOT NULL AND passing_yards > 0
            GROUP BY player_id, full_name HAVING SUM(passing_yards) > 1000
            ORDER BY SUM(passing_yards) DESC""", conn)
    elif position_abbrev == "RB":
        return pd.read_sql_query("""SELECT full_name AS "Player",
            COUNT(DISTINCT season) AS "Seasons", SUM(games_played) AS "GP",
            SUM(COALESCE(rushing_yards,0)) AS "Career Rush Yds",
            SUM(COALESCE(rushing_touchdowns,0)) AS "Career Rush TD",
            ROUND(AVG(COALESCE(yards_per_rush_attempt,0)),2) AS "Avg YPC",
            SUM(COALESCE(receptions,0)) AS "Career Rec",
            SUM(COALESCE(receiving_yards,0)) AS "Career Rec Yds"
            FROM nfl_player_stats
            WHERE postseason=0 AND position_abbrev='RB' AND rushing_yards IS NOT NULL
            GROUP BY player_id, full_name HAVING SUM(rushing_yards) > 500
            ORDER BY SUM(rushing_yards) DESC""", conn)
    else:
        return pd.read_sql_query(f"""SELECT full_name AS "Player", MAX(position) AS "Pos",
            COUNT(DISTINCT season) AS "Seasons", SUM(games_played) AS "GP",
            SUM(COALESCE(receiving_yards,0)) AS "Career Rec Yds",
            SUM(COALESCE(receiving_touchdowns,0)) AS "Career Rec TD",
            SUM(COALESCE(receptions,0)) AS "Career Rec",
            ROUND(AVG(COALESCE(yards_per_reception,0)),1) AS "Avg YPR"
            FROM nfl_player_stats
            WHERE postseason=0 AND position_abbrev IN ('WR','TE') AND receiving_yards IS NOT NULL
            GROUP BY player_id, full_name HAVING SUM(receiving_yards) > 500
            ORDER BY SUM(receiving_yards) DESC""", conn)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏆 Sports Dashboard")
    st.markdown("---")
    sport = st.radio("Sport", ["🏒 NHL", "⛳ PGA Tour", "🏈 NFL"], label_visibility="collapsed")
    st.markdown("---")
    if sport == "🏒 NHL":
        page = st.radio("Section", ["🏠 Overview","🏆 Skater Rankings","📈 Advanced Stats","🧤 Goalies","🏒 Standings","🔍 Player Search"], label_visibility="collapsed")
    elif sport == "⛳ PGA Tour":
        page = st.radio("Section", ["🏠 Golf Overview","🏆 Season Leaderboard","🏌️ Player Profile","🏅 Tournament Winners","📅 Schedule & Tournaments"], label_visibility="collapsed")
    else:
        page = st.radio("Section", ["🏠 NFL Overview","🏈 QB Stats","🏃 RB Stats","📡 WR/TE Stats","🛡️ Defense","🏟️ Standings","📊 Career Leaders","🔍 Player Search"], label_visibility="collapsed")
    st.markdown("---")
    st.markdown("<small style='color:#666'>NHL: NHL Stats API<br>Golf: SlashGolf API<br>NFL: BallDontLie API</small>", unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────────────────
skaters   = load_skaters()
goalies   = load_goalies()
standings = load_standings()
advanced  = load_advanced()
golf_stats    = load_golf_season_stats()
golf_results  = load_golf_results()
golf_tourneys = load_golf_tournaments()
golf_winners  = load_golf_winners()

# =============================================================================
# NHL PAGES
# =============================================================================
if sport == "🏒 NHL":
    st.markdown("""<div class="sport-header nhl-header"><h1>🏒 NHL Stats Dashboard</h1><p>Live statistics powered by the NHL Stats API</p></div>""", unsafe_allow_html=True)
    if skaters.empty:
        st.error("⚠️ NHL database not found. Run the NHL collector first."); st.stop()

    if page == "🏠 Overview":
        top = skaters.iloc[0]; top_goals = skaters.nlargest(1,"G").iloc[0]
        top_g = goalies.iloc[0].to_dict() if not goalies.empty else {}
        top_team = standings.iloc[0] if not standings.empty else {}
        c1,c2,c3,c4 = st.columns(4)
        for col, label, value, sub, css in [
            (c1,"Points Leader",top.get("PTS","-"),f"{top.get('Player','')} · {top.get('Team','')}","nhl-sub"),
            (c2,"Goals Leader",top_goals.get("G","-"),f"{top_goals.get('Player','')} · {top_goals.get('Team','')}","nhl-sub"),
            (c3,"Top SV%",top_g.get("SV%","-"),f"{top_g.get('Goalie','')} · {top_g.get('Team','')}","nhl-sub"),
            (c4,"League Leader",f"{top_team.get('PTS','-')} pts",top_team.get("Team","-"),"nhl-sub"),
        ]:
            col.markdown(f"""<div class="metric-card"><div class="label">{label}</div><div class="value">{value}</div><div class="sub {css}">{sub}</div></div>""", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        c1,c2 = st.columns(2)
        with c1:
            st.markdown('<div class="section-header nhl-section">🏆 Top 15 Points Leaders</div>', unsafe_allow_html=True)
            fig = px.bar(skaters.head(15), x="PTS", y="Player", orientation="h", color="PTS", color_continuous_scale=["#001F5B","#C8102E"], text="PTS", hover_data=["Team","G","A"])
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", yaxis=dict(autorange="reversed"), xaxis=dict(showgrid=False), coloraxis_showscale=False, margin=dict(l=0,r=20,t=10,b=10), height=420)
            fig.update_traces(textposition="outside", textfont=dict(color="white"))
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            st.markdown('<div class="section-header nhl-section">🏒 Goals For vs Against</div>', unsafe_allow_html=True)
            t16 = standings.head(16)
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(name="Goals For", x=t16["Abbrev"], y=t16["GF"], marker_color="#001F5B"))
            fig2.add_trace(go.Bar(name="Goals Against", x=t16["Abbrev"], y=t16["GA"], marker_color="#C8102E"))
            fig2.update_layout(barmode="group", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", legend=dict(orientation="h",yanchor="bottom",y=1.02), xaxis=dict(showgrid=False), yaxis=dict(showgrid=True,gridcolor="#2d3748"), margin=dict(l=0,r=0,t=30,b=10), height=420)
            st.plotly_chart(fig2, use_container_width=True)
        c3,c4 = st.columns(2)
        with c3:
            st.markdown('<div class="section-header nhl-section">🎯 Top 15 Goal Scorers</div>', unsafe_allow_html=True)
            fig3 = px.bar(skaters.nlargest(15,"G"), x="G", y="Player", orientation="h", color="Sh%", color_continuous_scale=["#1a365d","#C8102E"], text="G", hover_data=["Team","Sh%"])
            fig3.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", yaxis=dict(autorange="reversed"), xaxis=dict(showgrid=False), margin=dict(l=0,r=20,t=10,b=10), height=420)
            fig3.update_traces(textposition="outside", textfont=dict(color="white"))
            st.plotly_chart(fig3, use_container_width=True)
        with c4:
            st.markdown('<div class="section-header nhl-section">📊 Points vs P/GP</div>', unsafe_allow_html=True)
            fig4 = px.scatter(skaters[skaters["PTS"]>=20], x="PTS", y="P/GP", color="Pos", hover_name="Player", hover_data=["Team","G","A"], size="PTS", size_max=18, color_discrete_map={"C":"#C8102E","L":"#001F5B","R":"#FFD700","D":"#48BB78"})
            fig4.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", xaxis=dict(showgrid=True,gridcolor="#2d3748"), yaxis=dict(showgrid=True,gridcolor="#2d3748"), margin=dict(l=0,r=0,t=10,b=10), height=420)
            st.plotly_chart(fig4, use_container_width=True)

    elif page == "🏆 Skater Rankings":
        st.markdown('<div class="section-header nhl-section">🏆 Skater Rankings</div>', unsafe_allow_html=True)
        c1,c2,c3 = st.columns(3)
        pos_f = c1.multiselect("Position", ["C","L","R","D"], default=["C","L","R","D"])
        team_f = c2.multiselect("Team", sorted(skaters["Team"].unique()), default=[])
        min_p = c3.slider("Min Points", 0, int(skaters["PTS"].max()), 0)
        df = skaters[skaters["Pos"].isin(pos_f)]
        if team_f: df = df[df["Team"].isin(team_f)]
        df = df[df["PTS"]>=min_p].reset_index(drop=True); df.index += 1
        st.dataframe(df, use_container_width=True, height=600, column_config={"Sh%": st.column_config.NumberColumn(format="%.1f%%"), "P/GP": st.column_config.NumberColumn(format="%.3f"), "G/GP": st.column_config.NumberColumn(format="%.3f")})
        st.download_button("⬇️ Download CSV", df.to_csv(index=False).encode(), "skaters.csv", "text/csv")

    elif page == "📈 Advanced Stats":
        st.markdown('<div class="section-header nhl-section">📈 Advanced Stats</div>', unsafe_allow_html=True)
        c1,c2 = st.columns(2)
        pos_f = c1.multiselect("Position", ["C","L","R","D"], default=["C","L","R","D"])
        min_p = c2.slider("Min Points", 10, int(advanced["PTS"].max()), 10)
        df = advanced[advanced["Pos"].isin(pos_f)][advanced["PTS"]>=min_p].reset_index(drop=True); df.index += 1
        st.dataframe(df, use_container_width=True, height=500)
        fig = px.box(advanced[advanced["Pos"].isin(pos_f)], x="Pos", y="P/GP", color="Pos", color_discrete_map={"C":"#C8102E","L":"#001F5B","R":"#FFD700","D":"#48BB78"}, points="all", hover_name="Player")
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", showlegend=False, height=380)
        st.plotly_chart(fig, use_container_width=True)

    elif page == "🧤 Goalies":
        st.markdown('<div class="section-header nhl-section">🧤 Goalie Rankings</div>', unsafe_allow_html=True)
        c1,c2 = st.columns(2)
        min_w = c1.slider("Min Wins", 0, int(goalies["W"].max()), 5)
        team_f = c2.multiselect("Team", sorted(goalies["Team"].unique()), default=[])
        df = goalies[goalies["W"]>=min_w]
        if team_f: df = df[df["Team"].isin(team_f)]
        df = df.reset_index(drop=True); df.index += 1
        st.dataframe(df, use_container_width=True, height=500)
        fig = px.scatter(goalies[goalies["W"]>=5], x="GAA", y="SV%", size="W", color="W", color_continuous_scale=["#001F5B","#C8102E"], hover_name="Goalie", hover_data=["Team","W","SO"], size_max=20)
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", xaxis=dict(showgrid=True,gridcolor="#2d3748",autorange="reversed"), yaxis=dict(showgrid=True,gridcolor="#2d3748"), height=400)
        st.plotly_chart(fig, use_container_width=True)

    elif page == "🏒 Standings":
        st.markdown('<div class="section-header nhl-section">🏒 Standings</div>', unsafe_allow_html=True)
        conf = st.radio("Conference", ["All","Eastern","Western"], horizontal=True)
        df = standings if conf=="All" else standings[standings["Conference"].str.contains(conf,case=False,na=False)]
        for div in df["Division"].unique():
            st.markdown(f"**{div}**")
            d = df[df["Division"]==div][["Team","Abbrev","W","L","OTL","PTS","GP","GF","GA","GDiff","GF/GP","GA/GP","PP%","PK%","HW","HL","AW","AL","L10W","L10L"]].reset_index(drop=True); d.index += 1
            st.dataframe(d, use_container_width=True)

    elif page == "🔍 Player Search":
        st.markdown('<div class="section-header nhl-section">🔍 Player Search</div>', unsafe_allow_html=True)
        search = st.text_input("Search by player name", placeholder="e.g. McDavid, Crosby...")
        if search:
            results = skaters[skaters["Player"].str.contains(search, case=False, na=False)]
            if results.empty: st.warning(f"No players found matching '{search}'")
            else:
                for _, row in results.iterrows():
                    with st.expander(f"🏒 {row['Player']}  —  {row['Team']}  |  {row['Pos']}"):
                        c1,c2,c3,c4,c5,c6 = st.columns(6)
                        c1.metric("Points",row["PTS"]); c2.metric("Goals",row["G"]); c3.metric("Assists",row["A"])
                        c4.metric("+/-",row["+/-"]); c5.metric("P/GP",row["P/GP"]); c6.metric("Sh%",f"{row['Sh%']}%")
        else:
            st.dataframe(skaters[["Player","Team","Pos","GP","G","A","PTS","P/GP","Sh%"]], use_container_width=True, height=500)

# =============================================================================
# GOLF PAGES
# =============================================================================
elif sport == "⛳ PGA Tour":
    st.markdown("""<div class="sport-header golf-header"><h1>⛳ PGA Tour Stats Dashboard</h1><p>2023 · 2024 · 2025 seasons  |  163 tournaments  |  Powered by SlashGolf API</p></div>""", unsafe_allow_html=True)
    if golf_stats.empty:
        st.error("⚠️ Golf database not found. Run golf/golf_collector.py first."); st.stop()

    if page == "🏠 Golf Overview":
        most_wins = golf_stats.nlargest(1,"Wins").iloc[0]
        best_cut  = golf_stats[golf_stats["Events"]>=10].nlargest(1,"Cut%").iloc[0]
        most_t10  = golf_stats.nlargest(1,"Top 10").iloc[0]
        wins_2025 = golf_stats[golf_stats["Year"]==2025]["Wins"].sum()
        c1,c2,c3,c4 = st.columns(4)
        for col, label, value, sub in [
            (c1,"Most Wins (Season)",most_wins["Wins"],f"{most_wins['Player']} · {most_wins['Year']}"),
            (c2,"Best Cut% (10+ ev)",f"{best_cut['Cut%']}%",f"{best_cut['Player']} · {best_cut['Year']}"),
            (c3,"Most Top 10s (Season)",most_t10["Top 10"],f"{most_t10['Player']} · {most_t10['Year']}"),
            (c4,"2025 Total Wins",wins_2025,"Across all players"),
        ]:
            col.markdown(f"""<div class="metric-card"><div class="label">{label}</div><div class="value">{value}</div><div class="sub golf-sub">{sub}</div></div>""", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        c1,c2 = st.columns(2)
        with c1:
            st.markdown('<div class="section-header golf-section">🏆 All-Time Win Leaders (2023–2025)</div>', unsafe_allow_html=True)
            wl = golf_stats.groupby("Player").agg(Total_Wins=("Wins","sum"),Top_10=("Top 10","sum"),Events=("Events","sum")).reset_index().nlargest(15,"Total_Wins")
            fig = px.bar(wl, x="Total_Wins", y="Player", orientation="h", color="Total_Wins", color_continuous_scale=["#1a4731","#2d7a4f"], text="Total_Wins", hover_data=["Top_10","Events"])
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", yaxis=dict(autorange="reversed"), xaxis=dict(showgrid=False), coloraxis_showscale=False, margin=dict(l=0,r=20,t=10,b=10), height=420)
            fig.update_traces(textposition="outside", textfont=dict(color="white"))
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            st.markdown('<div class="section-header golf-section">📊 Wins by Season — Top 8 Players</div>', unsafe_allow_html=True)
            top_p = golf_stats.groupby("Player")["Wins"].sum().nlargest(8).index.tolist()
            fig2 = px.bar(golf_stats[golf_stats["Player"].isin(top_p)], x="Year", y="Wins", color="Player", barmode="group", color_discrete_sequence=px.colors.qualitative.Set2)
            fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", xaxis=dict(showgrid=False), yaxis=dict(showgrid=True,gridcolor="#2d3748"), legend=dict(orientation="h",yanchor="bottom",y=1.02,font=dict(size=9)), margin=dict(l=0,r=0,t=40,b=10), height=420)
            st.plotly_chart(fig2, use_container_width=True)
        c3,c4 = st.columns(2)
        with c3:
            st.markdown('<div class="section-header golf-section">🎯 Top 10 Leaders (2023–2025)</div>', unsafe_allow_html=True)
            tl = golf_stats.groupby("Player").agg(Total_Top10=("Top 10","sum"),Wins=("Wins","sum")).reset_index().nlargest(15,"Total_Top10")
            fig3 = px.bar(tl, x="Total_Top10", y="Player", orientation="h", color="Wins", color_continuous_scale=["#1a4731","#FFD700"], text="Total_Top10")
            fig3.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", yaxis=dict(autorange="reversed"), xaxis=dict(showgrid=False), margin=dict(l=0,r=20,t=10,b=10), height=420)
            fig3.update_traces(textposition="outside", textfont=dict(color="white"))
            st.plotly_chart(fig3, use_container_width=True)
        with c4:
            st.markdown('<div class="section-header golf-section">✂️ Cut% vs Top 10s — 2025</div>', unsafe_allow_html=True)
            fig4 = px.scatter(golf_stats[(golf_stats["Events"]>=10)&(golf_stats["Year"]==2025)], x="Cut%", y="Top 10", size="Events", color="Wins", color_continuous_scale=["#1a4731","#FFD700"], hover_name="Player", size_max=18)
            fig4.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", xaxis=dict(showgrid=True,gridcolor="#2d3748"), yaxis=dict(showgrid=True,gridcolor="#2d3748"), margin=dict(l=0,r=0,t=10,b=10), height=420)
            st.plotly_chart(fig4, use_container_width=True)

    elif page == "🏆 Season Leaderboard":
        st.markdown('<div class="section-header golf-section">🏆 Season Leaderboard</div>', unsafe_allow_html=True)
        c1,c2,c3 = st.columns(3)
        year_f  = c1.selectbox("Season", [2025,2024,2023], index=0)
        min_ev  = c2.slider("Min Events", 1, 30, 5)
        sort_by = c3.selectbox("Sort By", ["Wins","Top 5","Top 10","Top 20","Cut%","Events"])
        df = load_golf_season_stats(year=year_f)
        df = df[df["Events"]>=min_ev].sort_values(sort_by, ascending=False).reset_index(drop=True); df.index += 1
        st.dataframe(df, use_container_width=True, height=600)
        st.download_button("⬇️ Download CSV", df.to_csv(index=False).encode(), f"golf_{year_f}.csv", "text/csv")

    elif page == "🏌️ Player Profile":
        st.markdown('<div class="section-header golf-section">🏌️ Player Profile</div>', unsafe_allow_html=True)
        all_players = sorted(golf_stats["Player"].unique())
        selected = st.selectbox("Select a player", all_players, index=all_players.index("Scottie Scheffler") if "Scottie Scheffler" in all_players else 0)
        ps = golf_stats[golf_stats["Player"]==selected].sort_values("Year")
        pr = golf_results[golf_results["Player"]==selected].sort_values(["Year","Pos#"])
        if not ps.empty:
            c1,c2,c3,c4,c5 = st.columns(5)
            c1.metric("Career Wins", ps["Wins"].sum()); c2.metric("Career Top 10s", ps["Top 10"].sum())
            c3.metric("Total Events", ps["Events"].sum()); c4.metric("Best Finish", ps["Best Finish"].min())
            c5.metric("Avg Cut%", f"{ps['Cut%'].mean():.1f}%")
            c1,c2 = st.columns(2)
            with c1:
                st.dataframe(ps[["Year","Events","Cuts","Wins","Top 5","Top 10","Top 20","Best Finish","Cut%"]], use_container_width=True, hide_index=True)
            with c2:
                fig = go.Figure()
                fig.add_trace(go.Bar(name="Wins", x=ps["Year"], y=ps["Wins"], marker_color="#FFD700"))
                fig.add_trace(go.Bar(name="Top 10s", x=ps["Year"], y=ps["Top 10"], marker_color="#2d7a4f"))
                fig.update_layout(barmode="group", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", height=300, margin=dict(t=10))
                st.plotly_chart(fig, use_container_width=True)
            wins_only = pr[pr["Win"]==1][["Year","Tournament","Finish","Score","Purse"]]
            if not wins_only.empty:
                st.markdown("🏆 **Wins:**"); st.dataframe(wins_only, use_container_width=True, hide_index=True)
            st.dataframe(pr[["Year","Tournament","Finish","Score","Made Cut","Top 10"]], use_container_width=True, height=400, hide_index=True)

    elif page == "🏅 Tournament Winners":
        st.markdown('<div class="section-header golf-section">🏅 Tournament Winners</div>', unsafe_allow_html=True)
        c1,c2 = st.columns(2)
        year_f = c1.selectbox("Season", ["All",2025,2024,2023], index=0)
        search_w = c2.text_input("Search player or tournament")
        df = golf_winners.copy()
        if year_f != "All": df = df[df["Year"]==int(year_f)]
        if search_w: df = df[df["Player"].str.contains(search_w,case=False,na=False)|df["Tournament"].str.contains(search_w,case=False,na=False)]
        df = df.reset_index(drop=True); df.index += 1
        st.dataframe(df, use_container_width=True, height=600, column_config={"Purse": st.column_config.NumberColumn(format="$%d")})
        st.download_button("⬇️ Download CSV", df.to_csv(index=False).encode(), "golf_winners.csv", "text/csv")

    elif page == "📅 Schedule & Tournaments":
        st.markdown('<div class="section-header golf-section">📅 Schedule & Tournaments</div>', unsafe_allow_html=True)
        year_f = st.selectbox("Season", [2025,2024,2023], index=0)
        df = load_golf_tournaments(year=year_f).reset_index(drop=True); df.index += 1
        st.dataframe(df, use_container_width=True, height=500, column_config={"Purse": st.column_config.NumberColumn(format="$%d"), "Winner's Share": st.column_config.NumberColumn(format="$%d")})
        purse_df = df[df["Purse"].notna()].nlargest(20,"Purse")
        fig = px.bar(purse_df, x="Purse", y="Tournament", orientation="h", color="Purse", color_continuous_scale=["#1a4731","#FFD700"], text="Purse")
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", yaxis=dict(autorange="reversed"), xaxis=dict(showgrid=False), coloraxis_showscale=False, margin=dict(l=0,r=20,t=10,b=10), height=520)
        fig.update_traces(texttemplate="$%{text:,.0f}", textposition="outside", textfont=dict(color="white",size=9))
        st.plotly_chart(fig, use_container_width=True)

# =============================================================================
# NFL PAGES
# =============================================================================
elif sport == "🏈 NFL":
    st.markdown("""<div class="sport-header nfl-header"><h1>🏈 NFL Stats Dashboard</h1><p>2018–2025 seasons  |  20,000+ player records  |  Powered by BallDontLie API</p></div>""", unsafe_allow_html=True)

    nfl_conn = get_nfl_conn()
    if not nfl_conn:
        st.error("⚠️ NFL database not found. Run nfl/nfl_collector.py first."); st.stop()

    # Season selector in sidebar for NFL
    with st.sidebar:
        st.markdown("---")
        nfl_season  = st.selectbox("Season", [2025,2024,2023,2022,2021,2020,2019,2018], index=0)
        nfl_playoffs = st.checkbox("Playoffs", value=False)
    nfl_ps = 1 if nfl_playoffs else 0

    # ── NFL OVERVIEW ──────────────────────────────────────────────────────
    if page == "🏠 NFL Overview":
        qbs = load_nfl_qbs(season=nfl_season, postseason=nfl_ps)
        rbs = load_nfl_rbs(season=nfl_season, postseason=nfl_ps)
        wrs = load_nfl_receivers(season=nfl_season, postseason=nfl_ps)
        std = load_nfl_standings(season=nfl_season)

        top_qb   = qbs.iloc[0].to_dict()  if not qbs.empty  else {}
        top_rb   = rbs.iloc[0].to_dict()  if not rbs.empty  else {}
        top_wr   = wrs.iloc[0].to_dict()  if not wrs.empty  else {}
        top_team = std.iloc[0].to_dict()  if not std.empty  else {}

        def fmt_yds(val):
            try: return f"{int(val):,}"
            except: return "-"

        c1,c2,c3,c4 = st.columns(4)
        for col, label, value, sub in [
            (c1, "Pass Yds Leader", fmt_yds(top_qb.get("Pass Yds")), top_qb.get("Player","-")),
            (c2, "Rush Yds Leader", fmt_yds(top_rb.get("Rush Yds")), top_rb.get("Player","-")),
            (c3, "Rec Yds Leader",  fmt_yds(top_wr.get("Rec Yds")),  top_wr.get("Player","-")),
            (c4, "Best Record",     top_team.get("Record","-"),       top_team.get("Team","-")),
        ]:
            col.markdown(f"""<div class="metric-card"><div class="label">{label}</div><div class="value">{value}</div><div class="sub nfl-sub">{sub}</div></div>""", unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        c1,c2 = st.columns(2)

        with c1:
            st.markdown(f'<div class="section-header nfl-section">🏈 Top 15 QBs — Passing Yards ({nfl_season})</div>', unsafe_allow_html=True)
            if not qbs.empty:
                fig = px.bar(qbs.head(15), x="Pass Yds", y="Player", orientation="h", color="Pass TD", color_continuous_scale=["#1a1a2e","#D4AF37"], text="Pass Yds", hover_data=["Pass TD","INT","QBR"])
                fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", yaxis=dict(autorange="reversed"), xaxis=dict(showgrid=False), coloraxis_colorbar=dict(title="Pass TD"), margin=dict(l=0,r=20,t=10,b=10), height=460)
                fig.update_traces(textposition="outside", textfont=dict(color="white",size=9))
                st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.markdown(f'<div class="section-header nfl-section">🏃 Top 15 RBs — Rushing Yards ({nfl_season})</div>', unsafe_allow_html=True)
            if not rbs.empty:
                fig2 = px.bar(rbs.head(15), x="Rush Yds", y="Player", orientation="h", color="Rush TD", color_continuous_scale=["#1a1a2e","#D4AF37"], text="Rush Yds", hover_data=["Rush TD","YPC","Att"])
                fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", yaxis=dict(autorange="reversed"), xaxis=dict(showgrid=False), coloraxis_colorbar=dict(title="Rush TD"), margin=dict(l=0,r=20,t=10,b=10), height=460)
                fig2.update_traces(textposition="outside", textfont=dict(color="white",size=9))
                st.plotly_chart(fig2, use_container_width=True)

        c3,c4 = st.columns(2)
        with c3:
            st.markdown(f'<div class="section-header nfl-section">📡 Top 15 WR/TEs — Receiving Yards ({nfl_season})</div>', unsafe_allow_html=True)
            if not wrs.empty:
                fig3 = px.bar(wrs.head(15), x="Rec Yds", y="Player", orientation="h", color="Rec TD", color_continuous_scale=["#1a1a2e","#D4AF37"], text="Rec Yds", hover_data=["Rec TD","Rec","YPR"])
                fig3.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", yaxis=dict(autorange="reversed"), xaxis=dict(showgrid=False), coloraxis_colorbar=dict(title="Rec TD"), margin=dict(l=0,r=20,t=10,b=10), height=460)
                fig3.update_traces(textposition="outside", textfont=dict(color="white",size=9))
                st.plotly_chart(fig3, use_container_width=True)

        with c4:
            st.markdown(f'<div class="section-header nfl-section">🏟️ Team Points For ({nfl_season})</div>', unsafe_allow_html=True)
            if not std.empty:
                fig4 = px.bar(std.sort_values("PF", ascending=False).head(16), x="Abbrev", y="PF", color="PF", color_continuous_scale=["#1a1a2e","#D4AF37"], text="PF")
                fig4.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", xaxis=dict(showgrid=False), yaxis=dict(showgrid=True,gridcolor="#2d3748"), coloraxis_showscale=False, margin=dict(l=0,r=0,t=10,b=10), height=460)
                fig4.update_traces(textposition="outside", textfont=dict(color="white"))
                st.plotly_chart(fig4, use_container_width=True)

    # ── QB STATS ──────────────────────────────────────────────────────────
    elif page == "🏈 QB Stats":
        st.markdown(f'<div class="section-header nfl-section">🏈 QB Stats — {nfl_season} {"Playoffs" if nfl_playoffs else "Regular Season"}</div>', unsafe_allow_html=True)
        qbs = load_nfl_qbs(season=nfl_season, postseason=nfl_ps)
        min_yds = st.slider("Min Passing Yards", 0, 5000, 500)
        df = qbs[qbs["Pass Yds"] >= min_yds].reset_index(drop=True); df.index += 1
        st.dataframe(df, use_container_width=True, height=600, column_config={"Comp%": st.column_config.NumberColumn(format="%.1f%%"), "QBR": st.column_config.NumberColumn(format="%.1f")})
        st.caption(f"{len(df)} QBs shown")
        st.download_button("⬇️ Download CSV", df.to_csv(index=False).encode(), f"qbs_{nfl_season}.csv", "text/csv")

        # TD vs INT scatter
        if not qbs.empty:
            st.markdown('<div class="section-header nfl-section">Pass TDs vs INTs</div>', unsafe_allow_html=True)
            fig = px.scatter(qbs[qbs["Pass Yds"]>=500], x="INT", y="Pass TD", size="Pass Yds", color="QBR", color_continuous_scale=["#C8102E","#D4AF37"], hover_name="Player", hover_data=["Pass Yds","Comp%"], size_max=20)
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", xaxis=dict(showgrid=True,gridcolor="#2d3748"), yaxis=dict(showgrid=True,gridcolor="#2d3748"), height=420)
            st.plotly_chart(fig, use_container_width=True)

    # ── RB STATS ──────────────────────────────────────────────────────────
    elif page == "🏃 RB Stats":
        st.markdown(f'<div class="section-header nfl-section">🏃 RB Stats — {nfl_season} {"Playoffs" if nfl_playoffs else "Regular Season"}</div>', unsafe_allow_html=True)
        rbs = load_nfl_rbs(season=nfl_season, postseason=nfl_ps)
        min_yds = st.slider("Min Rushing Yards", 0, 2000, 100)
        df = rbs[rbs["Rush Yds"] >= min_yds].reset_index(drop=True); df.index += 1
        st.dataframe(df, use_container_width=True, height=600)
        st.download_button("⬇️ Download CSV", df.to_csv(index=False).encode(), f"rbs_{nfl_season}.csv", "text/csv")

        if not rbs.empty:
            st.markdown('<div class="section-header nfl-section">Rush Yards vs YPC (min 50 att)</div>', unsafe_allow_html=True)
            fig = px.scatter(rbs[rbs["Att"]>=50], x="YPC", y="Rush Yds", size="Rush TD", color="Rush TD", color_continuous_scale=["#1a1a2e","#D4AF37"], hover_name="Player", hover_data=["Att","Rush TD"], size_max=20)
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", xaxis=dict(showgrid=True,gridcolor="#2d3748"), yaxis=dict(showgrid=True,gridcolor="#2d3748"), height=420)
            st.plotly_chart(fig, use_container_width=True)

    # ── WR/TE STATS ───────────────────────────────────────────────────────
    elif page == "📡 WR/TE Stats":
        st.markdown(f'<div class="section-header nfl-section">📡 WR/TE Stats — {nfl_season} {"Playoffs" if nfl_playoffs else "Regular Season"}</div>', unsafe_allow_html=True)
        wrs = load_nfl_receivers(season=nfl_season, postseason=nfl_ps)
        c1,c2 = st.columns(2)
        pos_f   = c1.multiselect("Position", ["Wide Receiver","Tight End"], default=["Wide Receiver","Tight End"])
        min_yds = c2.slider("Min Receiving Yards", 0, 1500, 100)
        pos_map = {"Wide Receiver": "Wide Receiver", "Tight End": "Tight End"}
        df = wrs[wrs["Pos"].isin(pos_f)][wrs["Rec Yds"] >= min_yds].reset_index(drop=True); df.index += 1
        st.dataframe(df, use_container_width=True, height=600)
        st.download_button("⬇️ Download CSV", df.to_csv(index=False).encode(), f"receivers_{nfl_season}.csv", "text/csv")

        if not wrs.empty:
            st.markdown('<div class="section-header nfl-section">Receptions vs Receiving Yards</div>', unsafe_allow_html=True)
            fig = px.scatter(wrs[wrs["Rec Yds"]>=100], x="Rec", y="Rec Yds", color="Pos", size="Rec TD", hover_name="Player", hover_data=["YPR","Rec TD"], size_max=20, color_discrete_map={"Wide Receiver":"#D4AF37","Tight End":"#825A2C"})
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", xaxis=dict(showgrid=True,gridcolor="#2d3748"), yaxis=dict(showgrid=True,gridcolor="#2d3748"), height=420)
            st.plotly_chart(fig, use_container_width=True)

    # ── DEFENSE ───────────────────────────────────────────────────────────
    elif page == "🛡️ Defense":
        st.markdown(f'<div class="section-header nfl-section">🛡️ Defensive Stats — {nfl_season} {"Playoffs" if nfl_playoffs else "Regular Season"}</div>', unsafe_allow_html=True)
        defense = load_nfl_defense(season=nfl_season, postseason=nfl_ps)
        min_t = st.slider("Min Tackles", 0, 150, 20)
        df = defense[defense["Tackles"] >= min_t].reset_index(drop=True); df.index += 1
        st.dataframe(df, use_container_width=True, height=600)
        st.download_button("⬇️ Download CSV", df.to_csv(index=False).encode(), f"defense_{nfl_season}.csv", "text/csv")

        if not defense.empty:
            c1,c2 = st.columns(2)
            with c1:
                st.markdown('<div class="section-header nfl-section">Top 15 Sack Leaders</div>', unsafe_allow_html=True)
                sacks = defense[defense["Sacks"]>0].nlargest(15,"Sacks")
                fig = px.bar(sacks, x="Sacks", y="Player", orientation="h", color="Sacks", color_continuous_scale=["#1a1a2e","#D4AF37"], text="Sacks")
                fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", yaxis=dict(autorange="reversed"), xaxis=dict(showgrid=False), coloraxis_showscale=False, height=420, margin=dict(l=0,r=20,t=10,b=10))
                fig.update_traces(textposition="outside", textfont=dict(color="white"))
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                st.markdown('<div class="section-header nfl-section">Top 15 Tackle Leaders</div>', unsafe_allow_html=True)
                tackles = defense.nlargest(15,"Tackles")
                fig2 = px.bar(tackles, x="Tackles", y="Player", orientation="h", color="Tackles", color_continuous_scale=["#1a1a2e","#D4AF37"], text="Tackles")
                fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", yaxis=dict(autorange="reversed"), xaxis=dict(showgrid=False), coloraxis_showscale=False, height=420, margin=dict(l=0,r=20,t=10,b=10))
                fig2.update_traces(textposition="outside", textfont=dict(color="white"))
                st.plotly_chart(fig2, use_container_width=True)

    # ── STANDINGS ─────────────────────────────────────────────────────────
    elif page == "🏟️ Standings":
        st.markdown(f'<div class="section-header nfl-section">🏟️ NFL Standings — {nfl_season}</div>', unsafe_allow_html=True)
        std = load_nfl_standings(season=nfl_season)
        conf_f = st.radio("Conference", ["All","AFC","NFC"], horizontal=True)
        df = std if conf_f == "All" else std[std["Conference"] == conf_f]

        for conf in (["AFC","NFC"] if conf_f == "All" else [conf_f]):
            st.markdown(f"### {conf}")
            conf_df = df[df["Conference"] == conf]
            for div in sorted(conf_df["Division"].unique()):
                st.markdown(f"**{conf} {div}**")
                d = conf_df[conf_df["Division"]==div][["Team","Abbrev","W","L","T","Record","PF","PA","Diff","Seed","Home","Away","Div","Conf"]].reset_index(drop=True); d.index += 1
                st.dataframe(d, use_container_width=True)

        # Points For vs Against
        st.markdown('<div class="section-header nfl-section">Points For vs Against</div>', unsafe_allow_html=True)
        fig = go.Figure()
        fig.add_trace(go.Bar(name="Points For",     x=std["Abbrev"], y=std["PF"], marker_color="#D4AF37"))
        fig.add_trace(go.Bar(name="Points Against", x=std["Abbrev"], y=std["PA"], marker_color="#825A2C"))
        fig.update_layout(barmode="group", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", xaxis=dict(showgrid=False), yaxis=dict(showgrid=True,gridcolor="#2d3748"), legend=dict(orientation="h",yanchor="bottom",y=1.02), height=380, margin=dict(t=40))
        st.plotly_chart(fig, use_container_width=True)

    # ── CAREER LEADERS ────────────────────────────────────────────────────
    elif page == "📊 Career Leaders":
        st.markdown('<div class="section-header nfl-section">📊 Career Leaders (2018–2025)</div>', unsafe_allow_html=True)
        pos_tab = st.radio("Position Group", ["QB","RB","WR/TE"], horizontal=True)
        key = "WR" if pos_tab == "WR/TE" else pos_tab
        df = load_nfl_career(position_abbrev=key).reset_index(drop=True); df.index += 1
        st.dataframe(df, use_container_width=True, height=600)
        st.download_button("⬇️ Download CSV", df.to_csv(index=False).encode(), f"career_{pos_tab}.csv", "text/csv")

        if not df.empty:
            if pos_tab == "QB":
                fig = px.bar(df.head(15), x="Career Pass Yds", y="Player", orientation="h", color="Career Pass TD", color_continuous_scale=["#1a1a2e","#D4AF37"], text="Career Pass Yds")
            elif pos_tab == "RB":
                fig = px.bar(df.head(15), x="Career Rush Yds", y="Player", orientation="h", color="Career Rush TD", color_continuous_scale=["#1a1a2e","#D4AF37"], text="Career Rush Yds")
            else:
                fig = px.bar(df.head(15), x="Career Rec Yds", y="Player", orientation="h", color="Career Rec TD", color_continuous_scale=["#1a1a2e","#D4AF37"], text="Career Rec Yds")
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", yaxis=dict(autorange="reversed"), xaxis=dict(showgrid=False), margin=dict(l=0,r=20,t=10,b=10), height=460)
            fig.update_traces(textposition="outside", textfont=dict(color="white",size=9))
            st.plotly_chart(fig, use_container_width=True)

    # ── PLAYER SEARCH ─────────────────────────────────────────────────────
    elif page == "🔍 Player Search":
        st.markdown('<div class="section-header nfl-section">🔍 Player Search</div>', unsafe_allow_html=True)
        search = st.text_input("Search player name", placeholder="e.g. Mahomes, Barkley, Jefferson...")
        season_s = st.selectbox("Season", [2025,2024,2023,2022,2021,2020,2019,2018], index=1, key="search_season")

        if search:
            conn = get_nfl_conn()
            df = pd.read_sql_query(f"""
                SELECT full_name AS "Player", position AS "Pos", season AS "Season",
                       games_played AS "GP",
                       passing_yards AS "Pass Yds", passing_touchdowns AS "Pass TD", passing_interceptions AS "INT",
                       ROUND(qbr,1) AS "QBR",
                       rushing_yards AS "Rush Yds", rushing_touchdowns AS "Rush TD",
                       receptions AS "Rec", receiving_yards AS "Rec Yds", receiving_touchdowns AS "Rec TD",
                       total_tackles AS "Tackles", defensive_sacks AS "Sacks"
                FROM nfl_player_stats
                WHERE postseason=0 AND season={season_s}
                  AND full_name LIKE '%{search}%'
                ORDER BY full_name, season
            """, conn)

            if df.empty:
                st.warning(f"No players found matching '{search}' in {season_s}")
            else:
                for _, row in df.iterrows():
                    pos = row["Pos"]
                    with st.expander(f"🏈 {row['Player']}  —  {row['Pos']}  |  {row['Season']}  |  {row['GP']} GP"):
                        if "Quarterback" in str(pos):
                            c1,c2,c3,c4,c5 = st.columns(5)
                            c1.metric("Pass Yds", f"{int(row['Pass Yds']):,}" if pd.notna(row['Pass Yds']) else "-")
                            c2.metric("Pass TD",  row["Pass TD"]); c3.metric("INT", row["INT"])
                            c4.metric("QBR", row["QBR"]); c5.metric("Rush Yds", row["Rush Yds"])
                        elif "Running" in str(pos):
                            c1,c2,c3,c4 = st.columns(4)
                            c1.metric("Rush Yds", f"{int(row['Rush Yds']):,}" if pd.notna(row['Rush Yds']) else "-")
                            c2.metric("Rush TD", row["Rush TD"]); c3.metric("Rec", row["Rec"]); c4.metric("Rec Yds", row["Rec Yds"])
                        elif any(x in str(pos) for x in ["Wide","Tight"]):
                            c1,c2,c3,c4 = st.columns(4)
                            c1.metric("Rec Yds", f"{int(row['Rec Yds']):,}" if pd.notna(row['Rec Yds']) else "-")
                            c2.metric("Rec TD", row["Rec TD"]); c3.metric("Rec", row["Rec"]); c4.metric("Rush Yds", row["Rush Yds"])
                        else:
                            c1,c2,c3 = st.columns(3)
                            c1.metric("Tackles", row["Tackles"]); c2.metric("Sacks", row["Sacks"])
        else:
            st.info("👆 Type a player name above to search across all positions")
