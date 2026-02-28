"""
Multi-Sport Stats Dashboard
============================
Combined NHL + PGA Tour stats in one Streamlit app.
"""

import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Sports Stats Dashboard",
    page_icon="🏆",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Styling
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    .stApp { background-color: #0e1117; }
    .sport-header {
        padding: 20px 28px;
        border-radius: 12px;
        margin-bottom: 20px;
        text-align: center;
    }
    .nhl-header  { background: linear-gradient(135deg, #001F5B 0%, #C8102E 100%); }
    .golf-header { background: linear-gradient(135deg, #1a4731 0%, #2d7a4f 100%); }
    .main-header { background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%); }
    .sport-header h1 { color: white; font-size: 2.2rem; font-weight: 800; margin: 0; }
    .sport-header p  { color: rgba(255,255,255,0.75); font-size: 0.9rem; margin: 6px 0 0 0; }
    .metric-card {
        background: #1c2333; border: 1px solid #2d3748;
        border-radius: 10px; padding: 16px 20px; text-align: center;
    }
    .metric-card .label { color: #a0aec0; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.8px; }
    .metric-card .value { color: #ffffff; font-size: 1.7rem; font-weight: 800; margin: 4px 0; }
    .metric-card .sub   { font-size: 0.8rem; font-weight: 600; }
    .nhl-sub  { color: #C8102E; }
    .golf-sub { color: #2d7a4f; }
    .section-header {
        color: #ffffff; font-size: 1.1rem; font-weight: 700;
        padding: 8px 0 10px 0; margin-bottom: 14px;
    }
    .nhl-section  { border-bottom: 2px solid #C8102E; }
    .golf-section { border-bottom: 2px solid #2d7a4f; }
    #MainMenu {visibility: hidden;} footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# DB connections
# ---------------------------------------------------------------------------
NHL_DB  = Path(__file__).parent / "data" / "nhl_stats.db"
GOLF_DB = Path(__file__).parent / "data" / "golf_stats.db"

@st.cache_resource
def get_nhl_conn():
    if not NHL_DB.exists(): return None
    conn = sqlite3.connect(str(NHL_DB), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

@st.cache_resource
def get_golf_conn():
    if not GOLF_DB.exists(): return None
    conn = sqlite3.connect(str(GOLF_DB), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

# ---------------------------------------------------------------------------
# NHL data loaders
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_skaters():
    conn = get_nhl_conn()
    if not conn: return pd.DataFrame()
    return pd.read_sql_query("""
        SELECT full_name AS "Player", team_abbrev AS "Team", position AS "Pos",
               games_played AS "GP", goals AS "G", assists AS "A", points AS "PTS",
               plus_minus AS "+/-", pp_goals AS "PPG", sh_goals AS "SHG",
               gw_goals AS "GWG", shots AS "SOG", penalty_minutes AS "PIM",
               ROUND(COALESCE(shooting_pct,0),1) AS "Sh%",
               ROUND(COALESCE(points_per_game,0),3) AS "P/GP",
               ROUND(COALESCE(goals_per_game,0),3) AS "G/GP"
        FROM skater_stats WHERE points IS NOT NULL ORDER BY points DESC, goals DESC
    """, conn)

@st.cache_data(ttl=300)
def load_goalies():
    conn = get_nhl_conn()
    if not conn: return pd.DataFrame()
    return pd.read_sql_query("""
        SELECT full_name AS "Goalie", team_abbrev AS "Team",
               wins AS "W", losses AS "L", ot_losses AS "OTL",
               ROUND(COALESCE(save_pct,0),3) AS "SV%",
               ROUND(COALESCE(gaa,0),2) AS "GAA", shutouts AS "SO"
        FROM goalie_stats WHERE wins IS NOT NULL ORDER BY save_pct DESC, wins DESC
    """, conn)

@st.cache_data(ttl=300)
def load_standings():
    conn = get_nhl_conn()
    if not conn: return pd.DataFrame()
    return pd.read_sql_query("""
        SELECT team_name AS "Team", team_abbrev AS "Abbrev", division AS "Division",
               conference AS "Conference", wins AS "W", losses AS "L",
               ot_losses AS "OTL", points AS "PTS", games_played AS "GP",
               goals_for AS "GF", goals_against AS "GA", goal_diff AS "GDiff",
               ROUND(CAST(goals_for AS REAL)/NULLIF(games_played,0),2) AS "GF/GP",
               ROUND(CAST(goals_against AS REAL)/NULLIF(games_played,0),2) AS "GA/GP",
               ROUND(pp_pct,1) AS "PP%", ROUND(pk_pct,1) AS "PK%",
               home_wins AS "HW", home_losses AS "HL",
               away_wins AS "AW", away_losses AS "AL",
               l10_wins AS "L10W", l10_losses AS "L10L"
        FROM standings ORDER BY points DESC, wins DESC
    """, conn)

@st.cache_data(ttl=300)
def load_advanced():
    conn = get_nhl_conn()
    if not conn: return pd.DataFrame()
    return pd.read_sql_query("""
        SELECT full_name AS "Player", team_abbrev AS "Team", position AS "Pos",
               points AS "PTS",
               ROUND(COALESCE(points_per_game,0),3) AS "P/GP",
               ROUND(COALESCE(goals_per_game,0),3) AS "G/GP",
               ROUND(CASE WHEN COALESCE(points,0)>0
                     THEN CAST(COALESCE(goals,0) AS REAL)/points*100 ELSE 0 END,1) AS "Goal%",
               ROUND(CASE WHEN COALESCE(points,0)>0
                     THEN CAST(COALESCE(assists,0) AS REAL)/points*100 ELSE 0 END,1) AS "Ast%",
               plus_minus AS "+/-",
               ROUND(COALESCE(shooting_pct,0),1) AS "Sh%",
               penalty_minutes AS "PIM"
        FROM skater_stats WHERE points >= 10 ORDER BY points_per_game DESC
    """, conn)

# ---------------------------------------------------------------------------
# Golf data loaders
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_golf_season_stats(year=None):
    conn = get_golf_conn()
    if not conn: return pd.DataFrame()
    where = f"WHERE year = {year}" if year else "WHERE year IN (2023,2024,2025)"
    return pd.read_sql_query(f"""
        SELECT full_name AS "Player", year AS "Year",
               events AS "Events", cuts_made AS "Cuts",
               wins AS "Wins", top_5 AS "Top 5", top_10 AS "Top 10",
               top_20 AS "Top 20", top_25 AS "Top 25",
               best_finish AS "Best Finish",
               ROUND(COALESCE(avg_score,0),1) AS "Avg Score",
               ROUND(COALESCE(cut_pct,0),1) AS "Cut%",
               ROUND(COALESCE(win_pct,0),1) AS "Win%"
        FROM golf_player_season_stats {where}
        ORDER BY wins DESC, top_10 DESC, cuts_made DESC
    """, conn)

@st.cache_data(ttl=300)
def load_golf_results(year=None):
    conn = get_golf_conn()
    if not conn: return pd.DataFrame()
    where = f"WHERE r.year = {year}" if year else "WHERE r.year IN (2023,2024,2025)"
    return pd.read_sql_query(f"""
        SELECT r.full_name AS "Player", r.year AS "Year",
               r.tournament_name AS "Tournament",
               r.position AS "Finish", r.position_num AS "Pos#",
               r.total_score AS "Score", r.total_strokes AS "Strokes",
               r.made_cut AS "Made Cut", r.win AS "Win",
               r.top_5 AS "Top 5", r.top_10 AS "Top 10",
               t.purse AS "Purse"
        FROM golf_results r
        LEFT JOIN golf_tournaments t ON r.tourn_id=t.tourn_id AND r.year=t.year
        {where}
        ORDER BY r.year DESC, r.position_num ASC
    """, conn)

@st.cache_data(ttl=300)
def load_golf_tournaments(year=None):
    conn = get_golf_conn()
    if not conn: return pd.DataFrame()
    where = f"WHERE year = {year}" if year else "WHERE year IN (2023,2024,2025)"
    return pd.read_sql_query(f"""
        SELECT name AS "Tournament", year AS "Year",
               start_date AS "Start", end_date AS "End",
               purse AS "Purse", winners_share AS "Winner's Share",
               fedex_points AS "FedEx Pts", format AS "Format"
        FROM golf_tournaments {where}
        ORDER BY year DESC, start_date DESC
    """, conn)

@st.cache_data(ttl=300)
def load_golf_winners():
    conn = get_golf_conn()
    if not conn: return pd.DataFrame()
    return pd.read_sql_query("""
        SELECT r.full_name AS "Player", r.year AS "Year",
               r.tournament_name AS "Tournament",
               r.total_score AS "Score",
               t.purse AS "Purse"
        FROM golf_results r
        LEFT JOIN golf_tournaments t ON r.tourn_id=t.tourn_id AND r.year=t.year
        WHERE r.win = 1
        ORDER BY r.year DESC, t.purse DESC
    """, conn)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("## 🏆 Sports Dashboard")
    st.markdown("---")
    sport = st.radio("Sport", ["🏒 NHL", "⛳ PGA Tour"], label_visibility="collapsed")
    st.markdown("---")

    if sport == "🏒 NHL":
        page = st.radio("Section", [
            "🏠 Overview", "🏆 Skater Rankings", "📈 Advanced Stats",
            "🧤 Goalies", "🏒 Standings", "🔍 Player Search"
        ], label_visibility="collapsed")
    else:
        page = st.radio("Section", [
            "🏠 Golf Overview", "🏆 Season Leaderboard", "🏌️ Player Profile",
            "🏅 Tournament Winners", "📅 Schedule & Tournaments"
        ], label_visibility="collapsed")

    st.markdown("---")
    st.markdown("<small style='color:#666'>NHL: NHL Stats API<br>Golf: RapidAPI / SlashGolf</small>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
nhl_ok  = NHL_DB.exists()
golf_ok = GOLF_DB.exists()

skaters   = load_skaters()   if nhl_ok  else pd.DataFrame()
goalies   = load_goalies()   if nhl_ok  else pd.DataFrame()
standings = load_standings() if nhl_ok  else pd.DataFrame()
advanced  = load_advanced()  if nhl_ok  else pd.DataFrame()

golf_stats    = load_golf_season_stats()   if golf_ok else pd.DataFrame()
golf_results  = load_golf_results()        if golf_ok else pd.DataFrame()
golf_tourneys = load_golf_tournaments()    if golf_ok else pd.DataFrame()
golf_winners  = load_golf_winners()        if golf_ok else pd.DataFrame()

# ===========================================================================
# NHL PAGES
# ===========================================================================

if sport == "🏒 NHL":

    st.markdown("""<div class="sport-header nhl-header">
        <h1>🏒 NHL Stats Dashboard</h1>
        <p>Live statistics powered by the NHL Stats API</p>
    </div>""", unsafe_allow_html=True)

    if skaters.empty:
        st.error("⚠️ NHL database not found. Run the NHL collector and loader first.")
        st.stop()

    # ── OVERVIEW ──────────────────────────────────────────────────────────
    if page == "🏠 Overview":
        top       = skaters.iloc[0]
        top_goals = skaters.nlargest(1,"G").iloc[0]
        top_g     = goalies.iloc[0].to_dict() if not goalies.empty else {}
        top_team  = standings.iloc[0] if not standings.empty else {}

        c1,c2,c3,c4 = st.columns(4)
        for col, label, value, sub, css in [
            (c1, "Points Leader",  top.get("PTS","-"),        f"{top.get('Player','')} · {top.get('Team','')}", "nhl-sub"),
            (c2, "Goals Leader",   top_goals.get("G","-"),    f"{top_goals.get('Player','')} · {top_goals.get('Team','')}", "nhl-sub"),
            (c3, "Top SV%",        top_g.get("SV%","-"),      f"{top_g.get('Goalie','')} · {top_g.get('Team','')}" if top_g else "", "nhl-sub"),
            (c4, "League Leader",  f"{top_team.get('PTS','-')} pts", top_team.get("Team","-"), "nhl-sub"),
        ]:
            col.markdown(f"""<div class="metric-card">
                <div class="label">{label}</div><div class="value">{value}</div>
                <div class="sub {css}">{sub}</div></div>""", unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        c1,c2 = st.columns(2)

        with c1:
            st.markdown('<div class="section-header nhl-section">🏆 Top 15 Points Leaders</div>', unsafe_allow_html=True)
            fig = px.bar(skaters.head(15), x="PTS", y="Player", orientation="h",
                         color="PTS", color_continuous_scale=["#001F5B","#C8102E"],
                         text="PTS", hover_data=["Team","G","A"])
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              font_color="white", yaxis=dict(autorange="reversed"),
                              xaxis=dict(showgrid=False), coloraxis_showscale=False,
                              margin=dict(l=0,r=20,t=10,b=10), height=420)
            fig.update_traces(textposition="outside", textfont=dict(color="white"))
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.markdown('<div class="section-header nhl-section">🏒 Goals For vs Against (Top 16)</div>', unsafe_allow_html=True)
            t16 = standings.head(16)
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(name="Goals For",     x=t16["Abbrev"], y=t16["GF"], marker_color="#001F5B"))
            fig2.add_trace(go.Bar(name="Goals Against", x=t16["Abbrev"], y=t16["GA"], marker_color="#C8102E"))
            fig2.update_layout(barmode="group", paper_bgcolor="rgba(0,0,0,0)",
                               plot_bgcolor="rgba(0,0,0,0)", font_color="white",
                               legend=dict(orientation="h",yanchor="bottom",y=1.02),
                               xaxis=dict(showgrid=False), yaxis=dict(showgrid=True,gridcolor="#2d3748"),
                               margin=dict(l=0,r=0,t=30,b=10), height=420)
            st.plotly_chart(fig2, use_container_width=True)

        c3,c4 = st.columns(2)
        with c3:
            st.markdown('<div class="section-header nhl-section">🎯 Top 15 Goal Scorers</div>', unsafe_allow_html=True)
            fig3 = px.bar(skaters.nlargest(15,"G"), x="G", y="Player", orientation="h",
                          color="Sh%", color_continuous_scale=["#1a365d","#C8102E"],
                          text="G", hover_data=["Team","Sh%"])
            fig3.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                               font_color="white", yaxis=dict(autorange="reversed"),
                               xaxis=dict(showgrid=False), margin=dict(l=0,r=20,t=10,b=10), height=420)
            fig3.update_traces(textposition="outside", textfont=dict(color="white"))
            st.plotly_chart(fig3, use_container_width=True)

        with c4:
            st.markdown('<div class="section-header nhl-section">📊 Points vs P/GP</div>', unsafe_allow_html=True)
            fig4 = px.scatter(skaters[skaters["PTS"]>=20], x="PTS", y="P/GP",
                              color="Pos", hover_name="Player", hover_data=["Team","G","A"],
                              size="PTS", size_max=18,
                              color_discrete_map={"C":"#C8102E","L":"#001F5B","R":"#FFD700","D":"#48BB78"})
            fig4.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                               font_color="white",
                               xaxis=dict(showgrid=True,gridcolor="#2d3748"),
                               yaxis=dict(showgrid=True,gridcolor="#2d3748"),
                               margin=dict(l=0,r=0,t=10,b=10), height=420)
            st.plotly_chart(fig4, use_container_width=True)

    # ── SKATER RANKINGS ───────────────────────────────────────────────────
    elif page == "🏆 Skater Rankings":
        st.markdown('<div class="section-header nhl-section">🏆 Skater Rankings</div>', unsafe_allow_html=True)
        c1,c2,c3 = st.columns(3)
        pos_f  = c1.multiselect("Position", ["C","L","R","D"], default=["C","L","R","D"])
        team_f = c2.multiselect("Team", sorted(skaters["Team"].unique()), default=[])
        min_p  = c3.slider("Min Points", 0, int(skaters["PTS"].max()), 0)
        df = skaters[skaters["Pos"].isin(pos_f)]
        if team_f: df = df[df["Team"].isin(team_f)]
        df = df[df["PTS"]>=min_p].reset_index(drop=True)
        df.index += 1
        st.dataframe(df, use_container_width=True, height=600,
                     column_config={"Sh%": st.column_config.NumberColumn(format="%.1f%%"),
                                    "P/GP": st.column_config.NumberColumn(format="%.3f"),
                                    "G/GP": st.column_config.NumberColumn(format="%.3f")})
        st.download_button("⬇️ Download CSV", df.to_csv(index=False).encode(), "skaters.csv", "text/csv")

    # ── ADVANCED STATS ────────────────────────────────────────────────────
    elif page == "📈 Advanced Stats":
        st.markdown('<div class="section-header nhl-section">📈 Advanced Stats</div>', unsafe_allow_html=True)
        c1,c2 = st.columns(2)
        pos_f = c1.multiselect("Position", ["C","L","R","D"], default=["C","L","R","D"])
        min_p = c2.slider("Min Points", 10, int(advanced["PTS"].max()), 10)
        df = advanced[advanced["Pos"].isin(pos_f)][advanced["PTS"]>=min_p].reset_index(drop=True)
        df.index += 1
        st.dataframe(df, use_container_width=True, height=500,
                     column_config={"P/GP": st.column_config.NumberColumn(format="%.3f"),
                                    "G/GP": st.column_config.NumberColumn(format="%.3f"),
                                    "Goal%": st.column_config.NumberColumn(format="%.1f%%"),
                                    "Ast%": st.column_config.NumberColumn(format="%.1f%%")})
        fig = px.box(advanced[advanced["Pos"].isin(pos_f)], x="Pos", y="P/GP", color="Pos",
                     color_discrete_map={"C":"#C8102E","L":"#001F5B","R":"#FFD700","D":"#48BB78"},
                     points="all", hover_name="Player")
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          font_color="white", showlegend=False,
                          xaxis=dict(showgrid=False), yaxis=dict(showgrid=True,gridcolor="#2d3748"), height=380)
        st.plotly_chart(fig, use_container_width=True)
        st.download_button("⬇️ Download CSV", df.to_csv(index=False).encode(), "advanced.csv", "text/csv")

    # ── GOALIES ───────────────────────────────────────────────────────────
    elif page == "🧤 Goalies":
        st.markdown('<div class="section-header nhl-section">🧤 Goalie Rankings</div>', unsafe_allow_html=True)
        c1,c2 = st.columns(2)
        min_w  = c1.slider("Min Wins", 0, int(goalies["W"].max()), 5)
        team_f = c2.multiselect("Team", sorted(goalies["Team"].unique()), default=[])
        df = goalies[goalies["W"]>=min_w]
        if team_f: df = df[df["Team"].isin(team_f)]
        df = df.reset_index(drop=True); df.index += 1
        st.dataframe(df, use_container_width=True, height=500,
                     column_config={"SV%": st.column_config.NumberColumn(format="%.3f"),
                                    "GAA": st.column_config.NumberColumn(format="%.2f")})
        fig = px.scatter(goalies[goalies["W"]>=5], x="GAA", y="SV%", size="W",
                         color="W", color_continuous_scale=["#001F5B","#C8102E"],
                         hover_name="Goalie", hover_data=["Team","W","SO"], size_max=20)
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          font_color="white", xaxis=dict(showgrid=True,gridcolor="#2d3748",autorange="reversed"),
                          yaxis=dict(showgrid=True,gridcolor="#2d3748"), height=400)
        st.plotly_chart(fig, use_container_width=True)

    # ── STANDINGS ─────────────────────────────────────────────────────────
    elif page == "🏒 Standings":
        st.markdown('<div class="section-header nhl-section">🏒 Standings</div>', unsafe_allow_html=True)
        conf = st.radio("Conference", ["All","Eastern","Western"], horizontal=True)
        df   = standings if conf=="All" else standings[standings["Conference"].str.contains(conf,case=False,na=False)]
        for div in df["Division"].unique():
            st.markdown(f"**{div}**")
            d = df[df["Division"]==div][["Team","Abbrev","W","L","OTL","PTS","GP","GF","GA","GDiff","GF/GP","GA/GP","PP%","PK%","HW","HL","AW","AL","L10W","L10L"]].reset_index(drop=True)
            d.index += 1
            st.dataframe(d, use_container_width=True,
                         column_config={"PP%": st.column_config.NumberColumn(format="%.1f%%"),
                                        "PK%": st.column_config.NumberColumn(format="%.1f%%")})
        fig = px.bar(standings.nlargest(16,"PP%"), x="Abbrev", y="PP%",
                     color="PP%", color_continuous_scale=["#001F5B","#C8102E"], text="PP%")
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          font_color="white", coloraxis_showscale=False, height=320, margin=dict(t=10))
        fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", textfont=dict(color="white"))
        st.plotly_chart(fig, use_container_width=True)

    # ── PLAYER SEARCH ─────────────────────────────────────────────────────
    elif page == "🔍 Player Search":
        st.markdown('<div class="section-header nhl-section">🔍 Player Search</div>', unsafe_allow_html=True)
        search = st.text_input("Search by player name", placeholder="e.g. McDavid, Crosby...")
        if search:
            results = skaters[skaters["Player"].str.contains(search, case=False, na=False)]
            if results.empty:
                st.warning(f"No players found matching '{search}'")
            else:
                for _, row in results.iterrows():
                    with st.expander(f"🏒 {row['Player']}  —  {row['Team']}  |  {row['Pos']}"):
                        c1,c2,c3,c4,c5,c6 = st.columns(6)
                        c1.metric("Points", row["PTS"]); c2.metric("Goals", row["G"])
                        c3.metric("Assists", row["A"]); c4.metric("+/-", row["+/-"])
                        c5.metric("P/GP", row["P/GP"]); c6.metric("Sh%", f"{row['Sh%']}%")
                        c7,c8,c9,c10 = st.columns(4)
                        c7.metric("PP Goals", row["PPG"]); c8.metric("SH Goals", row["SHG"])
                        c9.metric("GWG", row["GWG"]); c10.metric("Shots", row["SOG"])
        else:
            st.info("👆 Type a player name above to search")
            st.dataframe(skaters[["Player","Team","Pos","GP","G","A","PTS","P/GP","Sh%"]],
                         use_container_width=True, height=500)


# ===========================================================================
# GOLF PAGES
# ===========================================================================

elif sport == "⛳ PGA Tour":

    st.markdown("""<div class="sport-header golf-header">
        <h1>⛳ PGA Tour Stats Dashboard</h1>
        <p>2023 · 2024 · 2025 seasons  |  163 tournaments  |  Powered by SlashGolf API</p>
    </div>""", unsafe_allow_html=True)

    if golf_stats.empty:
        st.error("⚠️ Golf database not found. Run golf/golf_collector.py first.")
        st.stop()

    # ── GOLF OVERVIEW ─────────────────────────────────────────────────────
    if page == "🏠 Golf Overview":

        # Top metrics across all seasons
        most_wins    = golf_stats.nlargest(1,"Wins").iloc[0]
        best_cut_pct = golf_stats[golf_stats["Events"]>=10].nlargest(1,"Cut%").iloc[0]
        most_top10   = golf_stats.nlargest(1,"Top 10").iloc[0]
        winners_2025 = golf_stats[golf_stats["Year"]==2025]["Wins"].sum()

        c1,c2,c3,c4 = st.columns(4)
        for col, label, value, sub in [
            (c1, "Most Wins (Season)", most_wins["Wins"],    f"{most_wins['Player']} · {most_wins['Year']}"),
            (c2, "Best Cut% (10+ events)", f"{best_cut_pct['Cut%']}%", f"{best_cut_pct['Player']} · {best_cut_pct['Year']}"),
            (c3, "Most Top 10s (Season)", most_top10["Top 10"], f"{most_top10['Player']} · {most_top10['Year']}"),
            (c4, "2025 Total Wins", winners_2025, "Across all players"),
        ]:
            col.markdown(f"""<div class="metric-card">
                <div class="label">{label}</div><div class="value">{value}</div>
                <div class="sub golf-sub">{sub}</div></div>""", unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        c1,c2 = st.columns(2)

        with c1:
            st.markdown('<div class="section-header golf-section">🏆 All-Time Win Leaders (2023–2025)</div>', unsafe_allow_html=True)
            win_leaders = golf_stats.groupby("Player").agg(
                Total_Wins=("Wins","sum"),
                Top_10=("Top 10","sum"),
                Events=("Events","sum")
            ).reset_index().nlargest(15,"Total_Wins")
            fig = px.bar(win_leaders, x="Total_Wins", y="Player", orientation="h",
                         color="Total_Wins", color_continuous_scale=["#1a4731","#2d7a4f"],
                         text="Total_Wins", hover_data=["Top_10","Events"])
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              font_color="white", yaxis=dict(autorange="reversed"),
                              xaxis=dict(showgrid=False), coloraxis_showscale=False,
                              margin=dict(l=0,r=20,t=10,b=10), height=420)
            fig.update_traces(textposition="outside", textfont=dict(color="white"))
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.markdown('<div class="section-header golf-section">📊 Wins by Season</div>', unsafe_allow_html=True)
            top_players = golf_stats.groupby("Player")["Wins"].sum().nlargest(8).index.tolist()
            win_by_yr   = golf_stats[golf_stats["Player"].isin(top_players)]
            fig2 = px.bar(win_by_yr, x="Year", y="Wins", color="Player",
                          barmode="group", text="Wins",
                          color_discrete_sequence=px.colors.qualitative.Set2)
            fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                               font_color="white", xaxis=dict(showgrid=False),
                               yaxis=dict(showgrid=True,gridcolor="#2d3748"),
                               legend=dict(orientation="h",yanchor="bottom",y=1.02,font=dict(size=9)),
                               margin=dict(l=0,r=0,t=40,b=10), height=420)
            st.plotly_chart(fig2, use_container_width=True)

        c3,c4 = st.columns(2)
        with c3:
            st.markdown('<div class="section-header golf-section">🎯 Top 10 Finishes Leaders (2023–2025)</div>', unsafe_allow_html=True)
            top10_leaders = golf_stats.groupby("Player").agg(
                Total_Top10=("Top 10","sum"), Wins=("Wins","sum")
            ).reset_index().nlargest(15,"Total_Top10")
            fig3 = px.bar(top10_leaders, x="Total_Top10", y="Player", orientation="h",
                          color="Wins", color_continuous_scale=["#1a4731","#FFD700"],
                          text="Total_Top10")
            fig3.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                               font_color="white", yaxis=dict(autorange="reversed"),
                               xaxis=dict(showgrid=False), margin=dict(l=0,r=20,t=10,b=10), height=420)
            fig3.update_traces(textposition="outside", textfont=dict(color="white"))
            st.plotly_chart(fig3, use_container_width=True)

        with c4:
            st.markdown('<div class="section-header golf-section">✂️ Cut% vs Wins (min 10 events)</div>', unsafe_allow_html=True)
            scatter_df = golf_stats[(golf_stats["Events"]>=10) & (golf_stats["Year"]==2025)]
            fig4 = px.scatter(scatter_df, x="Cut%", y="Top 10", size="Events",
                              color="Wins", color_continuous_scale=["#1a4731","#FFD700"],
                              hover_name="Player", hover_data=["Wins","Events","Best Finish"],
                              size_max=18)
            fig4.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                               font_color="white",
                               xaxis=dict(showgrid=True,gridcolor="#2d3748"),
                               yaxis=dict(showgrid=True,gridcolor="#2d3748"),
                               margin=dict(l=0,r=0,t=10,b=10), height=420)
            st.plotly_chart(fig4, use_container_width=True)

    # ── SEASON LEADERBOARD ────────────────────────────────────────────────
    elif page == "🏆 Season Leaderboard":
        st.markdown('<div class="section-header golf-section">🏆 Season Leaderboard</div>', unsafe_allow_html=True)

        c1,c2,c3 = st.columns(3)
        year_f   = c1.selectbox("Season", [2025, 2024, 2023], index=0)
        min_ev   = c2.slider("Min Events", 1, 30, 5)
        sort_by  = c3.selectbox("Sort By", ["Wins","Top 5","Top 10","Top 20","Cut%","Events"])

        df = load_golf_season_stats(year=year_f)
        df = df[df["Events"] >= min_ev].sort_values(sort_by, ascending=False).reset_index(drop=True)
        df.index += 1

        st.dataframe(df, use_container_width=True, height=600,
                     column_config={
                         "Cut%": st.column_config.NumberColumn(format="%.1f%%"),
                         "Win%": st.column_config.NumberColumn(format="%.1f%%"),
                         "Avg Score": st.column_config.NumberColumn(format="%.1f"),
                     })
        st.caption(f"Showing {len(df)} players — {year_f} season")
        st.download_button("⬇️ Download CSV", df.to_csv(index=False).encode(), f"golf_{year_f}.csv", "text/csv")

    # ── PLAYER PROFILE ────────────────────────────────────────────────────
    elif page == "🏌️ Player Profile":
        st.markdown('<div class="section-header golf-section">🏌️ Player Profile</div>', unsafe_allow_html=True)

        all_players = sorted(golf_stats["Player"].unique())
        selected    = st.selectbox("Select a player", all_players,
                                   index=all_players.index("Scottie Scheffler") if "Scottie Scheffler" in all_players else 0)

        player_stats   = golf_stats[golf_stats["Player"]==selected].sort_values("Year")
        player_results = golf_results[golf_results["Player"]==selected].sort_values(["Year","Pos#"])

        if not player_stats.empty:
            # Career summary metrics
            total_wins  = player_stats["Wins"].sum()
            total_top10 = player_stats["Top 10"].sum()
            total_ev    = player_stats["Events"].sum()
            best        = player_stats["Best Finish"].min()
            avg_cut     = player_stats["Cut%"].mean()

            c1,c2,c3,c4,c5 = st.columns(5)
            c1.metric("Career Wins",    total_wins)
            c2.metric("Career Top 10s", total_top10)
            c3.metric("Total Events",   total_ev)
            c4.metric("Best Finish",    f"T{best}" if best and best > 1 else "1st" if best==1 else "-")
            c5.metric("Avg Cut%",       f"{avg_cut:.1f}%")

            st.markdown("<br>", unsafe_allow_html=True)
            c1,c2 = st.columns(2)

            with c1:
                st.markdown(f'<div class="section-header golf-section">Season Stats — {selected}</div>', unsafe_allow_html=True)
                st.dataframe(player_stats[["Year","Events","Cuts","Wins","Top 5","Top 10","Top 20","Best Finish","Cut%"]],
                             use_container_width=True, hide_index=True)

            with c2:
                st.markdown('<div class="section-header golf-section">Wins & Top 10s by Season</div>', unsafe_allow_html=True)
                fig = go.Figure()
                fig.add_trace(go.Bar(name="Wins", x=player_stats["Year"], y=player_stats["Wins"], marker_color="#FFD700"))
                fig.add_trace(go.Bar(name="Top 10s", x=player_stats["Year"], y=player_stats["Top 10"], marker_color="#2d7a4f"))
                fig.update_layout(barmode="group", paper_bgcolor="rgba(0,0,0,0)",
                                  plot_bgcolor="rgba(0,0,0,0)", font_color="white",
                                  xaxis=dict(showgrid=False), yaxis=dict(showgrid=True,gridcolor="#2d3748"),
                                  height=300, margin=dict(t=10))
                st.plotly_chart(fig, use_container_width=True)

            # Tournament results
            st.markdown(f'<div class="section-header golf-section">Tournament Results (2023–2025)</div>', unsafe_allow_html=True)
            wins_only = player_results[player_results["Win"]==1][["Year","Tournament","Finish","Score","Purse"]]
            if not wins_only.empty:
                st.markdown("🏆 **Wins:**")
                st.dataframe(wins_only, use_container_width=True, hide_index=True)

            st.dataframe(player_results[["Year","Tournament","Finish","Score","Made Cut","Top 10"]],
                         use_container_width=True, height=400, hide_index=True)

    # ── TOURNAMENT WINNERS ────────────────────────────────────────────────
    elif page == "🏅 Tournament Winners":
        st.markdown('<div class="section-header golf-section">🏅 Tournament Winners</div>', unsafe_allow_html=True)

        c1,c2 = st.columns(2)
        year_f   = c1.selectbox("Season", ["All",2025,2024,2023], index=0)
        search_w = c2.text_input("Search player or tournament", placeholder="e.g. Scheffler, Masters...")

        df = golf_winners.copy()
        if year_f != "All":
            df = df[df["Year"]==int(year_f)]
        if search_w:
            mask = (df["Player"].str.contains(search_w,case=False,na=False) |
                    df["Tournament"].str.contains(search_w,case=False,na=False))
            df   = df[mask]

        df = df.reset_index(drop=True); df.index += 1
        st.dataframe(df, use_container_width=True, height=600,
                     column_config={"Purse": st.column_config.NumberColumn(format="$%d")})
        st.caption(f"{len(df)} tournament wins shown")
        st.download_button("⬇️ Download CSV", df.to_csv(index=False).encode(), "golf_winners.csv", "text/csv")

    # ── SCHEDULE & TOURNAMENTS ────────────────────────────────────────────
    elif page == "📅 Schedule & Tournaments":
        st.markdown('<div class="section-header golf-section">📅 Schedule & Tournaments</div>', unsafe_allow_html=True)

        year_f = st.selectbox("Season", [2025, 2024, 2023], index=0)
        df     = load_golf_tournaments(year=year_f).reset_index(drop=True)
        df.index += 1

        st.dataframe(df, use_container_width=True, height=600,
                     column_config={"Purse": st.column_config.NumberColumn(format="$%d"),
                                    "Winner's Share": st.column_config.NumberColumn(format="$%d")})

        # Purse distribution
        st.markdown('<div class="section-header golf-section">💰 Purse by Tournament</div>', unsafe_allow_html=True)
        purse_df = df[df["Purse"].notna()].nlargest(20,"Purse")
        fig = px.bar(purse_df, x="Purse", y="Tournament", orientation="h",
                     color="Purse", color_continuous_scale=["#1a4731","#FFD700"], text="Purse")
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          font_color="white", yaxis=dict(autorange="reversed"),
                          xaxis=dict(showgrid=False), coloraxis_showscale=False,
                          margin=dict(l=0,r=20,t=10,b=10), height=520)
        fig.update_traces(texttemplate="$%{text:,.0f}", textposition="outside", textfont=dict(color="white",size=9))
        st.plotly_chart(fig, use_container_width=True)
