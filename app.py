"""
NHL Stats Dashboard
===================
A Streamlit app that displays NHL statistics pulled from the local SQLite database.

Run locally:
    streamlit run app.py
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
    page_title="NHL Stats Dashboard",
    page_icon="🏒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Styling
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    /* Main background */
    .stApp { background-color: #0e1117; }

    /* Header banner */
    .nhl-header {
        background: linear-gradient(135deg, #001F5B 0%, #C8102E 100%);
        padding: 24px 32px;
        border-radius: 12px;
        margin-bottom: 24px;
        text-align: center;
    }
    .nhl-header h1 {
        color: white;
        font-size: 2.4rem;
        font-weight: 800;
        margin: 0;
        letter-spacing: 1px;
    }
    .nhl-header p {
        color: rgba(255,255,255,0.75);
        font-size: 0.95rem;
        margin: 6px 0 0 0;
    }

    /* Metric cards */
    .metric-card {
        background: #1c2333;
        border: 1px solid #2d3748;
        border-radius: 10px;
        padding: 16px 20px;
        text-align: center;
    }
    .metric-card .label {
        color: #a0aec0;
        font-size: 0.78rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.8px;
    }
    .metric-card .value {
        color: #ffffff;
        font-size: 1.8rem;
        font-weight: 800;
        margin: 4px 0;
    }
    .metric-card .sub {
        color: #C8102E;
        font-size: 0.82rem;
        font-weight: 600;
    }

    /* Section headers */
    .section-header {
        color: #ffffff;
        font-size: 1.15rem;
        font-weight: 700;
        padding: 8px 0 12px 0;
        border-bottom: 2px solid #C8102E;
        margin-bottom: 16px;
    }

    /* Dataframe tweaks */
    .dataframe { font-size: 0.85rem; }

    /* Hide streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DB_PATH = Path(__file__).parent / "data" / "nhl_stats.db"


@st.cache_resource
def get_connection():
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


@st.cache_data(ttl=300)
def load_skaters():
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    return pd.read_sql_query("""
        SELECT
            full_name        AS "Player",
            team_abbrev      AS "Team",
            position         AS "Pos",
            games_played     AS "GP",
            goals            AS "G",
            assists          AS "A",
            points           AS "PTS",
            plus_minus       AS "+/-",
            pp_goals         AS "PPG",
            sh_goals         AS "SHG",
            gw_goals         AS "GWG",
            shots            AS "SOG",
            penalty_minutes  AS "PIM",
            ROUND(COALESCE(shooting_pct, 0), 1)    AS "Sh%",
            ROUND(COALESCE(points_per_game, 0), 3) AS "P/GP",
            ROUND(COALESCE(goals_per_game, 0), 3)  AS "G/GP"
        FROM skater_stats
        WHERE points IS NOT NULL
        ORDER BY points DESC, goals DESC
    """, conn)


@st.cache_data(ttl=300)
def load_goalies():
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    return pd.read_sql_query("""
        SELECT
            full_name   AS "Goalie",
            team_abbrev AS "Team",
            wins        AS "W",
            losses      AS "L",
            ot_losses   AS "OTL",
            ROUND(COALESCE(save_pct, 0), 3)  AS "SV%",
            ROUND(COALESCE(gaa, 0), 2)       AS "GAA",
            shutouts    AS "SO"
        FROM goalie_stats
        WHERE wins IS NOT NULL
        ORDER BY save_pct DESC, wins DESC
    """, conn)

@st.cache_data(ttl=300)
def load_standings():
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    return pd.read_sql_query("""
        SELECT
            team_name   AS "Team",
            team_abbrev AS "Abbrev",
            division    AS "Division",
            conference  AS "Conference",
            wins        AS "W",
            losses      AS "L",
            ot_losses   AS "OTL",
            points      AS "PTS",
            games_played AS "GP",
            goals_for   AS "GF",
            goals_against AS "GA",
            goal_diff   AS "GDiff",
            ROUND(CAST(goals_for AS REAL) / NULLIF(games_played,0), 2)     AS "GF/GP",
            ROUND(CAST(goals_against AS REAL) / NULLIF(games_played,0), 2) AS "GA/GP",
            ROUND(pp_pct, 1) AS "PP%",
            ROUND(pk_pct, 1) AS "PK%",
            home_wins   AS "HW",
            home_losses AS "HL",
            away_wins   AS "AW",
            away_losses AS "AL",
            l10_wins    AS "L10W",
            l10_losses  AS "L10L"
        FROM standings
        ORDER BY points DESC, wins DESC
    """, conn)


@st.cache_data(ttl=300)
def load_advanced():
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    return pd.read_sql_query("""
        SELECT
            full_name   AS "Player",
            team_abbrev AS "Team",
            position    AS "Pos",
            points      AS "PTS",
            ROUND(COALESCE(points_per_game,0), 3)  AS "P/GP",
            ROUND(COALESCE(goals_per_game,0), 3)   AS "G/GP",
            ROUND(
                CASE WHEN COALESCE(points,0) > 0
                THEN CAST(COALESCE(goals,0) AS REAL) / points * 100
                ELSE 0 END, 1
            ) AS "Goal%",
            ROUND(
                CASE WHEN COALESCE(points,0) > 0
                THEN CAST(COALESCE(assists,0) AS REAL) / points * 100
                ELSE 0 END, 1
            ) AS "Ast%",
            plus_minus  AS "+/-",
            ROUND(COALESCE(shooting_pct,0), 1) AS "Sh%",
            penalty_minutes AS "PIM"
        FROM skater_stats
        WHERE points >= 10
        ORDER BY points_per_game DESC
    """, conn)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("## 🏒 NHL Dashboard")
    st.markdown("---")
    page = st.radio(
        "Navigate",
        ["🏠 Overview", "🏆 Skater Rankings", "📈 Advanced Stats",
         "🧤 Goalies", "🏒 Standings", "🔍 Player Search"],
        label_visibility="collapsed"
    )
    st.markdown("---")
    st.markdown(
        "<small style='color:#666'>Data via NHL Stats API<br>Refresh: run data_collector.py</small>",
        unsafe_allow_html=True
    )

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown("""
<div class="nhl-header">
    <h1>🏒 NHL Stats Dashboard</h1>
    <p>Live statistics powered by the NHL Stats API</p>
</div>
""", unsafe_allow_html=True)

# Load data
skaters  = load_skaters()
goalies  = load_goalies()
standings = load_standings()
advanced = load_advanced()

if skaters.empty:
    st.error("⚠️ Database not found. Run `python collectors/data_collector.py` then `python db/load_database.py` first.")
    st.stop()

# ---------------------------------------------------------------------------
# OVERVIEW PAGE
# ---------------------------------------------------------------------------
if page == "🏠 Overview":

    # Top metrics
    top = skaters.iloc[0] if not skaters.empty else {}
    top_goals = skaters.nlargest(1, "G").iloc[0] if not skaters.empty else {}
    top_goalie = goalies.iloc[0] if not goalies.empty else {}
    top_team = standings.iloc[0] if not standings.empty else {}

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""<div class="metric-card">
            <div class="label">Points Leader</div>
            <div class="value">{top.get('PTS', '-')}</div>
            <div class="sub">{top.get('Player', '-')} · {top.get('Team', '')}</div>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""<div class="metric-card">
            <div class="label">Goals Leader</div>
            <div class="value">{top_goals.get('G', '-')}</div>
            <div class="sub">{top_goals.get('Player', '-')} · {top_goals.get('Team', '')}</div>
        </div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""<div class="metric-card">
            <div class="label">Top SV%</div>
            <div class="value">{top_goalie.get('SV%', '-')}</div>
            <div class="sub">{top_goalie.get('Goalie', '-')} · {top_goalie.get('Team', '')}</div>
        </div>""", unsafe_allow_html=True)
    with c4:
        st.markdown(f"""<div class="metric-card">
            <div class="label">League Leader</div>
            <div class="value">{top_team.get('PTS', '-')} pts</div>
            <div class="sub">{top_team.get('Team', '-')}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Charts row
    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="section-header">🏆 Top 15 Points Leaders</div>', unsafe_allow_html=True)
        top15 = skaters.head(15).copy()
        fig = px.bar(
            top15,
            x="PTS", y="Player",
            orientation="h",
            color="PTS",
            color_continuous_scale=["#001F5B", "#C8102E"],
            text="PTS",
            hover_data=["Team", "G", "A", "PTS"],
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="white",
            yaxis=dict(autorange="reversed", tickfont=dict(size=11)),
            xaxis=dict(showgrid=False),
            coloraxis_showscale=False,
            margin=dict(l=0, r=20, t=10, b=10),
            height=420,
        )
        fig.update_traces(textposition="outside", textfont=dict(color="white"))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<div class="section-header">🏒 Goals For vs Against (Top 16 Teams)</div>', unsafe_allow_html=True)
        t16 = standings.head(16).copy()
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            name="Goals For",
            x=t16["Abbrev"], y=t16["GF"],
            marker_color="#001F5B",
        ))
        fig2.add_trace(go.Bar(
            name="Goals Against",
            x=t16["Abbrev"], y=t16["GA"],
            marker_color="#C8102E",
        ))
        fig2.update_layout(
            barmode="group",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="white",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor="#2d3748"),
            margin=dict(l=0, r=0, t=30, b=10),
            height=420,
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Second charts row
    col3, col4 = st.columns(2)

    with col3:
        st.markdown('<div class="section-header">🎯 Top 15 Goal Scorers</div>', unsafe_allow_html=True)
        top_g = skaters.nlargest(15, "G")[["Player", "Team", "G", "Sh%"]].copy()
        fig3 = px.bar(
            top_g, x="G", y="Player",
            orientation="h",
            color="Sh%",
            color_continuous_scale=["#1a365d", "#C8102E"],
            text="G",
            hover_data=["Team", "Sh%"],
        )
        fig3.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="white",
            yaxis=dict(autorange="reversed", tickfont=dict(size=11)),
            xaxis=dict(showgrid=False),
            margin=dict(l=0, r=20, t=10, b=10),
            height=420,
            coloraxis_colorbar=dict(title="Sh%", tickfont=dict(color="white")),
        )
        fig3.update_traces(textposition="outside", textfont=dict(color="white"))
        st.plotly_chart(fig3, use_container_width=True)

    with col4:
        st.markdown('<div class="section-header">📊 Points per Game vs Total Points</div>', unsafe_allow_html=True)
        scatter_df = skaters[skaters["PTS"] >= 20].copy()
        fig4 = px.scatter(
            scatter_df,
            x="PTS", y="P/GP",
            color="Pos",
            hover_name="Player",
            hover_data=["Team", "G", "A"],
            size="PTS",
            size_max=18,
            color_discrete_map={"C": "#C8102E", "L": "#001F5B", "R": "#FFD700", "D": "#48BB78"},
        )
        fig4.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="white",
            xaxis=dict(showgrid=True, gridcolor="#2d3748"),
            yaxis=dict(showgrid=True, gridcolor="#2d3748"),
            margin=dict(l=0, r=0, t=10, b=10),
            height=420,
        )
        st.plotly_chart(fig4, use_container_width=True)

# ---------------------------------------------------------------------------
# SKATER RANKINGS PAGE
# ---------------------------------------------------------------------------
elif page == "🏆 Skater Rankings":
    st.markdown('<div class="section-header">🏆 Skater Rankings</div>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        pos_filter = st.multiselect("Position", ["C", "L", "R", "D"], default=["C", "L", "R", "D"])
    with col2:
        team_filter = st.multiselect("Team", sorted(skaters["Team"].unique()), default=[])
    with col3:
        min_pts = st.slider("Min Points", 0, int(skaters["PTS"].max()), 0)

    filtered = skaters[skaters["Pos"].isin(pos_filter)]
    if team_filter:
        filtered = filtered[filtered["Team"].isin(team_filter)]
    filtered = filtered[filtered["PTS"] >= min_pts].reset_index(drop=True)
    filtered.index += 1

    st.dataframe(
        filtered,
        use_container_width=True,
        height=600,
        column_config={
            "Sh%":  st.column_config.NumberColumn(format="%.1f%%"),
            "P/GP": st.column_config.NumberColumn(format="%.3f"),
            "G/GP": st.column_config.NumberColumn(format="%.3f"),
        }
    )
    st.caption(f"Showing {len(filtered)} players")

    # Download button
    csv = filtered.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download CSV", csv, "skater_rankings.csv", "text/csv")

# ---------------------------------------------------------------------------
# ADVANCED STATS PAGE
# ---------------------------------------------------------------------------
elif page == "📈 Advanced Stats":
    st.markdown('<div class="section-header">📈 Advanced Stats (min. 10 pts, ranked by P/GP)</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        pos_filter = st.multiselect("Position", ["C", "L", "R", "D"], default=["C", "L", "R", "D"], key="adv_pos")
    with col2:
        min_pts = st.slider("Min Points", 10, int(advanced["PTS"].max()), 10, key="adv_pts")

    filtered_adv = advanced[advanced["Pos"].isin(pos_filter)]
    filtered_adv = filtered_adv[filtered_adv["PTS"] >= min_pts].reset_index(drop=True)
    filtered_adv.index += 1

    st.dataframe(
        filtered_adv,
        use_container_width=True,
        height=500,
        column_config={
            "P/GP":  st.column_config.NumberColumn(format="%.3f"),
            "G/GP":  st.column_config.NumberColumn(format="%.3f"),
            "Goal%": st.column_config.NumberColumn(format="%.1f%%"),
            "Ast%":  st.column_config.NumberColumn(format="%.1f%%"),
            "Sh%":   st.column_config.NumberColumn(format="%.1f%%"),
        }
    )

    # P/GP by position box plot
    st.markdown('<div class="section-header">P/GP Distribution by Position</div>', unsafe_allow_html=True)
    fig = px.box(
        advanced[advanced["Pos"].isin(pos_filter)],
        x="Pos", y="P/GP",
        color="Pos",
        color_discrete_map={"C": "#C8102E", "L": "#001F5B", "R": "#FFD700", "D": "#48BB78"},
        points="all",
        hover_name="Player",
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="white",
        showlegend=False,
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor="#2d3748"),
        height=400,
    )
    st.plotly_chart(fig, use_container_width=True)

    csv = filtered_adv.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download CSV", csv, "advanced_stats.csv", "text/csv")

# ---------------------------------------------------------------------------
# GOALIES PAGE
# ---------------------------------------------------------------------------
elif page == "🧤 Goalies":
    st.markdown('<div class="section-header">🧤 Goalie Rankings</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        min_wins = st.slider("Min Wins", 0, int(goalies["W"].max()), 5)
    with col2:
        team_filter = st.multiselect("Team", sorted(goalies["Team"].unique()), default=[])

    filtered_g = goalies[goalies["W"] >= min_wins]
    if team_filter:
        filtered_g = filtered_g[filtered_g["Team"].isin(team_filter)]
    filtered_g = filtered_g.reset_index(drop=True)
    filtered_g.index += 1

    st.dataframe(
        filtered_g,
        use_container_width=True,
        height=500,
        column_config={
            "SV%": st.column_config.NumberColumn(format="%.3f"),
            "GAA": st.column_config.NumberColumn(format="%.2f"),
        }
    )

    # SV% vs GAA scatter
    st.markdown('<div class="section-header">SV% vs GAA</div>', unsafe_allow_html=True)
    fig = px.scatter(
        goalies[goalies["W"] >= 5],
        x="GAA", y="SV%",
        size="W",
        color="W",
        color_continuous_scale=["#001F5B", "#C8102E"],
        hover_name="Goalie",
        hover_data=["Team", "W", "SO"],
        size_max=20,
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="white",
        xaxis=dict(showgrid=True, gridcolor="#2d3748", autorange="reversed"),
        yaxis=dict(showgrid=True, gridcolor="#2d3748"),
        height=420,
        coloraxis_colorbar=dict(title="Wins", tickfont=dict(color="white")),
    )
    st.plotly_chart(fig, use_container_width=True)

    csv = filtered_g.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download CSV", csv, "goalie_rankings.csv", "text/csv")

# ---------------------------------------------------------------------------
# STANDINGS PAGE
# ---------------------------------------------------------------------------
elif page == "🏒 Standings":
    st.markdown('<div class="section-header">🏒 NHL Standings</div>', unsafe_allow_html=True)

    conf_filter = st.radio("Conference", ["All", "Eastern", "Western"], horizontal=True)
    filtered_s = standings if conf_filter == "All" else standings[standings["Conference"].str.contains(conf_filter, case=False, na=False)]

    # Group by division
    for division in filtered_s["Division"].unique():
        st.markdown(f"**{division}**")
        div_df = filtered_s[filtered_s["Division"] == division][
            ["Team", "Abbrev", "W", "L", "OTL", "PTS", "GP", "GF", "GA", "GDiff", "GF/GP", "GA/GP", "PP%", "PK%", "HW", "HL", "AW", "AL", "L10W", "L10L"]
        ].reset_index(drop=True)
        div_df.index += 1
        st.dataframe(
            div_df,
            use_container_width=True,
            column_config={
                "PP%": st.column_config.NumberColumn(format="%.1f%%"),
                "PK%": st.column_config.NumberColumn(format="%.1f%%"),
                "GF/GP": st.column_config.NumberColumn(format="%.2f"),
                "GA/GP": st.column_config.NumberColumn(format="%.2f"),
            }
        )

    # PP% chart
    st.markdown('<div class="section-header">Power Play % by Team</div>', unsafe_allow_html=True)
    pp_df = filtered_s.nlargest(16, "PP%")
    fig = px.bar(
        pp_df, x="Abbrev", y="PP%",
        color="PP%",
        color_continuous_scale=["#001F5B", "#C8102E"],
        text="PP%",
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="white",
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor="#2d3748"),
        coloraxis_showscale=False,
        height=350,
        margin=dict(t=10),
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", textfont=dict(color="white"))
    st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# PLAYER SEARCH PAGE
# ---------------------------------------------------------------------------
elif page == "🔍 Player Search":
    st.markdown('<div class="section-header">🔍 Player Search</div>', unsafe_allow_html=True)

    search = st.text_input("Search by player name", placeholder="e.g. McDavid, Crosby, MacKinnon...")

    if search:
        results = skaters[skaters["Player"].str.contains(search, case=False, na=False)]

        if results.empty:
            st.warning(f"No players found matching '{search}'")
        else:
            for _, row in results.iterrows():
                with st.expander(f"🏒 {row['Player']}  —  {row['Team']}  |  {row['Pos']}"):
                    c1, c2, c3, c4, c5, c6 = st.columns(6)
                    c1.metric("Points", row["PTS"])
                    c2.metric("Goals", row["G"])
                    c3.metric("Assists", row["A"])
                    c4.metric("+/-", row["+/-"])
                    c5.metric("P/GP", row["P/GP"])
                    c6.metric("Sh%", f"{row['Sh%']}%")

                    c7, c8, c9, c10 = st.columns(4)
                    c7.metric("PP Goals", row["PPG"])
                    c8.metric("SH Goals", row["SHG"])
                    c9.metric("GWG", row["GWG"])
                    c10.metric("Shots", row["SOG"])

                    # Mini bar chart of key stats
                    mini_df = pd.DataFrame({
                        "Stat":  ["Goals", "Assists", "PP Goals", "GW Goals"],
                        "Value": [row["G"], row["A"], row["PPG"], row["GWG"]],
                    })
                    fig = px.bar(
                        mini_df, x="Stat", y="Value",
                        color="Value",
                        color_continuous_scale=["#001F5B", "#C8102E"],
                        text="Value",
                    )
                    fig.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        font_color="white",
                        coloraxis_showscale=False,
                        showlegend=False,
                        height=250,
                        margin=dict(t=10, b=10),
                        xaxis=dict(showgrid=False),
                        yaxis=dict(showgrid=False),
                    )
                    fig.update_traces(textposition="outside", textfont=dict(color="white"))
                    st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("👆 Type a player name above to search")

        # Show all players as a quick reference
        st.markdown('<div class="section-header">All Players</div>', unsafe_allow_html=True)
        st.dataframe(skaters[["Player", "Team", "Pos", "GP", "G", "A", "PTS", "P/GP", "Sh%"]],
                     use_container_width=True, height=500)
