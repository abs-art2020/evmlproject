import streamlit as st
import duckdb
import pandas as pd
import os

# -----------------------------------------------------------------------------
# 1. UI ENGINE INITIALIZATION & GLOBAL THEME DESIGN
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="EV Market Intelligence Console",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom Enterprise CSS targeting strict visual hierarchy and metric rendering
st.markdown("""
    <style>
    /* Hero Container Blocks */
    .hero-container {
        padding: 28px;
        border-radius: 12px;
        text-align: center;
        margin-bottom: 25px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }
    .high-adoption-bg {
        background: linear-gradient(135deg, #133a26 0%, #091e14 100%);
        border: 2px solid #00e676;
    }
    .normal-growth-bg {
        background: linear-gradient(135deg, #232730 0%, #17191f 100%);
        border: 2px solid #546e7a;
    }
    .hero-title-text {
        font-size: 30px !important;
        font-weight: 800 !important;
        color: #ffffff !important;
        margin: 0px 0px 8px 0px !important;
        letter-spacing: 0.5px;
    }
    .hero-subtitle-text {
        font-size: 15px !important;
        color: #b0bec5 !important;
        max-width: 800px;
        margin: 0 auto !important;
        line-height: 1.4;
    }

    /* Post-Inference Metrics Matrix Grid */
    .matrix-card {
        background-color: #161922;
        border: 1px solid #2a2f3d;
        padding: 22px;
        border-radius: 8px;
        text-align: center;
        min-height: 140px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
    }
    .matrix-value {
        font-size: 34px;
        font-weight: 700;
        margin-bottom: 4px;
        line-height: 1.1;
    }
    .matrix-label {
        font-size: 12px;
        color: #78909c;
        text-transform: uppercase;
        letter-spacing: 0.8px;
    }

    /* Infrastructure Custom Action Alert Badges */
    .badge-gap-alert {
        background-color: #4a1515;
        border: 1px solid #ff5252;
        border-radius: 4px;
        padding: 10px;
        color: #ff8a80;
        font-weight: 700;
        font-size: 13px;
        text-align: center;
        letter-spacing: 0.5px;
    }
    .badge-stable-alert {
        background-color: #0d2d1e;
        border: 1px solid #00e676;
        border-radius: 4px;
        padding: 10px;
        color: #b9f6ca;
        font-weight: 700;
        font-size: 13px;
        text-align: center;
        letter-spacing: 0.5px;
    }
    </style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_ui_db_connection():
    """Establishes read-only connection to your isolated frontend DuckDB file."""
    IS_DOCKER = os.path.exists('/.dockerenv') or os.environ.get('AIRFLOW_HOME') is not None
    # Since app.py is directly inside the root workspace folder, resolve paths cleanly from here
    if IS_DOCKER:
    # Inside Docker, everything sits right inside the opt path
      base_dir = "/opt/airflow"
    else:
    # On Windows, your code uses the parent file layout structure
      base_dir = os.path.dirname(os.path.abspath(__file__))
    #base_dir = os.path.abspath(os.path.dirname(__file__))
    db_path = os.path.join(base_dir, "data", "3_gold", "market_predictions.duckdb")
    
    # Check if the folder architecture or the database file does not exist yet
    if not os.path.exists(db_path):
        st.warning(f"⚠️ **Serving Layer Cold-Start**: Database not detected at `{db_path}`. Run your pipeline code once to seed tables.")
        st.stop()



        
    return duckdb.connect(db_path, read_only=True)

db = get_ui_db_connection()


@st.cache_data
def calculate_system_baseline():
    """Pre-aggregates dataset baseline statistics for real-time alerting limits."""
    try:
        res = db.execute("SELECT AVG(chargers) FROM market_predictions_table").fetchone()
        return float(res[0]) if res and res[0] is not None else 0.0
    except Exception:
        return 0.0

SYSTEM_AVG_CHARGERS = calculate_system_baseline()

st.title("⚡ 2026 EV Market Intelligence Console")
st.caption("F1-calibrated machine learning classification running native server-side spatial filtering.")

# Render contextual filter selection row
with st.container():
    col_state, col_district = st.columns(2)
    
    with col_state:
        try:
            raw_states = db.execute("SELECT DISTINCT upper(state) FROM market_predictions_table ORDER BY state").fetchall()
            state_options = [row[0] for row in raw_states]
        except Exception:
            st.error("❌ Schema Exception: `market_predictions_table` was not found inside the database binary file.")
            st.stop()
            
        selected_state = st.selectbox("🌐 Region / State Target", options=state_options)
        
    with col_district:
        # Cascading Filter: Fetch only the districts belonging to the selected state
        raw_districts = db.execute(
            "SELECT DISTINCT upper(district) FROM market_predictions_table WHERE state = ? ORDER BY district",
            [selected_state.lower()]
        ).fetchall()
        district_options = [row[0] for row in raw_districts]
        
        selected_district = st.selectbox(
            "📍 Granular District Segment", 
            options=district_options,
            format_func=lambda x: str(x).title() # Capitalize tokens for clean UI aesthetics
        )

query_extraction = """
    SELECT 
        a."Predicted_EV_Share_%",
        a.Predicted_High_Adoption_Calibrated,
        a.total_operational_charging_points,
        a.charger_to_market_ratio
    FROM market_predictions_table a
    WHERE a.state = ? AND a.district = ?
    LIMIT 1
"""
target_vector = db.execute(query_extraction, [selected_state.lower(), selected_district.lower()]).fetchone()

if not target_vector:
    st.error("❌ Spatial Vector Error: Selected region combination not initialized in serving layer.")
    st.stop()

# Unpack variables cleanly from the atomic tuple result
pred_pct, is_high_zone, district_chargers,ratioval = target_vector


query_state_avg = """
SELECT AVG(charger_to_market_ratio)
FROM market_predictions_table
WHERE state = ?
"""
query_state_sum = """
SELECT sum(total_operational_charging_points)
FROM market_predictions_table
WHERE state = ?
"""


state_avg = db.execute(
    query_state_avg,
    [selected_state.lower()]
).fetchone()[0]

state_charging_points_sum = db.execute(
    query_state_sum,
    [selected_state.lower()]
).fetchone()[0]


st.markdown("<br>", unsafe_allow_html=True)

if int(is_high_zone) == 1:
    st.markdown(f"""
        <div class="hero-container high-adoption-bg">
            <div class="hero-title-text">🟢 HIGH ADOPTION ZONE</div>
            <div class="hero-subtitle-text">
                <b>Calibrated Trajectory Match:</b> The model's conservative tracking prediction has safely met or exceeded the empirical 
                F1-optimized decision boundary of <b>4.46%</b>. This region is confirmed on target to hit the 10.0% real-world business target for 2026.
            </div>
        </div>
    """, unsafe_allow_html=True)
else:
    st.markdown(f"""
        <div class="hero-container normal-growth-bg">
            <div class="hero-title-text">🟡 NORMAL GROWTH ZONE</div>
            <div class="hero-subtitle-text">
                <b>Steady Trajectory Window:</b> Growth pace indicators are tracking linearly. Deployment curves suggest standard baseline market scaling 
                requirements apply. Aggressive infrastructure expansion thresholds have not been triggered.
            </div>
        </div>
    """, unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# ZONE 3: POST-INFERENCE INFRASTRUCTURE MATRIX (CARD GRID OVERVIEW)
# -----------------------------------------------------------------------------
st.markdown("<h4 style='color:#cfd8dc; font-weight:600;'>📋 Regional Infrastructure Indicators</h4>", unsafe_allow_html=True)
col_card1, col_card2,col_card3 = st.columns(3)

# 1. Model Prediction Velocity Card
with col_card1:
    st.markdown(f"""
        <div class="matrix-card">
            <div class="matrix-value" style="color:#00e676;">{pred_pct:.2f}%</div>
            <div class="matrix-label">Predicted 2026 EV Share</div>
        </div>
    """, unsafe_allow_html=True)

# 2. Local Infrastructure Asset Counter Card
with col_card2:
    st.markdown(f"""
        <div class="matrix-card">
            <div class="matrix-value" style="color:#ffffff;">{district_chargers:,}</div>
            <div class="matrix-label">Active Charger Points/Assets district level</div>
        </div>
    """, unsafe_allow_html=True)
with col_card3:
    st.markdown(f"""
        <div class="matrix-card">
            <div class="matrix-value" style="color:#ffffff;">{state_charging_points_sum:,}</div>
            <div class="matrix-label">Active Points/Assets Assets state level</div>
        </div>
    """, unsafe_allow_html=True)    


# -----------------------------------------------------------------------------
# ZONE 3: POST-INFERENCE INFRASTRUCTURE MATRIX (CARD GRID OVERVIEW)
# -----------------------------------------------------------------------------

if float(ratioval) == 0 and float(state_avg) == 0:
    needs_more = None      # comparison not possible
else:
    needs_more = ratioval < state_avg
st.markdown("<h4 style='color:#cfd8dc; font-weight:600;'>📋 Chargers analysis </h4>", unsafe_allow_html=True)
col_card3= st.columns(1)[0]

# 1. Model Prediction Velocity Card
with col_card3:
    if needs_more is None:
        color = "#9e9e9e"
        title = "Insufficient data to evaluate charger requirement"
        value = "N/A" 
    elif int(needs_more) and int(is_high_zone):
        color = "#ff5252"
        title = "Needs More Chargers and shows high adoption"
        value = "⚠️ Below State Average"

    elif int(needs_more) and not(int(is_high_zone)):
        color = "#cfe600"
        title = "This district definitely needs more chargers but its not a high adoption zone yet"
        value = "⚠️ Below State Average"

    elif not(int(needs_more)) and int(is_high_zone):
        color = "#00e676"
        title = "Has sufficient chargers and also has high adoption rate"
        value = "✅ Sufficient" 
    elif not(int(needs_more)) and not(int(is_high_zone)):
        color = "#0092e6"
        title = "Has sufficient chargers but is not a high adoption zone yet"
        value = "✅ Sufficient"
    else :
        color = "#00e676"
        title = "Default section"
        value = "✅ Sufficient"         

    st.markdown(f"""
    <div class="matrix-card">
        <div class="matrix-value" style="color:{color};">
            {value}
        </div>
        <div class="matrix-label">{title}</div>
    </div>
    """, unsafe_allow_html=True)

