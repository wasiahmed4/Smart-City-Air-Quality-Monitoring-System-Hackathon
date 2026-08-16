"""
Streamlit dashboard — Smart City Air Quality Monitoring System
Run with: streamlit run dashboard.py
"""

import os
import pandas as pd
import streamlit as st
import snowflake.connector
from dotenv import load_dotenv

# Load environment configurations
load_dotenv()

DATABASE = os.getenv("SF_DATABASE")
CLEAN_SCHEMA = os.getenv("SF_CLEAN_SCHEMA", "CLEAN")
ANALYTICS_SCHEMA = os.getenv("SF_ANALYTICS_SCHEMA", "ANALYTICS")

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SF_USER"),
    "password": os.getenv("SF_PASSWORD"),
    "account": os.getenv("SF_ACCOUNT"),
    "warehouse": os.getenv("SF_WAREHOUSE"),
    "database": DATABASE,
}

# Map health risks to beautiful UI styling badges
SEVERITY_BADGE = {
    "LOW": "🟢 GREEN",
    "MEDIUM": "🟡 YELLOW",
    "HIGH": "🔴 RED",
    "CRITICAL": "🟣 PURPLE",
}

# 1. Page Configuration & Custom BLACK DARK MODE Theme Styling
st.set_page_config(page_title="Smart City AQI Console", layout="wide")

st.markdown("""
    <style>
        /* Force background layout canvas to pitch black */
        .main, .stApp { background-color: #000000 !important; color: #f8fafc !important; }
        
        /* Titles and Headers */
        h1 { color: #ff4d4d !important; font-weight: 800; }
        h2, h3, h4, p, span, label { color: #ffffff !important; }
        small { color: #cbd5e1 !important; }
        
        /* High-contrast Metric Cards overrides */
        div[data-testid="stMetricValue"] { font-size: 2.2rem; font-weight: 800; color: #ff3333 !important; }
        div[data-testid="stMetricLabel"] { font-size: 0.95rem; color: #94a3b8 !important; text-transform: uppercase; letter-spacing: 0.5px; }
        
        /* System Containers styling */
        .stAlert { border-radius: 8px; background-color: #1e1e1e !important; border-left: 5px solid #ff3333; color: #ffffff; }
        hr { border-top: 1px solid #334155 !important; }
    </style>
""", unsafe_allow_html=True)


def get_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


# 2. Optimized Data Connections with 30-Second Auto-Refresh TTL Cache
@st.cache_data(ttl=30)
def load_city_daily():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {DATABASE}.{ANALYTICS_SCHEMA}.CITY_DAILY")
            df = cur.fetch_pandas_all()
        df.columns = df.columns.str.upper()
        return df
    except Exception as e:
        st.error(f"Error loading analytical data: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


@st.cache_data(ttl=30)
def load_recent_readings():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""SELECT * FROM {DATABASE}.{CLEAN_SCHEMA}.AQI_CLEAN
                            WHERE recorded_at >= DATEADD(hour, -6, CURRENT_TIMESTAMP())
                            ORDER BY recorded_at DESC""")
            df = cur.fetch_pandas_all()
        df.columns = df.columns.str.upper()
        return df
    except Exception as e:
        st.error(f"Error loading live readings: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


# Header Banner
st.title("🔻 Smart City Air Quality Monitoring Platform")
st.caption("🚨 Live Telemetry Aggregation: Karachi · Lahore · Islamabad · Peshawar · Multan")
st.divider()

# Load Data Layers
city_daily = load_city_daily()
recent = load_recent_readings()

# --- 3. METRIC CARDS VISUALS ---
col1, col2, col3 = st.columns(3)

with col1:
    if not city_daily.empty and "AVG_AQI" in city_daily.columns and not city_daily["AVG_AQI"].isna().all():
        top_city_row = city_daily.loc[city_daily["AVG_AQI"].idxmax()]
        st.metric(label="Highest AQI City", value=str(top_city_row["CITY"]), delta=f"{float(top_city_row['AVG_AQI']):.0f} Avg AQI", delta_color="inverse")
    else:
        st.metric(label="Highest AQI City", value="—", delta="No Data Available")

with col2:
    st.metric(label="Total Readings Today (Last 6h)", value=f"{len(recent):,}")

with col3:
    if not recent.empty and "HEALTH_RISK" in recent.columns:
        pct_critical = (recent["HEALTH_RISK"].str.upper() == "CRITICAL").mean() * 100
        st.metric(label="% CRITICAL Readings", value=f"{pct_critical:.1f}%", delta="Action Required" if pct_critical > 0 else "Normal Scale")
    else:
        st.metric(label="% CRITICAL Readings", value="0.0%", delta="Normal Scale")

st.divider()

# --- 4. DATA VISUALIZATION CHARTS ---
left_chart, right_chart = st.columns(2)

with left_chart:
    st.subheader("📊 Average AQI per City Today")
    if not city_daily.empty and "CITY" in city_daily.columns:
        chart_data = city_daily.groupby("CITY", as_index=False)["AVG_AQI"].mean()
        
        # CHANGED: Added color="#ff3333" to make the bars bright neon red
        st.bar_chart(
            data=chart_data, 
            x="CITY", 
            y="AVG_AQI", 
            color="#ff3333", 
            width="stretch"
        )
    else:
        st.info("No gold metric aggregates available to visualize.")

with right_chart:
    st.subheader("📈 AQI Trend — Last 6 Hours (IoT Sensors)")
    if not recent.empty and "SENSOR_ID" in recent.columns:
        iot_only = recent[recent["SOURCE"].str.lower() == "iot_simulator"].copy()
        if not iot_only.empty:
            iot_only["RECORDED_AT"] = pd.to_datetime(iot_only["RECORDED_AT"])
            pivot = iot_only.pivot_table(
                index="RECORDED_AT", columns="SENSOR_ID", values="AQI_VALUE", aggfunc="mean"
            )
            st.line_chart(pivot, width="stretch")
        else:
            st.info("No structured IoT telemetry captured in window slice.")
    else:
        st.info("No recent sensor metrics trackable.")

st.divider()

# --- 5. COLOR-CODED DATATABLE ---
st.subheader("📋 Recent Consolidated Telemetry Logs")
if not recent.empty:
    required_cols = ["RECORDED_AT", "SOURCE", "CITY", "SENSOR_ID", "PM25", "AQI_VALUE", "HEALTH_RISK"]
    
    if all(col in recent.columns for col in required_cols):
        display_df = recent[required_cols].copy()
        
        display_df["SEVERITY_BADGE"] = (
            display_df["HEALTH_RISK"].str.upper().map(SEVERITY_BADGE).fillna("⚪ UNKNOWN")
        )
        
        ordered_cols = ["RECORDED_AT", "CITY", "SENSOR_ID", "AQI_VALUE", "SEVERITY_BADGE", "PM25", "SOURCE"]
        display_df = display_df[ordered_cols]
        
        st.dataframe(
            display_df.style.background_gradient(cmap="Reds", subset=["AQI_VALUE"]),
            width="stretch"
        )
    else:
        st.warning("Data schema mismatch. Required visualization columns are missing.")
else:
    st.info("No current logging rows inside data framework.")
