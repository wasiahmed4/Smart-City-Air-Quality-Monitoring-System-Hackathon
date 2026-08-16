"""
Silver ETL — cleans, validates, and merges IoT + OpenAQ Bronze data into CLEAN.AQI_CLEAN.
"""

import os
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector import errors
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

DATABASE = os.getenv("SF_DATABASE")
RAW_SCHEMA = os.getenv("SF_RAW_SCHEMA", "RAW")
CLEAN_SCHEMA = os.getenv("SF_CLEAN_SCHEMA", "CLEAN")

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SF_USER"),
    "password": os.getenv("SF_PASSWORD"),
    "account": os.getenv("SF_ACCOUNT"),
    "warehouse": os.getenv("SF_WAREHOUSE"),
    "database": DATABASE,
    "schema": RAW_SCHEMA,
}

EPA_BREAKPOINTS = [
    (0.0, 12.0, 0, 50, "GOOD"),
    (12.1, 35.4, 51, 100, "MODERATE"),
    (35.5, 55.4, 101, 150, "UNHEALTHY FOR SENSITIVE"),
    (55.5, 150.4, 151, 200, "UNHEALTHY"),
    (150.5, 250.4, 201, 300, "VERY UNHEALTHY"),
    (250.5, 500.4, 301, 500, "HAZARDOUS"),
]

HEALTH_RISK_MAP = {
    "GOOD": "LOW",
    "MODERATE": "LOW",
    "UNHEALTHY FOR SENSITIVE": "MEDIUM",
    "UNHEALTHY": "HIGH",
    "VERY UNHEALTHY": "HIGH",
    "HAZARDOUS": "CRITICAL",
}


def compute_epa_category(pm25):
    try:
        val = float(pm25)
    except (TypeError, ValueError):
        return None
    if val < 0 or pd.isna(val):
        return None
    for c_lo, c_hi, _, _, label in EPA_BREAKPOINTS:
        if c_lo <= val <= c_hi:
            return label
    return "HAZARDOUS"


def compute_aqi_value(pm25):
    try:
        val = float(pm25)
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        return None
    val = max(0.0, min(500.4, val))
    for c_lo, c_hi, i_lo, i_hi, _ in EPA_BREAKPOINTS:
        if c_lo <= val <= c_hi:
            return round(((i_hi - i_lo) / (c_hi - c_lo)) * (val - c_lo) + i_lo, 2)
    return 500.0


def map_health_risk(category):
    if not category:
        return None
    return HEALTH_RISK_MAP.get(str(category).upper().strip())


def transform_iot(df):
    if df.empty:
        return df
    df = df.dropna(subset=["PM25", "AQI_VALUE"])
    df = df[
        (df["PM25"] >= 0.0) & (df["PM25"] <= 500.0) &
        (df["CO2_PPM"] >= 400.0) & (df["CO2_PPM"] <= 2000.0) &
        (df["HUMIDITY_PCT"] >= 0.0) & (df["HUMIDITY_PCT"] <= 100.0)
    ]
    df = df.drop_duplicates(subset=["SENSOR_ID", "RECORDED_AT"])
    df["AQI_CATEGORY"] = df["PM25"].apply(compute_epa_category)
    df["HEALTH_RISK"] = df["AQI_CATEGORY"].apply(map_health_risk)
    df["SOURCE"] = "iot_simulator"
    df["PM10"] = df["PM10"].astype(float)
    df["LATITUDE"] = None
    df["LONGITUDE"] = None
    return df


def transform_openaq(df):
    if df.empty:
        return df
    df["POLLUTANT_TYPE"] = df["POLLUTANT_TYPE"].str.lower()
    df = df[df["POLLUTANT_TYPE"].isin(["pm25", "pm10"])]
    df = df[df["POLLUTANT_VALUE"] > 0.0]

    df["RECORDED_AT"] = pd.to_datetime(df["RECORDED_AT"], errors="coerce")
    df = df.dropna(subset=["RECORDED_AT"])
    df["RECORDED_AT"] = df["RECORDED_AT"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    df["SOURCE"] = "openaq_v3"
    is_pm25 = df["POLLUTANT_TYPE"] == "pm25"
    df["PM25"] = df["POLLUTANT_VALUE"].where(is_pm25).astype(float)
    df["PM10"] = df["POLLUTANT_VALUE"].where(~is_pm25).astype(float)
    df["CO2_PPM"] = None
    df["SENSOR_ID"] = None

    df["AQI_VALUE"] = df["PM25"].apply(compute_aqi_value)
    df["AQI_CATEGORY"] = df["PM25"].apply(compute_epa_category)
    df["HEALTH_RISK"] = df["AQI_CATEGORY"].apply(map_health_risk)
    return df


def clean_and_load_etl():
    print("Silver consolidation pipeline starting...")
    processed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    except errors.DatabaseError as db_err:
        print(f"[CONNECTION FAILED] {db_err}")
        return

    try:
        df_iot = pd.DataFrame()
        df_openaq = pd.DataFrame()
        
        try:
            df_iot = pd.read_sql(f"SELECT * FROM {DATABASE}.{RAW_SCHEMA}.IOT_READINGS", conn)
            df_iot.columns = df_iot.columns.str.upper()
        except Exception as e:
            print(f"[IoT extraction skipped] {e}")

        try:
            df_openaq = pd.read_sql(f"SELECT * FROM {DATABASE}.{RAW_SCHEMA}.OPENAQ_RAW", conn)
            df_openaq.columns = df_openaq.columns.str.upper()
        except Exception as e:
            print(f"[OpenAQ extraction skipped] {e}")

        df_iot = transform_iot(df_iot)
        df_openaq = transform_openaq(df_openaq)

        columns_to_keep = [
            "SOURCE", "CITY", "SENSOR_ID", "PM25", "PM10", "CO2_PPM",
            "AQI_VALUE", "AQI_CATEGORY", "HEALTH_RISK", "LATITUDE",
            "LONGITUDE", "RECORDED_AT",
        ]

        valid_dfs = [df for df in [df_iot, df_openaq] if not df.empty]
        if not valid_dfs:
            print("No data parsed to process.")
            return

        combined = pd.concat(valid_dfs, ignore_index=True)
        for col in columns_to_keep:
            if col not in combined.columns:
                combined[col] = None
                
        final_df = combined[columns_to_keep].copy()
        final_df["PROCESSED_AT"] = processed_at

        # Clear old rows to prevent scaling constraint duplications
        try:
            cursor = conn.cursor()
            cursor.execute(f"TRUNCATE TABLE {DATABASE}.{CLEAN_SCHEMA}.AQI_CLEAN")
            cursor.close()
        except Exception as e:
            print(f"[Truncate Info/Warning] {e}")

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=final_df,
            table_name="AQI_CLEAN",
            database=DATABASE,
            schema=CLEAN_SCHEMA,
        )
        if success:
            print(f"Success — {nrows} rows loaded.")
    except Exception as e:
        print(f"[PIPELINE ERROR] {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    clean_and_load_etl()