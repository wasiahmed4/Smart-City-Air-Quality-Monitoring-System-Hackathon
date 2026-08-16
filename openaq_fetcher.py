"""
OpenAQ V3 Fetcher — Pakistan reference air quality data (Bronze layer)
"""

import os
import time
import requests
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector import errors

load_dotenv()

OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
BASE_URL = "https://api.openaq.org/v3"
HEADERS = {"X-API-Key": OPENAQ_API_KEY} if OPENAQ_API_KEY else {}

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SF_USER"),
    "password": os.getenv("SF_PASSWORD"),
    "account": os.getenv("SF_ACCOUNT"),
    "warehouse": os.getenv("SF_WAREHOUSE"),
    "database": os.getenv("SF_DATABASE"),
    "schema": os.getenv("SF_RAW_SCHEMA", "RAW"),
}

RAW_OPENAQ_TABLE = "OPENAQ_RAW"
RATE_LIMIT_SLEEP = 1.0
VALID_PARAMS = {"pm25", "pm10", "co2"}
REQUIRED_ENV_VARS = ["SF_USER", "SF_PASSWORD", "SF_ACCOUNT", "SF_WAREHOUSE", "SF_DATABASE", "OPENAQ_API_KEY"]


def validate_env():
    missing = [v for v in REQUIRED_ENV_VARS if not os.getenv(v)]
    if missing:
        raise SystemExit(f"\nMissing required .env values: {', '.join(missing)}\n")


def get_pakistan_locations():
    url = f"{BASE_URL}/locations"
    params = {"country_id": "PK", "limit": 100}
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception as e:
        print(f"[API ERROR] locations: {e}")
        return []


def get_sensors_for_location(location_id):
    url = f"{BASE_URL}/locations/{location_id}/sensors"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        sensors = {}
        for s in resp.json().get("results", []):
            param = s.get("parameter", {}) or {}
            sensors[s.get("id")] = {
                "name": str(param.get("name", "")).lower(),
                "units": param.get("units"),
            }
        return sensors
    except Exception as e:
        print(f"[API ERROR] sensors for location {location_id}: {e}")
        return {}


def get_latest_for_location(location_id):
    url = f"{BASE_URL}/locations/{location_id}/latest"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception as e:
        print(f"[API ERROR] latest for location {location_id}: {e}")
        return []


def build_records(locations):
    records = []
    for loc in locations:
        location_id = loc.get("id")
        if location_id is None:
            continue

        station_name = loc.get("name", "Unknown Node")
        city = loc.get("locality") or "Unspecified City"
        country_code = (loc.get("country") or {}).get("code", "PK")
        coords = loc.get("coordinates") or {}
        latitude = coords.get("latitude")
        longitude = coords.get("longitude")

        sensors = get_sensors_for_location(location_id)
        time.sleep(RATE_LIMIT_SLEEP)
        latest = get_latest_for_location(location_id)
        time.sleep(RATE_LIMIT_SLEEP)

        for entry in latest:
            sensor_info = sensors.get(entry.get("sensorsId"))
            if not sensor_info or sensor_info["name"] not in VALID_PARAMS:
                continue

            value = entry.get("value")
            if value is None:
                continue

            recorded_at = (entry.get("datetime") or {}).get("utc")

            records.append((
                int(location_id),
                str(station_name),
                str(city),
                str(country_code),
                float(latitude) if latitude is not None else None,
                float(longitude) if longitude is not None else None,
                str(sensor_info["name"]),
                float(value),
                str(sensor_info["units"]) if sensor_info["units"] else "µg/m³",
                str(recorded_at) if recorded_at else None,
            ))
    return records


def insert_to_snowflake(records):
    if not records:
        print("No records to load.")
        return

    query = f"""
        INSERT INTO {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{RAW_OPENAQ_TABLE}
        (location_id, station_name, city, country_code, latitude, longitude,
         pollutant_type, pollutant_value, unit, recorded_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    conn = None
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        cursor.executemany(query, records)
        conn.commit()
        print(f"Loaded {len(records)} records into {RAW_OPENAQ_TABLE}.")
        cursor.close()
    except errors.Error as db_err:
        print(f"[SNOWFLAKE ERROR] {db_err}")
    finally:
        if conn:
            conn.close()


def main():
    validate_env()
    print("OpenAQ Pakistan Fetcher Starting...")
    locations = get_pakistan_locations()
    print(f"Found {len(locations)} locations.")
    records = build_records(locations)
    insert_to_snowflake(records)


if __name__ == "__main__":
    main()