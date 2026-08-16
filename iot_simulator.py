"""
IoT Sensor Simulator — Smart City Air Quality Monitoring System

Generates one reading per sensor every 10 seconds and writes it to:
  1) a local CSV backup (iot_readings.csv)
  2) Snowflake RAW.IOT_READINGS (Bronze layer)
"""

import os
import csv
import math
import time
import random
from datetime import datetime, timezone
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector import errors

load_dotenv()

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SF_USER"),
    "password": os.getenv("SF_PASSWORD"),
    "account": os.getenv("SF_ACCOUNT"),
    "warehouse": os.getenv("SF_WAREHOUSE"),
    "database": os.getenv("SF_DATABASE"),
    "schema": os.getenv("SF_RAW_SCHEMA", "RAW"),
}

RAW_IOT_TABLE = "IOT_READINGS"
LOCAL_CSV_BACKUP = "iot_readings.csv"
REQUIRED_ENV_VARS = ["SF_USER", "SF_PASSWORD", "SF_ACCOUNT", "SF_WAREHOUSE", "SF_DATABASE"]


def validate_env():
    missing = [v for v in REQUIRED_ENV_VARS if not os.getenv(v)]
    if missing:
        raise SystemExit(
            f"\nMissing required .env values: {', '.join(missing)}\n"
        )


SENSORS = [
    {"id": "PKS_KHI_IND_01", "city": "Karachi", "zone": "industrial"},
    {"id": "PKS_KHI_TRF_02", "city": "Karachi", "zone": "traffic"},
    {"id": "PKS_LHR_RES_01", "city": "Lahore", "zone": "residential"},
    {"id": "PKS_LHR_IND_02", "city": "Lahore", "zone": "industrial"},
    {"id": "PKS_ISB_PRK_01", "city": "Islamabad", "zone": "park"},
    {"id": "PKS_ISB_TRF_02", "city": "Islamabad", "zone": "traffic"},
    {"id": "PKS_PEW_IND_01", "city": "Peshawar", "zone": "industrial"},
    {"id": "PKS_PEW_RES_02", "city": "Peshawar", "zone": "residential"},
    {"id": "PKS_MUL_TRF_01", "city": "Multan", "zone": "traffic"},
    {"id": "PKS_MUL_PRK_02", "city": "Multan", "zone": "park"},
]

ZONE_RANGES = {
    "industrial": {"pm25": (80.0, 120.0), "co2": (600.0, 900.0), "temp": (30.0, 42.0), "humidity": (10.0, 45.0), "wind": (5.0, 25.0)},
    "traffic": {"pm25": (55.0, 80.0), "co2": (500.0, 700.0), "temp": (28.0, 40.0), "humidity": (20.0, 55.0), "wind": (8.0, 30.0)},
    "residential": {"pm25": (25.0, 50.0), "co2": (420.0, 500.0), "temp": (25.0, 38.0), "humidity": (30.0, 70.0), "wind": (4.0, 18.0)},
    "park": {"pm25": (8.0, 20.0), "co2": (400.0, 430.0), "temp": (22.0, 35.0), "humidity": (40.0, 90.0), "wind": (3.0, 15.0)},
}

EPA_BREAKPOINTS = [
    (0.0, 12.0, 0, 50, "GOOD"),
    (12.1, 35.4, 51, 100, "MODERATE"),
    (35.5, 55.4, 101, 150, "UNHEALTHY FOR SENSITIVE"),
    (55.5, 150.4, 151, 200, "UNHEALTHY"),
    (150.5, 250.4, 201, 300, "VERY UNHEALTHY"),
    (250.5, 500.4, 301, 500, "HAZARDOUS"),
]

SEVERITY_COLLAPSE = {
    "GOOD": "GOOD",
    "MODERATE": "MODERATE",
    "UNHEALTHY FOR SENSITIVE": "UNHEALTHY",
    "UNHEALTHY": "UNHEALTHY",
    "VERY UNHEALTHY": "HAZARDOUS",
    "HAZARDOUS": "HAZARDOUS",
}


def time_of_day_multiplier():
    now = datetime.now()
    decimal_hour = now.hour + now.minute / 60.0
    return 1.0 + 0.3 * math.sin((decimal_hour - 8) * math.pi / 12)


def generate_sensor_data(zone_type):
    ranges = ZONE_RANGES[zone_type]
    base = {
        "pm25": random.uniform(*ranges["pm25"]),
        "co2": random.uniform(*ranges["co2"]),
        "temp": random.uniform(*ranges["temp"]),
        "humidity": random.uniform(*ranges["humidity"]),
        "wind": random.uniform(*ranges["wind"]),
    }
    tod = time_of_day_multiplier()
    noisy = {k: v * tod * random.uniform(0.85, 1.15) for k, v in base.items()}

    if random.random() < 0.15:
        noisy["pm25"] *= random.uniform(2.5, 4.0)
        noisy["co2"] *= random.uniform(1.2, 1.5)

    pm25 = min(500.0, max(0.0, noisy["pm25"]))
    pm10 = min(600.0, max(pm25, pm25 * random.uniform(1.2, 1.6)))
    co2 = min(2000.0, max(400.0, noisy["co2"]))
    temp = min(45.0, max(15.0, noisy["temp"]))
    humidity = min(90.0, max(10.0, noisy["humidity"]))
    wind = min(60.0, max(0.0, noisy["wind"]))

    return {
        "pm25": round(pm25, 2),
        "pm10": round(pm10, 2),
        "co2": round(co2, 2),
        "temp": round(temp, 2),
        "humidity": round(humidity, 2),
        "wind": round(wind, 2),
    }


def compute_aqi(pm25):
    val = max(0.0, min(500.4, pm25))
    for c_lo, c_hi, i_lo, i_hi, label in EPA_BREAKPOINTS:
        if c_lo <= val <= c_hi:
            aqi = ((i_hi - i_lo) / (c_hi - c_lo)) * (val - c_lo) + i_lo
            return round(aqi, 2), SEVERITY_COLLAPSE[label]
    return 500.0, "HAZARDOUS"


def write_local_csv(record):
    try:
        file_exists = os.path.exists(LOCAL_CSV_BACKUP)
        with open(LOCAL_CSV_BACKUP, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=record.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(record)
    except Exception as e:
        print(f"[LOCAL BACKUP FAIL] {e}")


def insert_batch_to_snowflake(conn, records):
    query = f"""
        INSERT INTO {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{RAW_IOT_TABLE}
        (sensor_id, city, zone_type, pm25, pm10, co2_ppm, temperature_c,
         humidity_pct, wind_speed_kmh, aqi_value, severity, recorded_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    rows = [
        (
            str(r["sensor_id"]), str(r["city"]), str(r["zone_type"]), float(r["pm25"]), float(r["pm10"]),
            float(r["co2_ppm"]), float(r["temperature_c"]), float(r["humidity_pct"]),
            float(r["wind_speed_kmh"]), float(r["aqi_value"]), str(r["severity"]), str(r["recorded_at"]),
        )
        for r in records
    ]
    cursor = conn.cursor()
    try:
        cursor.executemany(query, rows)
        conn.commit()
    except errors.Error as db_err:
        print(f"[SNOWFLAKE INSERT FAIL] {db_err}")
    finally:
        cursor.close()


def main():
    validate_env()
    print("IoT Smart City Streaming Simulator — Running...")
    
    while True:
        conn = None
        try:
            conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            while True:
                utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                batch = []
                for sensor in SENSORS:
                    metrics = generate_sensor_data(sensor["zone"])
                    aqi_value, severity = compute_aqi(metrics["pm25"])
                    record = {
                        "sensor_id": sensor["id"],
                        "city": sensor["city"],
                        "zone_type": sensor["zone"],
                        "pm25": metrics["pm25"],
                        "pm10": metrics["pm10"],
                        "co2_ppm": metrics["co2"],
                        "temperature_c": metrics["temp"],
                        "humidity_pct": metrics["humidity"],
                        "wind_speed_kmh": metrics["wind"],
                        "aqi_value": aqi_value,
                        "severity": severity,
                        "recorded_at": utc_now,
                    }
                    write_local_csv(record)
                    batch.append(record)

                insert_batch_to_snowflake(conn, batch)
                print(f"[{utc_now}] Loaded {len(batch)} readings. Sleeping 10s...")
                time.sleep(10)
        except KeyboardInterrupt:
            print("\nStopped by user.")
            break
        except Exception as err:
            print(f"[RECOVERY CONNECTION ERROR] {err}. Retrying in 10s...")
            time.sleep(10)
        finally:
            if conn:
                conn.close()


if __name__ == "__main__":
    main()
