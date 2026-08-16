-- ============================================================================
-- Smart City Air Quality Monitoring System
-- Snowflake DDL — Medallion Architecture (Bronze / Silver / Gold)
-- Run this ONCE in a Snowflake worksheet before running any Python scripts.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- DATABASE & SCHEMAS
-- ----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS SMART_CITY_AQI;

USE DATABASE SMART_CITY_AQI;

CREATE SCHEMA IF NOT EXISTS RAW;         -- Bronze
CREATE SCHEMA IF NOT EXISTS CLEAN;       -- Silver
CREATE SCHEMA IF NOT EXISTS ANALYTICS;   -- Gold

-- ============================================================================
-- BRONZE — RAW DATA AS-IS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- RAW.IOT_READINGS — one row per simulated sensor reading
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW.IOT_READINGS (
    reading_id      NUMBER AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    sensor_id       VARCHAR(30)     NOT NULL,
    city            VARCHAR(100)    NOT NULL,
    zone_type       VARCHAR(30)     NOT NULL,
    pm25            FLOAT,
    pm10            FLOAT,
    co2_ppm         FLOAT,
    temperature_c   FLOAT,
    humidity_pct    FLOAT,
    wind_speed_kmh  FLOAT,
    aqi_value       FLOAT,
    severity        VARCHAR(30),
    recorded_at     TIMESTAMP_NTZ   NOT NULL,
    ingested_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------------
-- RAW.OPENAQ_RAW — one row per (location, pollutant) latest measurement
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW.OPENAQ_RAW (
    raw_id          NUMBER AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    location_id     INTEGER         NOT NULL,
    station_name    VARCHAR(200),
    city            VARCHAR(100),
    country_code    VARCHAR(5),
    latitude        FLOAT,
    longitude       FLOAT,
    pollutant_type  VARCHAR(20),
    pollutant_value FLOAT,
    unit            VARCHAR(20),
    recorded_at     TIMESTAMP_NTZ,
    ingested_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- SILVER — VALIDATED & ENRICHED (both sources joined into one shape)
-- ============================================================================

CREATE TABLE IF NOT EXISTS CLEAN.AQI_CLEAN (
    clean_id        NUMBER AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    source          VARCHAR(20)     NOT NULL,   -- 'iot_simulator' OR 'openaq_v3'
    city            VARCHAR(100),
    sensor_id       VARCHAR(30),                -- NULL for OpenAQ rows
    pm25            FLOAT,
    pm10            FLOAT,
    co2_ppm         FLOAT,
    aqi_value       FLOAT,
    aqi_category    VARCHAR(40),                -- Good / Moderate / Unhealthy etc.
    health_risk     VARCHAR(10),                -- LOW / MEDIUM / HIGH / CRITICAL
    latitude        FLOAT,
    longitude       FLOAT,
    recorded_at     TIMESTAMP_NTZ,
    processed_at    TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- GOLD — DAILY AGGREGATES PER CITY
-- ============================================================================

CREATE TABLE IF NOT EXISTS ANALYTICS.CITY_DAILY (
    daily_id        NUMBER AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    city            VARCHAR(100)    NOT NULL,
    report_date     DATE            NOT NULL,
    avg_aqi         FLOAT,
    max_aqi         FLOAT,
    min_aqi         FLOAT,
    avg_pm25        FLOAT,
    avg_co2         FLOAT,
    dominant_risk   VARCHAR(10),                -- most common health_risk of the day
    reading_count   NUMBER,                     -- total rows from both sources
    created_at      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- Sanity check — confirm all 4 tables exist
-- ============================================================================
SHOW TABLES IN SCHEMA RAW;
SHOW TABLES IN SCHEMA CLEAN;
SHOW TABLES IN SCHEMA ANALYTICS;
