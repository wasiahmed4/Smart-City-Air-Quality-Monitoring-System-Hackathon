-- ============================================================================
-- Gold layer — daily aggregates per city
-- Run this AFTER each etl_silver.py run (manually, or wire it into a
-- Snowflake Task later if you want it automated).
--
-- Idempotent: clears any (city, report_date) rows that are about to be
-- recomputed, then re-inserts fresh aggregates, so re-running this script
-- never creates duplicates.
-- ============================================================================

DELETE FROM SMART_CITY_AQI.ANALYTICS.CITY_DAILY
WHERE (city, report_date) IN (
    SELECT city, TO_DATE(recorded_at)
    FROM SMART_CITY_AQI.CLEAN.AQI_CLEAN
    WHERE city IS NOT NULL
    GROUP BY city, TO_DATE(recorded_at)
);

INSERT INTO SMART_CITY_AQI.ANALYTICS.CITY_DAILY
    (city, report_date, avg_aqi, max_aqi, min_aqi, avg_pm25, avg_co2, dominant_risk, reading_count)
SELECT
    city,
    TO_DATE(recorded_at)   AS report_date,
    AVG(aqi_value)         AS avg_aqi,
    MAX(aqi_value)         AS max_aqi,
    MIN(aqi_value)         AS min_aqi,
    AVG(pm25)              AS avg_pm25,
    AVG(co2_ppm)           AS avg_co2,
    MODE(health_risk)      AS dominant_risk,
    COUNT(*)               AS reading_count
FROM SMART_CITY_AQI.CLEAN.AQI_CLEAN
WHERE city IS NOT NULL
GROUP BY city, TO_DATE(recorded_at);

-- Quick sanity check
SELECT * FROM SMART_CITY_AQI.ANALYTICS.CITY_DAILY ORDER BY report_date DESC, city;
