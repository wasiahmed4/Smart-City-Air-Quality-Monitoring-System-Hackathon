# Smart City Air Quality Monitoring System

End-to-end data pipeline: simulated IoT sensors + real OpenAQ reference data
→ Snowflake Bronze/Silver/Gold → Streamlit dashboard.

## Setup

1. **Snowflake schema** — run `snowflake_schema.sql` once in a Snowflake
   worksheet. Creates database `SMART_CITY_AQI` with schemas `RAW` (Bronze),
   `CLEAN` (Silver), `ANALYTICS` (Gold), and all four tables.

2. **Credentials** — copy `.env.example` to `.env` and fill in your real
   Snowflake credentials and OpenAQ API key (register free at
   explore.openaq.org/register). `.env` is git-ignored — never commit it.

3. **Install dependencies**
   ```
   pip install -r requirements.txt
   ```

## Run order

```
# 1. Start the IoT simulator — leave running 30+ min to build up data
python iot_simulator.py

# 2. Pull real Pakistan air quality data from OpenAQ (run once, or on a schedule)
python openaq_fetcher.py

# 3. Clean + merge both sources into the Silver layer
python etl_silver.py

# 4. Aggregate Silver into Gold daily stats — run in a Snowflake worksheet
#    (or via `snowsql -f gold_layer.sql`)
gold_layer.sql

# 5. Launch the dashboard
streamlit run dashboard.py
```

Re-run steps 2–4 periodically as new data comes in; the dashboard auto-
refreshes every 30 seconds against whatever is currently in Snowflake.

## Design decisions worth knowing about

- **`RAW.IOT_READINGS.severity` is a 4-value field** (GOOD / MODERATE /
  UNHEALTHY / HAZARDOUS), per the Reading Specification table. The full
  6-tier EPA category (`UNHEALTHY FOR SENSITIVE`, `VERY UNHEALTHY`, etc.) is
  computed independently in the Silver ETL as `aqi_category` — the two are
  not meant to match 1:1.
- **Time-of-day multiplier** is implemented literally as
  `1.0 + 0.3 * sin((hour - 8) * pi / 12)` per the spec formula, with no
  `abs()`. This does dip below 1.0 during low-traffic hours rather than only
  ever boosting readings.
- **OpenAQ AQI/health_risk is only computed for `pm25` rows.** The EPA
  breakpoint table is PM2.5-specific; applying it to a `pm10` reading would
  be scientifically wrong, so `pm10` rows land in `CLEAN.AQI_CLEAN` with
  `pm10` populated but `aqi_category`/`health_risk` left `NULL`.
- **`pm10` OpenAQ readings do not get a fabricated AQI value.** Earlier
  drafts of this pipeline copied the raw µg/m³ concentration into an `AQI`
  column, which isn't a real AQI. That's fixed — OpenAQ `pm25` rows get a
  real EPA-formula AQI, directly comparable to IoT sensor AQI values.

## Known limitations / next steps

- `gold_layer.sql` is run manually — wire it into a Snowflake Task (e.g.
  `CREATE TASK ... SCHEDULE = '10 MINUTE'`) if you want it automated for the
  demo.
- OpenAQ coverage in Pakistan is mainly Karachi and Lahore — Islamabad,
  Peshawar, and Multan may show sparse or no OpenAQ reference data. This is
  an OpenAQ data-availability limitation, not a bug in the fetcher.
- No retry/backoff on OpenAQ API calls beyond the flat 1-second rate-limit
  sleep — fine for a hackathon demo volume, not production-grade.

## Security

- Rotate your Snowflake password and OpenAQ API key if they were ever
  pasted into a chat, doc, or committed to git.
- `.env` is git-ignored. Double check with `git status` before your first
  commit that it isn't showing up as staged.
