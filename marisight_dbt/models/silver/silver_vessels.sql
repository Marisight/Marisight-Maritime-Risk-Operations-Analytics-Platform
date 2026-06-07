-- models/silver/silver_vessels.sql
-- Grain    : one row per vessel per report date (NAME + REPORT_DATE)
-- Source   : PROJECT_DB.DBO.VESSEL (VesselFinder daily scrape, all VARCHAR)
-- Strategy : MERGE on NAME + REPORT_DATE; incremental watermark on _INGESTED_AT
-- Notes    :
--   - REPORT_DATE parsed from VARCHAR 'YYYY-MM-DD HH24:MI' → TIMESTAMP
--   - Departure/arrival raw strings: 'ATD: Mon DD, HH:MI UTC(...)' / 'ATA:...' / 'ETA:...'
--   - ETA falls forward to base_year+1 for year-end crossovers (future dates only)
--   - ATD/ATA fall back to base_year-1 for year-start crossovers (past dates only)
--   - RAW_ATD / RAW_ARRIVAL retained in CTEs for parsing; not emitted to Silver

{{
    config(
        materialized     = 'incremental',
        unique_key       = ['NAME', 'REPORT_DATE'],
        on_schema_change = 'sync_all_columns'
    )
}}

WITH raw AS (

    SELECT
        *,
        TRY_TO_TIMESTAMP(REPORT_DATE, 'YYYY-MM-DD HH24:MI') AS REPORT_TS
    FROM {{ source('bronze', 'VESSEL') }}
    WHERE NAME IS NOT NULL

    {% if is_incremental() %}
        -- _INGESTED_AT is a precise pipeline timestamp — safer watermark than REPORT_DATE
        -- (REPORT_DATE is minute-truncated VARCHAR; _INGESTED_AT is monotonically increasing)
        AND _INGESTED_AT > (SELECT MAX(_INGESTED_AT) FROM {{ this }})
    {% endif %}
),

--------------------------------------------------------------------
-- 1. BASIC CLEANING
--------------------------------------------------------------------
base AS (

    SELECT
        TRIM(NAME)                                                    AS NAME,
        TRIM(TYPE)                                                    AS TYPE,

        TRY_TO_NUMBER(YEAR_BUILT)                                     AS YEAR_BUILT,
        TRY_TO_NUMBER(GROSS_TONNAGE)                                  AS GROSS_TONNAGE,
        TRY_TO_NUMBER(DEADWEIGHT)                                     AS DEADWEIGHT,
        TRY_TO_DOUBLE("length(m)")                                    AS LENGTH_M,
        TRY_TO_DOUBLE("beam(m)")                                      AS BEAM_M,

        REPORT_TS                                                     AS REPORT_DATE,

        DETAIL_LINK,
        _INGESTED_AT,

        -- Raw strings retained for date parsing downstream; not emitted to Silver
        DEPARTURE_DATE                                                AS RAW_ATD,
        ARRIVAL_DATE                                                  AS RAW_ARRIVAL,

        LAST_PORT_NAME,
        LAST_PORT_COUNTRY,
        DESTINATION_PORT_NAME,
        DESTINATION_PORT_COUNTRY,

        TRY_TO_DOUBLE(NULLIF(TRIM(DESTINATION_PORT_LAT), 'None'))     AS DESTINATION_LAT,
        TRY_TO_DOUBLE(NULLIF(TRIM(DESTINATION_PORT_LON), 'None'))     AS DESTINATION_LON,

        CASE
            WHEN TRIM(REPORTED_STATUS) IN ('None', '-', '', 'N/A')
            THEN NULL
            ELSE TRIM(REPORTED_STATUS)
        END AS REPORTED_STATUS

    FROM raw
),

--------------------------------------------------------------------
-- 2. DATE PARSING  (extract Mon DD fragment per field)
--------------------------------------------------------------------
parsed AS (

    SELECT
        *,

        YEAR(REPORT_DATE) AS base_year,

        -- ATD (Actual Time of Departure) — lives in Bronze DEPARTURE_DATE column
        CASE
            WHEN RAW_ATD LIKE '%ATD:%'
            THEN REGEXP_SUBSTR(RAW_ATD, '[A-Z][a-z]{2}\\s+\\d{1,2}')
            ELSE NULL
        END AS ATD_MD,

        -- ATA (Actual Time of Arrival)
        CASE
            WHEN RAW_ARRIVAL LIKE '%ATA:%'
            THEN REGEXP_SUBSTR(RAW_ARRIVAL, '[A-Z][a-z]{2}\\s+\\d{1,2}')
            ELSE NULL
        END AS ATA_MD,

        -- ETA (Estimated Time of Arrival)
        -- 'ETA: -' produces no regex match → NULL (correct)
        CASE
            WHEN RAW_ARRIVAL LIKE '%ETA:%'
            THEN REGEXP_SUBSTR(RAW_ARRIVAL, '[A-Z][a-z]{2}\\s+\\d{1,2}')
            ELSE NULL
        END AS ETA_MD

    FROM base
),

--------------------------------------------------------------------
-- 3. SAFE DATE RESOLUTION
--    ATD / ATA : past events  → fallback to base_year - 1 (year-start crossover)
--    ETA       : future event → accept same-year only if date >= report date;
--                               else advance to base_year + 1 (year-end crossover)
--------------------------------------------------------------------
dates AS (

    SELECT
        *,

        ----------------------------------------------------------------
        -- Departure date (ATD)
        ----------------------------------------------------------------
        CASE
            WHEN ATD_MD IS NULL THEN NULL
            ELSE COALESCE(
                TRY_TO_DATE(ATD_MD || ' ' || base_year,       'Mon DD YYYY'),
                TRY_TO_DATE(ATD_MD || ' ' || (base_year - 1), 'Mon DD YYYY')
            )
        END AS DEPARTURE_DATE,

        ----------------------------------------------------------------
        -- Actual arrival date (ATA)
        ----------------------------------------------------------------
        CASE
            WHEN ATA_MD IS NULL THEN NULL
            ELSE COALESCE(
                TRY_TO_DATE(ATA_MD || ' ' || base_year,       'Mon DD YYYY'),
                TRY_TO_DATE(ATA_MD || ' ' || (base_year - 1), 'Mon DD YYYY')
            )
        END AS ACTUAL_ARRIVAL_DATE,

        ----------------------------------------------------------------
        -- Estimated arrival date (ETA)
        -- Accept same-year result only when it is not before the report date
        -- (guards against year-end crossover: "Jan 3" scraped on Dec 28 → next year)
        ----------------------------------------------------------------
        CASE
            WHEN ETA_MD IS NULL THEN NULL
            ELSE COALESCE(
                CASE
                    WHEN TRY_TO_DATE(ETA_MD || ' ' || base_year, 'Mon DD YYYY')
                             >= DATEADD(day, -1, REPORT_DATE::DATE)
                    THEN TRY_TO_DATE(ETA_MD || ' ' || base_year, 'Mon DD YYYY')
                END,
                -- year-end crossover: ETA month is in next calendar year
                TRY_TO_DATE(ETA_MD || ' ' || (base_year + 1), 'Mon DD YYYY')
            )
        END AS ESTIMATED_ARRIVAL_DATE

    FROM parsed
),

--------------------------------------------------------------------
-- 4. VALIDATION FLAGS
--------------------------------------------------------------------
validated AS (

    SELECT
        *,

        -- Coordinate validity
        CASE
            WHEN DESTINATION_LAT BETWEEN -90  AND 90
             AND DESTINATION_LON BETWEEN -180 AND 180
            THEN TRUE ELSE FALSE
        END AS IS_VALID_COORDINATES,

        -- ATA before ATD → data quality flag (date-grain; hour-level checks belong in Gold)
        CASE
            WHEN ACTUAL_ARRIVAL_DATE IS NOT NULL
             AND DEPARTURE_DATE      IS NOT NULL
             AND ACTUAL_ARRIVAL_DATE < DEPARTURE_DATE
            THEN TRUE ELSE FALSE
        END AS IS_TEMPORAL_ANOMALY,

        -- No voyage timing at all
        CASE
            WHEN DEPARTURE_DATE         IS NULL
             AND ACTUAL_ARRIVAL_DATE    IS NULL
             AND ESTIMATED_ARRIVAL_DATE IS NULL
            THEN TRUE ELSE FALSE
        END AS IS_MISSING_TIMES

    FROM dates
),

--------------------------------------------------------------------
-- 5. DEDUPLICATION
--------------------------------------------------------------------
deduped AS (

    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY NAME, REPORT_DATE
            ORDER BY _INGESTED_AT DESC NULLS LAST
        ) AS rn
    FROM validated
)

SELECT
    NAME,
    TYPE,
    YEAR_BUILT,
    GROSS_TONNAGE,
    DEADWEIGHT,
    LENGTH_M,
    BEAM_M,

    REPORT_DATE,

    DEPARTURE_DATE,
    ACTUAL_ARRIVAL_DATE,
    ESTIMATED_ARRIVAL_DATE,

    LAST_PORT_NAME,
    LAST_PORT_COUNTRY,
    DESTINATION_PORT_NAME,
    DESTINATION_PORT_COUNTRY,

    DESTINATION_LAT,
    DESTINATION_LON,

    IS_VALID_COORDINATES,
    IS_TEMPORAL_ANOMALY,
    IS_MISSING_TIMES,

    REPORTED_STATUS,
    DETAIL_LINK,
    _INGESTED_AT

FROM deduped
WHERE rn = 1
