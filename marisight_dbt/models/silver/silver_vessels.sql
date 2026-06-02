-- models/silver/silver_vessels.sql
-- Production-grade maritime vessel silver layer
-- Grain: one row per vessel per report_date

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
        AND TRY_TO_TIMESTAMP(REPORT_DATE, 'YYYY-MM-DD HH24:MI')
            > (SELECT MAX(REPORT_DATE) FROM {{ this }})
    {% endif %}
),

--------------------------------------------------------------------
-- 1. BASIC CLEANING
--------------------------------------------------------------------
base AS (

    SELECT
        TRIM(NAME) AS NAME,
        TRIM(TYPE) AS TYPE,

        TRY_TO_NUMBER(YEAR_BUILT)    AS YEAR_BUILT,
        TRY_TO_NUMBER(GROSS_TONNAGE) AS GROSS_TONNAGE,
        TRY_TO_NUMBER(DEADWEIGHT)    AS DEADWEIGHT,
        TRY_TO_DOUBLE("length(m)")   AS LENGTH_M,
        TRY_TO_DOUBLE("beam(m)")     AS BEAM_M,

        REPORT_TS AS REPORT_DATE,

        DETAIL_LINK,
        _INGESTED_AT,

        -- raw fields preserved for debugging
        DEPARTURE_DATE AS RAW_DEPARTURE,
        ARRIVAL_DATE   AS RAW_ARRIVAL,

        LAST_PORT_NAME,
        LAST_PORT_COUNTRY,
        DESTINATION_PORT_NAME,
        DESTINATION_PORT_COUNTRY,

        TRY_TO_DOUBLE(NULLIF(TRIM(DESTINATION_PORT_LAT), 'None')) AS DESTINATION_LAT,
        TRY_TO_DOUBLE(NULLIF(TRIM(DESTINATION_PORT_LON), 'None')) AS DESTINATION_LON,

        CASE
            WHEN TRIM(REPORTED_STATUS) IN ('None', '-', '', 'N/A')
            THEN NULL
            ELSE TRIM(REPORTED_STATUS)
        END AS REPORTED_STATUS

    FROM raw
),

--------------------------------------------------------------------
-- 2. DATE PARSING STRATEGY (ROBUST)
--------------------------------------------------------------------
parsed AS (

    SELECT
        *,

        ----------------------------------------------------------------
        -- Extract month-day only (maritime feeds usually omit year)
        ----------------------------------------------------------------
        REGEXP_SUBSTR(RAW_DEPARTURE, '[A-Z][a-z]{2}\\s+\\d{1,2}') AS DEP_MD,
        REGEXP_SUBSTR(RAW_ARRIVAL,   '[A-Z][a-z]{2}\\s+\\d{1,2}') AS ARR_MD,

        YEAR(REPORT_DATE) AS base_year

    FROM base
),

--------------------------------------------------------------------
-- 3. SAFE DATE RESOLUTION (NO HARD GUESSING)
--------------------------------------------------------------------
dates AS (

    SELECT
        *,

        ----------------------------------------------------------------
        -- Departure date resolution
        ----------------------------------------------------------------
        CASE
            WHEN DEP_MD IS NULL THEN NULL
            ELSE
                COALESCE(
                    -- try same year
                    TRY_TO_DATE(DEP_MD || ' ' || base_year, 'Mon DD YYYY'),

                    -- fallback previous year only if needed
                    TRY_TO_DATE(DEP_MD || ' ' || (base_year - 1), 'Mon DD YYYY')
                )
        END AS DEPARTURE_DATE,

        ----------------------------------------------------------------
        -- Arrival date resolution
        ----------------------------------------------------------------
        CASE
            WHEN ARR_MD IS NULL THEN NULL
            ELSE
                COALESCE(
                    TRY_TO_DATE(ARR_MD || ' ' || base_year, 'Mon DD YYYY'),
                    TRY_TO_DATE(ARR_MD || ' ' || (base_year - 1), 'Mon DD YYYY')
                )
        END AS ARRIVAL_DATE

    FROM parsed
),

--------------------------------------------------------------------
-- 4. TEMPORAL VALIDATION LAYER
--------------------------------------------------------------------
validated AS (

    SELECT
        *,

        ----------------------------------------------------------------
        -- coordinate validity
        ----------------------------------------------------------------
        CASE
            WHEN DESTINATION_LAT BETWEEN -90 AND 90
             AND DESTINATION_LON BETWEEN -180 AND 180
            THEN TRUE ELSE FALSE
        END AS IS_VALID_COORDINATES,

        ----------------------------------------------------------------
        -- temporal consistency checks
        ----------------------------------------------------------------
        CASE
            WHEN ARRIVAL_DATE IS NOT NULL
             AND DEPARTURE_DATE IS NOT NULL
             AND ARRIVAL_DATE < DEPARTURE_DATE
            THEN TRUE
            ELSE FALSE
        END AS IS_TEMPORAL_ANOMALY,

        ----------------------------------------------------------------
        -- missing critical fields
        ----------------------------------------------------------------
        CASE
            WHEN DEPARTURE_DATE IS NULL AND ARRIVAL_DATE IS NULL
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
    ARRIVAL_DATE,

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
