-- models/silver/silver_vessels.sql
-- Grain: one row per vessel per report_date (deduplicated)
-- Source: PROJECT_DB.DBO.VESSEL (daily scrape, all VARCHAR columns)

{{
    config(
        materialized     = 'incremental',
        unique_key       = ['NAME', 'REPORT_DATE'],
        on_schema_change = 'sync_all_columns'
    )
}}

-----------------> Step 0: parse REPORT_DATE once, apply incremental filter early
WITH
raw AS (
    SELECT
        *,
        TRY_TO_DATE(REPORT_DATE, 'YYYY-MM-DD HH24:MI') AS _report_date_parsed
    FROM {{ source('bronze', 'VESSEL') }}
    WHERE NAME IS NOT NULL

    {% if is_incremental() %}
        AND TRY_TO_DATE(REPORT_DATE, 'YYYY-MM-DD HH24:MI')
            > (SELECT MAX(REPORT_DATE) FROM {{ this }})
    {% endif %}
),

-----------------> Step 1: cast + null normalisation
base AS (
    SELECT
        TRIM(NAME)                                      AS NAME,
        TRIM(TYPE)                                      AS TYPE,

        -- Numeric casts (VARCHAR → NUMBER)
        TRY_TO_NUMBER(YEAR_BUILT)                       AS YEAR_BUILT,
        TRY_TO_NUMBER(GROSS_TONNAGE)                    AS GROSS_TONNAGE,
        TRY_TO_NUMBER(DEADWEIGHT)                       AS DEADWEIGHT,
        TRY_TO_DOUBLE("length(m)")                      AS LENGTH_M,
        TRY_TO_DOUBLE("beam(m)")                        AS BEAM_M,

        -- REPORT_DATE: already parsed in raw CTE
        _report_date_parsed                             AS REPORT_DATE,

        -- DEPARTURE_DATE: 'ATD: May 14, 19:11 UTC(8 days ago)' or 'None'/NULL
        -- Extract 'Mon DD', append report year; subtract 1 year if result is in the future
        CASE
            WHEN DEPARTURE_DATE IS NULL
                OR UPPER(TRIM(DEPARTURE_DATE)) = 'NONE'
                OR TRIM(DEPARTURE_DATE) = '' THEN NULL
            ELSE
                CASE
                    WHEN TRY_TO_DATE(
                            REGEXP_SUBSTR(DEPARTURE_DATE, '[A-Z][a-z]{2}\\s+\\d{1,2}')
                            || ' ' || YEAR(_report_date_parsed),
                            'Mon DD YYYY')
                         > _report_date_parsed
                    THEN TRY_TO_DATE(
                            REGEXP_SUBSTR(DEPARTURE_DATE, '[A-Z][a-z]{2}\\s+\\d{1,2}')
                            || ' ' || (YEAR(_report_date_parsed) - 1),
                            'Mon DD YYYY')
                    ELSE TRY_TO_DATE(
                            REGEXP_SUBSTR(DEPARTURE_DATE, '[A-Z][a-z]{2}\\s+\\d{1,2}')
                            || ' ' || YEAR(_report_date_parsed),
                            'Mon DD YYYY')
                END
        END AS DEPARTURE_DATE,

        -- ARRIVAL_DATE: 'ETA: May 24, 13:00' or 'ETA: -' or 'None'/NULL
        -- Same year-rollover logic as DEPARTURE_DATE
        CASE
            WHEN ARRIVAL_DATE IS NULL
                OR UPPER(TRIM(ARRIVAL_DATE)) IN ('NONE', 'ETA: -', '-')
                OR TRIM(ARRIVAL_DATE) = '' THEN NULL
            ELSE
                CASE
                    WHEN TRY_TO_DATE(
                            REGEXP_SUBSTR(ARRIVAL_DATE, '[A-Z][a-z]{2}\\s+\\d{1,2}')
                            || ' ' || YEAR(_report_date_parsed),
                            'Mon DD YYYY')
                         > _report_date_parsed
                    THEN TRY_TO_DATE(
                            REGEXP_SUBSTR(ARRIVAL_DATE, '[A-Z][a-z]{2}\\s+\\d{1,2}')
                            || ' ' || (YEAR(_report_date_parsed) - 1),
                            'Mon DD YYYY')
                    ELSE TRY_TO_DATE(
                            REGEXP_SUBSTR(ARRIVAL_DATE, '[A-Z][a-z]{2}\\s+\\d{1,2}')
                            || ' ' || YEAR(_report_date_parsed),
                            'Mon DD YYYY')
                END
        END AS ARRIVAL_DATE,

        -- Port name/country: treat 'None' and blanks as NULL
        NULLIF(NULLIF(TRIM(LAST_PORT_NAME),           ''), 'None') AS LAST_PORT_NAME,
        NULLIF(NULLIF(TRIM(LAST_PORT_COUNTRY),        ''), 'None') AS LAST_PORT_COUNTRY,
        NULLIF(NULLIF(TRIM(DESTINATION_PORT_NAME),    ''), 'None') AS DESTINATION_PORT_NAME,
        NULLIF(NULLIF(TRIM(DESTINATION_PORT_COUNTRY), ''), 'None') AS DESTINATION_PORT_COUNTRY,

        -- Coordinates
        TRY_TO_DOUBLE(NULLIF(TRIM(DESTINATION_PORT_LAT), 'None')) AS DESTINATION_PORT_LAT,
        TRY_TO_DOUBLE(NULLIF(TRIM(DESTINATION_PORT_LON), 'None')) AS DESTINATION_PORT_LON,

        -- Status: null out noise values
        CASE
            WHEN TRIM(REPORTED_STATUS) IN ('None', '-', '', 'N/A')
              OR REPORTED_STATUS IS NULL
            THEN NULL
            ELSE TRIM(REPORTED_STATUS)
        END                                             AS REPORTED_STATUS,

        DETAIL_LINK,

        -- Audit: when was this row loaded into Bronze
        -- Falls back to NULL for rows ingested before the column was added
        _INGESTED_AT

    FROM raw
),

-----------------> Step 2: computed flags
enriched AS (
    SELECT
        *,
        CASE
            WHEN DESTINATION_PORT_LAT IS NOT NULL
             AND DESTINATION_PORT_LON IS NOT NULL
             AND DESTINATION_PORT_LAT BETWEEN -90  AND 90
             AND DESTINATION_PORT_LON BETWEEN -180 AND 180
            THEN TRUE
            ELSE FALSE
        END AS IS_VALID_COORDINATES
    FROM base
),

-----------------> Step 3: deduplicate (same vessel, same report day → keep latest)
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY NAME, REPORT_DATE
            ORDER BY
                _INGESTED_AT DESC NULLS LAST,   -- prefer the most recently ingested row
                DETAIL_LINK                      -- stable tiebreak when _INGESTED_AT is tied or NULL
        ) AS _row_num
    FROM enriched
)

SELECT
    NAME,
    TYPE,
    YEAR_BUILT,
    GROSS_TONNAGE,
    DEADWEIGHT,
    LENGTH_M,
    BEAM_M,
    DEPARTURE_DATE,
    ARRIVAL_DATE,
    LAST_PORT_NAME,
    LAST_PORT_COUNTRY,
    DESTINATION_PORT_NAME,
    DESTINATION_PORT_COUNTRY,
    DESTINATION_PORT_LAT,
    DESTINATION_PORT_LON,
    IS_VALID_COORDINATES,
    REPORTED_STATUS,
    REPORT_DATE,
    DETAIL_LINK,
    CURRENT_TIMESTAMP()                             AS LOADED_AT   -- when dbt wrote this row

FROM deduped
WHERE _row_num = 1