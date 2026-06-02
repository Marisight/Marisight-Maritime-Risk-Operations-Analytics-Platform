-- models/silver/silver_seismic_events.sql
-- Grain    : one row per unique seismic event, deduplicated on EVENT_ID (UNID)
-- Source   : PROJECT_DB.DBO."marisight.public.seismic_events" (Kafka/Debezium CDC, real-time)
-- Strategy : MERGE on EVENT_ID; incremental predicate on EARTHQUAKE_TIME
-- Notes    :
--   - LATITUDE, LONGITUDE, MAGNITUDE, DEPTH_KM are NUMBER(38,0) in Bronze (integer precision only)
--     Decimal precision is lost at ingestion by the Kafka connector — cannot recover
--   - __DELETED is 'false' for all observed records; filter kept defensively
--   DROPPED COLUMNS: ACTION / RECORD_METADATA / SOURCE_CATALOG dropped — CDC internals / zero-variance noise

{{
    config(
        materialized     = 'incremental',
        unique_key       = 'EVENT_ID',
        on_schema_change = 'sync_all_columns'
    )
}}

WITH base AS (

    SELECT
        UNID                AS EVENT_ID,
        SOURCE_ID,
        FLYNN_REGION        AS REGION,
        AUTH                AS REPORTING_AGENCY,
        EVTYPE              AS EVENT_TYPE,
        MAGTYPE             AS MAGNITUDE_TYPE,

        -- Cast integer columns to FLOAT
        -- Precision is lost at ingestion (Kafka connector schema inference); analytically acceptable
        LAT::FLOAT          AS LATITUDE,
        LON::FLOAT          AS LONGITUDE,
        MAG::FLOAT          AS MAGNITUDE,
        DEPTH::FLOAT        AS DEPTH_KM,

        -- Timestamps
        TIME                AS EARTHQUAKE_TIME,
        LASTUPDATE          AS LAST_UPDATED_AT

    FROM {{ source('bronze', 'SEISMIC_EVENTS') }}

    -- Exclude CDC-deleted records (defensive — all observed values are 'false')
    WHERE COALESCE(__DELETED, 'false') = 'false'

    {% if is_incremental() %}
        -- Only process events newer than the latest already in Silver
        AND TIME > (SELECT MAX(EARTHQUAKE_TIME) FROM {{ this }})
    {% endif %}

),

-- Deduplicate: EMSC emits 'update' records when magnitude or location is revised
-- Keep the most recently updated version of each event
deduped AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY EVENT_ID
            ORDER BY LAST_UPDATED_AT DESC
        ) AS _row_num
    FROM base

)

SELECT
    EVENT_ID,
    SOURCE_ID,
    REGION,
    REPORTING_AGENCY,
    EVENT_TYPE,
    MAGNITUDE_TYPE,
    LATITUDE,
    LONGITUDE,
    MAGNITUDE,
    DEPTH_KM,
    EARTHQUAKE_TIME,
    LAST_UPDATED_AT,
    CURRENT_TIMESTAMP() AS LOADED_AT

FROM deduped
WHERE _row_num = 1