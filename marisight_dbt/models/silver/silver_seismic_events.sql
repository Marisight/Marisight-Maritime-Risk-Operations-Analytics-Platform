-- models/silver/silver_seismic_events.sql
-- Grain    : one row per unique seismic event, deduplicated on EVENT_ID (UNID)
-- Source   : PROJECT_DB.DBO."marisight.public.seismic_events" (Kafka/Debezium CDC, real-time)
-- Strategy : MERGE on EVENT_ID; incremental watermark on LAST_UPDATED_AT
-- Notes    :
--   - LATITUDE, LONGITUDE, MAGNITUDE, DEPTH_KM are NUMBER(38,0) in Bronze (integer precision only)
--     Decimal precision is lost at ingestion by the Kafka connector — cannot recover
--   - __DELETED is 'false' for all observed records; filter kept defensively
--   - EVENT_TYPE / MAGNITUDE_TYPE: raw codes preserved + human-readable label added inline
--   - DROPPED: ACTION / RECORD_METADATA / SOURCE_CATALOG — CDC internals / zero-variance noise

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

        -- Raw codes preserved for filtering; labels added for readability
        EVTYPE              AS EVENT_TYPE_CODE,
        CASE EVTYPE
            WHEN 'ke' THEN 'Confirmed Earthquake'
            WHEN 'se' THEN 'Suspected Earthquake'
            WHEN 'sl' THEN 'Landslide'
            WHEN 'uk' THEN 'Unknown'
            ELSE             EVTYPE          -- future EMSC codes pass through
        END                 AS EVENT_TYPE,

        MAGTYPE             AS MAGNITUDE_TYPE_CODE,
        CASE MAGTYPE
            WHEN 'ml' THEN 'Local Magnitude'
            WHEN 'mb' THEN 'Body Wave Magnitude'
            WHEN 'mw' THEN 'Moment Magnitude'
            WHEN 'ms' THEN 'Surface Wave Magnitude'
            WHEN 'md' THEN 'Duration Magnitude'
            WHEN 'mr' THEN 'Richter Magnitude'
            WHEN 'm'  THEN 'Magnitude (unspecified)'
            ELSE MAGTYPE
        END AS MAGNITUDE_TYPE,

        -- Cast integer columns to FLOAT
        -- Precision lost at ingestion (Kafka connector schema inference); analytically acceptable
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
        -- LAST_UPDATED_AT (EMSC publish/update time) is the pipeline watermark:
        -- - Captures both new events (create) and revised events (update) 
        AND LASTUPDATE >= (
            SELECT DATEADD(
                HOUR, -24,
                MAX(LAST_UPDATED_AT)
            )
            FROM {{ this }}
        )
    {% endif %}

),

-- Deduplicate: EMSC emits 'update' records when magnitude or location is revised
-- Keep the most recently updated version of each event within this batch
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
    EVENT_TYPE_CODE,
    EVENT_TYPE,
    MAGNITUDE_TYPE_CODE,
    MAGNITUDE_TYPE,
    LATITUDE,
    LONGITUDE,
    MAGNITUDE,
    DEPTH_KM,
    EARTHQUAKE_TIME,
    LAST_UPDATED_AT,
    CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_LTZ AS LOADED_AT

FROM deduped
WHERE _row_num = 1
