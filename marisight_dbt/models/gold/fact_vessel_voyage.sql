-- -- ============================================================
-- -- FACT_VESSEL_VOYAGE
-- -- Grain   : one row per vessel per report_date
-- -- Sources : gold_vessels_with_port_details  →  dim_port (SK join)
-- -- ============================================================

-- {{ config(materialized='table', schema='star') }}

-- WITH vessel AS (
--     SELECT * FROM {{ ref('gold_vessels_with_port_details') }}
-- ),

-- port_dim AS (
--     SELECT
--         WORLD_PORT_INDEX_NUMBER,
--         MAIN_PORT_NAME,
--         COUNTRY_CODE,
--         PORT_SK
--     FROM {{ ref('dim_port') }}
-- ),

-- -- Resolve destination port SK via name + country (mirrors gold join logic)
-- aliases AS (
--     SELECT RAW_NAME, MAPPED_NAME
--     FROM {{ ref('port_aliases') }}
-- ),

-- joined AS (
--     SELECT
--         v.*,
--         -- destination port SK (NULL when no match)
--         dp.PORT_SK       AS DESTINATION_PORT_SK,
--         dp.WORLD_PORT_INDEX_NUMBER AS DEST_PORT_INDEX_NUMBER
--     FROM vessel v
--     LEFT JOIN aliases a
--         ON UPPER(TRIM(v.DESTINATION_PORT_NAME)) = UPPER(TRIM(a.RAW_NAME))
--     LEFT JOIN port_dim dp
--         ON UPPER(TRIM(COALESCE(a.MAPPED_NAME, v.DESTINATION_PORT_NAME)))
--             = UPPER(TRIM(dp.MAIN_PORT_NAME))
--         AND (
--             v.DESTINATION_PORT_COUNTRY IS NULL
--             OR UPPER(TRIM(v.DESTINATION_PORT_COUNTRY))
--                 = UPPER(TRIM(dp.COUNTRY_CODE))
--         )
-- )

-- SELECT
--     -- ── Surrogate key ────────────────────────────────────────
--     {{ dbt_utils.generate_surrogate_key(['NAME', 'REPORT_DATE']) }}
--         AS VOYAGE_SK,

--     -- ── Foreign keys ─────────────────────────────────────────
--     DESTINATION_PORT_SK,            -- → dim_port
--     TO_NUMBER(TO_CHAR(CAST(REPORT_DATE AS DATE), 'YYYYMMDD')) AS DATE_SK,
--     -- ── Degenerate dimensions (low cardinality, stay in fact) 
--     NAME                        AS VESSEL_NAME,
--     TYPE                        AS VESSEL_TYPE,
--     REPORTED_STATUS,
--     VOYAGE_STATUS,
--     AGE_CATEGORY,
--     SIZE_CATEGORY,

--     -- ── Date / time keys ─────────────────────────────────────
--     REPORT_DATE,
--     DEPARTURE_DATE,
--     ACTUAL_ARRIVAL_DATE,
--     ESTIMATED_ARRIVAL_DATE,
--     LATEST_ARRIVAL_DATE,

--     -- ── Measures ─────────────────────────────────────────────
--     GROSS_TONNAGE,
--     DEADWEIGHT,
--     LENGTH_M,
--     BEAM_M,
--     YEAR_BUILT,
--     VESSEL_AGE,
--     DAYS_SINCE_DEPARTURE,

--     -- ── Boolean flags (additive measures or filter axes) ─────
--     HAS_VALID_DESTINATION,
--     IS_ANCHORED,
--     IS_MOORED,
--     IS_UNDERWAY,
--     IS_TEMPORAL_ANOMALY,
--     IS_MISSING_TIMES,
--     DESTINATION_PORT_FOUND,

--     -- ── Destination port enrichment (denormalised for perf) ──
--     DESTINATION_PORT_NAME,
--     DESTINATION_PORT_COUNTRY,
--     DESTINATION_PORT_LAT,
--     DESTINATION_PORT_LON,
--     LAST_PORT_NAME,
--     LAST_PORT_COUNTRY,
--     DEST_HARBOR_SIZE,
--     DEST_PORT_DEPTH_CLASS,
--     DEST_CARGO_PIER_DEPTH_M,
--     DEST_MAX_VESSEL_LENGTH_M,
--     DEST_MAX_VESSEL_DRAFT_M,
--     DEST_SUPPLY_RATING,
--     DEST_SUPPLY_SCORE,
--     DEST_COMMUNICATION_RATING,
--     DEST_CONTAINER_FACILITY,
--     DEST_OIL_TERMINAL,
--     DEST_LIQUID_BULK,

--     -- ── Vessel activity status ───────────────────────────────
--     VESSEL_STATUS,

--     -- ── Reference link ───────────────────────────────────────
--     DETAIL_LINK,

--     -- ── Audit ────────────────────────────────────────────────
--     CURRENT_TIMESTAMP() AS FACT_LOADED_AT

-- FROM joined
-- ============================================================
-- FACT_VESSEL_VOYAGE
-- Grain   : one row per vessel per report_date
-- Sources : gold_vessels_with_port_details  →  dim_port (SK join)
-- ============================================================

{{ config(materialized='table', schema='star') }}

WITH vessel AS (
    SELECT * FROM {{ ref('gold_vessels_with_port_details') }}
),

port_dim AS (
    SELECT
        WORLD_PORT_INDEX_NUMBER,
        MAIN_PORT_NAME,
        COUNTRY_CODE,
        PORT_SK
    FROM {{ ref('dim_port') }}
),

-- Resolve destination port SK via name + country (mirrors gold join logic)
aliases AS (
    SELECT RAW_NAME, MAPPED_NAME
    FROM {{ ref('port_aliases') }}
),

joined AS (
    SELECT
        v.*,
        -- destination port SK (NULL when no match)
        dp.PORT_SK       AS DESTINATION_PORT_SK,
        dp.WORLD_PORT_INDEX_NUMBER AS DEST_PORT_INDEX_NUMBER
    FROM vessel v
    LEFT JOIN aliases a
        ON UPPER(TRIM(v.DESTINATION_PORT_NAME)) = UPPER(TRIM(a.RAW_NAME))
    LEFT JOIN port_dim dp
        ON UPPER(TRIM(COALESCE(a.MAPPED_NAME, v.DESTINATION_PORT_NAME)))
            = UPPER(TRIM(dp.MAIN_PORT_NAME))
        AND (
            v.DESTINATION_PORT_COUNTRY IS NULL
            OR UPPER(TRIM(v.DESTINATION_PORT_COUNTRY))
                = UPPER(TRIM(dp.COUNTRY_CODE))
        )
)

SELECT
    -- ── Surrogate key (يعتمد على الحقول الأساسية للـ Grain) ──
    {{ dbt_utils.generate_surrogate_key(['NAME', 'REPORT_DATE']) }}
        AS VOYAGE_SK,

    -- ── Foreign keys ─────────────────────────────────────────
    DESTINATION_PORT_SK,            -- → dim_port
    
    -- ── Date Key (الربط الموحد مع dim_date للأداء العالي) ────
    TO_NUMBER(TO_CHAR(CAST(REPORT_DATE AS DATE), 'YYYYMMDD')) AS DATE_SK,

    -- ── Degenerate dimensions (low cardinality, stay in fact) 
    NAME                        AS VESSEL_NAME,
    TYPE                        AS VESSEL_TYPE,
    REPORTED_STATUS,
    VOYAGE_STATUS,
    AGE_CATEGORY,
    SIZE_CATEGORY,

    -- ── Date / time keys ─────────────────────────────────────
    REPORT_DATE,
    DEPARTURE_DATE,
    ACTUAL_ARRIVAL_DATE,
    ESTIMATED_ARRIVAL_DATE,
    LATEST_ARRIVAL_DATE,

    -- ── Measures ─────────────────────────────────────────────
    GROSS_TONNAGE,
    DEADWEIGHT,
    LENGTH_M,
    BEAM_M,
    YEAR_BUILT,
    VESSEL_AGE,
    DAYS_SINCE_DEPARTURE,

    -- ── Boolean flags (additive measures or filter axes) ─────
    HAS_VALID_DESTINATION,
    IS_ANCHORED,
    IS_MOORED,
    IS_UNDERWAY,
    IS_TEMPORAL_ANOMALY,
    IS_MISSING_TIMES,
    DESTINATION_PORT_FOUND,

    -- ── Destination port enrichment (denormalised for perf) ──
    DESTINATION_PORT_NAME,
    DESTINATION_PORT_COUNTRY,
    DESTINATION_PORT_LAT,
    DESTINATION_PORT_LON,
    LAST_PORT_NAME,
    LAST_PORT_COUNTRY,
    DEST_HARBOR_SIZE,
    DEST_PORT_DEPTH_CLASS,
    DEST_CARGO_PIER_DEPTH_M,
    DEST_MAX_VESSEL_LENGTH_M,
    DEST_MAX_VESSEL_DRAFT_M,
    DEST_SUPPLY_RATING,
    DEST_SUPPLY_SCORE,
    DEST_COMMUNICATION_RATING,
    DEST_CONTAINER_FACILITY,
    DEST_OIL_TERMINAL,
    DEST_LIQUID_BULK,

    -- ── Vessel activity status ───────────────────────────────
    VESSEL_STATUS,

    -- ── Reference link ───────────────────────────────────────
    DETAIL_LINK,

    -- ── Audit ────────────────────────────────────────────────
    CURRENT_TIMESTAMP() AS FACT_LOADED_AT

FROM joined

-- ── الـ QUALIFY لحل مشكلة التكرار نهائياً وتمرير اختبار الفرادة ──
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY NAME, REPORT_DATE 
    ORDER BY DESTINATION_PORT_SK DESC NULLS LAST, DETAIL_LINK
) = 1