-- ============================================================
-- DIM_SEISMIC_EVENT
-- Grain   : one row per seismic event (EVENT_ID)
-- Source  : gold_seismic_events
-- ============================================================

{{ config(materialized='table', schema='star') }}

SELECT
    -- ── Surrogate key ────────────────────────────────────────
    {{ dbt_utils.generate_surrogate_key(['EVENT_ID']) }}
        AS SEISMIC_EVENT_SK,

    -- ── Natural / business key ───────────────────────────────
    EVENT_ID,

    -- ── Descriptive ─────────────────────────────────────────
    REGION,
    MAGNITUDE,
    MAGNITUDE_CLASS,
    DEPTH_KM,
    EARTHQUAKE_TIME,

    -- ── Derived time attributes (handy for BI slicing) ───────
    DATE_TRUNC('day',   EARTHQUAKE_TIME)  AS EVENT_DATE,
    DATE_TRUNC('month', EARTHQUAKE_TIME)  AS EVENT_MONTH,
    EXTRACT(YEAR  FROM  EARTHQUAKE_TIME)  AS EVENT_YEAR,
    EXTRACT(HOUR  FROM  EARTHQUAKE_TIME)  AS EVENT_HOUR,

    -- ── Geography ────────────────────────────────────────────
    LATITUDE,
    LONGITUDE,

    -- ── Audit ────────────────────────────────────────────────
    CURRENT_TIMESTAMP() AS DIM_LOADED_AT

FROM {{ ref('gold_seismic_events') }}
