-- ============================================================
-- FACT_DAILY_SEISMIC
-- Grain   : one row per (event_date × region)
-- Source  : daily_aggregated_seismic_events
--           → dim_seismic_event (region + date link)
-- ============================================================

{{ config(materialized='table', schema='star') }}

SELECT
    -- ── Surrogate key ────────────────────────────────────────
    {{ dbt_utils.generate_surrogate_key(['EVENT_DATE', 'REGION']) }}
        AS DAILY_SEISMIC_SK,

    -- ── Date / time keys ─────────────────────────────────────
    EVENT_DATE,
    TO_NUMBER(TO_CHAR(CAST(EVENT_DATE AS DATE), 'YYYYMMDD')) AS DATE_SK,
    DATE_TRUNC('month', EVENT_DATE) AS EVENT_MONTH,
    EXTRACT(YEAR FROM EVENT_DATE)   AS EVENT_YEAR,
    EXTRACT(WEEK FROM EVENT_DATE)   AS EVENT_WEEK,

    -- ── Degenerate dimension ─────────────────────────────────
    REGION,

    -- ── Measures ─────────────────────────────────────────────
    TOTAL_EVENTS,
    AVG_MAGNITUDE,
    MAX_MAGNITUDE,
    ROLLING_7D_AVG_EVENTS,

    -- ── Derived / classification ─────────────────────────────
    DAILY_RISK_LEVEL,

    -- ── Numeric encoding of risk (for aggregation in BI) ─────
    CASE DAILY_RISK_LEVEL
        WHEN 'CRITICAL' THEN 4
        WHEN 'HIGH'     THEN 3
        WHEN 'MODERATE' THEN 2
        ELSE                 1
    END AS DAILY_RISK_SCORE,

    -- ── Audit ────────────────────────────────────────────────
    CURRENT_TIMESTAMP() AS FACT_LOADED_AT

FROM {{ ref('daily_aggregated_seismic_events') }}
