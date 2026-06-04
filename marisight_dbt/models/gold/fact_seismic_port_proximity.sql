-- -- ============================================================
-- -- FACT_SEISMIC_PORT_PROXIMITY
-- -- Grain   : one row per (seismic_event × port) pair ≤ 500 km
-- -- Sources : gold_seismic_port_proximity
-- --           → dim_seismic_event (SK)
-- --           → dim_port (SK)
-- -- ============================================================

-- {{ config(materialized='table', schema='star') }}

-- WITH proximity AS (
--     SELECT * FROM {{ ref('gold_seismic_port_proximity') }}
-- ),

-- event_dim AS (
--     SELECT EVENT_ID, SEISMIC_EVENT_SK
--     FROM {{ ref('dim_seismic_event') }}
-- ),

-- port_dim AS (
--     SELECT MAIN_PORT_NAME, WORLD_PORT_INDEX_NUMBER, PORT_SK
--     FROM {{ ref('dim_port') }}
-- )

-- SELECT
--     -- ── Surrogate key ────────────────────────────────────────
--     {{ dbt_utils.generate_surrogate_key(['p.PROXIMITY_ID']) }}
--         AS PROXIMITY_SK,

--     -- ── Natural / business key ───────────────────────────────
--     p.PROXIMITY_ID,

--     -- ── Foreign keys ─────────────────────────────────────────
--     ed.SEISMIC_EVENT_SK,            -- → dim_seismic_event
--     pd.PORT_SK,                     -- → dim_port

--     -- ── Degenerate dimensions ────────────────────────────────
--     p.EVENT_ID,
--     p.PORT_NAME,

--     -- ── Measures ─────────────────────────────────────────────
--     p.MAGNITUDE,
--     p.DISTANCE_KM,
--     p.RISK_SCORE,

--     -- ── Derived risk band (useful for BI grouping) ───────────
--     CASE
--         WHEN p.RISK_SCORE >= 75 THEN 'CRITICAL'
--         WHEN p.RISK_SCORE >= 50 THEN 'HIGH'
--         WHEN p.RISK_SCORE >= 25 THEN 'MODERATE'
--         ELSE 'LOW'
--     END AS RISK_BAND,

--     -- ── Audit ────────────────────────────────────────────────
--     CURRENT_TIMESTAMP() AS FACT_LOADED_AT

-- FROM proximity p
-- LEFT JOIN event_dim ed ON p.EVENT_ID   = ed.EVENT_ID
-- LEFT JOIN port_dim  pd ON p.PORT_NAME  = pd.MAIN_PORT_NAME

{{ config(materialized='table', schema='star') }}

WITH proximity AS (
    SELECT * FROM {{ ref('gold_seismic_port_proximity') }}
),

event_dim AS (
    SELECT EVENT_ID, SEISMIC_EVENT_SK
    FROM {{ ref('dim_seismic_event') }}
),

port_dim AS (
    SELECT MAIN_PORT_NAME, WORLD_PORT_INDEX_NUMBER, PORT_SK
    FROM {{ ref('dim_port') }}
)

SELECT
    -- تعديل الـ Surrogate key ليعتمد على فرادة الحدث والمنفذ معاً لمنع التكرار تماماً
    {{ dbt_utils.generate_surrogate_key(['p.EVENT_ID', 'pd.PORT_SK', 'p.DISTANCE_KM']) }}
        AS PROXIMITY_SK,

    p.PROXIMITY_ID,
    ed.SEISMIC_EVENT_SK,
    pd.PORT_SK,
    p.EVENT_ID,
    p.PORT_NAME,
    p.DISTANCE_KM,
    p.RISK_SCORE,
    
    CASE
        WHEN p.RISK_SCORE >= 75 THEN 'CRITICAL'
        WHEN p.RISK_SCORE >= 50 THEN 'HIGH'
        WHEN p.RISK_SCORE >= 25 THEN 'MODERATE'
        ELSE 'LOW'
    END AS RISK_BAND,

    CURRENT_TIMESTAMP() AS FACT_LOADED_AT
FROM proximity p
LEFT JOIN event_dim ed ON p.EVENT_ID   = ed.EVENT_ID
LEFT JOIN port_dim  pd ON UPPER(TRIM(p.PORT_NAME))  = UPPER(TRIM(pd.MAIN_PORT_NAME))