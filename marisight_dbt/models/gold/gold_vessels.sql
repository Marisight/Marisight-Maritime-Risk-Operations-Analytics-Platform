-- models/gold/gold_vessels.sql
-- Grain: one row per vessel per report_date (inherits silver dedup)
-- Adds: age classification, size classification, destination validity flag,
--       days-since-departure, and voyage status inference

SELECT
    -- ── Pass-through from silver ─────────────────────────────────────────────
    NAME,
    TYPE,
    YEAR_BUILT,
    GROSS_TONNAGE,
    DEADWEIGHT,
    LENGTH_M,
    BEAM_M,
    DEPARTURE_DATE,
    ACTUAL_ARRIVAL_DATE,
    ESTIMATED_ARRIVAL_DATE,
    COALESCE(ACTUAL_ARRIVAL_DATE, ESTIMATED_ARRIVAL_DATE) AS LATEST_ARRIVAL_DATE,
    LAST_PORT_NAME,
    LAST_PORT_COUNTRY,
    DESTINATION_PORT_NAME,
    DESTINATION_PORT_COUNTRY,
    DESTINATION_LAT AS DESTINATION_PORT_LAT,
    DESTINATION_LON AS DESTINATION_PORT_LON,
    IS_VALID_COORDINATES,
    REPORTED_STATUS,
    REPORT_DATE,
    DETAIL_LINK,
    IS_TEMPORAL_ANOMALY,
    IS_MISSING_TIMES,

    -- ── Derived: Vessel Age ──────────────────────────────────────────────────
    CASE
        WHEN YEAR_BUILT IS NULL THEN NULL
        ELSE EXTRACT(YEAR FROM CURRENT_DATE) - YEAR_BUILT
    END AS VESSEL_AGE,

    CASE
        WHEN YEAR_BUILT IS NULL THEN 'Unknown'
        WHEN (EXTRACT(YEAR FROM CURRENT_DATE) - YEAR_BUILT) < 5   THEN 'New'
        WHEN (EXTRACT(YEAR FROM CURRENT_DATE) - YEAR_BUILT) <= 15  THEN 'Mid-life'
        WHEN (EXTRACT(YEAR FROM CURRENT_DATE) - YEAR_BUILT) <= 25  THEN 'Aging'
        ELSE 'Old'
    END AS AGE_CATEGORY,

    -- ── Derived: Size Classification (by Deadweight Tonnage) ─────────────────
    CASE
        WHEN DEADWEIGHT IS NULL   THEN 'Unknown'
        WHEN DEADWEIGHT < 40000   THEN 'Handysize'
        WHEN DEADWEIGHT < 60000   THEN 'Handymax'
        WHEN DEADWEIGHT < 80000   THEN 'Panamax'
        ELSE                           'Capesize'
    END AS SIZE_CATEGORY,

    -- ── Derived: Destination validity ────────────────────────────────────────
    CASE
        WHEN DESTINATION_PORT_NAME IS NOT NULL
        AND IS_VALID_COORDINATES = TRUE
        THEN TRUE
        ELSE FALSE
    END AS HAS_VALID_DESTINATION,

    -- ── Derived: Days since departure ────────────────────────────────────────
    CASE
        WHEN DEPARTURE_DATE IS NOT NULL
        AND REPORT_DATE    IS NOT NULL
        THEN DATEDIFF('day', DEPARTURE_DATE, REPORT_DATE)
        ELSE NULL
    END AS DAYS_SINCE_DEPARTURE,

    -- ── Derived: Voyage status inference ─────────────────────────────────────
    -- Adds a human-readable status when REPORTED_STATUS is NULL
    CASE
        WHEN REPORTED_STATUS IS NOT NULL              THEN REPORTED_STATUS
        WHEN DESTINATION_PORT_NAME IS NOT NULL
        AND DEPARTURE_DATE        IS NOT NULL        THEN 'In Transit (inferred)'
        WHEN LAST_PORT_NAME        IS NOT NULL
        AND DESTINATION_PORT_NAME IS NULL            THEN 'At Port (inferred)'
        ELSE 'Unknown'
    END AS VOYAGE_STATUS,

    -- ── Derived: Status Flags (for easy calculation) ─────────────────────
    CASE WHEN REPORTED_STATUS ILIKE '%anchor%'              THEN TRUE ELSE FALSE END AS IS_ANCHORED,
    CASE WHEN REPORTED_STATUS ILIKE '%moor%'                THEN TRUE ELSE FALSE END AS IS_MOORED,
    CASE WHEN REPORTED_STATUS ILIKE '%under way%'
    OR REPORTED_STATUS ILIKE '%underway%'
    OR REPORTED_STATUS ILIKE '%sailing%'              THEN TRUE ELSE FALSE END AS IS_UNDERWAY

FROM {{ ref('silver_vessels') }}