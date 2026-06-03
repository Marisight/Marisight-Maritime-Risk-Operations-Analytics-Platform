-- models/silver/silver_ports.sql
-- Grain: one row per port (deduplicated on WORLD_PORT_INDEX_NUMBER)
-- Source: PROJECT_DB.DBO.PORTS (NGA World Port Index, 3,804 rows, 109 cols)
-- Strategy: 57 analytically useful columns kept; categoricals normalised via
--           clean_categorical macro ('Unknown'/blank/'-'/'N/A' -> NULL);
--           0-sentinel depths/dimensions converted to NULL;
--           coordinate validity flag added; deduplicated by OID_.

WITH base AS (

    SELECT
        ----------------------------------------------------------------
        -- Identity & Geography
        ----------------------------------------------------------------
        WORLD_PORT_INDEX_NUMBER,
        NULLIF(TRIM(MAIN_PORT_NAME),      '')   AS MAIN_PORT_NAME,
        NULLIF(TRIM(ALTERNATE_PORT_NAME), '')   AS ALTERNATE_PORT_NAME,
        NULLIF(TRIM(COUNTRY_CODE),        '')   AS COUNTRY_CODE,
        NULLIF(TRIM(REGION_NAME),         '')   AS REGION_NAME,
        NULLIF(TRIM(WORLD_WATER_BODY),    '')   AS WORLD_WATER_BODY,
        NULLIF(TRIM(UN_LOCODE),           '')   AS UN_LOCODE,
        LATITUDE,
        LONGITUDE,

        ----------------------------------------------------------------
        -- Physical characteristics
        ----------------------------------------------------------------
        {{ clean_categorical('HARBOR_SIZE') }}      AS HARBOR_SIZE,
        {{ clean_categorical('HARBOR_TYPE') }}      AS HARBOR_TYPE,
        {{ clean_categorical('HARBOR_USE') }}       AS HARBOR_USE,
        {{ clean_categorical('SHELTER_AFFORDED') }} AS SHELTER_AFFORDED,

        ----------------------------------------------------------------
        -- Depths & dimensions  (0 = "no data" sentinel -> NULL)
        ----------------------------------------------------------------
        NULLIF(TIDAL_RANGE_M,          0)       AS TIDAL_RANGE_M,
        NULLIF(ENTRANCE_WIDTH_M,       0)       AS ENTRANCE_WIDTH_M,
        NULLIF(CHANNEL_DEPTH_M,        0)       AS CHANNEL_DEPTH_M,
        NULLIF(ANCHORAGE_DEPTH_M,      0)       AS ANCHORAGE_DEPTH_M,
        NULLIF(CARGO_PIER_DEPTH_M,     0)       AS CARGO_PIER_DEPTH_M,
        NULLIF(OIL_TERMINAL_DEPTH_M,   0)       AS OIL_TERMINAL_DEPTH_M,
        NULLIF(LIQUIFIED_NATURAL_GAS_TERMINAL_DEPTH_M, 0) AS LNG_TERMINAL_DEPTH_M,

        -- Maximum vessel constraints: critical for port recommendation
        NULLIF(MAXIMUM_VESSEL_LENGTH_M, 0)      AS MAXIMUM_VESSEL_LENGTH_M,
        NULLIF(MAXIMUM_VESSEL_BEAM_M,   0)      AS MAXIMUM_VESSEL_BEAM_M,
        NULLIF(MAXIMUM_VESSEL_DRAFT_M,  0)      AS MAXIMUM_VESSEL_DRAFT_M,

        ----------------------------------------------------------------
        -- Entrance restrictions  (feed safety score)
        ----------------------------------------------------------------
        {{ clean_categorical('ENTRANCE_RESTRICTION_TIDE') }}        AS ENTRANCE_RESTRICTION_TIDE,
        {{ clean_categorical('ENTRANCE_RESTRICTION_HEAVY_SWELL') }} AS ENTRANCE_RESTRICTION_HEAVY_SWELL,
        {{ clean_categorical('ENTRANCE_RESTRICTION_ICE') }}         AS ENTRANCE_RESTRICTION_ICE,
        {{ clean_categorical('ENTRANCE_RESTRICTION_OTHER') }}       AS ENTRANCE_RESTRICTION_OTHER,

        ----------------------------------------------------------------
        -- Supplies  (feed Gold supply score)
        ----------------------------------------------------------------
        {{ clean_categorical('SUPPLIES_PROVISIONS') }}    AS SUPPLIES_PROVISIONS,
        {{ clean_categorical('SUPPLIES_POTABLE_WATER') }} AS SUPPLIES_POTABLE_WATER,
        {{ clean_categorical('SUPPLIES_FUEL_OIL') }}      AS SUPPLIES_FUEL_OIL,
        {{ clean_categorical('SUPPLIES_DIESEL_OIL') }}    AS SUPPLIES_DIESEL_OIL,

        ----------------------------------------------------------------
        -- Services & repairs
        ----------------------------------------------------------------
        {{ clean_categorical('REPAIRS') }}   AS REPAIRS,
        {{ clean_categorical('DRY_DOCK') }}  AS DRY_DOCK,

        ----------------------------------------------------------------
        -- Communications  (feed Gold communication score)
        ----------------------------------------------------------------
        {{ clean_categorical('COMMUNICATIONS_TELEPHONE') }} AS COMMUNICATIONS_TELEPHONE,
        {{ clean_categorical('COMMUNICATIONS_TELEFAX') }}   AS COMMUNICATIONS_TELEFAX,
        {{ clean_categorical('COMMUNICATIONS_RADIO') }}     AS COMMUNICATIONS_RADIO,
        {{ clean_categorical('COMMUNICATIONS_AIRPORT') }}   AS COMMUNICATIONS_AIRPORT,
        {{ clean_categorical('COMMUNICATIONS_RAIL') }}      AS COMMUNICATIONS_RAIL,

        ----------------------------------------------------------------
        -- Cargo facilities  (vessel-type matching)
        ----------------------------------------------------------------
        {{ clean_categorical('FACILITIES_WHARVES') }}              AS FACILITIES_WHARVES,
        {{ clean_categorical('FACILITIES_ANCHORAGE') }}            AS FACILITIES_ANCHORAGE,
        {{ clean_categorical('FACILITIES_DANGEROUS_CARGO_ANCHORAGE') }} AS FACILITIES_DANGEROUS_CARGO_ANCHORAGE,
        {{ clean_categorical('FACILITIES_RO_RO') }}                AS FACILITIES_RO_RO,
        {{ clean_categorical('FACILITIES_SOLID_BULK') }}           AS FACILITIES_SOLID_BULK,
        {{ clean_categorical('FACILITIES_LIQUID_BULK') }}          AS FACILITIES_LIQUID_BULK,
        {{ clean_categorical('FACILITIES_CONTAINER') }}            AS FACILITIES_CONTAINER,
        {{ clean_categorical('FACILITIES_BREAKBULK') }}            AS FACILITIES_BREAKBULK,
        {{ clean_categorical('FACILITIES_OIL_TERMINAL') }}         AS FACILITIES_OIL_TERMINAL,
        {{ clean_categorical('FACILITIES_LNG_TERMINAL') }}         AS FACILITIES_LNG_TERMINAL,

        ----------------------------------------------------------------
        -- Lifting equipment
        ----------------------------------------------------------------
        {{ clean_categorical('CRANES_CONTAINER') }}  AS CRANES_CONTAINER,

        ----------------------------------------------------------------
        -- Safety & emergency  (feed Gold safety score)
        ----------------------------------------------------------------
        {{ clean_categorical('PORT_SECURITY') }}                AS PORT_SECURITY,
        {{ clean_categorical('SEARCH_AND_RESCUE') }}            AS SEARCH_AND_RESCUE,
        {{ clean_categorical('MEDICAL_FACILITIES') }}           AS MEDICAL_FACILITIES,
        {{ clean_categorical('VESSEL_TRAFFIC_SERVICE') }}       AS VESSEL_TRAFFIC_SERVICE,
        {{ clean_categorical('TUGS_SALVAGE') }}                 AS TUGS_SALVAGE,
        {{ clean_categorical('TUGS_ASSISTANCE') }}              AS TUGS_ASSISTANCE,

        ----------------------------------------------------------------
        -- Routing
        ----------------------------------------------------------------
        {{ clean_categorical('FIRST_PORT_OF_ENTRY') }} AS FIRST_PORT_OF_ENTRY,

        -- Tiebreak for dedup only (not exposed in final SELECT)
        OID_

    FROM {{ source('bronze', 'PORTS') }}

),

enriched AS (

    SELECT
        *,
        CASE
            WHEN LATITUDE  IS NOT NULL
            AND LONGITUDE IS NOT NULL
            AND LATITUDE  BETWEEN -90  AND 90
            AND LONGITUDE BETWEEN -180 AND 180
            THEN TRUE ELSE FALSE
        END AS IS_VALID_COORDINATES
    FROM base

),

deduped AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY WORLD_PORT_INDEX_NUMBER
            ORDER BY OID_
        ) AS _row_num
    FROM enriched

)

SELECT
    -- Identity & Geography
    WORLD_PORT_INDEX_NUMBER,
    MAIN_PORT_NAME,
    ALTERNATE_PORT_NAME,
    COUNTRY_CODE,
    REGION_NAME,
    WORLD_WATER_BODY,
    UN_LOCODE,
    LATITUDE,
    LONGITUDE,
    IS_VALID_COORDINATES,

    -- Physical characteristics
    HARBOR_SIZE,
    HARBOR_TYPE,
    HARBOR_USE,
    SHELTER_AFFORDED,

    -- Depths & dimensions
    TIDAL_RANGE_M,
    ENTRANCE_WIDTH_M,
    CHANNEL_DEPTH_M,
    ANCHORAGE_DEPTH_M,
    CARGO_PIER_DEPTH_M,
    OIL_TERMINAL_DEPTH_M,
    LNG_TERMINAL_DEPTH_M,
    MAXIMUM_VESSEL_LENGTH_M,
    MAXIMUM_VESSEL_BEAM_M,
    MAXIMUM_VESSEL_DRAFT_M,

    -- Entrance restrictions
    ENTRANCE_RESTRICTION_TIDE,
    ENTRANCE_RESTRICTION_HEAVY_SWELL,
    ENTRANCE_RESTRICTION_ICE,
    ENTRANCE_RESTRICTION_OTHER,

    -- Supplies
    SUPPLIES_PROVISIONS,
    SUPPLIES_POTABLE_WATER,
    SUPPLIES_FUEL_OIL,
    SUPPLIES_DIESEL_OIL,

    -- Services & repairs
    REPAIRS,
    DRY_DOCK,

    -- Communications
    COMMUNICATIONS_TELEPHONE,
    COMMUNICATIONS_TELEFAX,
    COMMUNICATIONS_RADIO,
    COMMUNICATIONS_AIRPORT,
    COMMUNICATIONS_RAIL,

    -- Cargo facilities
    FACILITIES_WHARVES,
    FACILITIES_ANCHORAGE,
    FACILITIES_DANGEROUS_CARGO_ANCHORAGE,
    FACILITIES_RO_RO,
    FACILITIES_SOLID_BULK,
    FACILITIES_LIQUID_BULK,
    FACILITIES_CONTAINER,
    FACILITIES_BREAKBULK,
    FACILITIES_OIL_TERMINAL,
    FACILITIES_LNG_TERMINAL,

    -- Lifting
    CRANES_CONTAINER,

    -- Safety & emergency
    PORT_SECURITY,
    SEARCH_AND_RESCUE,
    MEDICAL_FACILITIES,
    VESSEL_TRAFFIC_SERVICE,
    TUGS_SALVAGE,
    TUGS_ASSISTANCE,

    -- Routing
    FIRST_PORT_OF_ENTRY,

    -- Audit
    CURRENT_TIMESTAMP() AS LOADED_AT

FROM deduped
WHERE _row_num = 1