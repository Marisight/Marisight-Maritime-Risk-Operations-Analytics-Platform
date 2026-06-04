-- ============================================================
-- DIM_PORT
-- Grain   : one row per port (WORLD_PORT_INDEX_NUMBER)
-- Sources : gold_ports  +  gold_port_infrastructure_facilities
--           +  gold_port_supply_details
-- ============================================================

{{ config(materialized='table', schema='star') }}

WITH infra AS (
    SELECT
        WORLD_PORT_INDEX_NUMBER,
        HAS_CONTAINER_FACILITY,
        HAS_RO_RO_FACILITY,
        HAS_LIQUID_BULK_FACILITY,
        HAS_SOLID_BULK_FACILITY,
        HAS_OIL_TERMINAL_FACILITY,
        CRANES_CONTAINER,
        DRY_DOCK_SIZE,
        REPAIR_CAPABILITY,
        PORT_PRIMARY_TYPE
    FROM {{ ref('gold_port_infrastructure_facilities') }}
),

supply AS (
    SELECT
        WORLD_PORT_INDEX_NUMBER,
        HAS_PROVISIONS,
        HAS_POTABLE_WATER,
        HAS_FUEL_OIL,
        HAS_DIESEL_OIL,
        HAS_REPAIRS,
        HAS_RADIO,
        HAS_TELEPHONE,
        HAS_AIRPORT,
        HAS_TELEFAX,
        SUPPLY_SCORE,
        SUPPLY_RATING,
        COMMUNICATION_SCORE,
        COMMUNICATION_RATING,
        OVERALL_PORT_SCORE
    FROM {{ ref('gold_port_supply_details') }}
),

base AS (
    SELECT
        WORLD_PORT_INDEX_NUMBER,
        MAIN_PORT_NAME,
        ALTERNATE_PORT_NAME,
        COUNTRY_CODE,
        REGION_NAME,
        UN_LOCODE,
        WORLD_WATER_BODY,
        LATITUDE,
        LONGITUDE,
        IS_VALID_COORDINATES,
        HARBOR_SIZE,
        HARBOR_TYPE,
        HARBOR_USE,
        SHELTER_AFFORDED,
        PORT_DEPTH_CLASS,
        CHANNEL_DEPTH_M,
        ANCHORAGE_DEPTH_M,
        CARGO_PIER_DEPTH_M,
        OIL_TERMINAL_DEPTH_M,
        MAXIMUM_VESSEL_LENGTH_M,
        MAXIMUM_VESSEL_BEAM_M,
        MAXIMUM_VESSEL_DRAFT_M,
        ENTRANCE_RESTRICTION_TIDE,
        ENTRANCE_RESTRICTION_HEAVY_SWELL,
        ENTRANCE_RESTRICTION_ICE,
        PORT_SECURITY,
        FIRST_PORT_OF_ENTRY
    FROM {{ ref('gold_ports') }}
)

SELECT
    -- ── Surrogate key ────────────────────────────────────────
    {{ dbt_utils.generate_surrogate_key(['base.WORLD_PORT_INDEX_NUMBER']) }}
        AS PORT_SK,

    -- ── Natural / business keys ──────────────────────────────
    base.WORLD_PORT_INDEX_NUMBER,
    base.UN_LOCODE,

    -- ── Descriptive ─────────────────────────────────────────
    base.MAIN_PORT_NAME,
    base.ALTERNATE_PORT_NAME,
    base.COUNTRY_CODE,
    base.REGION_NAME,
    base.WORLD_WATER_BODY,

    -- ── Geography ────────────────────────────────────────────
    base.LATITUDE,
    base.LONGITUDE,
    base.IS_VALID_COORDINATES,

    -- ── Physical characteristics ─────────────────────────────
    base.HARBOR_SIZE,
    base.HARBOR_TYPE,
    base.HARBOR_USE,
    base.SHELTER_AFFORDED,
    base.PORT_DEPTH_CLASS,
    base.CHANNEL_DEPTH_M,
    base.ANCHORAGE_DEPTH_M,
    base.CARGO_PIER_DEPTH_M,
    base.OIL_TERMINAL_DEPTH_M,
    base.MAXIMUM_VESSEL_LENGTH_M,
    base.MAXIMUM_VESSEL_BEAM_M,
    base.MAXIMUM_VESSEL_DRAFT_M,

    -- ── Restrictions ─────────────────────────────────────────
    base.ENTRANCE_RESTRICTION_TIDE,
    base.ENTRANCE_RESTRICTION_HEAVY_SWELL,
    base.ENTRANCE_RESTRICTION_ICE,
    base.PORT_SECURITY,
    base.FIRST_PORT_OF_ENTRY,

    -- ── Infrastructure (from gold_port_infrastructure_facilities)
    infra.PORT_PRIMARY_TYPE,
    infra.HAS_CONTAINER_FACILITY,
    infra.HAS_RO_RO_FACILITY,
    infra.HAS_LIQUID_BULK_FACILITY,
    infra.HAS_SOLID_BULK_FACILITY,
    infra.HAS_OIL_TERMINAL_FACILITY,
    infra.CRANES_CONTAINER,
    infra.DRY_DOCK_SIZE,
    infra.REPAIR_CAPABILITY,

    -- ── Supply & communications (from gold_port_supply_details)
    supply.HAS_PROVISIONS,
    supply.HAS_POTABLE_WATER,
    supply.HAS_FUEL_OIL,
    supply.HAS_DIESEL_OIL,
    supply.HAS_REPAIRS,
    supply.HAS_RADIO,
    supply.HAS_TELEPHONE,
    supply.HAS_AIRPORT,
    supply.HAS_TELEFAX,
    supply.SUPPLY_SCORE,
    supply.SUPPLY_RATING,
    supply.COMMUNICATION_SCORE,
    supply.COMMUNICATION_RATING,
    supply.OVERALL_PORT_SCORE,

    -- ── Audit ────────────────────────────────────────────────
    CURRENT_TIMESTAMP() AS DIM_LOADED_AT

FROM base
LEFT JOIN infra  USING (WORLD_PORT_INDEX_NUMBER)
LEFT JOIN supply USING (WORLD_PORT_INDEX_NUMBER)
