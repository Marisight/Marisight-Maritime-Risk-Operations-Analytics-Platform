-- models/gold/gold_port_infrastructure_facilities.sql
-- Grain: one row per port
-- Detailed 0/1 scoring for infrastructure and engineered port classification
-- Powers the port cargo handling capabilities and infrastructure dashboard

{{ config(
    materialized='table'
) }}

WITH base_ports AS (
    SELECT
        WORLD_PORT_INDEX_NUMBER,
        MAIN_PORT_NAME,
        COUNTRY_CODE,
        REGION_NAME,
        HARBOR_SIZE,
        PORT_DEPTH_CLASS,
        IS_VALID_COORDINATES,
        LATITUDE,
        LONGITUDE,

        -- ── Individual infrastructure factors (1 = available, 0 = not/unknown) ──
        CASE WHEN UPPER(FACILITIES_CONTAINER)     = 'YES' THEN 1 ELSE 0 END AS HAS_CONTAINER_FACILITY,
        CASE WHEN UPPER(FACILITIES_RO_RO)         = 'YES' THEN 1 ELSE 0 END AS HAS_RO_RO_FACILITY,
        CASE WHEN UPPER(FACILITIES_LIQUID_BULK)   = 'YES' THEN 1 ELSE 0 END AS HAS_LIQUID_BULK_FACILITY,
        CASE WHEN UPPER(FACILITIES_SOLID_BULK)    = 'YES' THEN 1 ELSE 0 END AS HAS_SOLID_BULK_FACILITY,
        CASE WHEN UPPER(FACILITIES_OIL_TERMINAL)  = 'YES' THEN 1 ELSE 0 END AS HAS_OIL_TERMINAL_FACILITY,
        
        -- ── Maintenance & Equipment factors ─────────────────────────────────────
        CRANES_CONTAINER,
        COALESCE(DRY_DOCK, 'None') AS DRY_DOCK_SIZE,
        COALESCE(REPAIRS, 'None') AS REPAIR_CAPABILITY
    
    FROM {{ ref('gold_ports') }} 
)

SELECT
    *,
    -- ── Engineered Port Classification (PORT_PRIMARY_TYPE) ───────────────────
    CASE 
        -- 1. موانئ البترول والغاز والموائع
        WHEN HAS_OIL_TERMINAL_FACILITY = 1 AND HAS_LIQUID_BULK_FACILITY = 1 THEN 'Liquid Cargo & Oil Hub'
        WHEN HAS_OIL_TERMINAL_FACILITY = 1 THEN 'Oil Terminal'
        WHEN HAS_LIQUID_BULK_FACILITY = 1 AND HAS_CONTAINER_FACILITY = 0 AND HAS_SOLID_BULK_FACILITY = 0 THEN 'Liquid Bulk Specialist'
        
        -- 2. موانئ الحاويات والـ Ro-Ro (موانئ تجارية حديثة)
        WHEN HAS_CONTAINER_FACILITY = 1 AND HAS_RO_RO_FACILITY = 1 THEN 'Container & Ro-Ro Port'
        WHEN HAS_CONTAINER_FACILITY = 1 THEN 'Container Port'
        WHEN HAS_RO_RO_FACILITY = 1 AND HAS_CONTAINER_FACILITY = 0 AND HAS_SOLID_BULK_FACILITY = 0 THEN 'Ro-Ro Specialist (Vehicles)'
        
        -- 3. موانئ البضائع الصلبة والصب (Bulk)
        WHEN HAS_SOLID_BULK_FACILITY = 1 AND HAS_CONTAINER_FACILITY = 0 AND HAS_LIQUID_BULK_FACILITY = 0 THEN 'Dry Bulk Port'
        
        -- 4. الموانئ المتكاملة العملاقة (بتخدم كل أنواع البضائع)
        WHEN (HAS_CONTAINER_FACILITY = 1 OR HAS_RO_RO_FACILITY = 1) 
             AND (HAS_SOLID_BULK_FACILITY = 1 OR HAS_LIQUID_BULK_FACILITY = 1) THEN 'Multi-Purpose Commercial Port'
             
        -- 5. موانئ صيانة وبناء السفن
        WHEN DRY_DOCK_SIZE IN ('Medium', 'Large') OR REPAIR_CAPABILITY = 'Major' THEN 'Shipyard & Maintenance Port'
        
        -- 6. الافتراضي لو البيانات غير كافية للتصنيف
        ELSE 'General Cargo / Undefined'
    END AS PORT_PRIMARY_TYPE

FROM base_ports