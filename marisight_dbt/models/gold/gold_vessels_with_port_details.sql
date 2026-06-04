-- models/gold/gold_vessels_with_port_details.sql
-- Grain: one row per vessel per report_date
-- Joins gold_vessels → gold_ports on destination port name
-- Result: vessel voyage data enriched with destination port capabilities

SELECT
    -- ── Vessel columns ───────────────────────────────────────────────────────
    v.NAME,
    v.TYPE,
    v.YEAR_BUILT,
    v.GROSS_TONNAGE,
    v.DEADWEIGHT,
    v.LENGTH_M,
    v.BEAM_M,
    v.DEPARTURE_DATE,
    v.ACTUAL_ARRIVAL_DATE,
    v.ESTIMATED_ARRIVAL_DATE,
    v.LATEST_ARRIVAL_DATE,
    v.LAST_PORT_NAME,
    v.LAST_PORT_COUNTRY,
    v.DESTINATION_PORT_NAME,
    v.DESTINATION_PORT_COUNTRY,
    
    -- 🛠️ [تعديل 3] دمج الإحداثيات من جدول السفن والموانئ بشكل صحيح
    COALESCE(v.DESTINATION_PORT_LAT, p.LATITUDE) AS DESTINATION_PORT_LAT,
    COALESCE(v.DESTINATION_PORT_LON, p.LONGITUDE) AS DESTINATION_PORT_LON,
    
    -- 🛠️ [تعديل 4] احتساب صحة الإحداثيات ديناميكياً
    CASE 
        WHEN COALESCE(v.DESTINATION_PORT_LAT, p.LATITUDE) IS NOT NULL 
        AND COALESCE(v.DESTINATION_PORT_LON, p.LONGITUDE) IS NOT NULL 
        THEN TRUE 
        ELSE FALSE 
    END AS IS_VALID_COORDINATES,
    
    v.REPORTED_STATUS,
    v.REPORT_DATE,
    v.DETAIL_LINK,
    v.VESSEL_AGE,
    v.AGE_CATEGORY,
    v.SIZE_CATEGORY,
    
    -- 🛠️ [تعديل 5] تحديث صحة الوجهة بناءً على نجاح عملية الربط
    CASE 
        WHEN v.DESTINATION_PORT_NAME IS NOT NULL 
        AND (p.WORLD_PORT_INDEX_NUMBER IS NOT NULL OR v.DESTINATION_PORT_LAT IS NOT NULL)
        THEN TRUE 
        ELSE FALSE 
    END AS HAS_VALID_DESTINATION,
    
    v.DAYS_SINCE_DEPARTURE,
    v.VOYAGE_STATUS,
    v.IS_ANCHORED,
    v.IS_MOORED,
    v.IS_UNDERWAY,
    IS_TEMPORAL_ANOMALY,
    IS_MISSING_TIMES,

    -- 🛠️ [تعديل 6] إضافة علم الوصول (IS_ARRIVED) بناءً على الدمج وحالة الرحلة
    CASE 
        WHEN v.ACTUAL_ARRIVAL_DATE IS NOT NULL THEN TRUE
        WHEN p.WORLD_PORT_INDEX_NUMBER IS NOT NULL 
             AND (v.REPORTED_STATUS ILIKE '%moor%' OR v.REPORTED_STATUS ILIKE '%anchor%') THEN TRUE
        WHEN v.VOYAGE_STATUS IN ('Arrived / At Berth', 'At Port') THEN TRUE
        ELSE FALSE 
    END AS IS_ARRIVED,

    -- ── Matched port capabilities (NULL when no match) ───────────────────────
    p.WORLD_PORT_INDEX_NUMBER           AS DEST_PORT_INDEX_NUMBER,
    p.COUNTRY_CODE                      AS DEST_PORT_COUNTRY_CODE,
    p.HARBOR_SIZE                       AS DEST_HARBOR_SIZE,
    p.PORT_DEPTH_CLASS                  AS DEST_PORT_DEPTH_CLASS,
    p.CARGO_PIER_DEPTH_M                AS DEST_CARGO_PIER_DEPTH_M,
    p.MAXIMUM_VESSEL_LENGTH_M           AS DEST_MAX_VESSEL_LENGTH_M,
    p.MAXIMUM_VESSEL_DRAFT_M            AS DEST_MAX_VESSEL_DRAFT_M,
    p.SUPPLY_RATING                     AS DEST_SUPPLY_RATING,
    p.SUPPLY_SCORE                      AS DEST_SUPPLY_SCORE,
    p.COMMUNICATION_RATING              AS DEST_COMMUNICATION_RATING,
    p.FACILITIES_CONTAINER              AS DEST_CONTAINER_FACILITY,
    p.FACILITIES_OIL_TERMINAL           AS DEST_OIL_TERMINAL,
    p.FACILITIES_LIQUID_BULK            AS DEST_LIQUID_BULK,

    -- ── Match quality flag ───────────────────────────────────────────────────
    CASE
        WHEN p.WORLD_PORT_INDEX_NUMBER IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS DESTINATION_PORT_FOUND,

    -- ── Added Vessel Activity Status ─────────────────────────────────────────
    CASE 
        WHEN v.REPORT_DATE >= DATEADD(hour, -48, CURRENT_TIMESTAMP()) THEN 'Active'
        ELSE 'Inactive'
    END AS VESSEL_STATUS

FROM {{ ref('gold_vessels') }}    v
LEFT JOIN {{ ref('port_aliases') }} a
    ON UPPER(TRIM(v.DESTINATION_PORT_NAME)) = UPPER(TRIM(a.RAW_NAME))
LEFT JOIN {{ ref('gold_ports') }} p
    ON UPPER(TRIM(COALESCE(a.MAPPED_NAME, v.DESTINATION_PORT_NAME))) = UPPER(TRIM(p.MAIN_PORT_NAME))
    -- 🛠️ [تعديل 7] تم إيقاف شرط مطابقة الدولة لتجنب فشل الربط
    /*
    AND (
        v.DESTINATION_PORT_COUNTRY IS NULL
        OR UPPER(TRIM(v.DESTINATION_PORT_COUNTRY)) = UPPER(TRIM(p.COUNTRY_CODE))
    )
    */