{{ config(materialized='table', schema='gold') }}

WITH seismic_events AS (
    SELECT * FROM {{ ref('gold_seismic_events') }}
),
ports AS (
    SELECT * FROM {{ ref('silver_ports') }}
    WHERE is_valid_coordinates = true
)

SELECT
    -- دمج مباشر للـ ID لضمان الفرادة بدون مكتبات
    concat(e.event_id, '-', p.world_port_index_number) as proximity_id,
    e.event_id,
    e.magnitude,
    p.main_port_name as port_name,
    
    -- حساب المسافة بالمعادلة الرياضية مباشرة
    2 * 6371 * asin(
        sqrt(
            pow(sin(radians((p.latitude - e.latitude) / 2)), 2)
            + cos(radians(e.latitude))
            * cos(radians(p.latitude))
            * pow(sin(radians((p.longitude - e.longitude) / 2)), 2)
        )
    ) as distance_km,

    -- حساب درجة الخطر مباشرة
    round(
        least(100,
            (e.magnitude / 10.0 * 40) + 
            (CASE 
                WHEN e.depth_km < 70 THEN 30
                WHEN e.depth_km BETWEEN 70 AND 300 THEN 15
                ELSE 5 
            END) + 
            (greatest(0, (500 - 
                (2 * 6371 * asin(sqrt(pow(sin(radians((p.latitude - e.latitude) / 2)), 2) + cos(radians(e.latitude)) * cos(radians(p.latitude)) * pow(sin(radians((p.longitude - e.longitude) / 2)), 2))))
            ) / 500) * 30)
        ), 1
    ) as risk_score

FROM seismic_events e
CROSS JOIN ports p
-- التصفية لتقليل الحجم
WHERE distance_km <= 500