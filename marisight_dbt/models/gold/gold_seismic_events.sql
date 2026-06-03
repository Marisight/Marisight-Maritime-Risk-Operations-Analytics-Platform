{{ config(materialized='table', schema='gold') }}

SELECT 
    EVENT_ID,
    REGION,
    MAGNITUDE,
    DEPTH_KM,
    EARTHQUAKE_TIME,
    LATITUDE,     -- تأكدي أن هذه موجودة في الـ SELECT
    LONGITUDE,    -- وهذه أيضاً
    CASE 
        WHEN MAGNITUDE >= 7.0 THEN 'Major'
        WHEN MAGNITUDE >= 5.0 THEN 'Moderate'
        ELSE 'Minor'
    END as MAGNITUDE_CLASS
FROM {{ ref('silver_seismic_events') }}