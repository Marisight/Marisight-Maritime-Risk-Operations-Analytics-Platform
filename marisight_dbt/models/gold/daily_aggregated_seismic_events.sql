{{ config(materialized='table', schema='gold') }}

WITH daily_stats AS (
    SELECT
        date_trunc('day', earthquake_time) as event_date,
        region,
        count(event_id) as total_events,
        avg(magnitude) as avg_magnitude,
        max(magnitude) as max_magnitude,
        sum(CASE WHEN magnitude >= 7 THEN 2 ELSE 1 END) as seismic_intensity_index
    FROM {{ ref('gold_seismic_events') }}
    GROUP BY 1, 2
),
rolling_stats AS (
    SELECT
        *,
        -- حساب متوسط عدد الزلازل خلال الـ 7 أيام الماضية
        avg(total_events) OVER (
            PARTITION BY region 
            ORDER BY event_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7d_avg_events
    FROM daily_stats
)

SELECT
    event_date,
    region,
    total_events,
    round(avg_magnitude, 2) as avg_magnitude,
    max_magnitude,
    round(rolling_7d_avg_events, 1) as rolling_7d_avg_events,
    CASE 
        WHEN max_magnitude >= 7.0 OR seismic_intensity_index > 5 THEN 'CRITICAL'
        WHEN max_magnitude >= 5.5 OR total_events > (rolling_7d_avg_events * 1.5) THEN 'HIGH'
        WHEN max_magnitude >= 4.0 THEN 'MODERATE'
        ELSE 'LOW'
    END as daily_risk_level
FROM rolling_stats
ORDER BY event_date DESC