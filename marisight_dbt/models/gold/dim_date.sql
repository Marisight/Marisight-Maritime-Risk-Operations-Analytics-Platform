-- models/star/dim_date.sql
{{ config(materialized='table', schema='star') }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2025-01-01' as date)",
        end_date="cast('2027-01-01' as date)"
    ) }}
)

SELECT
    TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD'))  AS DATE_SK,
    date_day                                   AS FULL_DATE,
    EXTRACT(YEAR  FROM date_day)               AS YEAR,
    EXTRACT(MONTH FROM date_day)               AS MONTH,
    EXTRACT(DAY   FROM date_day)               AS DAY,
    EXTRACT(WEEK  FROM date_day)               AS WEEK_OF_YEAR,
    EXTRACT(QUARTER FROM date_day)             AS QUARTER,
    DAYNAME(date_day)                          AS DAY_NAME,
    MONTHNAME(date_day)                        AS MONTH_NAME,
    DATE_TRUNC('month', date_day)              AS MONTH_START_DATE,
    DATE_TRUNC('year',  date_day)              AS YEAR_START_DATE
FROM date_spine