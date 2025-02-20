CREATE TABLE
    CLYMAPPO.calendar AS
SELECT
    date_part('DAY', slice_time)     AS DAY,
    date_part('WEEK', slice_time)    AS week,
    date_part('YEAR', slice_time)    AS YEAR,
    date_part('MONTH', slice_time)   AS MONTH,
    date_part('QUARTER', slice_time) AS quarter
FROM
    (
        SELECT
            '2024-01-01 00:00:00' AS sample_date
        UNION
        SELECT
            '2024-12-31 00:00:00' AS sample_date ) a TIMESERIES slice_time AS '1 DAY' OVER(ORDER BY
    sample_date::VARCHAR::TIMESTAMP)