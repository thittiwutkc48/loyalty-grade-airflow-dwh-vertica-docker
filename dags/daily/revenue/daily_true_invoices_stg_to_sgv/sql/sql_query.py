delete_duplicates_staging = """
DELETE FROM {{ grading_schema }}.{{ staging_table }} 
WHERE partition_date = '{{ partition_date }}';
"""

extract_to_staging = """
INSERT INTO {{ grading_schema }}.{{ staging_table }} 
SELECT 
    rowkey,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'notificationId')::VARCHAR AS notification_id,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'eventBeginTime')::TIMESTAMP AS event_begin_time,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'billingSeqNo')::VARCHAR AS billing_seq_no,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'ban')::INTEGER AS ban,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'accountId')::INTEGER AS account_id,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'billingChargeSeqNo')::VARCHAR AS billing_charge_seq_no,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'billingSubNo')::VARCHAR AS billing_sub_no,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'chargeType')::VARCHAR AS charge_type,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'amount')::NUMERIC AS amount,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'taxAmount')::NUMERIC AS tax_amount,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'activityDate')::TIMESTAMP AS activity_date,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'coveragePeriodStartDate')::TIMESTAMP AS coverage_period_start_date,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'coveragePeriodEndDate')::TIMESTAMP AS coverage_period_end_date,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'groupType')::VARCHAR AS group_type,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'revenueCode')::VARCHAR AS revenue_code,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'discountCode')::VARCHAR AS discount_code,
    TO_CHAR(MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'coveragePeriodStartDate')::TIMESTAMP, 'YYYYMM') AS bill_month,
    CASE 
        WHEN CEIL(DATEDIFF('month', 
                MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'coveragePeriodStartDate')::TIMESTAMP, 
                MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'coveragePeriodEndDate')::TIMESTAMP)) = 0 
        THEN 1
        ELSE CEIL(DATEDIFF('month', 
                MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'coveragePeriodStartDate')::TIMESTAMP, 
                MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'coveragePeriodEndDate')::TIMESTAMP))
    END AS months_between,
    TO_CHAR(vc_par_key, 'YYYY-MM-DD')::DATE AS transaction_date,
    TO_CHAR(vc_syn_date, 'YYYY-MM-DD')::DATE AS partition_date,
    TO_CHAR(vc_syn_date, 'YYYYMM') AS partition_month,
    CURRENT_DATE() AS processed_date
FROM {{ grading_schema }}.{{ source_table }} 
WHERE TO_CHAR(vc_syn_date, 'YYYY-MM-DD') = '{{ partition_date }}';
"""

validate_json = """
INSERT INTO {{ grading_schema }}.{{ source_error_table }} 
SELECT 
    a.rowkey,
    a.topic,
    a.raw,
    'Json Data Issue' AS error_message,
    a."key",
    a.header,
    CAST(a.vc_par_key AS DATE) AS vc_par_key,
    a.vc_syn_date
FROM {{ grading_schema }}.{{ source_table }} a
LEFT JOIN 
    (SELECT row_key FROM {{ grading_schema }}.{{ staging_table }}
     WHERE partition_date = '{{ partition_date }}') b
    ON a.rowkey = b.row_key
WHERE 
    TO_CHAR(vc_syn_date, 'YYYY-MM-DD')::DATE = '{{ partition_date }}' 
    AND b.row_key IS NULL;
"""

delete_duplicates_single_view = """
DELETE FROM {{ grading_schema }}.{{ single_view_table }}
WHERE partition_date = '{{ partition_date }}'
AND EXISTS (
    SELECT 1 FROM {{ grading_schema }}.{{ staging_table }} 
    WHERE partition_date = '{{ partition_date }}'
);
"""

transform_to_single_view = """
INSERT INTO {{ grading_schema }}.{{ single_view_table }}
WITH RECURSIVE RankedInvoices AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY notification_id, ban ORDER BY event_begin_time DESC) AS row_rank
    FROM {{ grading_schema }}.{{ staging_table }}
    WHERE partition_date = '{{ partition_date }}'
),
FilteredInvoices AS (
    SELECT * FROM RankedInvoices 
    WHERE row_rank = 1 
        AND UPPER(discount_code) NOT IN ({{ discount_codes }})
        OR discount_code IS NULL
),
MonthSeries AS (
    SELECT 
        ban,
        billing_charge_seq_no,
        billing_sub_no,
        billing_seq_no,
        account_id,
        charge_type,
        amount / months_between AS amount,
        tax_amount / months_between AS tax_amount,
        activity_date,
        coverage_period_start_date,
        coverage_period_end_date,
        group_type,
        revenue_code,
        discount_code,
        bill_month,
        months_between,
        transaction_date,
        partition_date,
        partition_month,
        processed_date,
        1 AS month_index  
    FROM FilteredInvoices 
    UNION ALL
    SELECT
        ms.ban,
        ms.billing_charge_seq_no,
        ms.billing_sub_no,
        ms.billing_seq_no,
        ms.account_id,
        ms.charge_type,
        ms.amount,  
        ms.tax_amount,  
        ms.activity_date,
        TO_CHAR(ADD_MONTHS(TO_TIMESTAMP(ms.coverage_period_start_date, 'YYYY-MM-DD HH24:MI:SS'), 1), 'YYYY-MM-DD HH24:MI:SS') AS coverage_period_start_date, 
        TO_CHAR(ADD_MONTHS(TO_TIMESTAMP(ms.coverage_period_start_date, 'YYYY-MM-DD HH24:MI:SS'), 2), 'YYYY-MM-DD HH24:MI:SS') AS coverage_period_end_date,   
        ms.group_type,
        ms.revenue_code,
        ms.discount_code,
        TO_CHAR(ADD_MONTHS(TO_DATE(ms.bill_month, 'YYYYMM'), 1), 'YYYYMM') AS bill_month,
        ms.months_between,
        ms.transaction_date,
        ms.partition_date,
        ms.partition_month,
        ms.processed_date,
        ms.month_index + 1  
    FROM MonthSeries ms
    WHERE ms.month_index < ms.months_between
)
SELECT 
    ban,
    billing_charge_seq_no,
    billing_sub_no,
    billing_seq_no,
    account_id,
    charge_type,
    amount,  
    tax_amount,  
    activity_date,
    coverage_period_start_date,
    coverage_period_end_date,
    group_type,
    revenue_code,
    discount_code,
    bill_month,
    months_between,
    transaction_date,
    partition_date,
    partition_month,
    processed_date
FROM MonthSeries
ORDER BY months_between, ban, bill_month;
"""