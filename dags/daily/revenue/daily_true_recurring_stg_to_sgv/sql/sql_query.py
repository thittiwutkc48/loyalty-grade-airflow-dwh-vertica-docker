delete_duplicates_staging = """
DELETE FROM {{ grading_schema }}.{{ staging_table }} 
WHERE 
    partition_date = '{{ partition_date }}';
"""

extract_to_staging = """
INSERT INTO {{ grading_schema }}.{{ staging_table }} 
SELECT  
    notificationid AS notification_id,
    NULL AS request_id,
    toppingid AS topping_id,
    chainid AS chain_id,
    eventbegintime AS event_begin_time,
    msisdn,
    imsi,
    accounttype AS account_type,
    effectivedate AS effective_date,
    expiredate AS expire_date,
    price,
    subscriberpriceplancode AS subscriber_price_plan_code,
    result,
    TO_CHAR(hd_loaddate, 'YYYY-MM-DD')::DATE AS transaction_date,
    TO_CHAR(vc_syn_date, 'YYYY-MM-DD')::DATE AS partition_date,
    TO_CHAR(vc_syn_date, 'YYYYMM') AS partition_month,
    CURRENT_DATE() AS processed_date
FROM {{ grading_schema }}.{{ source_table }} 
WHERE TO_CHAR(vc_syn_date, 'YYYY-MM-DD') = '{{ partition_date }}';
"""

transform_to_single_view = """
INSERT INTO {{ grading_schema }}.{{ single_view_table }}
WITH rank_topping AS (
    SELECT 
        notification_id,
        request_id,
        topping_id,
        chain_id,
        msisdn,
        imsi,
        account_type,
        'RC' AS topping_type,
        effective_date,
        expire_date,
        price,
        subscriber_price_plan_code,
        transaction_date,
        partition_date,
        partition_month,
        processed_date,
        ROW_NUMBER() OVER (PARTITION BY topping_id, notification_id, msisdn ORDER BY event_begin_time DESC) AS rn
    FROM {{ grading_schema }}.{{ staging_table }}
    WHERE partition_date = '{{ partition_date }}' 
        AND price > 0
        AND UPPER(result) = 'SUCCESS'
)
SELECT 
    notification_id,
    request_id,
    topping_id,
    chain_id,
    CONCAT('0', SUBSTRING(msisdn FROM 3)) AS msisdn,
    imsi,
    account_type,
    'RECURRING' AS topping_type,
    effective_date,
    expire_date,
    price/10000 AS price,
    subscriber_price_plan_code,
    transaction_date,
    partition_date,
    partition_month,
    processed_date
FROM rank_topping
WHERE rn = 1;
"""