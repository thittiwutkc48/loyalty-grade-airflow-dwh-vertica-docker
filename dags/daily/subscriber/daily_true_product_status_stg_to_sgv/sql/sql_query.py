delete_duplicates_staging = """
DELETE FROM {{ grading_schema }}.{{ staging_table }} 
WHERE partition_date = '{{ partition_date }}';
"""

extract_to_staging = """
INSERT INTO {{ grading_schema }}.{{ staging_table }} 
SELECT  
	rowkey AS row_key,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'notificationId')::VARCHAR AS notification_id,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'eventBeginTime')::TIMESTAMP AS event_begin_time,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'billingSeqNo')::VARCHAR AS billing_seq_no,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'billingSubNo')::VARCHAR AS billing_sub_no,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'productId')::VARCHAR AS product_id,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'sysCreationDate')::TIMESTAMP AS sys_creation_date,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'sysUpdateDate')::TIMESTAMP AS sys_update_date,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'subStatus')::VARCHAR AS sub_status,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'subStatusRsn')::VARCHAR AS sub_status_rsn,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'barByReq')::BOOLEAN AS bar_by_req,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'barByUsage')::BOOLEAN AS bar_by_usage,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'collRsn')::VARCHAR AS coll_rsn,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'creditRsn')::VARCHAR AS credit_rsn,
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

transform_to_single_view = """
INSERT INTO {{ grading_schema }}.{{ single_view_table }}
WITH RankedProductST AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id 
            ORDER BY event_begin_time DESC
        ) AS row_rank
    FROM {{ grading_schema }}.{{ staging_table }} 
    WHERE partition_date = '{{ partition_date }}' 
)
SELECT 
    billing_seq_no,  
    billing_sub_no,  
    product_id,  
    sys_creation_date,  
    sys_update_date,  
    sub_status,  
    sub_status_rsn,  
    bar_by_req,  
    bar_by_usage,  
    coll_rsn,  
    credit_rsn,  
    transaction_date,  
    partition_date,
    partition_month,  
    processed_date
FROM RankedProductST  
WHERE row_rank = 1;
"""