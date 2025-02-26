delete_duplicates_staging = """
DELETE FROM {{ grading_schema }}.{{ staging_table }} 
WHERE partition_date = '{{ partition_date }}';
"""

delete_duplicates_single_view = """
DELETE FROM {{ grading_schema }}.{{ single_view_invoice_table }}
WHERE partition_date = '{{ partition_date }}'
AND EXISTS (
    SELECT 1 FROM {{ grading_schema }}.{{ staging_table }} 
    WHERE partition_date = '{{ partition_date }}'
);
"""

extract_to_staging = """
INSERT INTO {{ grading_schema }}.{{ staging_table }}
SELECT 
    invoice_id,
    invoice_no,
    parent_invoice_id,
    trans_type AS transaction_type,
    acct_id AS account_id,
    NULL AS customer_no, 
    CASE 
        WHEN LENGTH(pri_identity) > 0 AND pri_identity IS NOT NULL
        THEN VoltageSecureAccess(pri_identity USING PARAMETERS format='AlphaNumeric', config_dfs_path='/voltagesecure/conf')
        ELSE pri_identity
    END AS product_id,
    invoice_amt AS invoice_amount,
    tax_amt AS tax_amount,
    invoice_date,
    due_date,
    invoice_status,
    bill_cycle_id,
    TO_CHAR(invoice_date, 'YYYYMM') AS bill_month,
    last_update_date,
    entry_date,
    load_date,
    TO_CHAR(file_date, 'YYYY-MM-DD')::DATE AS transaction_date, 
    TO_CHAR(load_date, 'YYYY-MM-DD')::DATE AS partition_date, 
    TO_CHAR(load_date, 'YYYY-MM') AS partition_month, 
    CURRENT_DATE AS processed_date
FROM {{ grading_schema }}.{{ source_table }}
WHERE TO_CHAR(load_date, 'YYYY-MM-DD') = '{{ partition_date }}'
;
"""

transform_to_single_view = """
INSERT INTO {{ grading_schema }}.{{ single_view_invoice_table }}
WITH RankedInvoices AS (
    SELECT 
        sdi.invoice_id, 
        sdi.invoice_no, 
        sdi.parent_invoice_id, 
        sdi.transaction_type, 
        sdi.account_id, 
        sdab.customer_no, 
        sdi.product_id, 
        sdi.invoice_amount, 
        sdi.tax_amount, 
        sdi.invoice_date, 
        sdi.due_date, 
        sdi.invoice_status, 
        sdi.bill_cycle_id, 
        sdi.bill_month, 
        sdi.last_update_date, 
        sdi.entry_date, 
        sdi.load_date, 
        sdi.transaction_date, 
        sdi.partition_date, 
        sdi.partition_month, 
        sdi.processed_date,
        SUBSTRING(sdi.invoice_no FROM 8 FOR 1) AS indy_flag,
        ROW_NUMBER() OVER (
            PARTITION BY sdi.invoice_no , sdi.invoice_id
            ORDER BY sdi.bill_cycle_id DESC, sdi.last_update_date DESC
        ) AS rn
    FROM {{ grading_schema }}.{{ staging_table }} sdi 
    LEFT JOIN {{ grading_schema }}.{{ single_view_balance_table }} sdab 
    ON sdi.account_id = sdab.account_id
    WHERE sdi.partition_date = '{{ partition_date }}' 
    AND sdi.transaction_type IN ('CBF', 'DNT', 'BLL')
)
SELECT 
    invoice_id, 
    invoice_no, 
    parent_invoice_id, 
    transaction_type, 
    account_id, 
    customer_no, 
    CASE 
        WHEN LEFT(product_id, 2) = '66' 
        THEN CONCAT('0', SUBSTRING(product_id FROM 3)) 
        ELSE product_id 
    END AS product_id,
    invoice_amount, 
    tax_amount, 
    invoice_date, 
    due_date, 
    invoice_status, 
    bill_cycle_id, 
    bill_month, 
    last_update_date, 
    entry_date, 
    load_date, 
    transaction_date, 
    partition_date, 
    partition_month, 
    processed_date
FROM RankedInvoices
WHERE rn = 1 
AND indy_flag = 'I';
"""