delete_duplicates_staging = """
DELETE FROM {{ grading_schema }}.{{ staging_table }} 
WHERE 
    partition_date = '{{ partition_date }}';
"""

extract_to_staging = """
INSERT INTO {{ grading_schema }}.{{ staging_table }} 
SELECT 
    *, 
    '{{ partition_date }}'::DATE AS partition_date
FROM 
    {{ dwh_schema }}.{{ source_table }}
WHERE 
    substr(eventBeginTime, 1, 10) = '{{ partition_date }}';
"""

delete_duplicates_single_view = """
DELETE FROM {{ grading_schema }}.{{ single_view }} 
WHERE 
    partition_date = '{{ partition_date }}';
"""

transform_to_single_view = """
INSERT INTO {{ grading_schema }}.{{ single_view }}
WITH ranked_invoices AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY partition_date, ban, billingsubno
            ORDER BY eventbegintime DESC, partition_date DESC
        ) AS row_num
    FROM 
        grade.true_invoice_staging
    WHERE 
        partition_date = '{{ partition_date }}'
)
SELECT  
    ban, 
    billingchargeseqno AS billing_charge_seq_no, 
    billingsubno AS billing_sub_no, 
    chargetype AS charge_type, 
    CAST(amount AS NUMERIC) AS amount, 
    CAST(taxamount AS NUMERIC) AS tax_amount, 
    activitydate AS activity_date, 
    coverageperiodstartdate AS coverage_period_start_date, 
    coverageperiodenddate AS coverage_period_end_date, 
    grouptype AS group_type, 
    revenuecode AS revenue_code, 
    discountcode AS discount_code, 
    NULL AS due_date, 
    NULL AS invoice_status, 
    NULL AS open_amount, 
    NULL AS open_tax_amount,
    CURRENT_TIMESTAMP AS create_date,           
    CURRENT_TIMESTAMP AS last_update_date,          
    'TRUE' AS operator_name,
    partition_date,
    CURRENT_DATE AS processed_date
FROM 
    ranked_invoices
WHERE 
    row_num = 1;
"""