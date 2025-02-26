delete_duplicates_staging = """
DELETE FROM {{ grading_schema }}.{{ staging_table }} 
WHERE partition_date = '{{ partition_date }}';
"""

extract_to_staging = """
INSERT INTO {{ grading_schema }}.{{ staging_table }}
SELECT 
    acct_id AS account_id,
    acct_code AS customer_no,
    ar_balance,
    open_amt AS open_amount,
    unapplied_amt AS unapplied_amount,
    dispute_amt AS dispute_amount,
    pending_cr_amt AS pending_cr_amount,
    last_trx_date,
    balance_update_date,
    last_payment_date,
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
MERGE INTO {{ grading_schema }}.{{ single_view_table }} AS target
USING (
    SELECT * 
    FROM {{ grading_schema }}.{{ staging_table }}
    WHERE partition_date = '{{ partition_date }}'
) AS source
ON target.account_id = source.account_id
AND target.customer_no = source.customer_no
WHEN MATCHED THEN 
    UPDATE SET 
        ar_balance = source.ar_balance,
        unapplied_amount = source.unapplied_amount,
        open_amount = source.open_amount,
        dispute_amount = source.dispute_amount,
        balance_update_date = source.balance_update_date,
        pending_cr_amount = source.pending_cr_amount,
        last_payment_date = source.last_payment_date,
        last_trx_date = source.last_trx_date,
        load_date = source.load_date,
        update_at = CURRENT_TIMESTAMP,
        partition_date = source.partition_date,
        processed_date = source.processed_date,
        transaction_date = source.transaction_date,
        partition_month = source.partition_month
WHEN NOT MATCHED THEN 
    INSERT (
        account_id,
        customer_no,
        ar_balance,
        unapplied_amount,
        open_amount,
        dispute_amount,
        balance_update_date,
        pending_cr_amount,
        last_payment_date,
        last_trx_date,
        load_date,
        create_at,
        update_at,
        partition_date,
        processed_date,
        transaction_date,
        partition_month
    ) 
    VALUES (
        source.account_id,
        source.customer_no,
        source.ar_balance,
        source.unapplied_amount,
        source.open_amount,
        source.dispute_amount,
        source.balance_update_date,
        source.pending_cr_amount,
        source.last_payment_date,
        source.last_trx_date,
        source.load_date,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        source.partition_date,
        source.processed_date,
        source.transaction_date,
        source.partition_month
    );
"""