CREATE TABLE CLYMAPPO.sgv_true_invoices (
    ban VARCHAR(50),
    billing_charge_seq_no VARCHAR(100),
    billing_sub_no VARCHAR(100),
    billing_seq_no VARCHAR(100),
    account_id VARCHAR(50),  
    charge_type VARCHAR(10),
    amount FLOAT,
    tax_amount FLOAT,
    activity_date VARCHAR(100),
    coverage_period_start_date VARCHAR(100),
    coverage_period_end_date VARCHAR(100),
    group_type VARCHAR(10),
    revenue_code VARCHAR(10),
    discount_code VARCHAR(10),
    bill_month VARCHAR(10),
    months_between VARCHAR(10),
    transaction_date DATE,
    partition_date DATE,  
    partition_month VARCHAR(10),
    processed_date DATE
    )  
UNSEGMENTED ALL NODES
PARTITION BY partition_date;


CREATE PROJECTION CLYMAPPO.sgv_true_invoices_super
(
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
)
AS 
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
FROM CLYMAPPO.sgv_true_invoices
ORDER BY partition_date, transaction_date, ban
SEGMENTED BY HASH(
    partition_date, 
    partition_month, 
    ban,
    billing_charge_seq_no,
    billing_sub_no,
    billing_seq_no,
    account_id,  
    coverage_period_start_date,
    coverage_period_end_date,
    discount_code,
    bill_month,
    months_between
) ALL NODES;

