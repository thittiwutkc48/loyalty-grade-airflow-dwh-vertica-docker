

CREATE TABLE CLYMAPPO.stg_true_invoices
(
    row_key varchar(60),
    notification_id varchar(100),
    event_begin_time timestamp,
    billing_seq_no varchar(100),
    ban varchar(50),
    account_id varchar(50),
    billing_charge_seq_no varchar(100),
    billing_sub_no varchar(100),
    charge_type varchar(10),
    amount float,
    tax_amount float,
    activity_date varchar(100),
    coverage_period_start_date varchar(100),
    coverage_period_end_date varchar(100),
    group_type varchar(10),
    revenue_code varchar(10),
    discount_code varchar(10),
    bill_month varchar(10),
    months_between varchar(10),
    transaction_date date,
    partition_date date,
    partition_month varchar(10),
    processed_date date
)
PARTITION BY (stg_true_invoices.partition_date);


CREATE PROJECTION CLYMAPPO.stg_true_invoices_super /*+basename(stg_true_invoices),createtype(P)*/ 
(
 row_key,
 notification_id,
 event_begin_time,
 billing_seq_no,
 ban,
 account_id,
 billing_charge_seq_no,
 billing_sub_no,
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
 SELECT stg_true_invoices.row_key,
        stg_true_invoices.notification_id,
        stg_true_invoices.event_begin_time,
        stg_true_invoices.billing_seq_no,
        stg_true_invoices.ban,
        stg_true_invoices.account_id,
        stg_true_invoices.billing_charge_seq_no,
        stg_true_invoices.billing_sub_no,
        stg_true_invoices.charge_type,
        stg_true_invoices.amount,
        stg_true_invoices.tax_amount,
        stg_true_invoices.activity_date,
        stg_true_invoices.coverage_period_start_date,
        stg_true_invoices.coverage_period_end_date,
        stg_true_invoices.group_type,
        stg_true_invoices.revenue_code,
        stg_true_invoices.discount_code,
        stg_true_invoices.bill_month,
        stg_true_invoices.months_between,
        stg_true_invoices.transaction_date,
        stg_true_invoices.partition_date,
        stg_true_invoices.partition_month,
        stg_true_invoices.processed_date
 FROM CLYMAPPO.stg_true_invoices
 ORDER BY stg_true_invoices.row_key,
          stg_true_invoices.notification_id,
          stg_true_invoices.event_begin_time,
          stg_true_invoices.billing_seq_no,
          stg_true_invoices.ban,
          stg_true_invoices.account_id,
          stg_true_invoices.billing_charge_seq_no,
          stg_true_invoices.billing_sub_no
UNSEGMENTED ALL NODES;


SELECT MARK_DESIGN_KSAFE(1);
