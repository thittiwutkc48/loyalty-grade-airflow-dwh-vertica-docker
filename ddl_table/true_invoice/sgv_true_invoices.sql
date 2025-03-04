

CREATE TABLE CLYMAPPO.sgv_true_invoices
(
    ban varchar(50),
    billing_charge_seq_no varchar(100),
    billing_sub_no varchar(100),
    billing_seq_no varchar(100),
    account_id varchar(50),
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
PARTITION BY (sgv_true_invoices.partition_date);


CREATE PROJECTION CLYMAPPO.sgv_true_invoices_super /*+basename(sgv_true_invoices),createtype(P)*/ 
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
 SELECT sgv_true_invoices.ban,
        sgv_true_invoices.billing_charge_seq_no,
        sgv_true_invoices.billing_sub_no,
        sgv_true_invoices.billing_seq_no,
        sgv_true_invoices.account_id,
        sgv_true_invoices.charge_type,
        sgv_true_invoices.amount,
        sgv_true_invoices.tax_amount,
        sgv_true_invoices.activity_date,
        sgv_true_invoices.coverage_period_start_date,
        sgv_true_invoices.coverage_period_end_date,
        sgv_true_invoices.group_type,
        sgv_true_invoices.revenue_code,
        sgv_true_invoices.discount_code,
        sgv_true_invoices.bill_month,
        sgv_true_invoices.months_between,
        sgv_true_invoices.transaction_date,
        sgv_true_invoices.partition_date,
        sgv_true_invoices.partition_month,
        sgv_true_invoices.processed_date
 FROM CLYMAPPO.sgv_true_invoices
 ORDER BY sgv_true_invoices.ban,
          sgv_true_invoices.billing_charge_seq_no,
          sgv_true_invoices.billing_sub_no,
          sgv_true_invoices.billing_seq_no,
          sgv_true_invoices.account_id,
          sgv_true_invoices.charge_type,
          sgv_true_invoices.amount,
          sgv_true_invoices.tax_amount
UNSEGMENTED ALL NODES;


SELECT MARK_DESIGN_KSAFE(1);
