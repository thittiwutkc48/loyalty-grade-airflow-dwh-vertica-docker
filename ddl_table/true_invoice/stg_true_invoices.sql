
CREATE TABLE CLYMAPPO.stg_true_invoices (
    row_key varchar(60),
	notification_id varchar(100) ,
	event_begin_time TIMESTAMP ,
	billing_seq_no varchar(100) ,
	ban varchar(50) ,
    account_id varchar(50),
	billing_charge_seq_no varchar(100) ,
	billing_sub_no varchar(100) ,
	charge_type varchar(10) ,
	amount float ,
	tax_amount float ,
	activity_date varchar(100) ,
	coverage_period_start_date varchar(100),
	coverage_period_end_date varchar(100) ,
	group_type varchar(10) ,
	revenue_code varchar(10) ,
	discount_code varchar(10) ,
    bill_month VARCHAR(10),
    months_between VARCHAR(10),
    transaction_date DATE,
    partition_date DATE ,
    partition_month VARCHAR(10) ,
    processed_date DATE	
)
UNSEGMENTED ALL NODES
PARTITION BY partition_date;
;

CREATE PROJECTION CLYMAPPO.stg_true_invoices_super
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
SELECT 
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
FROM CLYMAPPO.stg_true_invoices
ORDER BY partition_date, transaction_date, event_begin_time, ban
SEGMENTED BY HASH(
    partition_date, 
    partition_month, 
    row_key,
    event_begin_time,
    ban, 
    partition_date, 
    billing_seq_no, 
    notification_id,
    coverage_period_start_date,
    coverage_period_end_date,
    discount_code,
    bill_month,
    months_between
    ) ALL NODES;


-- {
--   "notificationId": "2c437548-6a4e-3088-e063-b43610ac9398",
--   "eventBeginTime": "2025-01-22T09:38:06.000+07:00",
--   "billingSeqNo": "1586334472-303495967-20250122093806",
--   "ban": 303495967,
--   "accountId": 303495967,
--   "billingChargeSeqNo": ,
--   "billingSubNo": ,
--   "chargeType": "CRD",
--   "amount": -55,
--   "taxAmount": -3.85,
--   "activityDate": "2025-01-22T00:00:00.000+07:00",
--   "coveragePeriodStartDate": "2025-01-22T00:00:00.000+07:00",
--   "coveragePeriodEndDate": "2025-01-22T00:00:00.000+07:00",
--   "groupType": "IMCG",
--   "revenueCode": "UC",
--   "discountCode": 
-- }

-- INSERT INTO CLYMAPPO.stg_tru_ccbs_kf_invoice_errors (
--     rowkey,
--     partitions,
--     "timestamp",
--     topic,
--     raw,
--     error_message,
--     "key",
--     offset,
--     par_key,
--     kafkadate,
--     header,
--     vc_par_key,
--     loadtimestamp,
--     vc_syn_date
-- )
-- SELECT 
--     a.rowkey,
--     a.partitions,
--     a."timestamp",
--     a.topic,
--     a.raw,
--     'Json Data Issue' AS error_message,
--     a."key",
--     a.offset,
--     a.par_key,
--     a.kafkadate,
--     a.header,
--     a.vc_par_key,
--     a.loadtimestamp,
--     a.vc_syn_date
-- FROM 
--     CLYMAPPO.stg_tru_ccbs_kf_invoice a
-- LEFT JOIN 
--     CLYMAPPO.stg_true_invoices b
--     ON a.rowkey = b.rowkey
-- WHERE 
--     b.rowkey IS ;
