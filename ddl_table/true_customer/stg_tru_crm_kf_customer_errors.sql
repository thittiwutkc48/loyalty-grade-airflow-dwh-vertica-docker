

CREATE TABLE CLYMAPPO.stg_tru_crm_kf_customer_errors
(
    rowkey varchar(80),
    topic varchar(80),
    raw long varchar(500000),
    error_message varchar(80),
    key varchar(80),
    header varchar(80),
    vc_par_key date,
    vc_syn_date timestamp DEFAULT (now())::timestamp
)
PARTITION BY (stg_tru_crm_kf_customer_errors.vc_par_key);


CREATE PROJECTION CLYMAPPO.stg_tru_crm_kf_customer_errors_super /*+basename(stg_tru_crm_kf_customer_errors),createtype(P)*/ 
(
 rowkey,
 topic,
 raw,
 error_message,
 key,
 header,
 vc_par_key,
 vc_syn_date
)
AS
 SELECT stg_tru_crm_kf_customer_errors.rowkey,
        stg_tru_crm_kf_customer_errors.topic,
        stg_tru_crm_kf_customer_errors.raw,
        stg_tru_crm_kf_customer_errors.error_message,
        stg_tru_crm_kf_customer_errors.key,
        stg_tru_crm_kf_customer_errors.header,
        stg_tru_crm_kf_customer_errors.vc_par_key,
        stg_tru_crm_kf_customer_errors.vc_syn_date
 FROM CLYMAPPO.stg_tru_crm_kf_customer_errors
 ORDER BY stg_tru_crm_kf_customer_errors.rowkey,
          stg_tru_crm_kf_customer_errors.topic,
          stg_tru_crm_kf_customer_errors.error_message,
          stg_tru_crm_kf_customer_errors.key,
          stg_tru_crm_kf_customer_errors.header,
          stg_tru_crm_kf_customer_errors.vc_par_key,
          stg_tru_crm_kf_customer_errors.vc_syn_date
UNSEGMENTED ALL NODES;

CREATE PROJECTION CLYMAPPO.stg_tru_crm_kf_customer_errors_super_v1
(
 rowkey,
 topic,
 raw,
 error_message,
 key,
 header,
 vc_par_key,
 vc_syn_date
)
AS
 SELECT stg_tru_crm_kf_customer_errors.rowkey,
        stg_tru_crm_kf_customer_errors.topic,
        stg_tru_crm_kf_customer_errors.raw,
        stg_tru_crm_kf_customer_errors.error_message,
        stg_tru_crm_kf_customer_errors.key,
        stg_tru_crm_kf_customer_errors.header,
        stg_tru_crm_kf_customer_errors.vc_par_key,
        stg_tru_crm_kf_customer_errors.vc_syn_date
 FROM CLYMAPPO.stg_tru_crm_kf_customer_errors
 ORDER BY stg_tru_crm_kf_customer_errors.rowkey,
          stg_tru_crm_kf_customer_errors.topic,
          stg_tru_crm_kf_customer_errors.raw,
          stg_tru_crm_kf_customer_errors.error_message,
          stg_tru_crm_kf_customer_errors.key,
          stg_tru_crm_kf_customer_errors.header
SEGMENTED BY hash(stg_tru_crm_kf_customer_errors.vc_par_key, stg_tru_crm_kf_customer_errors.vc_syn_date) ALL NODES;


SELECT MARK_DESIGN_KSAFE(1);
