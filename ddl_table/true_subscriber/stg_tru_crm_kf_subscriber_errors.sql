

CREATE TABLE CLYMAPPO.stg_tru_crm_kf_subscriber_errors
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
PARTITION BY (stg_tru_crm_kf_subscriber_errors.vc_par_key);


CREATE PROJECTION CLYMAPPO.stg_tru_crm_kf_subscriber_errors_super /*+basename(stg_tru_crm_kf_subscriber_errors),createtype(P)*/ 
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
 SELECT stg_tru_crm_kf_subscriber_errors.rowkey,
        stg_tru_crm_kf_subscriber_errors.topic,
        stg_tru_crm_kf_subscriber_errors.raw,
        stg_tru_crm_kf_subscriber_errors.error_message,
        stg_tru_crm_kf_subscriber_errors.key,
        stg_tru_crm_kf_subscriber_errors.header,
        stg_tru_crm_kf_subscriber_errors.vc_par_key,
        stg_tru_crm_kf_subscriber_errors.vc_syn_date
 FROM CLYMAPPO.stg_tru_crm_kf_subscriber_errors
 ORDER BY stg_tru_crm_kf_subscriber_errors.rowkey,
          stg_tru_crm_kf_subscriber_errors.topic,
          stg_tru_crm_kf_subscriber_errors.error_message,
          stg_tru_crm_kf_subscriber_errors.key,
          stg_tru_crm_kf_subscriber_errors.header,
          stg_tru_crm_kf_subscriber_errors.vc_par_key,
          stg_tru_crm_kf_subscriber_errors.vc_syn_date
UNSEGMENTED ALL NODES;


SELECT MARK_DESIGN_KSAFE(1);
