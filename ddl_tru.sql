

CREATE TABLE CLYMAPPO.stg_tru_ccbs_kf_invoice
(
    rowkey varchar(80),
    topic varchar(80),
    partitions int,
    "offset" float,
    "timestamp" float,
    raw long varchar(500000),
    key varchar(80),
    header varchar(80),
    kafkadate float,
    loadtimestamp float,
    par_key int,
    vc_par_key date,
    vc_syn_date timestamp DEFAULT (now())::timestamp
)
PARTITION BY (stg_tru_ccbs_kf_invoice.par_key);


CREATE PROJECTION CLYMAPPO.stg_tru_ccbs_kf_invoice_super /*+basename(stg_tru_ccbs_kf_invoice),createtype(L)*/ 
(
 rowkey,
 topic,
 partitions,
 "offset",
 "timestamp",
 raw,
 key,
 header,
 kafkadate,
 loadtimestamp,
 par_key,
 vc_par_key,
 vc_syn_date
)
AS
 SELECT stg_tru_ccbs_kf_invoice.rowkey,
        stg_tru_ccbs_kf_invoice.topic,
        stg_tru_ccbs_kf_invoice.partitions,
        stg_tru_ccbs_kf_invoice."offset",
        stg_tru_ccbs_kf_invoice."timestamp",
        stg_tru_ccbs_kf_invoice.raw,
        stg_tru_ccbs_kf_invoice.key,
        stg_tru_ccbs_kf_invoice.header,
        stg_tru_ccbs_kf_invoice.kafkadate,
        stg_tru_ccbs_kf_invoice.loadtimestamp,
        stg_tru_ccbs_kf_invoice.par_key,
        stg_tru_ccbs_kf_invoice.vc_par_key,
        stg_tru_ccbs_kf_invoice.vc_syn_date
 FROM CLYMAPPO.stg_tru_ccbs_kf_invoice
 ORDER BY stg_tru_ccbs_kf_invoice.rowkey,
          stg_tru_ccbs_kf_invoice.topic,
          stg_tru_ccbs_kf_invoice.partitions,
          stg_tru_ccbs_kf_invoice."offset",
          stg_tru_ccbs_kf_invoice."timestamp",
          stg_tru_ccbs_kf_invoice.raw,
          stg_tru_ccbs_kf_invoice.key,
          stg_tru_ccbs_kf_invoice.header
SEGMENTED BY hash(stg_tru_ccbs_kf_invoice.partitions, stg_tru_ccbs_kf_invoice."offset", stg_tru_ccbs_kf_invoice."timestamp", stg_tru_ccbs_kf_invoice.kafkadate, stg_tru_ccbs_kf_invoice.loadtimestamp, stg_tru_ccbs_kf_invoice.par_key, stg_tru_ccbs_kf_invoice.vc_par_key, stg_tru_ccbs_kf_invoice.vc_syn_date) ALL NODES;


SELECT MARK_DESIGN_KSAFE(1);




********************************



CREATE TABLE CLYMAPPO.stg_tru_ccbs_kf_productstatus
(
    rowkey varchar(80),
    topic varchar(80),
    partitions int,
    "offset" float,
    "timestamp" float,
    raw long varchar(500000),
    key varchar(80),
    header varchar(80),
    kafkadate float,
    loadtimestamp float,
    par_key int,
    vc_par_key date,
    vc_syn_date timestamp DEFAULT (now())::timestamp
)
PARTITION BY (stg_tru_ccbs_kf_productstatus.par_key);


CREATE PROJECTION CLYMAPPO.stg_tru_ccbs_kf_productstatus_super /*+basename(stg_tru_ccbs_kf_productstatus),createtype(L)*/ 
(
 rowkey,
 topic,
 partitions,
 "offset",
 "timestamp",
 raw,
 key,
 header,
 kafkadate,
 loadtimestamp,
 par_key,
 vc_par_key,
 vc_syn_date
)
AS
 SELECT stg_tru_ccbs_kf_productstatus.rowkey,
        stg_tru_ccbs_kf_productstatus.topic,
        stg_tru_ccbs_kf_productstatus.partitions,
        stg_tru_ccbs_kf_productstatus."offset",
        stg_tru_ccbs_kf_productstatus."timestamp",
        stg_tru_ccbs_kf_productstatus.raw,
        stg_tru_ccbs_kf_productstatus.key,
        stg_tru_ccbs_kf_productstatus.header,
        stg_tru_ccbs_kf_productstatus.kafkadate,
        stg_tru_ccbs_kf_productstatus.loadtimestamp,
        stg_tru_ccbs_kf_productstatus.par_key,
        stg_tru_ccbs_kf_productstatus.vc_par_key,
        stg_tru_ccbs_kf_productstatus.vc_syn_date
 FROM CLYMAPPO.stg_tru_ccbs_kf_productstatus
 ORDER BY stg_tru_ccbs_kf_productstatus.rowkey,
          stg_tru_ccbs_kf_productstatus.topic,
          stg_tru_ccbs_kf_productstatus.partitions,
          stg_tru_ccbs_kf_productstatus."offset",
          stg_tru_ccbs_kf_productstatus."timestamp",
          stg_tru_ccbs_kf_productstatus.raw,
          stg_tru_ccbs_kf_productstatus.key,
          stg_tru_ccbs_kf_productstatus.header
SEGMENTED BY hash(stg_tru_ccbs_kf_productstatus.partitions, stg_tru_ccbs_kf_productstatus."offset", stg_tru_ccbs_kf_productstatus."timestamp", stg_tru_ccbs_kf_productstatus.kafkadate, stg_tru_ccbs_kf_productstatus.loadtimestamp, stg_tru_ccbs_kf_productstatus.par_key, stg_tru_ccbs_kf_productstatus.vc_par_key, stg_tru_ccbs_kf_productstatus.vc_syn_date) ALL NODES;


SELECT MARK_DESIGN_KSAFE(1);


**********************



CREATE TABLE CLYMAPPO.stg_tru_ccp_kf_recurring
(
    eventbegintime timestamp,
    eventtype varchar(255),
    msisdn varchar(255),
    notificationid varchar(255),
    accounttype varchar(255),
    action varchar(255),
    chainid int,
    effectivedate timestamp,
    expiredate timestamp,
    expireflag varchar(255),
    imsi varchar(255),
    indipriceplanstartdate timestamp,
    language varchar(255),
    price varchar(255),
    recurringtimes varchar(255),
    result varchar(255),
    subscriberpriceplancode varchar(255),
    toppingid varchar(255),
    loaddate int,
    hd_loaddate timestamp,
    par_key int,
    vc_par_key date,
    vc_syn_date timestamp DEFAULT (now())::timestamp
)
PARTITION BY (stg_tru_ccp_kf_recurring.par_key);


CREATE PROJECTION CLYMAPPO.stg_tru_ccp_kf_recurring_super /*+basename(stg_tru_ccp_kf_recurring),createtype(L)*/ 
(
 eventbegintime,
 eventtype,
 msisdn,
 notificationid,
 accounttype,
 action,
 chainid,
 effectivedate,
 expiredate,
 expireflag,
 imsi,
 indipriceplanstartdate,
 language,
 price,
 recurringtimes,
 result,
 subscriberpriceplancode,
 toppingid,
 loaddate,
 hd_loaddate,
 par_key,
 vc_par_key,
 vc_syn_date
)
AS
 SELECT stg_tru_ccp_kf_recurring.eventbegintime,
        stg_tru_ccp_kf_recurring.eventtype,
        stg_tru_ccp_kf_recurring.msisdn,
        stg_tru_ccp_kf_recurring.notificationid,
        stg_tru_ccp_kf_recurring.accounttype,
        stg_tru_ccp_kf_recurring.action,
        stg_tru_ccp_kf_recurring.chainid,
        stg_tru_ccp_kf_recurring.effectivedate,
        stg_tru_ccp_kf_recurring.expiredate,
        stg_tru_ccp_kf_recurring.expireflag,
        stg_tru_ccp_kf_recurring.imsi,
        stg_tru_ccp_kf_recurring.indipriceplanstartdate,
        stg_tru_ccp_kf_recurring.language,
        stg_tru_ccp_kf_recurring.price,
        stg_tru_ccp_kf_recurring.recurringtimes,
        stg_tru_ccp_kf_recurring.result,
        stg_tru_ccp_kf_recurring.subscriberpriceplancode,
        stg_tru_ccp_kf_recurring.toppingid,
        stg_tru_ccp_kf_recurring.loaddate,
        stg_tru_ccp_kf_recurring.hd_loaddate,
        stg_tru_ccp_kf_recurring.par_key,
        stg_tru_ccp_kf_recurring.vc_par_key,
        stg_tru_ccp_kf_recurring.vc_syn_date
 FROM CLYMAPPO.stg_tru_ccp_kf_recurring
 ORDER BY stg_tru_ccp_kf_recurring.eventbegintime,
          stg_tru_ccp_kf_recurring.eventtype,
          stg_tru_ccp_kf_recurring.msisdn,
          stg_tru_ccp_kf_recurring.notificationid,
          stg_tru_ccp_kf_recurring.accounttype,
          stg_tru_ccp_kf_recurring.action,
          stg_tru_ccp_kf_recurring.chainid,
          stg_tru_ccp_kf_recurring.effectivedate
SEGMENTED BY hash(stg_tru_ccp_kf_recurring.eventbegintime, stg_tru_ccp_kf_recurring.chainid, stg_tru_ccp_kf_recurring.effectivedate, stg_tru_ccp_kf_recurring.expiredate, stg_tru_ccp_kf_recurring.indipriceplanstartdate, stg_tru_ccp_kf_recurring.loaddate, stg_tru_ccp_kf_recurring.hd_loaddate, stg_tru_ccp_kf_recurring.par_key) ALL NODES;


SELECT MARK_DESIGN_KSAFE(1);


***********************



CREATE TABLE CLYMAPPO.stg_tru_ccp_kf_topping
(
    eventbegintime timestamp,
    eventtype varchar(255),
    msisdn varchar(255),
    notificationid varchar(255),
    accounttype varchar(255),
    cella varchar(255),
    chainid int,
    effectivedate timestamp,
    expiredate timestamp,
    extparam1 varchar(255),
    extparam2 varchar(255),
    extparam3 varchar(255),
    extparam4 varchar(255),
    extparam5 varchar(255),
    imsi varchar(255),
    language int,
    price float,
    requestid varchar(255),
    subscriberpriceplancode varchar(255),
    toppingid varchar(255),
    loaddate int,
    hd_loaddate timestamp,
    par_key int,
    vc_par_key date,
    vc_syn_date timestamp DEFAULT (now())::timestamp
)
PARTITION BY (stg_tru_ccp_kf_topping.par_key);


CREATE PROJECTION CLYMAPPO.stg_tru_ccp_kf_topping_super /*+basename(stg_tru_ccp_kf_topping),createtype(L)*/ 
(
 eventbegintime,
 eventtype,
 msisdn,
 notificationid,
 accounttype,
 cella,
 chainid,
 effectivedate,
 expiredate,
 extparam1,
 extparam2,
 extparam3,
 extparam4,
 extparam5,
 imsi,
 language,
 price,
 requestid,
 subscriberpriceplancode,
 toppingid,
 loaddate,
 hd_loaddate,
 par_key,
 vc_par_key,
 vc_syn_date
)
AS
 SELECT stg_tru_ccp_kf_topping.eventbegintime,
        stg_tru_ccp_kf_topping.eventtype,
        stg_tru_ccp_kf_topping.msisdn,
        stg_tru_ccp_kf_topping.notificationid,
        stg_tru_ccp_kf_topping.accounttype,
        stg_tru_ccp_kf_topping.cella,
        stg_tru_ccp_kf_topping.chainid,
        stg_tru_ccp_kf_topping.effectivedate,
        stg_tru_ccp_kf_topping.expiredate,
        stg_tru_ccp_kf_topping.extparam1,
        stg_tru_ccp_kf_topping.extparam2,
        stg_tru_ccp_kf_topping.extparam3,
        stg_tru_ccp_kf_topping.extparam4,
        stg_tru_ccp_kf_topping.extparam5,
        stg_tru_ccp_kf_topping.imsi,
        stg_tru_ccp_kf_topping.language,
        stg_tru_ccp_kf_topping.price,
        stg_tru_ccp_kf_topping.requestid,
        stg_tru_ccp_kf_topping.subscriberpriceplancode,
        stg_tru_ccp_kf_topping.toppingid,
        stg_tru_ccp_kf_topping.loaddate,
        stg_tru_ccp_kf_topping.hd_loaddate,
        stg_tru_ccp_kf_topping.par_key,
        stg_tru_ccp_kf_topping.vc_par_key,
        stg_tru_ccp_kf_topping.vc_syn_date
 FROM CLYMAPPO.stg_tru_ccp_kf_topping
 ORDER BY stg_tru_ccp_kf_topping.eventbegintime,
          stg_tru_ccp_kf_topping.eventtype,
          stg_tru_ccp_kf_topping.msisdn,
          stg_tru_ccp_kf_topping.notificationid,
          stg_tru_ccp_kf_topping.accounttype,
          stg_tru_ccp_kf_topping.cella,
          stg_tru_ccp_kf_topping.chainid,
          stg_tru_ccp_kf_topping.effectivedate
SEGMENTED BY hash(stg_tru_ccp_kf_topping.eventbegintime, stg_tru_ccp_kf_topping.chainid, stg_tru_ccp_kf_topping.effectivedate, stg_tru_ccp_kf_topping.expiredate, stg_tru_ccp_kf_topping.language, stg_tru_ccp_kf_topping.price, stg_tru_ccp_kf_topping.loaddate, stg_tru_ccp_kf_topping.hd_loaddate) ALL NODES;


SELECT MARK_DESIGN_KSAFE(1);

*************************



CREATE TABLE CLYMAPPO.stg_tru_crm_kf_customer
(
    rowkey varchar(80),
    topic varchar(80),
    partitions int,
    "offset" float,
    "timestamp" float,
    raw long varchar(500000),
    key varchar(80),
    header varchar(80),
    kafkadate float,
    loadtimestamp float,
    par_key int,
    vc_par_key date,
    vc_syn_date timestamp DEFAULT (now())::timestamp
)
PARTITION BY (stg_tru_crm_kf_customer.par_key);


CREATE PROJECTION CLYMAPPO.stg_tru_crm_kf_customer_super /*+basename(stg_tru_crm_kf_customer),createtype(L)*/ 
(
 rowkey,
 topic,
 partitions,
 "offset",
 "timestamp",
 raw,
 key,
 header,
 kafkadate,
 loadtimestamp,
 par_key,
 vc_par_key,
 vc_syn_date
)
AS
 SELECT stg_tru_crm_kf_customer.rowkey,
        stg_tru_crm_kf_customer.topic,
        stg_tru_crm_kf_customer.partitions,
        stg_tru_crm_kf_customer."offset",
        stg_tru_crm_kf_customer."timestamp",
        stg_tru_crm_kf_customer.raw,
        stg_tru_crm_kf_customer.key,
        stg_tru_crm_kf_customer.header,
        stg_tru_crm_kf_customer.kafkadate,
        stg_tru_crm_kf_customer.loadtimestamp,
        stg_tru_crm_kf_customer.par_key,
        stg_tru_crm_kf_customer.vc_par_key,
        stg_tru_crm_kf_customer.vc_syn_date
 FROM CLYMAPPO.stg_tru_crm_kf_customer
 ORDER BY stg_tru_crm_kf_customer.rowkey,
          stg_tru_crm_kf_customer.topic,
          stg_tru_crm_kf_customer.partitions,
          stg_tru_crm_kf_customer."offset",
          stg_tru_crm_kf_customer."timestamp",
          stg_tru_crm_kf_customer.raw,
          stg_tru_crm_kf_customer.key,
          stg_tru_crm_kf_customer.header
SEGMENTED BY hash(stg_tru_crm_kf_customer.partitions, stg_tru_crm_kf_customer."offset", stg_tru_crm_kf_customer."timestamp", stg_tru_crm_kf_customer.kafkadate, stg_tru_crm_kf_customer.loadtimestamp, stg_tru_crm_kf_customer.par_key, stg_tru_crm_kf_customer.vc_par_key, stg_tru_crm_kf_customer.vc_syn_date) ALL NODES;


SELECT MARK_DESIGN_KSAFE(1);

****************



CREATE TABLE CLYMAPPO.stg_tru_crm_kf_subscriber
(
    rowkey varchar(80),
    topic varchar(80),
    partitions int,
    "offset" float,
    "timestamp" float,
    raw long varchar(500000),
    key varchar(80),
    header varchar(80),
    kafkadate float,
    loadtimestamp float,
    par_key int,
    vc_par_key date,
    vc_syn_date timestamp DEFAULT (now())::timestamp
)
PARTITION BY (stg_tru_crm_kf_subscriber.par_key);


CREATE PROJECTION CLYMAPPO.stg_tru_crm_kf_subscriber_super /*+basename(stg_tru_crm_kf_subscriber),createtype(L)*/ 
(
 rowkey,
 topic,
 partitions,
 "offset",
 "timestamp",
 raw,
 key,
 header,
 kafkadate,
 loadtimestamp,
 par_key,
 vc_par_key,
 vc_syn_date
)
AS
 SELECT stg_tru_crm_kf_subscriber.rowkey,
        stg_tru_crm_kf_subscriber.topic,
        stg_tru_crm_kf_subscriber.partitions,
        stg_tru_crm_kf_subscriber."offset",
        stg_tru_crm_kf_subscriber."timestamp",
        stg_tru_crm_kf_subscriber.raw,
        stg_tru_crm_kf_subscriber.key,
        stg_tru_crm_kf_subscriber.header,
        stg_tru_crm_kf_subscriber.kafkadate,
        stg_tru_crm_kf_subscriber.loadtimestamp,
        stg_tru_crm_kf_subscriber.par_key,
        stg_tru_crm_kf_subscriber.vc_par_key,
        stg_tru_crm_kf_subscriber.vc_syn_date
 FROM CLYMAPPO.stg_tru_crm_kf_subscriber
 ORDER BY stg_tru_crm_kf_subscriber.rowkey,
          stg_tru_crm_kf_subscriber.topic,
          stg_tru_crm_kf_subscriber.partitions,
          stg_tru_crm_kf_subscriber."offset",
          stg_tru_crm_kf_subscriber."timestamp",
          stg_tru_crm_kf_subscriber.raw,
          stg_tru_crm_kf_subscriber.key,
          stg_tru_crm_kf_subscriber.header
SEGMENTED BY hash(stg_tru_crm_kf_subscriber.partitions, stg_tru_crm_kf_subscriber."offset", stg_tru_crm_kf_subscriber."timestamp", stg_tru_crm_kf_subscriber.kafkadate, stg_tru_crm_kf_subscriber.loadtimestamp, stg_tru_crm_kf_subscriber.par_key, stg_tru_crm_kf_subscriber.vc_par_key, stg_tru_crm_kf_subscriber.vc_syn_date) ALL NODES;


SELECT MARK_DESIGN_KSAFE(1);
