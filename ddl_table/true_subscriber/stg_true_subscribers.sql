

CREATE TABLE CLYMAPPO.stg_true_subscribers
(
    row_key varchar(60),
    notification_id varchar(60),
    event_begin_time timestamp,
    id_num varchar(100),
    id_type varchar(100),
    customer_type varchar(100),
    crm_integration_id varchar(100),
    golden_id varchar(100),
    product_id varchar(100),
    product_name varchar(200),
    product_name_desc varchar(500),
    product_status varchar(50),
    product_line varchar(100),
    ban varchar(100),
    billing_sub_no varchar(100),
    start_date timestamp,
    end_date timestamp,
    language varchar(20),
    street varchar(300),
    house_number varchar(300),
    moo varchar(300),
    postal_code varchar(300),
    city varchar(300),
    district varchar(300),
    sub_district varchar(300),
    country varchar(300),
    first_name varchar(300),
    last_name varchar(300),
    ou_id varchar(100),
    vip varchar(100),
    company_code varchar(50),
    product_comp_list long varchar(500000),
    transaction_date date,
    partition_date date,
    partition_month varchar(10),
    processed_date date
)
PARTITION BY (stg_true_subscribers.partition_date);


CREATE PROJECTION CLYMAPPO.stg_true_subscribers_super /*+basename(stg_true_subscribers),createtype(P)*/ 
(
 row_key,
 notification_id,
 event_begin_time,
 id_num,
 id_type,
 customer_type,
 crm_integration_id,
 golden_id,
 product_id,
 product_name,
 product_name_desc,
 product_status,
 product_line,
 ban,
 billing_sub_no,
 start_date,
 end_date,
 language,
 street,
 house_number,
 moo,
 postal_code,
 city,
 district,
 sub_district,
 country,
 first_name,
 last_name,
 ou_id,
 vip,
 company_code,
 product_comp_list,
 transaction_date,
 partition_date,
 partition_month,
 processed_date
)
AS
 SELECT stg_true_subscribers.row_key,
        stg_true_subscribers.notification_id,
        stg_true_subscribers.event_begin_time,
        stg_true_subscribers.id_num,
        stg_true_subscribers.id_type,
        stg_true_subscribers.customer_type,
        stg_true_subscribers.crm_integration_id,
        stg_true_subscribers.golden_id,
        stg_true_subscribers.product_id,
        stg_true_subscribers.product_name,
        stg_true_subscribers.product_name_desc,
        stg_true_subscribers.product_status,
        stg_true_subscribers.product_line,
        stg_true_subscribers.ban,
        stg_true_subscribers.billing_sub_no,
        stg_true_subscribers.start_date,
        stg_true_subscribers.end_date,
        stg_true_subscribers.language,
        stg_true_subscribers.street,
        stg_true_subscribers.house_number,
        stg_true_subscribers.moo,
        stg_true_subscribers.postal_code,
        stg_true_subscribers.city,
        stg_true_subscribers.district,
        stg_true_subscribers.sub_district,
        stg_true_subscribers.country,
        stg_true_subscribers.first_name,
        stg_true_subscribers.last_name,
        stg_true_subscribers.ou_id,
        stg_true_subscribers.vip,
        stg_true_subscribers.company_code,
        stg_true_subscribers.product_comp_list,
        stg_true_subscribers.transaction_date,
        stg_true_subscribers.partition_date,
        stg_true_subscribers.partition_month,
        stg_true_subscribers.processed_date
 FROM CLYMAPPO.stg_true_subscribers
 ORDER BY stg_true_subscribers.row_key,
          stg_true_subscribers.notification_id,
          stg_true_subscribers.event_begin_time,
          stg_true_subscribers.id_num,
          stg_true_subscribers.id_type,
          stg_true_subscribers.customer_type,
          stg_true_subscribers.crm_integration_id,
          stg_true_subscribers.golden_id
UNSEGMENTED ALL NODES;


SELECT MARK_DESIGN_KSAFE(1);
