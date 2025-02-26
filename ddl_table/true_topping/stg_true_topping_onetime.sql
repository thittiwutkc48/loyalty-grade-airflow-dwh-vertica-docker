
CREATE TABLE CLYMAPPO.stg_true_topping_onetime (
    notification_id VARCHAR(255),
    request_id VARCHAR(255),
    topping_id VARCHAR(255),
    chain_id INT,
    event_begin_time TIMESTAMP,
    msisdn VARCHAR(20),
    imsi VARCHAR(255),
    account_type VARCHAR(20),
    effective_date TIMESTAMP,
    expire_date TIMESTAMP,
    price FLOAT,
    subscriber_price_plan_code VARCHAR(50),
    transaction_date DATE,
    partition_date DATE,
    partition_month VARCHAR(10),
    processed_date DATE
)
UNSEGMENTED ALL NODES
PARTITION BY partition_date;


CREATE PROJECTION CLYMAPPO.stg_true_topping_onetime_super 
(
    notification_id,
    request_id,
    topping_id,
    chain_id,
    event_begin_time,
    msisdn,
    imsi,
    account_type,
    effective_date,
    expire_date,
    price,
    subscriber_price_plan_code,
    transaction_date,
    partition_date,
    partition_month,
    processed_date
)
AS 
  SELECT 
    notification_id,
    request_id,
    topping_id,
    chain_id,
    event_begin_time,
    msisdn,
    imsi,
    account_type,
    effective_date,
    expire_date,
    price,
    subscriber_price_plan_code,
    transaction_date,
    partition_date,
    partition_month,
    processed_date
  FROM CLYMAPPO.stg_true_topping_onetime
  SEGMENTED BY HASH(
    partition_date, 
    partition_month, 
    notification_id, 
    request_id, 
    topping_id, 
    chain_id, 
    event_begin_time,
    msisdn
  )
ALL NODES ;
