

CREATE TABLE CLYMAPPO.stg_true_subscribers_product
(
    product_id varchar(100),
    product_line varchar(100),
    idx int,
    code varchar(255),
    description varchar(255),
    subscriber_price_plan_code varchar(255),
    end_date date,
    start_date date,
    status varchar(50),
    type varchar(100),
    transaction_date date,
    partition_date date,
    partition_month varchar(10),
    processed_date date
)
PARTITION BY (stg_true_subscribers_product.partition_date);


CREATE PROJECTION CLYMAPPO.stg_true_subscribers_product_super /*+basename(stg_true_subscribers_product),createtype(P)*/ 
(
 product_id,
 product_line,
 idx,
 code,
 description,
 subscriber_price_plan_code,
 endDate,
 startDate,
 status,
 type,
 transaction_date,
 partition_date,
 partition_month,
 processed_date
)
AS
 SELECT stg_true_subscribers_product.product_id,
        stg_true_subscribers_product.product_line,
        stg_true_subscribers_product.idx,
        stg_true_subscribers_product.code,
        stg_true_subscribers_product.description,
        stg_true_subscribers_product.subscriber_price_plan_code,
        stg_true_subscribers_product.endDate,
        stg_true_subscribers_product.startDate,
        stg_true_subscribers_product.status,
        stg_true_subscribers_product.type,
        stg_true_subscribers_product.transaction_date,
        stg_true_subscribers_product.partition_date,
        stg_true_subscribers_product.partition_month,
        stg_true_subscribers_product.processed_date
 FROM CLYMAPPO.stg_true_subscribers_product
 ORDER BY stg_true_subscribers_product.product_id,
          stg_true_subscribers_product.product_line,
          stg_true_subscribers_product.idx,
          stg_true_subscribers_product.code,
          stg_true_subscribers_product.description,
          stg_true_subscribers_product.subscriber_price_plan_code,
          stg_true_subscribers_product.endDate,
          stg_true_subscribers_product.startDate
UNSEGMENTED ALL NODES;


SELECT MARK_DESIGN_KSAFE(1);
