CREATE TABLE CLYMAPPO.stg_true_subscribers_product (
    product_id VARCHAR(100), 
    product_line VARCHAR(100), 
    idx INT, 
    code VARCHAR(255), 
    description VARCHAR(255), 
    subscriber_price_plan_code VARCHAR(255), 
    endDate DATE, 
    startDate DATE, 
    status VARCHAR(50), 
    type VARCHAR(100), 
    transaction_date DATE, 
    partition_date DATE, 
    partition_month VARCHAR(10), 
    processed_date DATE
)
UNSEGMENTED ALL NODES
PARTITION BY partition_date;


CREATE PROJECTION CLYMAPPO.stg_true_subscribers_product_super
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
SELECT * FROM stg_true_subscribers_product 
ORDER BY partition_date, product_id, idx 
SEGMENTED BY HASH(
    partition_date,
    product_id,
    product_line,
    idx,
    code,
    description,
    subscriber_price_plan_code
    startDate,
    status
) 
ALL NODES;
