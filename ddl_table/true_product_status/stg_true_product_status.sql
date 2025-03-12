CREATE TABLE CLYMAPPO.stg_true_product_status (
    row_key VARCHAR(60) ,
    notification_id VARCHAR(100),  
    event_begin_time TIMESTAMP,  
    billing_seq_no VARCHAR(100),  
    billing_sub_no VARCHAR(100),  
    product_id VARCHAR(100),  
    sys_creation_date TIMESTAMP,  
    sys_update_date TIMESTAMP,  
    sub_status VARCHAR(50),  
    sub_status_rsn VARCHAR(50),  
    bar_by_req BOOLEAN,  
    bar_by_usage BOOLEAN,  
    coll_rsn VARCHAR(50),  
    credit_rsn VARCHAR(50),  
    transaction_date DATE,  
    partition_date DATE,
    partition_month VARCHAR(10),  
    processed_date DATE
)
UNSEGMENTED ALL NODES
PARTITION BY partition_date;