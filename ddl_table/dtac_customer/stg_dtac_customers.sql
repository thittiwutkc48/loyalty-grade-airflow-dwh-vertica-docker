
CREATE TABLE CLYMAPPO.stg_dtac_customers (
    file_id integer, 
    file_name VARCHAR(600),
    last_chng_dttm TIMESTAMP,
    identifier_no VARCHAR(100),
    id_type VARCHAR(100),
    id_type_description VARCHAR(100),
    customer_no VARCHAR(100),
    customer_status VARCHAR(200),
    customer_type VARCHAR(100),
    first_name VARCHAR(300),
    last_name VARCHAR(300),
    title VARCHAR(300),
    birth_date DATE,
    gender VARCHAR(20),
    email_address VARCHAR(254),  
    transaction_date DATE,
    partition_date DATE NOT NULL,
    partition_month VARCHAR(10),
    processed_date DATE
)
UNSEGMENTED ALL NODES
PARTITION BY partition_date;