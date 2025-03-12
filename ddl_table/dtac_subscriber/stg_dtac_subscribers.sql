
CREATE TABLE CLYMAPPO.stg_dtac_subscribers (
    file_id            integer,
    file_name          VARCHAR(600),
    last_chng_dttm     TIMESTAMP,
    identifier_no      VARCHAR(100),
    id_type            VARCHAR(100),
    customer_type      VARCHAR(100),
    customer_no        VARCHAR(100),
    product_id         VARCHAR(100),
    product_name       VARCHAR(200),
    product_name_desc  VARCHAR(500),
    product_status     VARCHAR(50),
    product_line       VARCHAR(100),
    product_group      VARCHAR(50),
    start_date         TIMESTAMP,
    end_date           TIMESTAMP,
    language           VARCHAR(20),
    first_name         VARCHAR(300),
    last_name          VARCHAR(300),
    vip                VARCHAR(100),
    transaction_date   DATE,
    partition_date     DATE NOT NULL,
    partition_month    VARCHAR(10),
    processed_date     DATE
)
UNSEGMENTED ALL NODES
PARTITION BY partition_date;

