

CREATE TABLE CLYMAPPO.stg_true_customers
(
    row_key varchar(60),
    notification_id varchar(60),
    event_begin_time timestamp,
    id_num varchar(100),
    id_type varchar(100),
    company_name varchar(200),
    customer_status varchar(200),
    customer_type varchar(100),
    first_name varchar(300),
    last_name varchar(300),
    title varchar(300),
    birth_date date,
    gender varchar(20),
    street varchar(300),
    house_number varchar(300),
    moo varchar(300),
    postal_code varchar(300),
    city varchar(300),
    district varchar(300),
    sub_district varchar(300),
    country varchar(300),
    room_number varchar(300),
    floor varchar(300),
    building_mooban varchar(300),
    soi varchar(300),
    email_address varchar(254),
    golden_id varchar(100),
    phone_number_list long varchar(500000),
    transaction_date date,
    partition_date date,
    partition_month varchar(10),
    processed_date date
)
PARTITION BY (stg_true_customers.partition_date);


CREATE PROJECTION CLYMAPPO.stg_true_customers_super /*+basename(stg_true_customers),createtype(P)*/ 
(
 row_key,
 notification_id,
 event_begin_time,
 id_num,
 id_type,
 company_name,
 customer_status,
 customer_type,
 first_name,
 last_name,
 title,
 birth_date,
 gender,
 street,
 house_number,
 moo,
 postal_code,
 city,
 district,
 sub_district,
 country,
 room_number,
 floor,
 building_mooban,
 soi,
 email_address,
 golden_id,
 phone_number_list,
 transaction_date,
 partition_date,
 partition_month,
 processed_date
)
AS
 SELECT stg_true_customers.row_key,
        stg_true_customers.notification_id,
        stg_true_customers.event_begin_time,
        stg_true_customers.id_num,
        stg_true_customers.id_type,
        stg_true_customers.company_name,
        stg_true_customers.customer_status,
        stg_true_customers.customer_type,
        stg_true_customers.first_name,
        stg_true_customers.last_name,
        stg_true_customers.title,
        stg_true_customers.birth_date,
        stg_true_customers.gender,
        stg_true_customers.street,
        stg_true_customers.house_number,
        stg_true_customers.moo,
        stg_true_customers.postal_code,
        stg_true_customers.city,
        stg_true_customers.district,
        stg_true_customers.sub_district,
        stg_true_customers.country,
        stg_true_customers.room_number,
        stg_true_customers.floor,
        stg_true_customers.building_mooban,
        stg_true_customers.soi,
        stg_true_customers.email_address,
        stg_true_customers.golden_id,
        stg_true_customers.phone_number_list,
        stg_true_customers.transaction_date,
        stg_true_customers.partition_date,
        stg_true_customers.partition_month,
        stg_true_customers.processed_date
 FROM CLYMAPPO.stg_true_customers
 ORDER BY stg_true_customers.row_key,
          stg_true_customers.notification_id,
          stg_true_customers.event_begin_time,
          stg_true_customers.id_num,
          stg_true_customers.id_type,
          stg_true_customers.company_name,
          stg_true_customers.customer_status,
          stg_true_customers.customer_type
UNSEGMENTED ALL NODES;


SELECT MARK_DESIGN_KSAFE(1);
