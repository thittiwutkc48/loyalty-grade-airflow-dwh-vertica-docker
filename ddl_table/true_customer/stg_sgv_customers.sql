

CREATE TABLE CLYMAPPO.stg_sgv_customers
(
    grading_account_id varchar(100),
    identifier_no varchar(100),
    id_type varchar(100),
    customer_no varchar(100),
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
    emp_employee_id varchar(50),
    emp_company_name varchar(100),
    emp_position_code varchar(50),
    emp_employee_flag char(10),
    operator_name varchar(10),
    transaction_date date,
    partition_date date NOT NULL,
    partition_month varchar(10),
    processed_date date
)
PARTITION BY (stg_sgv_customers.partition_date);


CREATE PROJECTION CLYMAPPO.stg_sgv_customers_super /*+basename(stg_sgv_customers),createtype(P)*/ 
(
 grading_account_id,
 identifier_no,
 id_type,
 customer_no,
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
 emp_employee_id,
 emp_company_name,
 emp_position_code,
 emp_employee_flag,
 operator_name,
 transaction_date,
 partition_date,
 partition_month,
 processed_date
)
AS
 SELECT stg_sgv_customers.grading_account_id,
        stg_sgv_customers.identifier_no,
        stg_sgv_customers.id_type,
        stg_sgv_customers.customer_no,
        stg_sgv_customers.company_name,
        stg_sgv_customers.customer_status,
        stg_sgv_customers.customer_type,
        stg_sgv_customers.first_name,
        stg_sgv_customers.last_name,
        stg_sgv_customers.title,
        stg_sgv_customers.birth_date,
        stg_sgv_customers.gender,
        stg_sgv_customers.street,
        stg_sgv_customers.house_number,
        stg_sgv_customers.moo,
        stg_sgv_customers.postal_code,
        stg_sgv_customers.city,
        stg_sgv_customers.district,
        stg_sgv_customers.sub_district,
        stg_sgv_customers.country,
        stg_sgv_customers.room_number,
        stg_sgv_customers.floor,
        stg_sgv_customers.building_mooban,
        stg_sgv_customers.soi,
        stg_sgv_customers.email_address,
        stg_sgv_customers.golden_id,
        stg_sgv_customers.phone_number_list,
        stg_sgv_customers.emp_employee_id,
        stg_sgv_customers.emp_company_name,
        stg_sgv_customers.emp_position_code,
        stg_sgv_customers.emp_employee_flag,
        stg_sgv_customers.operator_name,
        stg_sgv_customers.transaction_date,
        stg_sgv_customers.partition_date,
        stg_sgv_customers.partition_month,
        stg_sgv_customers.processed_date
 FROM CLYMAPPO.stg_sgv_customers
 ORDER BY stg_sgv_customers.grading_account_id,
          stg_sgv_customers.identifier_no,
          stg_sgv_customers.id_type,
          stg_sgv_customers.customer_no,
          stg_sgv_customers.company_name,
          stg_sgv_customers.customer_status,
          stg_sgv_customers.customer_type,
          stg_sgv_customers.first_name
UNSEGMENTED ALL NODES;


SELECT MARK_DESIGN_KSAFE(1);
