python -m venv airflow_venv

# Uninstall the current version of Python (optional)
brew uninstall python@3.13

# Install a stable version of Python (for example, Python 3.11)
brew install python@3.11

# Create a new virtual environment with Python 3.11
python3.11 -m venv venv
source venv/bin/activate

export PYTHONPATH=$(pwd)
pytest test

# Ensure core build tools and dependencies are in place
xcode-select --install
brew install abseil

# Upgrade build dependencies
pip install --upgrade setuptools wheel

# Attempt the installation again
pip install pendulum google-re2 msgspec


pip install pendulum


pip install Pyrebase4



pip install "apache-airflow==2.10.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.9.txt"

pip install apache-airflow-providers-postgres

pip install pytest pytest-cov


CREATE EXTENSION IF NOT EXISTS pgcrypto;




CREATE TABLE grade.true_customer_staging (
    notificationid varchar(60) NULL,
    eventbegintime varchar(60) NULL,
    eventtype varchar(50) NOT NULL,
    eventdescription varchar(100) NULL,
    idnum varchar(30) NULL,
    idtype varchar(50) NULL,
    companyname varchar(100) NULL,
    customerstatus varchar(30) NULL,
    firstname varchar(50) NULL,
    lastname varchar(50) NULL,
    title varchar(50) NULL,
    birthdate varchar(20) NULL,
    gender varchar(20) NULL,
    street varchar(200) NULL,
    housenumber varchar(50) NULL,
    moo varchar(50) NULL,
    postalcode varchar(10) NULL,
    city varchar(100) NULL,
    district varchar(100) NULL,
    subdistrict varchar(100) NULL,
    country varchar(50) NULL,
    roomnumber varchar(50) NULL,
    floor varchar(50) NULL,
    buildingmooban varchar(100) NULL,
    soi varchar(100) NULL,
    emailaddress varchar(254) NULL,
    goldenid varchar(50) NOT NULL, -- Unified type for goldenid
    customertype varchar(50) NULL,
    phonenumberlist text NULL,
    partition_date date NULL
);

-- Indexes
CREATE INDEX ind_staging_goldenid ON grade.true_customer_staging USING btree (goldenid);
CREATE INDEX ind_staging_idnum ON grade.true_customer_staging USING btree (idnum);
CREATE INDEX ind_staging_partition_date ON grade.true_customer_staging USING btree (partition_date);

-- Creating the second table with consistent types for similar columns
CREATE TABLE grade.single_customer_staging (
    gradingaccountid varchar(100) NULL, -- Reduced from 300 to 100
    identifier_no varchar(30) NULL,
    id_type varchar(50) NULL, -- Consistent with grade.true_customer_staging
    customer_no varchar(20) NULL, -- Added a reasonable limit
    company_name varchar(100) NULL, -- Consistent with grade.true_customer_staging
    customer_status varchar(30) NULL, -- Consistent with grade.true_customer_staging
    first_name varchar(50) NULL, -- Consistent with grade.true_customer_staging
    last_name varchar(50) NULL, -- Consistent with grade.true_customer_staging
    title varchar(50) NULL, -- Consistent with grade.true_customer_staging
    birth_date varchar(20) NULL, -- Consider timestamp if you need time as well
    gender varchar(20) NULL, -- Reduced from 30 to 10 (for consistency)
    street varchar(200) NULL, -- Reduced from 200 to 150
    house_number varchar(50) NULL, -- Reduced from 100 to 50
    moo varchar(50) NULL, -- Consistent with grade.true_customer_staging
    postal_code varchar(10) NULL, -- Postal codes usually fit within 10 characters
    city varchar(100) NULL, -- Consistent with grade.true_customer_staging
    district varchar(100) NULL, -- Consistent with grade.true_customer_staging
    sub_district varchar(100) NULL, -- Consistent with grade.true_customer_staging
    country varchar(50) NULL, -- Consistent with grade.true_customer_staging
    room_number varchar(50) NULL, -- Consistent with grade.true_customer_staging
    floor varchar(50) NULL, -- Consistent with grade.true_customer_staging
    building_mooban varchar(100) NULL, -- Consistent with grade.true_customer_staging
    soi varchar(100) NULL, -- Consistent with grade.true_customer_staging
    email_address varchar(254) NULL, -- Standard limit for email addresses
    golden_id varchar(50) NOT NULL, -- Consistent with grade.true_customer_staging
    customer_type varchar(20) NULL, -- Consistent with grade.true_customer_staging
    emp_employee_id varchar(50) NULL,
    emp_company_name varchar(100) NULL, -- Consistent with company_name
    emp_position_code varchar(50) NULL, -- Consistent with id_type
    emp_employee_flag char(10) NULL, -- 'Y' or 'N'
    operator_name varchar(10) NULL, -- 'TRUE' or 'DTAC'
    partition_date date NULL -- Consistent with grade.true_customer_staging
);

-- Indexes
CREATE INDEX ind_single_cust_goldenid ON grade.single_customer_staging (golden_id);
CREATE INDEX ind_single_cust_identifier ON grade.single_customer_staging (identifier_no);
CREATE INDEX ind_single_cust_partition_date ON grade.single_customer_staging (partition_date);



CREATE TABLE grade.employee (
    national_id VARCHAR(50) NOT NULL,
    employee_id VARCHAR(50) NOT NULL,
    eng_firstname VARCHAR(100)  NULL,
    eng_lastname VARCHAR(100)  NULL,
    thai_firstname VARCHAR(100)  NULL,
    thai_lastname VARCHAR(100)  NULL,
    company_name VARCHAR(100) NULL,
    position_name VARCHAR(50) NOT NULL,
    create_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    partition_date DATE NOT NULL,
    PRIMARY KEY (national_id)
);

