delete_duplicates_staging = """
DELETE FROM {{ grading_schema }}.{{ staging_table }} 
WHERE 
    partition_date = '{{ partition_date }}';
"""

extract_to_staging = """
INSERT INTO {{ grading_schema }}.{{ staging_table }} 
SELECT 
    *, 
    '{{ partition_date }}'::DATE AS partition_date
FROM 
    {{ dwh_schema }}.{{ source_table }}
WHERE 
    substr(eventBeginTime, 1, 10) = '{{ partition_date }}';
"""

delete_duplicates_single_staging = """
DELETE FROM {{ grading_schema }}.{{ single_staging_table }} 
WHERE 
    partition_date = '{{ partition_date }}';
"""

transform_to_single_staging = """
INSERT INTO {{ grading_schema }}.{{ single_staging_table }} 
WITH ranked_customers AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY partition_date, UPPER(idnum)
            ORDER BY partition_date
        ) AS row_num
    FROM 
        {{ grading_schema }}.{{ staging_table }} 
    WHERE 
        partition_date = '{{ partition_date }}'
)
SELECT 
    'PG' || 
    (
        ('x' || substring(md5(CAST(UPPER(idnum) AS TEXT) || LENGTH(idnum)) FROM 1 FOR 10))::bit(40)::bigint
    ) AS gradingaccountid,
    UPPER(idnum) AS identifier_no,
    CASE
        WHEN REPLACE(UPPER(idtype), ' ', '') = 'PERSONALIDENTITY' THEN 'PASSPORT'
        WHEN REPLACE(UPPER(idtype), ' ', '') = 'THAIID' THEN 'THAIID'
        ELSE UPPER(idtype)
    END AS id_type,
    NULL AS customer_no, 
    companyname AS company_name, 
    UPPER(customerstatus) AS customer_status, 
    firstname AS first_name, 
    lastname AS last_name, 
    title, 
    birthdate AS birth_date, 
    gender, 
    street, 
    housenumber AS house_number, 
    moo, 
    postalcode AS postal_code, 
    city, 
    district, 
    subdistrict AS sub_district, 
    country, 
    roomnumber AS room_number, 
    floor, 
    buildingmooban AS building_mooban, 
    soi, 
    emailaddress AS email_address, 
    goldenid AS golden_id, 
    UPPER(customertype) AS customer_type, 
    NULL AS emp_employee_id, 
    NULL AS emp_company_name, 
    NULL AS emp_position_code, 
    NULL AS emp_employee_flag, 
    'TRUE' AS operator_name, 
    partition_date
FROM 
    ranked_customers
WHERE 
    row_num = 1;
"""
