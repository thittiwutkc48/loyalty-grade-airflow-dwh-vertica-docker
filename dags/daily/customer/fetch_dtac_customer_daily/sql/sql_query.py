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
    substr(last_chng_dttm, 1, 10) = '{{ partition_date }}';
"""

delete_duplicates_single_staging = """
DELETE FROM {{ grading_schema }}.{{ single_staging_table }} 
WHERE 
    operator_name = 'DTAC' AND
    partition_date = '{{ partition_date }}';
"""

transform_to_single_staging = """
INSERT INTO {{ grading_schema }}.{{ single_staging_table }} 
WITH ranked_customers AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY partition_date, UPPER(id_numb)
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
        ('x' || SUBSTRING(MD5(CAST(UPPER(id_numb) AS TEXT) || LENGTH(id_numb)) FROM 1 FOR 10))::BIT(40)::BIGINT
    ) AS gradingaccountid,
    UPPER(id_numb) AS identifier_no,
    CASE
        WHEN LENGTH(id_numb) = 13 AND id_numb NOT LIKE '%[^0-9]%' THEN 'THAIID'
        WHEN id_numb ~ '^[A-Za-z]' AND id_numb ~ '[A-Za-z]' THEN 'PASSPORT'        
        ELSE 'OTHER'
    END AS id_type,
    cust_numb AS customer_no, 
    NULL AS company_name, 
    CASE
        WHEN UPPER(cust_stts) = 'A' THEN 'ACTIVE'
        WHEN UPPER(cust_stts) = 'O' THEN 'SWITCH OFF'
        WHEN UPPER(cust_stts) = 'C' THEN 'CLOSE'
        WHEN UPPER(cust_stts) = 'W' THEN 'WRITE OFF'
        ELSE NULL
    END AS customer_status, 
    frst_name AS first_name, 
    last_name, 
    titl AS title, 
    date_of_brth AS birth_date, 
    CASE
        WHEN UPPER(gndr) = 'M' THEN 'Male'
        WHEN UPPER(gndr) = 'F' THEN 'Female'
        ELSE 'N/A'
    END AS gender, 
    NULL AS street, 
    NULL AS house_number, 
    NULL AS moo, 
    NULL AS postal_code, 
    NULL AS city, 
    NULL AS district, 
    NULL AS sub_district, 
    CASE
        WHEN LENGTH(id_numb) = 13 AND id_numb NOT LIKE '%[^0-9]%' THEN 'Thailand'
        ELSE NULL
    END AS country,
    NULL AS room_number, 
    NULL AS floor, 
    NULL AS building_mooban, 
    NULL AS soi, 
    emal_addr AS email_address, 
    NULL AS golden_id, 
    CASE
        WHEN LENGTH(id_numb) = 13 AND id_numb NOT LIKE '%[^0-9]%' THEN 'INDIVIDUAL'
        WHEN id_numb ~ '^[A-Za-z]' AND id_numb ~ '[A-Za-z]' THEN 'INDIVIDUAL'
        ELSE 'OTHER'
    END AS customer_type,
    NULL AS emp_employee_id, 
    NULL AS emp_company_name, 
    NULL AS emp_position_code, 
    NULL AS emp_employee_flag, 
    'DTAC' AS operator_name, 
    partition_date
FROM 
    ranked_customers
WHERE 
    row_num = 1;
"""
