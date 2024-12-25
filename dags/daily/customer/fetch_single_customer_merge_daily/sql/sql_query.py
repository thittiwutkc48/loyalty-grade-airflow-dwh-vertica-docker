new_existing_to_single = """
INSERT INTO {{ grading_schema }}.{{ single_table }} 
SELECT 
    scs.gradingaccountid,
    scs.identifier_no,
    scs.id_type,
    scs.customer_no,
    scs.company_name,
    scs.customer_status,
    scs.first_name,
    scs.last_name,
    scs.title,
    scs.birth_date,
    scs.gender,
    scs.street,
    scs.house_number,
    scs.moo,
    scs.postal_code,
    scs.city,
    scs.district,
    scs.sub_district,
    scs.country,
    scs.room_number,
    scs.floor,
    scs.building_mooban,
    scs.soi,
    scs.email_address,
    scs.golden_id,
    scs.customer_type,
    emp.employee_id AS emp_employee_id,
    emp.company_name AS emp_company_name,
    emp.position_name AS emp_position_code,
    CASE 
        WHEN emp.employee_id IS NOT NULL THEN 'Y'
        ELSE 'N'
    END AS emp_employee_flag,
    scs.operator_name,
    scs.partition_date AS transaction_date,
    CURRENT_DATE AS partition_date,
    TO_CHAR(CURRENT_DATE, 'YYYYMM') AS partition_month,
    CURRENT_TIMESTAMP AS update_at,
    CURRENT_TIMESTAMP AS created_at  
FROM 
    {{ grading_schema }}.{{ single_staging_table }}  scs
LEFT JOIN 
    (SELECT DISTINCT gradingaccountid FROM {{ grading_schema }}.{{ single_table }}) sc 
    ON scs.gradingaccountid = sc.gradingaccountid
LEFT JOIN 
    {{ grading_schema }}.{{ employee_table }} emp 
    ON scs.identifier_no = emp.national_id
WHERE 
    scs.partition_date = '{{ partition_date }}' 
    AND sc.gradingaccountid IS NULL;
"""

existing_update_to_single = """
INSERT INTO {{ grading_schema }}.{{ single_table }} 
SELECT
    scs.gradingaccountid,
    scs.identifier_no,
    scs.id_type,
    scs.customer_no,
    scs.company_name,
    scs.customer_status,
    scs.first_name,
    scs.last_name,
    scs.title,
    scs.birth_date,
    scs.gender,
    scs.street,
    scs.house_number,
    scs.moo,
    scs.postal_code,
    scs.city,
    scs.district,
    scs.sub_district,
    scs.country,
    scs.room_number,
    scs.floor,
    scs.building_mooban,
    scs.soi,
    scs.email_address,
    scs.golden_id,
    scs.customer_type,
    emp.employee_id AS emp_employee_id,
    emp.company_name AS emp_company_name,
    emp.position_name AS emp_position_code,
    CASE 
        WHEN emp.employee_id IS NOT NULL THEN 'Y'
        ELSE 'N'
    END AS emp_employee_flag,
    scs.operator_name,
    scs.partition_date::DATE AS transaction_date,
    CURRENT_DATE AS partition_date,
    TO_CHAR(CURRENT_DATE, 'YYYYMM') AS partition_month,
    CURRENT_TIMESTAMP AS update_at,
    CURRENT_TIMESTAMP AS created_at  
FROM {{ grading_schema }}.{{ single_staging_table }} scs
LEFT JOIN (
    SELECT DISTINCT gradingaccountid 
    FROM {{ grading_schema }}.{{ single_table }}
) sc 
    ON scs.gradingaccountid = sc.gradingaccountid
LEFT JOIN 
    {{ grading_schema }}.{{ employee_table }} emp 
    ON scs.identifier_no = emp.national_id
WHERE 
    scs.partition_date = '{{ partition_date }}' 
    AND sc.gradingaccountid IS NOT NULL
    AND scs.operator_name = 'TRUE'
ON CONFLICT (gradingaccountid) 
DO UPDATE 
SET
    customer_no = EXCLUDED.customer_no,
    company_name = EXCLUDED.company_name,
    customer_status = EXCLUDED.customer_status,
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    title = EXCLUDED.title,
    birth_date = EXCLUDED.birth_date,
    gender = EXCLUDED.gender,
    street = EXCLUDED.street,
    house_number = EXCLUDED.house_number,
    moo = EXCLUDED.moo,
    postal_code = EXCLUDED.postal_code,
    city = EXCLUDED.city,
    district = EXCLUDED.district,
    sub_district = EXCLUDED.sub_district,
    country = EXCLUDED.country,
    room_number = EXCLUDED.room_number,
    floor = EXCLUDED.floor,
    building_mooban = EXCLUDED.building_mooban,
    soi = EXCLUDED.soi,
    email_address = EXCLUDED.email_address,
    golden_id = EXCLUDED.golden_id,
    customer_type = EXCLUDED.customer_type,
    emp_employee_id = EXCLUDED.emp_employee_id,
    emp_company_name = EXCLUDED.emp_company_name,
    emp_position_code = EXCLUDED.emp_position_code,
    emp_employee_flag = EXCLUDED.emp_employee_flag,
    transaction_date = EXCLUDED.transaction_date,
    partition_date = EXCLUDED.partition_date,
    partition_month = EXCLUDED.partition_month,
    updated_at = EXCLUDED.updated_at;
"""
    
emp_new_existing_to_single = """
INSERT INTO {{ grading_schema }}.{{ single_table }} 
SELECT  
    'PG' || 
    (
        ('x' || SUBSTRING(MD5(CAST(UPPER(emp.national_id) AS TEXT) || LENGTH(emp.national_id)) FROM 1 FOR 10))::BIT(40)::BIGINT
    ) AS gradingaccountid,
    UPPER(emp.national_id) AS identifier_no,
    CASE
        WHEN LENGTH(emp.national_id) = 13 AND emp.national_id NOT LIKE '%[^0-9]%' THEN 'THAIID'
        WHEN emp.national_id ~ '^[A-Za-z]' AND emp.national_id ~ '[A-Za-z]' THEN 'PASSPORT'        
        ELSE 'OTHER'
    END AS id_type,
    NULL AS customer_no, 
    emp.company_name AS company_name, 
    'ACTIVE' AS customer_status, 
    emp.thai_firstname AS first_name, 
    emp.thai_lastname AS last_name, 
    NULL AS title, 
    NULL AS birth_date, 
    NULL AS gender, 
    NULL AS street, 
    NULL AS house_number, 
    NULL AS moo, 
    NULL AS postal_code, 
    NULL AS city, 
    NULL AS district, 
    NULL AS sub_district, 
    CASE
        WHEN LENGTH(emp.national_id) = 13 AND emp.national_id NOT LIKE '%[^0-9]%' THEN 'Thailand'
        ELSE NULL
    END AS country,
    NULL AS room_number, 
    NULL AS floor, 
    NULL AS building_mooban, 
    NULL AS soi, 
    NULL AS email_address, 
    NULL AS golden_id, 
    'INDIVIDUAL' AS customer_type,
    emp.employee_id AS emp_employee_id, 
    emp.company_name AS emp_company_name, 
    emp.position_name AS emp_position_code, 
    'Y' AS emp_employee_flag, 
    'TRUE' AS operator_name, 
    emp.partition_date AS transaction_date,
    CURRENT_DATE AS partition_date,
    TO_CHAR(CURRENT_DATE, 'YYYYMM') AS partition_month,
    CURRENT_TIMESTAMP AS update_at,
    CURRENT_TIMESTAMP AS created_at 
FROM {{ grading_schema }}.{{ employee_table }}  emp
LEFT JOIN {{ grading_schema }}.{{ single_table }}  sc 
    ON emp.national_id = sc.identifier_no
WHERE sc.identifier_no IS NULL;
"""