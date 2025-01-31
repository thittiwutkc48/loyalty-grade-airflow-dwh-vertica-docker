upsert_new_and_existing_to_single = """
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
FROM {{ grading_schema }}.{{ staging_table }} scs
LEFT JOIN {{ grading_schema }}.{{ employee_table }} emp 
    ON scs.identifier_no = emp.national_id
WHERE 
    scs.partition_date = '{{ partition_date }}'
ON CONFLICT (gradingaccountid) 
DO UPDATE SET
    customer_no = EXCLUDED.customer_no,
    company_name = EXCLUDED.company_name,
    customer_status = EXCLUDED.customer_status,
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    title = EXCLUDED.title,
    birth_date = EXCLUDED.birth_date,
    gender = EXCLUDED.gender,
    street = CASE
        WHEN EXCLUDED.operator_name = 'DTAC' THEN {{ grading_schema }}.{{ single_table }}.street
        ELSE EXCLUDED.street
    END,
    house_number =  CASE
        WHEN EXCLUDED.operator_name = 'DTAC' THEN {{ grading_schema }}.{{ single_table }}.house_number
        ELSE EXCLUDED.house_number
    END,
    moo =  CASE
        WHEN EXCLUDED.operator_name = 'DTAC' THEN {{ grading_schema }}.{{ single_table }}.moo
        ELSE EXCLUDED.moo
    END,
    postal_code =  CASE
        WHEN EXCLUDED.operator_name = 'DTAC' THEN {{ grading_schema }}.{{ single_table }}.postal_code
        ELSE EXCLUDED.postal_code
    END,
    city =  CASE
        WHEN EXCLUDED.operator_name = 'DTAC' THEN {{ grading_schema }}.{{ single_table }}.city
        ELSE EXCLUDED.city
    END,
    district =  CASE
        WHEN EXCLUDED.operator_name = 'DTAC' THEN {{ grading_schema }}.{{ single_table }}.district
        ELSE EXCLUDED.district
    END,
    sub_district =  CASE
        WHEN EXCLUDED.operator_name = 'DTAC' THEN {{ grading_schema }}.{{ single_table }}.sub_district
        ELSE EXCLUDED.sub_district
    END,
    country =  CASE
        WHEN EXCLUDED.operator_name = 'DTAC' THEN {{ grading_schema }}.{{ single_table }}.country
        ELSE EXCLUDED.country
    END,
    room_number =  CASE
        WHEN EXCLUDED.operator_name = 'DTAC' THEN {{ grading_schema }}.{{ single_table }}.room_number
        ELSE EXCLUDED.room_number
    END,
    floor =  CASE
        WHEN EXCLUDED.operator_name = 'DTAC' THEN {{ grading_schema }}.{{ single_table }}.floor
        ELSE EXCLUDED.floor
    END,
    building_mooban = CASE
        WHEN EXCLUDED.operator_name = 'DTAC' THEN {{ grading_schema }}.{{ single_table }}.building_mooban
        ELSE EXCLUDED.building_mooban
    END,
    soi = CASE
        WHEN EXCLUDED.operator_name = 'DTAC' THEN {{ grading_schema }}.{{ single_table }}.soi
        ELSE EXCLUDED.soi
    END,
    email_address = EXCLUDED.email_address,
    golden_id = CASE
        WHEN EXCLUDED.operator_name = 'DTAC' THEN {{ grading_schema }}.{{ single_table }}.golden_id
        ELSE EXCLUDED.golden_id
    END,
    customer_type = EXCLUDED.customer_type,
    emp_employee_id = EXCLUDED.emp_employee_id,
    emp_company_name = EXCLUDED.emp_company_name,
    emp_position_code = EXCLUDED.emp_position_code,
    emp_employee_flag = EXCLUDED.emp_employee_flag,
    operator_name = CASE
        WHEN EXCLUDED.operator_name = 'TRUE' THEN EXCLUDED.operator_name
        ELSE {{ grading_schema }}.{{ single_table }}.operator_name 
    END,
    transaction_date = EXCLUDED.transaction_date,
    partition_date = EXCLUDED.partition_date,
    partition_month = EXCLUDED.partition_month,
    updated_at = EXCLUDED.updated_at ;
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
FROM {{ grading_schema }}.{{ employee_table }} emp
LEFT JOIN {{ grading_schema }}.{{ single_table }} sc 
    ON emp.national_id = sc.identifier_no
WHERE sc.identifier_no IS NULL;
"""

update_emp_quit_single = """
UPDATE {{ grading_schema }}.{{ single_table }} sc
SET
    emp_employee_id = null,
    emp_company_name = null,
    emp_position_code = null,
    emp_employee_flag = 'N',
    partition_date = CURRENT_DATE,
    partition_month = TO_CHAR(CURRENT_DATE, 'YYYYMM'),
    updated_at = CURRENT_TIMESTAMP
FROM (
    SELECT gradingaccountid, identifier_no 
    FROM {{ grading_schema }}.{{ single_table }}  sc
    LEFT JOIN {{ grading_schema }}.{{ employee_table }} emp 
    ON sc.identifier_no = emp.national_id 
    WHERE emp_employee_flag = 'Y' AND emp.national_id IS NULL
) sce
WHERE sc.identifier_no = sce.identifier_no;
"""

update_emp_info_to_single = """
UPDATE {{ grading_schema }}.{{ single_table }}  sc
SET
    emp_employee_id = e.employee_id,
    emp_company_name = e.company_name,
    emp_position_code = e.position_name,
    emp_employee_flag = 'Y',
    partition_date = CURRENT_DATE,
    partition_month = TO_CHAR(CURRENT_DATE, 'YYYYMM'),
    updated_at = CURRENT_TIMESTAMP
FROM {{ grading_schema }}.{{ employee_table }} e
WHERE sc.identifier_no = e.national_id
  AND sc.emp_employee_flag = 'Y'
  AND e.national_id IS NOT NULL;
"""





