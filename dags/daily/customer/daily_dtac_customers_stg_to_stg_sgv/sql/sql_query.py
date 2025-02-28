delete_duplicates_staging = """
DELETE FROM {{ grading_schema }}.{{ staging_table }} 
WHERE 
    partition_date = '{{ partition_date }}';
"""

extract_to_staging = """
INSERT INTO {{ grading_schema }}.{{ staging_table }} 
SELECT 
 file_id,
    file_name,
    last_chng_dttm,
    CASE 
        WHEN LENGTH(id_numb) > 1 AND id_numb IS NOT NULL
        THEN UPPER({{ function_schema }}VoltageSecureAccess(id_numb USING PARAMETERS format='AlphaNumeric', config_dfs_path='/voltagesecure/conf'))
        ELSE UPPER(id_numb)
    END AS identifier_no,
    id_type,
    CASE 
        WHEN id_type = '01' THEN 'THAIID'
        WHEN id_type IN ('04', '05') THEN 'PASSPORT'
        ELSE NULL 
    END AS id_type_description,
    cust_numb AS customer_no,
 	CASE
        WHEN UPPER(cust_stts) = 'A' THEN 'ACTIVE'
        WHEN UPPER(cust_stts) = 'O' THEN 'SWITCH OFF'
        WHEN UPPER(cust_stts) = 'C' THEN 'CLOSE'
        WHEN UPPER(cust_stts) = 'W' THEN 'WRITE OFF'
        ELSE NULL
    END AS customer_status,     
    CASE 
        WHEN id_type IN ('01', '04', '05') THEN 'INDIVIDUAL'
        ELSE NULL 
    END AS customer_type,
    CASE 
        WHEN LENGTH(frst_name) > 1  AND frst_name IS NOT NULL
        THEN {{ function_schema }}VoltageSecureAccess(frst_name USING PARAMETERS format='PREDEFINED::UNICODE_BASE32K', config_dfs_path='/voltagesecure/conf') 
        ELSE frst_name
    END AS first_name,
    CASE 
        WHEN LENGTH(last_name) > 1 AND last_name IS NOT NULL
        THEN {{ function_schema }}VoltageSecureAccess(last_name USING PARAMETERS format='PREDEFINED::UNICODE_BASE32K', config_dfs_path='/voltagesecure/conf')
        ELSE last_name
    END AS last_name,
    titl AS title,
    date_of_brth AS birth_date,
    CASE
        WHEN UPPER(gndr) = 'M' THEN 'Male'
        WHEN UPPER(gndr) = 'F' THEN 'Female'
        ELSE NULL
    END AS gender,
    CASE 
        WHEN LENGTH(emal_addr) >1 AND emal_addr IS NOT NULL AND emal_addr NOT IN ( '','@' )
        THEN {{ function_schema }}VoltageSecureAccess(emal_addr USING PARAMETERS format='AlphaNumeric', config_dfs_path='/voltagesecure/conf') 
        ELSE emal_addr
    END AS email_address,
    TO_CHAR(file_date, 'YYYY-MM-DD')::DATE AS transaction_date,
    TO_CHAR(load_date, 'YYYY-MM-DD')::DATE AS partition_date,
    TO_CHAR(load_date, 'YYYYMM') AS partition_month,
    CURRENT_DATE AS processed_date
FROM {{ grading_schema }}.{{ source_table }} 
WHERE TO_CHAR(load_date, 'YYYY-MM-DD') = '{{ partition_date }}' 
	AND id_numb IS NOT NULL;
"""

delete_duplicates_single_staging = """
DELETE FROM {{ grading_schema }}.{{ single_staging_table }} 
WHERE 
    partition_date = '{{ partition_date }}'
    AND operator_name = 'DTAC';
"""

transform_to_single_staging = """
INSERT INTO {{ grading_schema }}.{{ single_staging_table }} 
WITH rank_customers AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_no 
            ORDER BY 
                CASE 
                    WHEN id_type = '01' THEN 1 
                    WHEN id_type IN ('04', '05') THEN 2 
                    ELSE 3
                END,
                last_chng_dttm DESC
        ) AS row_rank
    FROM 
        {{ grading_schema }}.{{ staging_table }}
    WHERE 
        partition_date = '{{ partition_date }}'
),
filtered_customers AS (
    SELECT * FROM rank_customers 
    WHERE row_rank = 1 
)
SELECT 
    UPPER('PG' || 
    CAST(
        CAST(
            substring(md5(identifier_no) FROM 1 FOR 10) AS VARCHAR
        ) AS VARCHAR
    )) AS grading_account_id,
    identifier_no AS identifier_no,
    id_type_description AS id_type,
    customer_no, 
    NULL AS company_name, 
    customer_status AS customer_status, 
    customer_type AS customer_type, 
    first_name, 
    last_name, 
    title, 
    birth_date, 
    gender, 
    NULL AS street, 
    NULL AS house_number, 
    NULL AS moo, 
    NULL AS postal_code, 
    NULL AS city, 
    NULL AS district, 
    NULL AS sub_district, 
    NULL AS country, 
    NULL AS room_number, 
    NULL AS floor, 
    NULL AS  building_mooban, 
    NULL AS soi, 
    email_address, 
    NULL AS golden_id, 
    NULL AS phone_number_list,
    NULL AS emp_employee_id, 
    NULL AS emp_company_name, 
    NULL AS emp_position_code, 
    NULL AS emp_employee_flag, 
    'DTAC' AS operator_name, 
    transaction_date,
    partition_date,
    partition_month,
    processed_date
FROM 
    filtered_customers ;
"""