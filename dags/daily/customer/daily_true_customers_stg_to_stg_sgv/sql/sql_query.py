delete_duplicates_staging = """
DELETE FROM {{ grading_schema }}.{{ staging_table }} 
WHERE 
    partition_date = '{{ partition_date }}';
"""

extract_to_staging = """
INSERT INTO {{ grading_schema }}.{{ staging_table }} 
SELECT 
    rowkey AS row_key,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'notificationId')::VARCHAR AS notification_id,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'eventBeginTime')::TIMESTAMP AS event_begin_time,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'idNum')::VARCHAR AS id_num,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'idType')::VARCHAR AS id_type,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'companyName')::VARCHAR AS company_name,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'customerStatus')::VARCHAR AS customer_status,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'customerType')::VARCHAR AS customer_type,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'firstName')::VARCHAR AS first_name,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'lastName')::VARCHAR AS last_name,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'title')::VARCHAR AS title,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'birthDate')::TIMESTAMP AS birth_date,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'gender')::VARCHAR AS gender,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'street')::VARCHAR AS street,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'houseNumber')::VARCHAR AS house_number,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'moo')::VARCHAR AS moo,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'postalCode')::VARCHAR AS postal_code,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'city')::VARCHAR AS city,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'district')::VARCHAR AS district,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'subDistrict')::VARCHAR AS sub_district,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'country')::VARCHAR AS country,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'roomNumber')::VARCHAR AS room_number,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'floor')::VARCHAR AS floor,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'buildingMooban')::VARCHAR AS building_mooban,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'soi')::VARCHAR AS soi,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'emailAddress')::VARCHAR AS email_address,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'goldenId')::VARCHAR AS golden_id,
    MAPTOSTRING(MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'phoneNumberList'))::VARCHAR AS phone_number_list,
    TO_CHAR(vc_par_key, 'YYYY-MM-DD')::DATE AS transaction_date,
    TO_CHAR(vc_syn_date, 'YYYY-MM-DD')::DATE AS partition_date,
    TO_CHAR(vc_syn_date, 'YYYYMM') AS partition_month,
    CURRENT_DATE() AS processed_date
FROM 
    {{ grading_schema }}.{{ source_table }}
WHERE 
    TO_CHAR(vc_syn_date, 'YYYY-MM-DD') = '{{ partition_date }}';
"""

validate_json = """
INSERT INTO {{ grading_schema }}.{{ source_error_table }} 
SELECT 
    a.rowkey,
    a.topic,
    a.raw,
    'Json Data Issue' AS error_message,
    a.header,
    CAST(a.vc_par_key AS DATE) AS vc_par_key,
    a.vc_syn_date
FROM {{ grading_schema }}.{{ source_table }} a
LEFT JOIN 
    (SELECT row_key FROM {{ grading_schema }}.{{ staging_table }}
     WHERE partition_date = '{{ partition_date }}') b
    ON a.rowkey = b.row_key
WHERE 
    TO_CHAR(vc_syn_date, 'YYYY-MM-DD')::DATE = '{{ partition_date }}' 
    AND b.row_key IS NULL;
"""

delete_duplicates_single_staging = """
DELETE FROM {{ grading_schema }}.{{ single_staging_table }} 
WHERE 
    partition_date = '{{ partition_date }}'
    AND operator_name = 'TRUE';
"""

transform_to_single_staging = """
INSERT INTO {{ grading_schema }}.{{ single_staging_table }} 
WITH rank_customers AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY id_num ORDER BY event_begin_time DESC) AS row_rank
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
            substring(md5(UPPER(id_num)) FROM 1 FOR 10) AS VARCHAR
        ) AS VARCHAR
    )) AS grading_account_id,
    UPPER(id_num) AS identifier_no,
    CASE
        WHEN REPLACE(UPPER(id_type), ' ', '') = 'PERSONALIDENTITY' THEN 'PASSPORT'
        WHEN REPLACE(UPPER(id_type), ' ', '') = 'THAIID' THEN 'THAIID'
        ELSE UPPER(id_type)
    END AS id_type,
    NULL AS customer_no, 
    company_name, 
    UPPER(customer_status) AS customer_status, 
    UPPER(customer_type) AS customer_type, 
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
    NULL AS emp_employee_id, 
    NULL AS emp_company_name, 
    NULL AS emp_position_code, 
    NULL AS emp_employee_flag, 
    'TRUE' AS operator_name, 
    transaction_date,
    partition_date,
    partition_month,
    processed_date
FROM 
    filtered_customers ;
"""