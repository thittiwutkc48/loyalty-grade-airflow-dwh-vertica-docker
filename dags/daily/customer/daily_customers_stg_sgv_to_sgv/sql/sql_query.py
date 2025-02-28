transform_dtac_to_dtac_single = """
MERGE INTO {{ grading_schema }}.{{ single_view_dtac_table }}  AS target
USING (
    SELECT * 
    FROM {{ grading_schema }}.{{ single_staging_table }} 
    WHERE partition_date = '{{ partition_date }}'
    AND operator_name = 'DTAC'
) AS source
ON target.customer_no = source.customer_no 
   AND target.operator_name = source.operator_name
WHEN MATCHED THEN
    UPDATE SET 
        grading_account_id = source.grading_account_id,
        identifier_no = source.identifier_no,
        id_type = source.id_type,
        company_name = source.company_name,
        customer_status = source.customer_status,
        customer_type = source.customer_type,
        first_name = source.first_name,
        last_name = source.last_name,
        title = source.title,
        birth_date = source.birth_date,
        gender = source.gender,
        street = source.street,
        house_number = source.house_number,
        moo = source.moo,
        postal_code = source.postal_code,
        city = source.city,
        district = source.district,
        sub_district = source.sub_district,
        country = source.country,
        room_number = source.room_number,
        floor = source.floor,
        building_mooban = source.building_mooban,
        soi = source.soi,
        email_address = source.email_address,
        golden_id = source.golden_id,
        phone_number_list = source.phone_number_list,
        emp_employee_id = source.emp_employee_id,
        emp_company_name = source.emp_company_name,
        emp_position_code = source.emp_position_code,
        emp_employee_flag = source.emp_employee_flag,
        operator_name = source.operator_name,
        transaction_date = source.transaction_date,
        partition_date = source.partition_date,
        partition_month = source.partition_month,
        processed_date = source.processed_date,
        update_at = NOW()
WHEN NOT MATCHED THEN
    INSERT (
        grading_account_id, identifier_no, id_type, customer_no, company_name, 
        customer_status, customer_type, first_name, last_name, title, 
        birth_date, gender, street, house_number, moo, postal_code, 
        city, district, sub_district, country, room_number, floor, 
        building_mooban, soi, email_address, golden_id, phone_number_list, 
        emp_employee_id, emp_company_name, emp_position_code, emp_employee_flag, 
        operator_name, create_at, update_at, transaction_date, partition_date, partition_month, processed_date
    )
    VALUES (
        source.grading_account_id, source.identifier_no, source.id_type, source.customer_no, source.company_name, 
        source.customer_status, source.customer_type, source.first_name, source.last_name, source.title, 
        source.birth_date, source.gender, source.street, source.house_number, source.moo, source.postal_code, 
        source.city, source.district, source.sub_district, source.country, source.room_number, source.floor, 
        source.building_mooban, source.soi, source.email_address, source.golden_id, source.phone_number_list, 
        source.emp_employee_id, source.emp_company_name, source.emp_position_code, source.emp_employee_flag, 
        source.operator_name, NOW(), NOW(), source.transaction_date, source.partition_date, source.partition_month, source.processed_date
    );
"""

transform_true_to_true_single = """
MERGE INTO {{ grading_schema }}.{{ single_view_true_table }} AS target
USING (
    SELECT * 
    FROM {{ grading_schema }}.{{ single_staging_table }} 
    WHERE partition_date = '{{ partition_date }}'
    AND operator_name = 'TRUE'
) AS source
ON target.grading_account_id = source.grading_account_id 
   AND target.identifier_no = source.identifier_no
   AND target.operator_name = source.operator_name
WHEN MATCHED THEN
    UPDATE SET 
        id_type = source.id_type,
        company_name = source.company_name,
        customer_status = source.customer_status,
        customer_type = source.customer_type,
        first_name = source.first_name,
        last_name = source.last_name,
        title = source.title,
        birth_date = source.birth_date,
        gender = source.gender,
        street = source.street,
        house_number = source.house_number,
        moo = source.moo,
        postal_code = source.postal_code,
        city = source.city,
        district = source.district,
        sub_district = source.sub_district,
        country = source.country,
        room_number = source.room_number,
        floor = source.floor,
        building_mooban = source.building_mooban,
        soi = source.soi,
        email_address = source.email_address,
        golden_id = source.golden_id,
        phone_number_list = source.phone_number_list,
        emp_employee_id = source.emp_employee_id,
        emp_company_name = source.emp_company_name,
        emp_position_code = source.emp_position_code,
        emp_employee_flag = source.emp_employee_flag,
        transaction_date = source.transaction_date,
        partition_date = source.partition_date,
        partition_month = source.partition_month,
        processed_date = source.processed_date,
        update_at = NOW()
WHEN NOT MATCHED THEN
    INSERT (
        grading_account_id, identifier_no, id_type, customer_no, company_name, 
        customer_status, customer_type, first_name, last_name, title, 
        birth_date, gender, street, house_number, moo, postal_code, 
        city, district, sub_district, country, room_number, floor, 
        building_mooban, soi, email_address, golden_id, phone_number_list, 
        emp_employee_id, emp_company_name, emp_position_code, emp_employee_flag, 
        operator_name, create_at, update_at, transaction_date, partition_date, partition_month, processed_date
    )
    VALUES (
        source.grading_account_id, source.identifier_no, source.id_type, source.customer_no, source.company_name, 
        source.customer_status, source.customer_type, source.first_name, source.last_name, source.title, 
        source.birth_date, source.gender, source.street, source.house_number, source.moo, source.postal_code, 
        source.city, source.district, source.sub_district, source.country, source.room_number, source.floor, 
        source.building_mooban, source.soi, source.email_address, source.golden_id, source.phone_number_list, 
        source.emp_employee_id, source.emp_company_name, source.emp_position_code, source.emp_employee_flag, 
        source.operator_name, NOW(), NOW(), source.transaction_date, source.partition_date, source.partition_month, source.processed_date
    );
"""

create_view_true_dtac_single = """
CREATE OR REPLACE VIEW {{ grading_schema }}.{{ single_view_table }} AS
SELECT 
    COALESCE(true_tbl.grading_account_id, dtac_tbl.grading_account_id) AS grading_account_id,
    COALESCE(true_tbl.identifier_no, dtac_tbl.identifier_no) AS identifier_no,
    COALESCE(true_tbl.customer_no, dtac_tbl.customer_no) AS customer_no,
    COALESCE(true_tbl.company_name, dtac_tbl.company_name) AS company_name,
    COALESCE(true_tbl.customer_status, dtac_tbl.customer_status) AS customer_status,
    COALESCE(true_tbl.customer_type, dtac_tbl.customer_type) AS customer_type,
    COALESCE(true_tbl.first_name, dtac_tbl.first_name) AS first_name,
    COALESCE(true_tbl.last_name, dtac_tbl.last_name) AS last_name,
    COALESCE(true_tbl.title, dtac_tbl.title) AS title,
    COALESCE(true_tbl.birth_date, dtac_tbl.birth_date) AS birth_date,
    COALESCE(true_tbl.gender, dtac_tbl.gender) AS gender,
    COALESCE(true_tbl.street, dtac_tbl.street) AS street,
    COALESCE(true_tbl.house_number, dtac_tbl.house_number) AS house_number,
    COALESCE(true_tbl.moo, dtac_tbl.moo) AS moo,
    COALESCE(true_tbl.postal_code, dtac_tbl.postal_code) AS postal_code,
    COALESCE(true_tbl.city, dtac_tbl.city) AS city,
    COALESCE(true_tbl.district, dtac_tbl.district) AS district,
    COALESCE(true_tbl.sub_district, dtac_tbl.sub_district) AS sub_district,
    COALESCE(true_tbl.country, dtac_tbl.country) AS country,
    COALESCE(true_tbl.room_number, dtac_tbl.room_number) AS room_number,
    COALESCE(true_tbl.floor, dtac_tbl.floor) AS floor,
    COALESCE(true_tbl.building_mooban, dtac_tbl.building_mooban) AS building_mooban,
    COALESCE(true_tbl.soi, dtac_tbl.soi) AS soi,
    COALESCE(true_tbl.email_address, dtac_tbl.email_address) AS email_address,
    COALESCE(true_tbl.golden_id, dtac_tbl.golden_id) AS golden_id,
    COALESCE(true_tbl.phone_number_list, dtac_tbl.phone_number_list) AS phone_number_list,
    COALESCE(true_tbl.emp_employee_id, dtac_tbl.emp_employee_id) AS emp_employee_id,
    COALESCE(true_tbl.emp_company_name, dtac_tbl.emp_company_name) AS emp_company_name,
    COALESCE(true_tbl.emp_position_code, dtac_tbl.emp_position_code) AS emp_position_code,
    COALESCE(true_tbl.emp_employee_flag, dtac_tbl.emp_employee_flag) AS emp_employee_flag,
    CASE 
        WHEN true_tbl.operator_name IS NOT NULL THEN 'TRUE' 
        ELSE 'DTAC' 
    END AS operator_name,  -- Prioritizing TRUE if available
    COALESCE(true_tbl.create_at, dtac_tbl.create_at) AS create_at,
    COALESCE(true_tbl.update_at, dtac_tbl.update_at) AS update_at,
    COALESCE(true_tbl.transaction_date, dtac_tbl.transaction_date) AS transaction_date,
    COALESCE(true_tbl.partition_date, dtac_tbl.partition_date) AS partition_date,
    COALESCE(true_tbl.partition_month, dtac_tbl.partition_month) AS partition_month,
    COALESCE(true_tbl.processed_date, dtac_tbl.processed_date) AS processed_date
FROM {{ grading_schema }}.{{ single_view_true_table }} AS true_tbl
FULL OUTER JOIN {{ grading_schema }}.{{ single_view_dtac_table }} AS dtac_tbl
ON  true_tbl.grading_account_id = dtac_tbl.grading_account_id
    AND true_tbl.identifier_no = dtac_tbl.identifier_no;
"""