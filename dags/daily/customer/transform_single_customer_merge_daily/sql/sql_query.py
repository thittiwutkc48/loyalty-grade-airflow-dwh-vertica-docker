truncate_to_single = """
TRUNCATE {{ grading_schema }}.{{ single_table }} 
"""

transform_to_single = """
INSERT INTO {{ grading_schema }}.{{ single_table }} 
WITH ranked_customers AS (
    SELECT 
        gradingaccountid, 
        identifier_no, 
        id_type, 
        customer_no, 
        company_name, 
        customer_status, 
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
        customer_type, 
        emp_employee_id, 
        emp_company_name, 
        emp_position_code, 
        emp_employee_flag, 
        operator_name,
        partition_date,
        ROW_NUMBER() OVER (
            PARTITION BY gradingaccountid
            ORDER BY 
                CASE 
                    WHEN operator_name = 'TRUE' THEN 1
                    ELSE 2 
                END,
                partition_date DESC
        ) AS row_num
    FROM 
        {{ grading_schema }}.{{ staging_table }} 
)
SELECT 
    rn_cus.gradingaccountid,
    rn_cus.identifier_no,
    rn_cus.id_type,
    rn_cus.customer_no,
    rn_cus.company_name,
    rn_cus.customer_status,
    rn_cus.first_name,
    rn_cus.last_name,
    rn_cus.title,
    rn_cus.birth_date,
    rn_cus.gender,
    rn_cus.street,
    rn_cus.house_number,
    rn_cus.moo,
    rn_cus.postal_code,
    rn_cus.city,
    rn_cus.district,
    rn_cus.sub_district,
    rn_cus.country,
    rn_cus.room_number,
    rn_cus.floor,
    rn_cus.building_mooban,
    rn_cus.soi,
    rn_cus.email_address,
    rn_cus.golden_id,
    rn_cus.customer_type,
    emp.employee_id AS emp_employee_id,
    emp.company_name AS emp_company_name,
    emp.position_name AS emp_position_code,
    CASE 
        WHEN emp.employee_id IS NOT NULL THEN 'Y'
        ELSE 'N'
    END AS emp_employee_flag,
    CASE
        WHEN op.distinct_operator_count > 1 THEN 'TRUE'
        ELSE rn_cus.operator_name
    END AS operator_name,
    rn_cus.partition_date AS transaction_date,
    CURRENT_DATE AS partition_date,
    TO_CHAR(CURRENT_DATE, 'YYYYMM') AS partition_month
FROM 
    ranked_customers rn_cus
LEFT JOIN 
    {{ grading_schema }}.{{ employee_table }} emp 
    ON emp.national_id = rn_cus.identifier_no
LEFT JOIN (
    SELECT 
        gradingaccountid,
        COUNT(DISTINCT operator_name) AS distinct_operator_count
    FROM 
        {{ grading_schema }}.{{ staging_table }} 
    GROUP BY 
        gradingaccountid
) op 
    ON op.gradingaccountid = rn_cus.gradingaccountid
WHERE 
    rn_cus.row_num = 1;
"""