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
    NULL AS identifier_no,
    NULL AS id_type,
    NULL AS customer_type,
    cust_numb AS customer_no,
    CASE 
      WHEN LENGTH(subr_numb) > 1 AND subr_numb IS NOT NULL
      THEN {{ function_schema }}VoltageSecureAccess(subr_numb USING PARAMETERS format='AlphaNumeric', config_dfs_path='/voltagesecure/conf')
      ELSE subr_numb
    END AS product_id,
    CASE 
        WHEN telp_type = 'T' THEN 'Postpay'
        WHEN telp_type = 'P' THEN 'Prepay'
        ELSE NULL
    END AS product_name,
    CASE 
        WHEN telp_type = 'T' THEN 'Mobile Postpay'
        WHEN telp_type = 'P' THEN 'Mobile Prepay'
        ELSE NULL 
    END AS product_name_desc,
    CASE 
        WHEN subr_stts = 'A' THEN 'ACTIVE'
        WHEN subr_stts = 'C' THEN 'INACTIVE'
        WHEN subr_stts = 'O' THEN 'SWITCH OFF'
        WHEN subr_stts = 'S' THEN 'SUSPEND'
        ELSE NULL
    END AS product_status,
    'Dtac Mobile' AS product_line,
    CASE 
        WHEN telp_type = 'T' THEN 'POST'
        WHEN telp_type = 'P' THEN 'PREP'
        ELSE NULL
    END AS product_group,
    CASE 
        WHEN telp_type = 'T' THEN pkgp_strt_date
        WHEN telp_type = 'P' THEN pkgp_strt_date
        ELSE NULL
    END AS start_date,
    NULL AS end_date,
    CASE 
        WHEN sms_lang = 'T' THEN 'TH'
        WHEN sms_lang = 'E' THEN 'EN'
        ELSE NULL
    END AS language,
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
    CASE 
        WHEN subr_type = 'V' THEN 'VIP' 
        ELSE NULL
    END AS vip,
    TO_CHAR(file_date, 'YYYY-MM-DD')::DATE AS transaction_date,
    TO_CHAR(load_date, 'YYYY-MM-DD')::DATE AS partition_date,
    TO_CHAR(load_date, 'YYYYMM') AS partition_month,
    CURRENT_DATE AS processed_date
FROM {{ grading_schema }}.{{ source_table }}
WHERE TO_CHAR(load_date, 'YYYY-MM-DD') = '{{ partition_date }}'
    AND cust_numb IS NOT NULL;
"""

delete_duplicates_single_staging = """
DELETE FROM {{ grading_schema }}.{{ single_staging_table }} 
WHERE 
    partition_date = '{{ partition_date }}'
    AND operator_name = 'DTAC';
"""

transform_to_single_staging = """
INSERT INTO {{ grading_schema }}.{{ single_staging_table }}
WITH ranked_subscribers AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_no, product_id
            ORDER BY last_chng_dttm DESC
        ) AS row_num
    FROM {{ grading_schema }}.{{ staging_table }}
    WHERE partition_date = '{{ partition_date }}'
),
ranked_subscribers_filter AS (
    SELECT * 
    FROM ranked_subscribers
    WHERE row_num = 1 
)
SELECT 
    sdc.grading_account_id,
    sdc.identifier_no,
    sdc.id_type,
    sdc.customer_type,
    rsf.customer_no,
    NULL AS crm_integration_id,
    NULL AS golden_id,
    CASE 
        WHEN rsf.product_id LIKE '66%' THEN '0' || SUBSTRING(rsf.product_id FROM 3) 
        ELSE rsf.product_id 
    END AS product_id,
    rsf.product_name,
    rsf.product_name_desc,
    rsf.product_status,
    rsf.product_line,
    rsf.product_group,
    NULL AS ban,
    NULL AS billing_sub_no,
    rsf.start_date,
    rsf.end_date,
    rsf.language,
    NULL AS street,
    NULL AS house_number,
    NULL AS moo,
    NULL AS postal_code,
    NULL AS city,
    NULL AS district,
    NULL AS sub_district,
    NULL AS country,
    rsf.first_name,
    rsf.last_name,
    NULL AS ou_id,
    rsf.vip,
    NULL AS company_code,
    NULL AS subscriber_code,
    NULL AS subscriber_description,
    NULL AS subscriber_type,
    NULL AS subscriber_price_plan_code,
    NULL AS subscriber_status,
    NULL AS subscriber_start_date,
    NULL AS subscriber_end_date,
    'DTAC' AS operator_name,
    rsf.transaction_date,
    rsf.partition_date,
    rsf.partition_month,
    rsf.processed_date
FROM ranked_subscribers_filter rsf
LEFT JOIN {{ grading_schema }}.{{ single_view_customer_table }} sdc
    ON rsf.customer_no = sdc.customer_no
WHERE sdc.grading_account_id IS NOT NULL
    AND sdc.id_type IN ('THAIID','PASSPORT');
"""

transform_to_unmatched_single_staging= """
INSERT INTO {{ grading_schema }}.{{ single_staging_unmatched_table }}
WITH ranked_subscribers AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_no, product_id
            ORDER BY last_chng_dttm DESC
        ) AS row_num
    FROM {{ grading_schema }}.{{ staging_table }}
    WHERE partition_date = '{{ partition_date }}'
),
ranked_subscribers_filter AS (
    SELECT * 
    FROM ranked_subscribers
    WHERE row_num = 1 
)
SELECT 
    sdc.grading_account_id,
    sdc.identifier_no,
    sdc.id_type,
    sdc.customer_type,
    rsf.customer_no,
    NULL AS crm_integration_id,
    NULL AS golden_id,
    CASE 
        WHEN rsf.product_id LIKE '66%' THEN '0' || SUBSTRING(rsf.product_id FROM 3) 
        ELSE rsf.product_id 
    END AS product_id,
    rsf.product_name,
    rsf.product_name_desc,
    rsf.product_status,
    rsf.product_line,
    rsf.product_group,
    NULL AS ban,
    NULL AS billing_sub_no,
    rsf.start_date,
    rsf.end_date,
    rsf.language,
    NULL AS street,
    NULL AS house_number,
    NULL AS moo,
    NULL AS postal_code,
    NULL AS city,
    NULL AS district,
    NULL AS sub_district,
    NULL AS country,
    rsf.first_name,
    rsf.last_name,
    NULL AS ou_id,
    rsf.vip,
    NULL AS company_code,
    NULL AS subscriber_code,
    NULL AS subscriber_description,
    NULL AS subscriber_type,
    NULL AS subscriber_price_plan_code,
    NULL AS subscriber_status,
    NULL AS subscriber_start_date,
    NULL AS subscriber_end_date,
    'DTAC' AS operator_name,
    rsf.transaction_date,
    rsf.partition_date,
    rsf.partition_month,
    rsf.processed_date
FROM ranked_subscribers_filter rsf
LEFT JOIN {{ grading_schema }}.{{ single_view_customer_table }} sdc
    ON rsf.customer_no = sdc.customer_no
WHERE sdc.grading_account_id IS NULL;
"""

transform_inactive_to_single_view = """
MERGE INTO {{ grading_schema }}.{{ single_view_subscriber_table }} AS target
USING (
    SELECT * 
    FROM {{ grading_schema }}.{{ single_staging_table }}
    WHERE partition_date = '{{ partition_date }}'
        AND operator_name = 'DTAC'
        AND product_status = 'INACTIVE'
) AS source
ON target.product_id = source.product_id
AND target.customer_no = source.customer_no
AND target.operator_name = source.operator_name
WHEN MATCHED THEN 
    UPDATE SET 
        grading_account_id = source.grading_account_id,
        identifier_no = source.identifier_no,
        id_type = source.id_type,
        customer_type = source.customer_type,
        golden_id = source.golden_id,
        product_name = source.product_name,
        product_name_desc = source.product_name_desc,
        product_status = source.product_status,
        product_line = source.product_line,
        product_group = source.product_group,
        ban = source.ban,
        billing_sub_no = source.billing_sub_no,
        start_date = source.start_date,
        end_date = source.end_date,
        language = source.language,
        street = source.street,
        house_number = source.house_number,
        moo = source.moo,
        postal_code = source.postal_code,
        city = source.city,
        district = source.district,
        sub_district = source.sub_district,
        country = source.country,
        first_name = source.first_name,
        last_name = source.last_name,
        ou_id = source.ou_id,
        vip = source.vip,
        company_code = source.company_code,
        subscriber_code = source.subscriber_code,
        subscriber_description = source.subscriber_description,
        subscriber_type = source.subscriber_type,
        subscriber_price_plan_code = source.subscriber_price_plan_code,
        subscriber_status = source.subscriber_status,
        subscriber_start_date = source.subscriber_start_date,
        subscriber_end_date = source.subscriber_end_date,
        update_at = NOW(),
        transaction_date = source.transaction_date,
        partition_date = source.partition_date,
        partition_month = source.partition_month,
        processed_date = source.processed_date
WHEN NOT MATCHED THEN
    INSERT (
        grading_account_id,
        identifier_no,
        id_type,
        customer_type,
        customer_no,
        crm_integration_id,
        golden_id,
        product_id,
        product_name,
        product_name_desc,
        product_status,
        product_line,
        product_group,
        ban,
        billing_sub_no,
        start_date,
        end_date,
        language,
        street,
        house_number,
        moo,
        postal_code,
        city,
        district,
        sub_district,
        country,
        first_name,
        last_name,
        ou_id,
        vip,
        company_code,
        subscriber_code,
        subscriber_description,
        subscriber_type,
        subscriber_price_plan_code,
        subscriber_status,
        subscriber_start_date,
        subscriber_end_date,
        operator_name,
        create_at,
        update_at,
        transaction_date,
        partition_date,
        partition_month,
        processed_date
    )
    VALUES (
        source.grading_account_id,
        source.identifier_no,
        source.id_type,
        source.customer_type,
        source.customer_no,
        source.crm_integration_id,
        source.golden_id,
        source.product_id,
        source.product_name,
        source.product_name_desc,
        source.product_status,
        source.product_line,
        source.product_group,
        source.ban,
        source.billing_sub_no,
        source.start_date,
        source.end_date,
        source.language,
        source.street,
        source.house_number,
        source.moo,
        source.postal_code,
        source.city,
        source.district,
        source.sub_district,
        source.country,
        source.first_name,
        source.last_name,
        source.ou_id,
        source.vip,
        source.company_code,
        source.subscriber_code,
        source.subscriber_description,
        source.subscriber_type,
        source.subscriber_price_plan_code,
        source.subscriber_status,
        source.subscriber_start_date,
        source.subscriber_end_date,
        source.operator_name,
        NOW(),
        NOW(),
        source.transaction_date,
        source.partition_date,
        source.partition_month,
        source.processed_date
    );
"""

insert_to_product_history = """
INSERT INTO {{ grading_schema }}.{{ single_view_history_table }}
SELECT * FROM {{ grading_schema }}.{{ single_view_subscriber_table }}
WHERE product_status = 'INACTIVE'
    AND operator_name = 'DTAC';
"""

delete_inactive_in_single_view = """
DELETE FROM {{ grading_schema }}.{{ single_view_subscriber_table }}
WHERE product_status = 'INACTIVE' 
    AND operator_name = 'DTAC';
"""

transform_active_to_single_view = """
MERGE INTO {{ grading_schema }}.{{ single_view_subscriber_table }} AS target
USING (
    SELECT * 
    FROM {{ grading_schema }}.{{ single_staging_table }}
    WHERE partition_date = '{{ partition_date }}'
        AND operator_name = 'DTAC'
        AND product_status = 'ACTIVE'
) AS source
ON target.product_id = source.product_id
AND target.customer_no = source.customer_no
AND target.operator_name = source.operator_name
WHEN MATCHED THEN 
    UPDATE SET 
        grading_account_id = source.grading_account_id,
        identifier_no = source.identifier_no,
        id_type = source.id_type,
        customer_type = source.customer_type,
        golden_id = source.golden_id,
        product_name = source.product_name,
        product_name_desc = source.product_name_desc,
        product_status = source.product_status,
        product_line = source.product_line,
        product_group = source.product_group,
        ban = source.ban,
        billing_sub_no = source.billing_sub_no,
        start_date = source.start_date,
        end_date = source.end_date,
        language = source.language,
        street = source.street,
        house_number = source.house_number,
        moo = source.moo,
        postal_code = source.postal_code,
        city = source.city,
        district = source.district,
        sub_district = source.sub_district,
        country = source.country,
        first_name = source.first_name,
        last_name = source.last_name,
        ou_id = source.ou_id,
        vip = source.vip,
        company_code = source.company_code,
        subscriber_code = source.subscriber_code,
        subscriber_description = source.subscriber_description,
        subscriber_type = source.subscriber_type,
        subscriber_price_plan_code = source.subscriber_price_plan_code,
        subscriber_status = source.subscriber_status,
        subscriber_start_date = source.subscriber_start_date,
        subscriber_end_date = source.subscriber_end_date,
        update_at = NOW(),
        transaction_date = source.transaction_date,
        partition_date = source.partition_date,
        partition_month = source.partition_month,
        processed_date = source.processed_date
WHEN NOT MATCHED THEN
    INSERT (
        grading_account_id,
        identifier_no,
        id_type,
        customer_type,
        customer_no,
        crm_integration_id,
        golden_id,
        product_id,
        product_name,
        product_name_desc,
        product_status,
        product_line,
        product_group,
        ban,
        billing_sub_no,
        start_date,
        end_date,
        language,
        street,
        house_number,
        moo,
        postal_code,
        city,
        district,
        sub_district,
        country,
        first_name,
        last_name,
        ou_id,
        vip,
        company_code,
        subscriber_code,
        subscriber_description,
        subscriber_type,
        subscriber_price_plan_code,
        subscriber_status,
        subscriber_start_date,
        subscriber_end_date,
        operator_name,
        create_at,
        update_at,
        transaction_date,
        partition_date,
        partition_month,
        processed_date
    )
    VALUES (
        source.grading_account_id,
        source.identifier_no,
        source.id_type,
        source.customer_type,
        source.customer_no,
        source.crm_integration_id,
        source.golden_id,
        source.product_id,
        source.product_name,
        source.product_name_desc,
        source.product_status,
        source.product_line,
        source.product_group,
        source.ban,
        source.billing_sub_no,
        source.start_date,
        source.end_date,
        source.language,
        source.street,
        source.house_number,
        source.moo,
        source.postal_code,
        source.city,
        source.district,
        source.sub_district,
        source.country,
        source.first_name,
        source.last_name,
        source.ou_id,
        source.vip,
        source.company_code,
        source.subscriber_code,
        source.subscriber_description,
        source.subscriber_type,
        source.subscriber_price_plan_code,
        source.subscriber_status,
        source.subscriber_start_date,
        source.subscriber_end_date,
        source.operator_name,
        NOW(),
        NOW(),
        source.transaction_date,
        source.partition_date,
        source.partition_month,
        source.processed_date
    );
"""
