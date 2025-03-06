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
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'customerType')::VARCHAR AS customer_type,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'crmIntegrationId')::VARCHAR AS crm_integration_id,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'goldenId')::VARCHAR AS golden_id,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'productId')::VARCHAR AS product_id,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'productName')::VARCHAR AS product_name,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'productNameDesc')::VARCHAR AS product_name_desc,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'productStatus')::VARCHAR AS product_status,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'productLine')::VARCHAR AS product_line,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'ban')::VARCHAR AS ban,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'billingSubNo')::VARCHAR AS billing_sub_no,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'startDate')::TIMESTAMP AS start_date,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'endDate')::TIMESTAMP AS end_date,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'language')::VARCHAR AS language,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'street')::VARCHAR AS street,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'houseNumber')::VARCHAR AS house_number,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'moo')::VARCHAR AS moo,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'postalCode')::VARCHAR AS postal_code,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'city')::VARCHAR AS city,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'district')::VARCHAR AS district,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'subDistrict')::VARCHAR AS sub_district,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'country')::VARCHAR AS country,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'firstName')::VARCHAR AS first_name,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'lastName')::VARCHAR AS last_name,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'ouId')::VARCHAR AS ou_id,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'vip')::VARCHAR AS vip,
    MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'companyCode')::VARCHAR AS company_code,
    MAPTOSTRING(MAPLOOKUP(MAPJSONEXTRACTOR(raw), 'productCompList'))::VARCHAR AS product_comp_list, 
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

extract_to_staging_product = """
INSERT INTO {{ grading_schema }}.{{ staging_product_table }}     
WITH product_comp_map AS (
    SELECT 
        product_id,
        product_line,
        transaction_date,
        partition_date,
        partition_month,
        product_comp_list,
        REGEXP_COUNT(product_comp_list, '.code') AS item_list
    FROM {{ grading_schema }}.{{ staging_table }}
    WHERE partition_date = '{{ partition_date }}'
      AND product_comp_list IS NOT NULL 
      AND product_comp_list != '{}'
      AND REGEXP_COUNT(product_comp_list, '.code') > 0 
),
filtered_id_series AS (
    SELECT 
        pcm.product_id,
        pcm.product_line,
        pcm.transaction_date,
        pcm.partition_date,
        pcm.partition_month,
        pcm.product_comp_list,
        pcm.item_list,
        id_series.id AS idx,
        CASE 
            WHEN id_series.id < pcm.item_list THEN TRUE 
            ELSE FALSE 
        END AS extract_status
    FROM product_comp_map pcm
    INNER JOIN (
        SELECT id
        FROM CLYMAPPO.id_series_sorted_table 
        WHERE id <= (SELECT MAX(item_list) FROM product_comp_map)
        ORDER BY id
    ) id_series
    ON pcm.item_list > id_series.id
),
filtered_id_series_status AS (
    SELECT * FROM filtered_id_series
    WHERE extract_status IS TRUE
),
product_component_details AS ( 
    SELECT 
        product_id,
        product_line,
        idx,
        REGEXP_SUBSTR(product_comp_list, '"' || idx::VARCHAR || '.code": ?"([^"]+)"', 1, 1, '', 1)::VARCHAR AS code,
        REGEXP_SUBSTR(product_comp_list, '"' || idx::VARCHAR || '.desc": ?"([^"]+)"', 1, 1, '', 1)::VARCHAR AS description,
        NULL AS subscriber_price_plan_code,
        REGEXP_SUBSTR(product_comp_list, '"' || idx::VARCHAR || '.endDate": ?"([^"]+)"', 1, 1, '', 1)::DATE AS end_date,
        REGEXP_SUBSTR(product_comp_list, '"' || idx::VARCHAR || '.startDate": ?"([^"]+)"', 1, 1, '', 1)::DATE AS start_date,
        REGEXP_SUBSTR(product_comp_list, '"' || idx::VARCHAR || '.status": ?"([^"]+)"', 1, 1, '', 1)::VARCHAR AS status,
        REGEXP_SUBSTR(product_comp_list, '"' || idx::VARCHAR || '.type": ?"([^"]+)"', 1, 1, '', 1)::VARCHAR AS type,
        transaction_date,
        partition_date,
        partition_month,
        CURRENT_DATE AS processed_date
    FROM filtered_id_series_status
)
SELECT 
 	product_id,
    product_line,
    idx,
    code,
    description,
    CASE 
        WHEN product_line IN ('True Mobile', 'True Visions') THEN 
            CASE 
                WHEN POSITION('-' IN description) > 0 THEN 
                    TRIM(SPLIT_PART(description, '-', 2))
                ELSE 
                    TRIM(description)
            END
        WHEN product_line IN ('True Online') THEN 
            TRIM(description)
        ELSE NULL
    END AS subscriber_price_plan_code,
    end_date,
    start_date,
    status,
    type,
    transaction_date,
    partition_date,
    partition_month,
    CURRENT_DATE() AS processed_date
FROM product_component_details
WHERE code IS NOT NULL AND type IN ('Channel Group', 'Price Plan') 
    AND status NOT IN ('Inactive') ;
"""

create_view_temp_product_id_inactive = """
CREATE OR REPLACE VIEW {{ grading_schema }}.{{ temp_product_table }} AS
SELECT 
    product_id, 
    partition_date, 
    COUNT(*) AS product_count
FROM {{ grading_schema }}.{{ single_view_table }}
WHERE product_status = 'INACTIVE'
        AND operator_name = 'TRUE'
GROUP BY product_id, partition_date
HAVING COUNT(*) > 1;
"""

update_product_id_in_temp_inactive = """
UPDATE {{ grading_schema }}.{{ single_view_table }}
SET product_status = 'INACTIVE'
WHERE product_id IN (SELECT product_id FROM {{ grading_schema }}.{{ temp_product_table }});
"""

insert_to_product_history = """
INSERT INTO {{ grading_schema }}.{{ single_view_history_table }}
SELECT * FROM {{ grading_schema }}.{{ single_view_table }}
WHERE product_status = 'INACTIVE'
    AND operator_name = 'TRUE';
"""

delete_inactive_in_single_view = """
DELETE FROM {{ grading_schema }}.{{ single_view_table }}
WHERE product_status = 'INACTIVE' 
    AND operator_name = 'TRUE';
"""

delete_duplicates_single_staging = """
DELETE FROM {{ grading_schema }}.{{ single_staging_table }} 
WHERE partition_date = '{{ partition_date }}'
    AND operator_name = 'TRUE';
"""

transform_inactive_to_single_staging = """
INSERT INTO {{ grading_schema }}.{{ single_staging_table }}
WITH ranked_subscribers AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY UPPER(id_num), product_id, crm_integration_id
                ORDER BY event_begin_time DESC
            ) AS row_num
        FROM {{ grading_schema }}.{{ staging_table }} 
        WHERE partition_date = '{{ partition_date }}'
            AND UPPER(product_status) = 'INACTIVE'
    ), 
    priceplan_ranked_data AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY product_id, product_line
                ORDER BY start_date DESC
            ) AS row_num
        FROM {{ grading_schema }}.{{ staging_product_table }}     
        WHERE partition_date = '{{ partition_date }}'
            AND (
                (product_line IN ('True Mobile', 'True Online', 'Fixedline') AND type = 'Price Plan') 
                OR (product_line = 'True Visions' AND type = 'Channel Group')
            )
    ),
    ranked_subscribers_filter AS (
        SELECT * 
        FROM ranked_subscribers
        WHERE row_num = 1 
    ),
    ranked_subscribers_product AS (
        SELECT 
            UPPER('PG' || CAST(SUBSTRING(md5(UPPER(id_num)) FROM 1 FOR 10) AS VARCHAR)) AS grading_account_id,
            rn_sub.id_num AS identifier_no,
            CASE
                WHEN REPLACE(UPPER(rn_sub.id_type), ' ', '') = 'PERSONALIDENTITY' THEN 'PASSPORT'
                WHEN REPLACE(UPPER(rn_sub.id_type), ' ', '') = 'THAIID' THEN 'THAIID'
                ELSE UPPER(rn_sub.id_type)
            END AS id_type,
            UPPER(rn_sub.customer_type) AS customer_type,
            NULL AS customer_no,
            rn_sub.crm_integration_id,
            rn_sub.golden_id,
            rn_sub.product_id,
            rn_sub.product_name,
            NULL AS product_name_desc,
            UPPER(rn_sub.product_status) AS product_status,
            rn_sub.product_line,
            CASE 
                WHEN rn_sub.product_name IN ('Postpay', 'Prepay') THEN UPPER(SUBSTRING(rn_sub.product_name, 1, 4))
                WHEN rn_sub.product_line = 'True Visions' THEN 'TVS'
                WHEN rn_sub.product_line = 'True Online' THEN 'INTERNET'
                WHEN UPPER(SUBSTRING(rn_sub.product_name, 1, 3)) = 'FIX' THEN 'FIXEDLINE'
                ELSE rn_sub.product_name        
            END AS product_group,
            rn_sub.ban,
            rn_sub.billing_sub_no,
            rn_sub.start_date,
            rn_sub.end_date,
            rn_sub."language",
            rn_sub.street,
            rn_sub.house_number,
            rn_sub.moo,
            rn_sub.postal_code,
            rn_sub.city,
            rn_sub.district,
            rn_sub.sub_district,
            rn_sub.country,
            rn_sub.first_name,
            rn_sub.last_name,
            rn_sub.ou_id,
            rn_sub.vip,
            rn_sub.company_code,
            pp_rn.code AS subscriber_code,
            pp_rn.description AS subscriber_description,
            pp_rn.type AS subscriber_type,
            pp_rn.subscriber_price_plan_code,
            UPPER(pp_rn.status) AS subscriber_status,
            pp_rn.start_date AS subscriber_start_date,
            pp_rn.end_date AS subscriber_end_date,
            'TRUE' AS operator_name,
            rn_sub.transaction_date,
            rn_sub.partition_date,
            rn_sub.partition_month,
            CURRENT_DATE AS processed_date
        FROM ranked_subscribers_filter rn_sub
        LEFT JOIN (
            SELECT 
                product_id,
                code,
                description,
                type,
                subscriber_price_plan_code,
                status,
                start_date,
                end_date
            FROM priceplan_ranked_data 
            WHERE row_num = 1
        ) pp_rn 
        ON pp_rn.product_id = rn_sub.product_id
    )
    SELECT * 
    FROM ranked_subscribers_product
    WHERE id_type IN ('PASSPORT', 'THAIID');
"""

transform_non_inactive_to_single_staging = """
INSERT INTO {{ grading_schema }}.{{ single_staging_table }}
WITH ranked_subscribers AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY event_begin_time DESC
            ) AS row_num
        FROM {{ grading_schema }}.{{ staging_table }} 
        WHERE partition_date = '{{ partition_date }}'
            AND UPPER(product_status) != 'INACTIVE'
    ), 
    priceplan_ranked_data AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY product_id, product_line
                ORDER BY start_date DESC
            ) AS row_num
        FROM {{ grading_schema }}.{{ staging_product_table }}     
        WHERE partition_date = '{{ partition_date }}'
            AND (
                (product_line IN ('True Mobile', 'True Online', 'Fixedline') AND type = 'Price Plan') 
                OR (product_line = 'True Visions' AND type = 'Channel Group')
            )
    ),
    ranked_subscribers_filter AS (
        SELECT * 
        FROM ranked_subscribers
        WHERE row_num = 1 
    ),
    ranked_subscribers_product AS (
        SELECT 
            UPPER('PG' || CAST(SUBSTRING(md5(UPPER(id_num)) FROM 1 FOR 10) AS VARCHAR)) AS grading_account_id,
            rn_sub.id_num AS identifier_no,
            CASE
                WHEN REPLACE(UPPER(rn_sub.id_type), ' ', '') = 'PERSONALIDENTITY' THEN 'PASSPORT'
                WHEN REPLACE(UPPER(rn_sub.id_type), ' ', '') = 'THAIID' THEN 'THAIID'
                ELSE UPPER(rn_sub.id_type)
            END AS id_type,
            UPPER(rn_sub.customer_type) AS customer_type,
            NULL AS customer_no,
            rn_sub.crm_integration_id,
            rn_sub.golden_id,
            rn_sub.product_id,
            rn_sub.product_name,
            NULL AS product_name_desc,
            UPPER(rn_sub.product_status) AS product_status,
            rn_sub.product_line,
            CASE 
                WHEN rn_sub.product_name IN ('Postpay', 'Prepay') THEN UPPER(SUBSTRING(rn_sub.product_name, 1, 4))
                WHEN rn_sub.product_line = 'True Visions' THEN 'TVS'
                WHEN rn_sub.product_line = 'True Online' THEN 'INTERNET'
                WHEN UPPER(SUBSTRING(rn_sub.product_name, 1, 3)) = 'FIX' THEN 'FIXEDLINE'
                ELSE rn_sub.product_name        
            END AS product_group,
            rn_sub.ban,
            rn_sub.billing_sub_no,
            rn_sub.start_date,
            rn_sub.end_date,
            rn_sub."language",
            rn_sub.street,
            rn_sub.house_number,
            rn_sub.moo,
            rn_sub.postal_code,
            rn_sub.city,
            rn_sub.district,
            rn_sub.sub_district,
            rn_sub.country,
            rn_sub.first_name,
            rn_sub.last_name,
            rn_sub.ou_id,
            rn_sub.vip,
            rn_sub.company_code,
            pp_rn.code AS subscriber_code,
            pp_rn.description AS subscriber_description,
            pp_rn.type AS subscriber_type,
            pp_rn.subscriber_price_plan_code,
            UPPER(pp_rn.status) AS subscriber_status,
            pp_rn.start_date AS subscriber_start_date,
            pp_rn.end_date AS subscriber_end_date,
            'TRUE' AS operator_name,
            rn_sub.transaction_date,
            rn_sub.partition_date,
            rn_sub.partition_month,
            CURRENT_DATE AS processed_date
        FROM ranked_subscribers_filter rn_sub
        LEFT JOIN (
            SELECT 
                product_id,
                code,
                description,
                type,
                subscriber_price_plan_code,
                status,
                start_date,
                end_date
            FROM priceplan_ranked_data 
            WHERE row_num = 1
        ) pp_rn 
        ON pp_rn.product_id = rn_sub.product_id
    )
    SELECT * 
    FROM ranked_subscribers_product
    WHERE id_type IN ('PASSPORT', 'THAIID');
"""

transform_inactive_to_single_view = """
MERGE INTO {{ grading_schema }}.{{ single_view_table }} AS target
USING (
    SELECT * 
    FROM {{ grading_schema }}.{{ single_staging_table }}
    WHERE partition_date = '{{ partition_date }}'
        AND operator_name = 'TRUE'
        AND product_status = 'INACTIVE'
) AS source
ON 
    target.grading_account_id = source.grading_account_id 
    AND target.identifier_no = source.identifier_no
    AND target.crm_integration_id = source.crm_integration_id
    AND target.product_id = source.product_id
    AND target.operator_name = source.operator_name
WHEN MATCHED THEN 
    UPDATE SET 
        customer_type = source.customer_type,
        customer_no = source.customer_no,
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

transform_non_inactive_to_single_view = """
MERGE INTO {{ grading_schema }}.{{ single_view_table }} AS target
USING (
    SELECT * 
    FROM {{ grading_schema }}.{{ single_staging_table }}
    WHERE partition_date = '{{ partition_date }}'
        AND operator_name = 'TRUE'
        AND product_status = 'ACTIVE'
) AS source
ON 
    target.product_id = source.product_id
    AND target.operator_name = source.operator_name
WHEN MATCHED THEN 
    UPDATE SET
        grading_account_id = source.grading_account_id,
        identifier_no = source.identifier_no,
        crm_integration_id = source.crm_integration_id,
        customer_type = source.customer_type,
        customer_no = source.customer_no,
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