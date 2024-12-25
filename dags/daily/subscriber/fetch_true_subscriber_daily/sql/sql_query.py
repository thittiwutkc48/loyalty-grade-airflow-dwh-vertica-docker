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

delete_duplicates_single_view = """
DELETE FROM {{ grading_schema }}.{{ single_view_subscriber }} 
WHERE 
    partition_date = '{{ partition_date }}';
"""

transform_to_single_view = """
INSERT INTO {{ grading_schema }}.{{ single_view_subscriber }} 
WITH ranked_subscribers AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY partition_date,UPPER(idnum), productid
            ORDER BY partition_date
        ) AS row_num
    FROM 
        {{ grading_schema }}.{{ staging_table }}
    WHERE 
        partition_date = '{{ partition_date }}'
), 
productcomplist_data AS (
    SELECT 
        idnum,
        productid,
        productline,
        productcomplist,
        j->>'code' AS code,
        j->>'desc' AS code_desc,
        j->>'type' AS type,
        j->>'status' AS status,
        j->>'startDate' AS startDate,
        CASE 
            WHEN productline IN ('True Mobile', 'True Visions') THEN 
                CASE 
                    WHEN POSITION('-' IN j->>'desc') > 0 THEN 
                        UPPER(TRIM(SPLIT_PART(j->>'desc', '-', 2)))
                    ELSE 
                        UPPER(TRIM(j->>'desc'))
                END
            WHEN productline IN ('True Online') THEN 
                UPPER(TRIM(j->>'desc'))
            ELSE NULL
        END AS pp_code
    FROM 
        ranked_subscribers,
        LATERAL jsonb_array_elements(productcomplist) AS j
    WHERE 
        row_num = 1 
        AND productcomplist IS NOT NULL
        AND j->>'type' IN ('Channel Group', 'Price Plan') 
        AND j->>'status' NOT IN ('Inactive')
),
priceplan_ranked_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY UPPER(idnum), productid, productline
            ORDER BY startDate DESC
        ) AS row_num
    FROM 
        productcomplist_data
    WHERE 
    
        (productline IN ('True Mobile', 'True Online', 'Fixedline') AND type = 'Price Plan') 
        OR (productline = 'True Visions' AND type = 'Channel Group')
)
SELECT 
    scs.gradingaccountid,
    scs.identifier_no,
    scs.id_type,
    UPPER(rn_sub.crmintegrationid) AS crm_integration_id,
    rn_sub.goldenid AS golden_id,
    rn_sub.productid AS product_id,
    rn_sub.productname AS product_name,
    UPPER(rn_sub.productstatus) AS product_status,
    rn_sub.productline AS product_line,
    rn_sub.ban,
    rn_sub.startdate AS start_date,
    rn_sub.enddate AS end_date,
    NULL AS aging_months,
    rn_sub."language",
    rn_sub.billingsubno,
    rn_sub.qrunaddressid,
    rn_sub.street,
    rn_sub.housenumber AS house_number,
    rn_sub.moo,
    rn_sub.postalcode AS postal_code,
    rn_sub.city,
    rn_sub.district,
    rn_sub.subdistrict AS sub_district,
    rn_sub.country,
    rn_sub.roomnumber AS room_number,
    rn_sub.floor,
    rn_sub.buildingmooban AS building_mooban,
    rn_sub.soi,
    rn_sub.firstname AS first_name,
    rn_sub.lastname AS last_name,
    rn_sub.ouid AS ou_id,
    rn_sub.companycode AS company_code,
    rn_sub.vip,
    pp_rn.code AS priceplan_code,
    pp_rn.pp_code AS priceplan_desc,
    case 
	    when productname in ('Postpay','Prepay') then upper(substring(productname ,1,4))
		when productline = 'True Visions' Then 'TVS'
		when productline = 'True Online' Then 'INTERNET'
		when upper(substring(productname ,1,3)) = 'FIX' then 'FIXEDLINE'
		else productname        
		END AS product_group,
    'TRUE' AS operator_name,
    rn_sub.productcomplist,
    rn_sub.partition_date as partition_date
FROM 
    ranked_subscribers rn_sub
LEFT JOIN 
    (
        SELECT
	        gradingaccountid,  	
	        identifier_no,
	        id_type
        FROM 
            {{ grading_schema }}.{{ single_view_customer }} 
    ) scs 
    ON scs.identifier_no = UPPER(rn_sub.idnum)
LEFT JOIN 
    (
        SELECT 
            UPPER(idnum) AS idnum,
            productid,
            code,
            code_desc,
            pp_code
        FROM 
            priceplan_ranked_data 
        WHERE 
            row_num = 1
    ) pp_rn 
    ON pp_rn.idnum = UPPER(rn_sub.idnum) 
    AND pp_rn.productid = rn_sub.productid
WHERE 
    rn_sub.row_num = 1 
    AND rn_sub.productstatus NOT IN ('Inactive');
"""
