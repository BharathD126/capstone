{{
    config(
        materialized = 'incremental',
        unique_key = ['pharmacy_id'],
        incremental_strategy = 'merge',
        merge_exclude_columns = ['ID','INSERT_DATE']
    )
}}

WITH SOURCE AS (
SELECT PRESCRIPTION_ID,
PATIENT_ID,
MEDICATION_CODE,
FILL_DATE,
DAYS_SUPPLY,
QUANTITY,
PHARMACY_ID,
INSERT_DATE AS BASE_ADD_DATE,
CURRENT_TIMESTAMP() AS INSERT_DATE,
CURRENT_TIMESTAMP() AS UPDATE_DATE
FROM {{ source('source_trans', 'pharmacy') }} p
{% if is_incremental() %}
  where p.insert_date > (select coalesce(max(base_add_date),'1900-01-01 00:00:00.000') from {{ this }})
{% endif %} 
)
SELECT 
SEQ_DIM_PHARMACY.NEXTVAL AS ID,
PRESCRIPTION_ID,
PATIENT_ID,
MEDICATION_CODE,
FILL_DATE,
DAYS_SUPPLY,
QUANTITY,
PHARMACY_ID,
MD5(
COALESCE(PRESCRIPTION_ID::varchar, '') || '-' ||
COALESCE(PATIENT_ID::varchar, '') || '-' ||
COALESCE(MEDICATION_CODE::varchar, '') || '-' ||
COALESCE(FILL_DATE::varchar, '') || '-' ||
COALESCE(DAYS_SUPPLY::varchar, '') || '-' ||
COALESCE(QUANTITY::varchar, '') || '-' ||
COALESCE(PHARMACY_ID::varchar, '') || '-' ||
COALESCE(BASE_ADD_DATE::varchar, '')
) AS HASH_KEY, 
BASE_ADD_DATE,
INSERT_DATE,
UPDATE_DATE
FROM SOURCE 