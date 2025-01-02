{{
    config(
        materialized = 'incremental',
        unique_key = ['gap_id'],
        incremental_strategy = 'merge',
        merge_exclude_columns = ['ID','INSERT_DATE']
    )
}}

WITH SOURCE AS (
SELECT GAP_ID,
PATIENT_ID,
GAP_TYPE,
IDENTIFIED_DATE,
STATUS,
RECOMMENDED_ACTION,
INSERT_DATE AS BASE_ADD_DATE,
CURRENT_TIMESTAMP() AS INSERT_DATE,
CURRENT_TIMESTAMP() AS UPDATE_DATE
FROM {{ source('source_trans', 'care_gaps') }} p
{% if is_incremental() %}
  where p.insert_date > (select coalesce(max(base_add_date),'1900-01-01 00:00:00.000') from {{ this }})
{% endif %} 
)
SELECT SEQ_DIM_CARE_GAPS.NEXTVAL AS ID,
GAP_ID,
PATIENT_ID,
GAP_TYPE,
IDENTIFIED_DATE,
STATUS,
RECOMMENDED_ACTION,
MD5(
COALESCE(GAP_ID::varchar, '') ||  '-' ||
COALESCE(PATIENT_ID::varchar, '') || '-' ||
COALESCE(GAP_TYPE::varchar, '') || '-' ||
COALESCE(IDENTIFIED_DATE::varchar, '') || '-' ||
COALESCE(STATUS::varchar, '') || '-' || 
COALESCE(RECOMMENDED_ACTION::varchar, '') || '-' ||
COALESCE(BASE_ADD_DATE::varchar, '')
) AS HASH_KEY, 
BASE_ADD_DATE, 
INSERT_DATE,
UPDATE_DATE
FROM SOURCE