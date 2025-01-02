{{
    config(
        materialized = 'incremental',
        unique_key = ['EVENT_ID'],
        incremental_strategy = 'merge',
        merge_exclude_columns = ['ID','INSERT_DATE']
    )
}}

WITH source AS (
SELECT * FROM --SRC_DATA.MEDICAL_EVENTS ME 
{{ source('source_trans', 'medical_events') }} me
{% if is_incremental() %}
  where ME.insert_date > (select coalesce(max(base_add_date),'1900-01-01 00:00:00.000') from {{ this }})
{% endif %}
)

SELECT
TGT_DM.SEQ_DIM_MEDICAL_EVENTS.NEXTVAL AS ID,
EVENT_ID,
PATIENT_ID,
EVENT_DATE,
EVENT_TYPE,
DIAGNOSIS_CODE,
PROCEDURE_CODE,
MEDICATION_CODE,
PROVIDER_ID,
FACILITY_ID,
NOTES,
MD5(
COALESCE(EVENT_ID::varchar, '') || '-' ||
COALESCE(PATIENT_ID::varchar, '') || '-' ||
COALESCE(EVENT_DATE::varchar, '') || '-' ||
COALESCE(EVENT_TYPE::varchar, '') || '-' ||
COALESCE(DIAGNOSIS_CODE::varchar, '') || '-' ||
COALESCE(PROCEDURE_CODE::varchar, '') || '-' ||
COALESCE(MEDICATION_CODE::varchar, '') || '-' ||
COALESCE(PROVIDER_ID::varchar, '') || '-' ||
COALESCE(FACILITY_ID::varchar, '') || '-' ||
COALESCE(NOTES::varchar, '') || '-' ||
COALESCE(INSERT_DATE::varchar, '')
) AS HASH_KEY,
INSERT_DATE AS base_add_date,
CURRENT_TIMESTAMP() AS INSERT_DATE,
CURRENT_TIMESTAMP() AS UPDATE_DATE,
FROM source

