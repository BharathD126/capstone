{{
    config(
        materialized = 'incremental',
        unique_key = ['patient_id'],
        incremental_strategy = 'merge',
        merge_exclude_columns = ['ID','INSERT_DATE']
    )
}}

WITH SOURCE AS (
SELECT PATIENT_ID,
NVL(FIRST_NAME,'')||' '||NVL(LAST_NAME,'') AS PATIENT_NAME,
DATE_OF_BIRTH,
DATEDIFF('YEAR',DATE_OF_BIRTH,CURRENT_DATE()) AS AGE,
GENDER,
RACE,
ETHNICITY,
ADDRESS,
CITY,
STATE,
ZIP_CODE,
INSURANCE_TYPE,
PRIMARY_CARE_PROVIDER_ID,
INSERT_DATE AS BASE_ADD_DATE,
CURRENT_TIMESTAMP() AS INSERT_DATE,
CURRENT_TIMESTAMP() AS UPDATE_DATE
FROM {{ source('source_trans', 'patient_t') }} p
{% if is_incremental() %}
  where p.insert_date > (select coalesce(max(base_add_date),'1900-01-01 00:00:00.000') from {{ this }})
{% endif %}
)
SELECT 
SEQ_DIM_PATIENT.NEXTVAL AS ID,
PATIENT_ID,
PATIENT_NAME,
DATE_OF_BIRTH,
AGE,
GENDER,
RACE,
ETHNICITY,
ADDRESS,
CITY,
STATE,
ZIP_CODE,
INSURANCE_TYPE,
PRIMARY_CARE_PROVIDER_ID,
MD5(
COALESCE(PATIENT_ID::varchar, '') || '-' ||
COALESCE(PATIENT_NAME::varchar, '') || '-' ||
COALESCE(DATE_OF_BIRTH::varchar, '') || '-' ||
COALESCE(AGE::varchar, '') || '-' ||
COALESCE(GENDER::varchar, '') || '-' ||
COALESCE(RACE::varchar, '') || '-' ||
COALESCE(ETHNICITY::varchar, '') || '-' ||
COALESCE(ADDRESS::varchar, '') || '-' ||
COALESCE(CITY::varchar, '') || '-' ||
COALESCE(STATE::varchar, '') || '-' ||
COALESCE(ZIP_CODE::varchar, '') || '-' ||
COALESCE(INSURANCE_TYPE::varchar, '') || '-' ||
COALESCE(PRIMARY_CARE_PROVIDER_ID::varchar, '') || '-' ||
COALESCE(BASE_ADD_DATE::varchar, '')
) AS HASH_KEY, 
BASE_ADD_DATE,
INSERT_DATE,
UPDATE_DATE
FROM SOURCE 
