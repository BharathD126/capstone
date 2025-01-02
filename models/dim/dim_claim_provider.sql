{{
    config(
        materialized = 'incremental',
        unique_key = ['CLAIM_ID','PROVIDER_ID'],
        incremental_strategy = 'merge',
        merge_exclude_columns = ['ID','INSERT_DATE']
    )
}}

WITH SOURCE AS (
SELECT CLAIM_ID,
PATIENT_ID,
SERVICE_DATE,
CLAIM_DATE,
CLAIM_AMOUNT,
CLAIM_STATUS,
P.PROVIDER_ID,
DIAGNOSIS_CODES,
PROCEDURE_CODES,
PROVIDER_NAME,
SPECIALTY,
NPI_NUMBER,
FACILITY_ID,
LEAST(C.INSERT_DATE,P.INSERT_DATE) AS BASE_ADD_DATE,
CURRENT_TIMESTAMP() AS INSERT_DATE,
CURRENT_TIMESTAMP() AS UPDATE_DATE
FROM {{ source('source_trans', 'claims') }} C 
JOIN {{ source('source_trans', 'provider') }} P ON C.PROVIDER_ID = P.PROVIDER_ID 
where LEAST(C.INSERT_DATE,P.INSERT_DATE) > (select coalesce(max(base_add_date),'1900-01-01 00:00:00.000') from {{ this }})
)
SELECT SEQ_DIM_CLAIM_PROVIDER.NEXTVAL AS ID,
CLAIM_ID,
PATIENT_ID,
SERVICE_DATE,
CLAIM_DATE,
CLAIM_AMOUNT,
CLAIM_STATUS,
PROVIDER_ID,
DIAGNOSIS_CODES,
PROCEDURE_CODES,
PROVIDER_NAME,
SPECIALTY,
NPI_NUMBER,
FACILITY_ID,
MD5(
COALESCE(CLAIM_ID::varchar, '') || '-' ||
COALESCE(PATIENT_ID::varchar, '') || '-' ||
COALESCE(SERVICE_DATE::varchar, '') || '-' ||
COALESCE(CLAIM_DATE::varchar, '') || '-' ||
COALESCE(CLAIM_AMOUNT::varchar, '') || '-' ||
COALESCE(CLAIM_STATUS::varchar, '') || '-' ||
COALESCE(PROVIDER_ID::varchar, '') || '-' ||
COALESCE(DIAGNOSIS_CODES::varchar, '') || '-' ||
COALESCE(PROCEDURE_CODES::varchar, '') || '-' ||
COALESCE(PROVIDER_NAME::varchar, '') || '-' ||
COALESCE(SPECIALTY::varchar, '') || '-' ||
COALESCE(NPI_NUMBER::varchar, '') || '-' ||
COALESCE(FACILITY_ID::varchar, '') || '-' ||
COALESCE(BASE_ADD_DATE::varchar, '')
) AS HASH_KEY,
BASE_ADD_DATE,
INSERT_DATE,
UPDATE_DATE
FROM SOURCE 
