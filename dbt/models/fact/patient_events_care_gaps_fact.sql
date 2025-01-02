{{
    config(
        materialized = 'incremental',
        unique_key = ['DIM_PATIENT_KEY','DIM_CLAIM_PROVIDER_KEY','DIM_MEDICAL_EVENTS_KEY','DIM_CARE_GAPS_KEY'],
        incremental_strategy = 'merge',
        merge_exclude_columns = ['ID','INSERT_DATE']
    )
}}

WITH BASE AS ( 
SELECT  PT.PATIENT_ID,
PH.PHARMACY_ID,
PROV.PROVIDER_ID,
CG.GAP_ID ,
CD.CLAIM_ID,
ME.EVENT_ID,
ME.EVENT_DATE::datetime as EVENT_DATE,
CD.CLAIM_DATE::datetime as CLAIM_DATE,
PH.FILL_DATE::datetime as FILL_DATE,
CG.IDENTIFIED_DATE::datetime as IDENTIFIED_DATE,
LEAST(PT.INSERT_DATE,ME.INSERT_DATE,CD.INSERT_DATE,PROV.INSERT_DATE,CG.INSERT_DATE) AS BASE_ADD_DATE
 FROM {{ source('source_trans', 'patient_t') }} PT 
 JOIN {{ source('source_trans', 'medical_events') }} ME ON PT.PATIENT_ID = ME.PATIENT_ID
 JOIN {{ source('source_trans', 'claims') }} CD ON PT.PATIENT_ID = CD.PATIENT_ID 
 JOIN {{ source('source_trans', 'provider') }} PROV ON PROV.PROVIDER_ID = CD.PROVIDER_ID
 JOIN {{ source('source_trans', 'care_gaps') }} CG ON CG.PATIENT_ID = PT.PATIENT_ID 
 left JOIN {{ source('source_trans', 'pharmacy') }} PH ON PT.PATIENT_ID = PH.PATIENT_ID 
 {% if is_incremental() %}
  where LEAST(PT.INSERT_DATE,ME.INSERT_DATE,CD.INSERT_DATE,PROV.INSERT_DATE,CG.INSERT_DATE)  > (select coalesce(max(base_add_date),'1900-01-01 00:00:00.000') from {{ this }})
{% endif %}
)

, SOURCE AS (
SELECT P.ID AS DIM_PATIENT_KEY,
CPR.ID AS DIM_CLAIM_PROVIDER_KEY,
ME.ID AS DIM_MEDICAL_EVENTS_KEY,
CG.ID AS DIM_CARE_GAPS_KEY,
PH.ID AS DIM_PHARMACY_KEY,
S.EVENT_DATE,
S.CLAIM_DATE,
S.FILL_DATE,
S.IDENTIFIED_DATE,
S.BASE_ADD_DATE
FROM BASE S 
JOIN {{ ref('dim_patient') }} P ON S.PATIENT_ID = P.PATIENT_ID
JOIN {{ ref('dim_claim_provider') }} CPR ON S.PROVIDER_ID = CPR.PROVIDER_ID AND S.CLAIM_ID = CPR.CLAIM_ID 
JOIN {{ ref('dim_medical_events') }} ME ON S.EVENT_ID = ME.EVENT_ID
JOIN {{ ref('dim_care_gaps') }} CG ON S.GAP_ID = CG.GAP_ID
LEFT JOIN {{ ref('dim_pharmacy') }} PH ON S.PHARMACY_ID = PH.PHARMACY_ID
)
SELECT SEQ_PATIENT_EVENTS_CARE_GAPS.NEXTVAL AS ID,
DIM_PATIENT_KEY,
DIM_CLAIM_PROVIDER_KEY,
DIM_MEDICAL_EVENTS_KEY,
DIM_CARE_GAPS_KEY,
DIM_PHARMACY_KEY,
EVENT_DATE,
CLAIM_DATE,
FILL_DATE,
IDENTIFIED_DATE,
MD5(
COALESCE(DIM_PATIENT_KEY::varchar, '') || '-' ||
COALESCE(DIM_CLAIM_PROVIDER_KEY::varchar, '') || '-' ||
COALESCE(DIM_MEDICAL_EVENTS_KEY::varchar, '') || '-' ||
COALESCE(DIM_CARE_GAPS_KEY::varchar, '') || '-' ||
COALESCE(DIM_PHARMACY_KEY::varchar, '') || '-' ||
COALESCE(EVENT_DATE::varchar, '') || '-' ||
COALESCE(CLAIM_DATE::varchar, '') || '-' ||
COALESCE(FILL_DATE::varchar, '') || '-' ||
COALESCE(FILL_DATE::varchar, '') || '-' ||
COALESCE(BASE_ADD_DATE::varchar, '') 
) AS HASH_KEY,
BASE_ADD_DATE,
CURRENT_TIMESTAMP() AS INSERT_DATE,
CURRENT_TIMESTAMP() AS UPDATE_DATE
FROM SOURCE