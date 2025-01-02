{{
    config(
        materialized = 'incremental',
        unique_key = ['CLAIM_ID'],
        incremental_strategy = 'merge',
        merge_exclude_columns = ['CLAIM_ID']
    )
}}
WITH SRC AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY CLAIM_ID ORDER BY INSERT_DATE DESC) as rnk
FROM {{ source('source_stg', 'src_claims_data') }} 
)
SELECT a.claim_id,
a.patient_id,
a.service_date,
a.claim_date,
a.claim_amount,
a.claim_status,
a.provider_id,
a.diagnosis_codes,
a.procedure_codes,
a.insert_date
FROM SRC A WHERE A.RNK = 1