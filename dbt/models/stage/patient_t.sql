{{
    config(
        materialized = 'incremental',
        unique_key = ['patient_id'],
        incremental_strategy = 'merge',
        merge_exclude_columns = ['patient_id']
    )
}}
WITH SRC AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY PATIENT_ID ORDER BY INSERT_DATE DESC) as rnk
FROM {{ source('source_stg', 'src_patient_data') }} 
)
SELECT a.patient_id,
a.first_name,
a.last_name,
a.date_of_birth,
a.gender,
a.race,
a.ethnicity,
a.address,
a.city,
a.state,
a.zip_code,
a.insurance_type,
a.primary_care_provider_id,
A.insert_date FROM SRC A WHERE A.RNK = 1