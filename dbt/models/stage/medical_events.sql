{{
    config(
        materialized = 'incremental',
        unique_key = ['event_id'],
        incremental_strategy = 'merge',
        merge_exclude_columns = ['event_id']
    )
}}
WITH SRC AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY event_id ORDER BY INSERT_DATE DESC) as rnk
FROM {{ source('source_stg', 'src_medical_events') }} 
)
SELECT a.event_id, 
a.patient_id, 
a.event_date, 
a.event_type, 
a.diagnosis_code,
a.procedure_code,
a.medication_code,
a.provider_id,
a.facility_id,
a.notes,
a.insert_date
FROM SRC A WHERE A.RNK = 1