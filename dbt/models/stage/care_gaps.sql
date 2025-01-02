{{
    config(
        materialized = 'incremental',
        unique_key = ['gap_id'],
        incremental_strategy = 'merge',
        merge_exclude_columns = ['gap_id']
    )
}}
WITH SRC AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY gap_id ORDER BY INSERT_DATE DESC) as rnk
FROM {{ source('source_stg', 'src_care_gaps') }} 
)
SELECT a.gap_id,
a.patient_id,
a.gap_type,
a.identified_date,
a.status,
a.recommended_action,
a.insert_date
FROM SRC A WHERE A.RNK = 1