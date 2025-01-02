{{
    config(
        materialized = 'incremental',
        unique_key = ['pharmacy_id'],
        incremental_strategy = 'merge',
        merge_exclude_columns = ['pharmacy_id']
    )
}}
WITH SRC AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY pharmacy_id ORDER BY INSERT_DATE DESC) as rnk
FROM {{ source('source_stg', 'src_pharmacy_data') }} 
)
SELECT a.pharmacy_id,
  a.prescription_id,
a.patient_id,
a.medication_code,
a.fill_date,
a.days_supply,
a.quantity,
a.insert_date
FROM SRC A WHERE A.RNK = 1