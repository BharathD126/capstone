{{
    config(
        materialized = 'incremental',
        unique_key = ['PROVIDER_ID'],
        incremental_strategy = 'merge',
        merge_exclude_columns = ['PROVIDER_ID']
    )
}}
WITH SRC AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY PROVIDER_ID ORDER BY INSERT_DATE DESC) as rnk
FROM {{ source('source_stg', 'src_provider_data') }} 
)
SELECT a.provider_id, 
a.provider_name, 
a.specialty, 
a.npi_number, 
a.facility_id, 
a.insert_date
FROM SRC A WHERE A.RNK = 1