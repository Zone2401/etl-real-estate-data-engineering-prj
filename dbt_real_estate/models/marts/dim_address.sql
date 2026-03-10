{{ config(materialized='table') }}

WITH base AS (
    SELECT DISTINCT
        district,
        city
    FROM {{ ref( 'stg_properties' ) }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['district', 'city']) }} AS address_id,
    COALESCE(district, 'Unknown') as district,
    COALESCE(city, 'Unknown') as city
FROM base