{{ config(materialized='table') }}

WITH base AS (
    SELECT 
        -- Quy tất cả các loại trống/rác về 'Unknown'
        TRIM(COALESCE(NULLIF(NULLIF(NULLIF(seller, ''), 'nan'), 'None'), 'Unknown')) AS clean_seller,
        total_posts,
        zalo_url
    FROM {{ ref('stg_properties') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['clean_seller']) }} AS seller_id,
    clean_seller AS seller,
    MAX(total_posts) AS total_posts,
    MAX(zalo_url) AS zalo_url
FROM base
GROUP BY 1, 2