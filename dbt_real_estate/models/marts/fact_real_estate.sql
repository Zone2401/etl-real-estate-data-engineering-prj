{{ config(materialized='table') }}

WITH staging AS (
    SELECT * FROM {{ ref('stg_properties') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['staging.title', 'staging.url']) }} AS post_id,
        {{ dbt_utils.generate_surrogate_key(['staging.district', 'staging.city']) }} AS address_id,
        COALESCE(
            CASE 
                WHEN staging.seller IS NOT NULL AND TRIM(staging.seller) != '' 
                THEN {{ dbt_utils.generate_surrogate_key(["TRIM(staging.seller)"]) }}
            END,
            {{ dbt_utils.generate_surrogate_key(["'Unknown'"]) }}
        ) AS seller_id,
        staging.price,
        staging.area,
        staging.bedroom,
        staging.bathroom 
    FROM staging
)

SELECT f.*
FROM final f
-- Sử dụng LEFT JOIN để không làm mất dòng dữ liệu ở Fact
LEFT JOIN {{ ref('dim_post') }} dp ON f.post_id = dp.post_id
LEFT JOIN {{ ref('dim_address') }} da ON f.address_id = da.address_id
LEFT JOIN {{ ref('dim_seller') }} ds ON f.seller_id = ds.seller_id