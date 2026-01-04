{{ config(materialized='table') }}

WITH staging AS (
    SELECT * FROM {{ ref('stg_properties') }}
    -- Chỉ lấy những dòng có URL (để khớp với dim_post)
    WHERE url IS NOT NULL AND url != ''
),

final AS (
    SELECT
        -- 1. Khớp với dim_post (Chỉ dùng url)
        {{ dbt_utils.generate_surrogate_key(['url']) }} AS post_id,
        
        -- 2. Khớp với dim_address
        {{ dbt_utils.generate_surrogate_key(['district', 'city']) }} AS address_id,
        
        -- 3. Khớp với dim_seller (Dùng cùng logic COALESCE/NULLIF)
        {{ dbt_utils.generate_surrogate_key([
            "TRIM(COALESCE(NULLIF(NULLIF(NULLIF(seller, ''), 'nan'), 'None'), 'Unknown'))"
        ]) }} AS seller_id,
        
        price,
        area,
        bedroom,
        bathroom 
    FROM staging
)

SELECT * FROM final