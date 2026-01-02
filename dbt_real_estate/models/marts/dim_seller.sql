{{ config(materialized='table') }}

WITH sellers_with_data AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(["TRIM(seller)"]) }} AS seller_id,
        TRIM(seller) AS seller,
        MAX(total_posts) AS total_posts,
        MAX(zalo_url) AS zalo_url
    FROM {{ ref('stg_properties') }}
    WHERE seller IS NOT NULL AND TRIM(seller) != ''
    GROUP BY seller_id, TRIM(seller)
),

unknown_seller AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["'Unknown'"]) }} AS seller_id,
        'Unknown' AS seller,
        0 AS total_posts,
        'N/A' AS zalo_url
)

SELECT * FROM sellers_with_data
UNION ALL
SELECT * FROM unknown_seller