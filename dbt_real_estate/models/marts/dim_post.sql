{{ config(materialized='table') }}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['url']) }} AS post_id,
    MAX(title) as title,
    url,
    MAX(posted_date) as posted_date,
    MAX(expired_date) as expired_date
FROM {{ ref('stg_properties') }}
WHERE url IS NOT NULL AND url != ''
GROUP BY url