{{ config(materialized='table') }}

SELECT 

    {{ dbt_utils.generate_surrogate_key(['title', 'url']) }} AS post_id,
    title,
    url,
    posted_date,
    expired_date
         
      
FROM {{ ref( 'stg_properties' ) }}
