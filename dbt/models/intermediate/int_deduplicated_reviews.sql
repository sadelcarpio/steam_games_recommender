-- models/intermediate/int_deduplicated_reviews.sql
{{ config(
    materialized='incremental',
    unique_key='review_id'
) }}

WITH new_reviews AS (  -- scraping guarantees no duplicate reviews accross batches
    SELECT *
    FROM {{ ref('int_reviews_with_canonical_id') }} r
    {% if is_incremental() %}
    WHERE r.scrape_date > (SELECT MAX(scrape_date) FROM {{ this }})
    {% endif %}
)
SELECT *
FROM new_reviews QUALIFY
ROW_NUMBER() OVER (
    PARTITION BY review_id
    ORDER BY game_id DESC
) = 1
