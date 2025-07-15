-- models/intermediate/int_deduplicated_reviews.sql
{{ config(materialized='view') }}

SELECT *
FROM {{ ref('int_reviews_with_canonical_id') }} QUALIFY
ROW_NUMBER() OVER (
    PARTITION BY review_id
    ORDER BY game_id DESC
) = 1
