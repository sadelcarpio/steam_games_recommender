-- models/intermediate/int_deduplicated_reviews.sql
{{ config(materialized='view') }}
SELECT *
FROM {{ ref('stg_reviews') }} QUALIFY
ROW_NUMBER() OVER (
    PARTITION BY user_id, game_id
) = 1
