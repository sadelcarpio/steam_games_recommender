-- models/intermediate/filtered_reviews.sql
{{ config(materialized='view', enabled=false) }}

WITH reviews AS (
    SELECT *
    FROM {{ ref('fact_reviews') }}
),

user_counts AS (
    SELECT user_id
    FROM reviews
    GROUP BY user_id
    HAVING COUNT(*) >= 5  -- equal or more than 10 reviews
),

game_counts AS (
    SELECT game_id
    FROM reviews
    GROUP BY game_id
    HAVING COUNT(*) >= 5
),

filtered_reviews AS (
    SELECT r.*
    FROM reviews r
    JOIN user_counts u ON r.user_id = u.user_id
    JOIN game_counts g ON r.game_id = g.game_id
)

SELECT *
FROM filtered_reviews
