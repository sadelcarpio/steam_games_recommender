-- models/intermediate/filtered_reviews.sql
{{ config(materialized='view') }}

WITH reviews AS (
    SELECT *
    FROM {{ ref('fact_reviews') }}
),
game_counts AS (
    SELECT game_id, COUNT(game_id) AS reviews_count
    FROM reviews
    GROUP BY game_id
),
filtered_game_counts AS (
    SELECT game_id
    FROM game_counts
    WHERE reviews_count > (SELECT quantile_cont(reviews_count, 0.95) FROM game_counts)
),
filtered_reviews AS (
    SELECT r.*
    FROM reviews r
    JOIN filtered_game_counts g ON r.game_id = g.game_id
)
SELECT *
FROM filtered_reviews
