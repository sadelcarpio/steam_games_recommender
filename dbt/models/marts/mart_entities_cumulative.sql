-- models/marts/mart_entities_cumulative.sql
{{ config(materialized='table') }}

SELECT
    review_day,
    SUM(daily_reviews_count) OVER (
        ORDER BY review_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_review_count,
    SUM(new_users) OVER (
        ORDER BY review_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_user_count,
    SUM(daily_games_count) OVER (
        ORDER BY review_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_game_count
FROM {{ ref('mart_entity_daily_counts') }}
