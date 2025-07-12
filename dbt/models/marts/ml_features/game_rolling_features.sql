-- models/marts/ml_features/mart_game_features.sql
{{ config(
    materialized='table'
) }}
WITH cumulative AS (
    SELECT
       game_index,
       DATE(timestamp_created) AS review_day,
       COUNT(*) OVER (
             PARTITION BY game_index ORDER BY DATE(timestamp_created)
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       )
       AS num_reviews,
       SUM(voted_up::int) OVER (
             PARTITION BY game_index ORDER BY DATE(timestamp_created)
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       )
       AS num_positive_reviews,
       SUM(NOT voted_up::int) OVER (
             PARTITION BY game_index ORDER BY DATE(timestamp_created)
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       )
       AS num_negative_reviews,
       SUM(voted_up::int * weighted_vote_score) OVER (
            PARTITION BY game_index ORDER BY DATE(timestamp_created)
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       )
       AS cum_weighted_votes,
       SUM(weighted_vote_score) OVER (
            PARTITION BY game_index ORDER BY DATE(timestamp_created)
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       )
       AS cum_weights
FROM {{ ref('fact_reviews') }}
), aggregated AS (
    SELECT
        game_index,
        review_day,
        MAX(num_reviews) AS num_reviews,
        MAX(num_positive_reviews) AS num_positive_reviews,
        MAX(num_negative_reviews) AS num_negative_reviews,
        MAX(cum_weighted_votes) / NULLIF(MAX(cum_weights), 0) AS weighted_score
    FROM cumulative
    GROUP BY game_index, review_day
)
SELECT
    g.*,
    a.review_day AS game_review_day,
    a.num_reviews AS game_num_reviews,
    a.num_positive_reviews AS game_num_positive_reviews,
    a.num_negative_reviews AS game_num_negative_reviews,
    a.weighted_score AS game_weighted_score
FROM aggregated a JOIN {{ ref('dim_games_imputed') }} g USING (game_index)
