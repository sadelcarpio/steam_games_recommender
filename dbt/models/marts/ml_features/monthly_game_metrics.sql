-- models/marts/ml_features/monthly_game_metrics.sql
{{ config(
    materialized='view'
) }}
WITH cumulative AS (
    SELECT
       game_id,
       DATE_TRUNC('month', timestamp_created) AS review_month,
       COUNT(*) OVER (
             PARTITION BY game_id ORDER BY DATE_TRUNC('month', timestamp_created)
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       )
       AS num_reviews,
       SUM(voted_up::int) OVER (
             PARTITION BY game_id ORDER BY DATE_TRUNC('month', timestamp_created)
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       )
       AS num_positive_reviews,
       SUM((NOT voted_up)::int) OVER (
             PARTITION BY game_id ORDER BY DATE_TRUNC('month', timestamp_created)
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       )
       AS num_negative_reviews,
       SUM(voted_up::int * weighted_vote_score) OVER (
            PARTITION BY game_id ORDER BY DATE_TRUNC('month', timestamp_created)
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       )
       AS cum_weighted_votes,
       SUM(weighted_vote_score) OVER (
            PARTITION BY game_id ORDER BY DATE_TRUNC('month', timestamp_created)
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       )
       AS cum_weights
FROM {{ ref('fact_reviews') }}
), aggregated AS (
    SELECT
        game_id,
        review_month,
        MAX(num_reviews) AS num_reviews,
        MAX(num_positive_reviews) AS num_positive_reviews,
        MAX(num_negative_reviews) AS num_negative_reviews,
        MAX(cum_weighted_votes) / NULLIF(MAX(cum_weights), 0) AS weighted_score
    FROM cumulative
    GROUP BY game_id, review_month
)
SELECT
    game_id,
    review_month AS game_review_month,
    num_reviews AS game_num_reviews,
    num_positive_reviews AS game_num_positive_reviews,
    num_negative_reviews AS game_num_negative_reviews,
    weighted_score AS game_weighted_score
FROM aggregated
