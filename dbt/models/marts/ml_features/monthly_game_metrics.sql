-- models/marts/ml_features/monthly_game_metrics.sql
{{ config(
    materialized='view'
) }}
WITH monthly AS (
    SELECT
        game_id,
        date_trunc('month', timestamp_created) AS review_month,
        COUNT(*) AS num_reviews,
        SUM(voted_up::int) AS num_positive_reviews,
        SUM((NOT voted_up)::int) AS num_negative_reviews,
        SUM(voted_up::int * weighted_vote_score) AS month_weighted_votes,
        SUM(weighted_vote_score) AS month_weights
    FROM {{ ref('fact_reviews') }}
    GROUP BY game_id, review_month
),
cumulative AS (
    SELECT
       game_id,
       review_month,
       num_reviews,
       num_positive_reviews,
       num_negative_reviews,
       SUM(num_reviews) OVER (
             PARTITION BY game_id ORDER BY review_month
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS cum_num_reviews,
       SUM(num_positive_reviews) OVER (
             PARTITION BY game_id ORDER BY review_month
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       )
       AS cum_num_positive_reviews,
       SUM(num_negative_reviews) OVER (
             PARTITION BY game_id ORDER BY review_month
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       )
       AS cum_num_negative_reviews,
       SUM(month_weighted_votes) OVER (
            PARTITION BY game_id ORDER BY review_month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       )
       AS cum_weighted_votes,
       SUM(month_weights) OVER (
            PARTITION BY game_id ORDER BY review_month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       )
       AS cum_weights
FROM monthly
)
SELECT
    game_id,
    review_month AS game_review_month,
    num_reviews AS game_num_reviews,
    num_positive_reviews AS game_num_positive_reviews,
    num_negative_reviews AS game_num_negative_reviews,
    cum_num_reviews AS game_cum_num_reviews,
    cum_num_positive_reviews AS game_cum_num_positive_reviews,
    cum_num_negative_reviews AS game_cum_num_negative_reviews,
    CASE WHEN cum_weights = 0 THEN NULL ELSE cum_weighted_votes / cum_weights END AS game_weighted_score
FROM cumulative
