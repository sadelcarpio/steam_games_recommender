-- models/marts/mart_game_features_with_ids.sql
{{ config(
    materialized='table'
) }}
WITH cumulative AS (
    SELECT
       game_id,
       DATE(timestamp_created) AS review_day,
       COUNT(*) OVER (PARTITION BY game_id ORDER BY DATE(timestamp_created) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS num_reviews,
       SUM(voted_up::int * weighted_vote_score) OVER (
         PARTITION BY game_id ORDER BY DATE(timestamp_created) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS cum_weighted_votes,
       SUM(weighted_vote_score) OVER (
         PARTITION BY game_id ORDER BY DATE(timestamp_created) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS cum_weights
FROM {{ ref('int_filtered_reviews') }}
), aggregated AS (
    SELECT
    game_id,
    review_day,
    MAX(num_reviews) AS num_reviews,
    MAX(cum_weighted_votes) / NULLIF(MAX(cum_weights), 0) AS weighted_score
    FROM cumulative
    GROUP BY game_id, review_day
)
SELECT
    g.*,
    a.review_day,
    a.num_reviews,
    a.weighted_score
FROM aggregated a JOIN {{ ref('int_deduplicated_games') }} g USING (game_id)
