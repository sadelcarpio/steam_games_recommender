-- models/marts/ml_features/game_features.sql
{{ config(
    materialized='table'
) }}
SELECT g.*,
       a.game_review_day,
       a.game_num_reviews,
       a.game_num_positive_reviews,
       a.game_num_negative_reviews,
       a.game_weighted_score
FROM {{ ref('daily_game_metrics') }} a JOIN {{ ref('dim_games') }} g using (game_index)
