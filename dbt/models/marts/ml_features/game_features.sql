-- models/marts/ml_features/game_features.sql
{{ config(
    materialized='incremental',
    unique_key=['game_id', 'game_review_day']
) }}
WITH filtered_game_metrics AS (
    SELECT *
    FROM {{ ref('daily_game_metrics') }}
    {% if is_incremental() %}
    WHERE game_review_day > (SELECT MAX(game_review_day) FROM {{ this }})
    {% endif %}
)
SELECT g.*,
       a.game_review_day,
       a.game_num_reviews,
       a.game_num_positive_reviews,
       a.game_num_negative_reviews,
       a.game_weighted_score
FROM filtered_game_metrics a JOIN {{ ref('dim_games') }} g using (game_id)
