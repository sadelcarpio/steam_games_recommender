-- models/marts/ml_features/game_features.sql
{{ config(
    materialized='incremental',
    unique_key=['game_id', 'game_review_month']
) }}
WITH game_metrics AS (
    SELECT *
    FROM {{ ref('monthly_game_metrics') }}
    {% if is_incremental() %}
    WHERE game_review_month > (SELECT MAX(game_review_month) FROM {{ this }})
    {% endif %}
)
SELECT g.*,
       gm.game_review_month,
       gm.game_num_reviews,
       gm.game_num_positive_reviews,
       gm.game_num_negative_reviews,
       gm.game_weighted_score
FROM game_metrics gm JOIN {{ ref('dim_games') }} g using (game_id)
