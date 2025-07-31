-- models/marts/ml_features/game_features.sql
{{ config(
    materialized='table'
) }}
SELECT g.*, a.*
FROM {{ ref('daily_game_metrics') }} a JOIN {{ ref('dim_games') }} g using (game_index)
