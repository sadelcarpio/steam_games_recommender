-- models/marts/ml_features/game_features.sql
{{ config(
    materialized='external',
    format='parquet',
    options={"partition_by": "game_review_month", "overwrite_or_ignore": true},
    tags=['training']
) }}
{% set current_month = var('current_month', none) %}
{% if current_month is none %}
{% do exceptions.raise("Set --vars 'current_month: YYYY-MM-01' to run month-by-month") %}
{% endif %}
WITH game_metrics AS (SELECT *
                      FROM {{ ref('monthly_game_metrics') }}
                      WHERE game_review_month = DATE '{{ current_month }}')
SELECT g.*,
       CAST(gm.game_review_month AS DATE)  AS game_review_month,
       gm.game_num_reviews,
       gm.game_num_positive_reviews,
       gm.game_num_negative_reviews,
       gm.game_cum_num_reviews,
       gm.game_cum_num_positive_reviews,
       gm.game_cum_num_negative_reviews,
       gm.game_weighted_score
FROM game_metrics gm
         JOIN {{ ref('dim_games') }} g using (game_id)
