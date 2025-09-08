-- models/marts/ml_features/training_features.sql
{{ config(
    materialized='incremental',
    unique_key=['review_id']
) }}
SELECT r.*, gf.* FROM {{ ref('filtered_reviews') }} r ASOF JOIN {{ ref('game_features') }} gf
ON r.game_id = gf.game_id AND DATE_TRUNC('month', r.timestamp_created) <= gf.game_review_month
