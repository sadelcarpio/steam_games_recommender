-- models/marts/ml_features/training_features.sql
{{ config(
    materialized='external',
    format='parquet',
    options={"partition_by": "current_month", "overwrite_or_ignore": true},
    tags=['training']
) }}
{% set current_month = var('current_month', none) %}
{% if current_month is none %}
    {% do exceptions.raise("Set --vars 'current_month: YYYY-MM-01' to run month-by-month") %}
{% endif %}
WITH source_rows AS (
    SELECT
        r.*,
        DATE_TRUNC('month', r.timestamp_created) AS current_month,
        DATE_TRUNC('month', r.timestamp_created) - INTERVAL 1 MONTH AS prev_month
    FROM {{ ref('fact_reviews') }} r
    WHERE DATE_TRUNC('month', r.timestamp_created) = '{{ current_month }}'
),
features AS (
    SELECT
        s.*,
        dg.* EXCLUDE (game_id),
        -- Default the feature month to the previous month when there is no match
        -- Counts default to 0 when missing
        COALESCE(gm.game_num_reviews, 0) AS game_num_reviews,
        COALESCE(gm.game_num_positive_reviews, 0) AS game_num_positive_reviews,
        COALESCE(gm.game_num_negative_reviews, 0) AS game_num_negative_reviews,
        COALESCE(gm.game_cum_num_reviews, 0) AS game_cum_num_reviews,
        COALESCE(gm.game_cum_num_positive_reviews, 0) AS game_cum_num_positive_reviews,
        COALESCE(gm.game_cum_num_negative_reviews, 0) AS game_cum_num_negative_reviews,
        gm.game_weighted_score
    FROM source_rows s
    ASOF LEFT JOIN {{ ref('monthly_game_metrics') }} gm
    ON s.game_id = gm.game_id
    AND gm.game_review_month <= s.prev_month
    LEFT JOIN {{ ref('dim_games') }} dg
    ON s.game_id = dg.game_id
)
SELECT * FROM features
