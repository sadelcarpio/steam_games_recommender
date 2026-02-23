{{ config(
    materialized='external',
    format='parquet',
    options={"partition_by": "current_month", "overwrite_or_ignore": true},
    tags=['training']
) }}
{% set current_month = var('current_month', none) %}
{% set prev_exists = (load_relation(this) is not none) %}
{% set prev_month_expr = "DATE_TRUNC('month', DATE '" ~ current_month ~ "' - INTERVAL 1 MONTH)" %}

WITH user_game AS (
    SELECT user_id,
           game_id,
           DATE_TRUNC('month', timestamp_created) AS current_month,
           array_agg(game_id) OVER (PARTITION BY user_id) AS all_game_ids
    FROM {{ ref('fact_reviews') }}
    WHERE voted_up = true
),
new_interactions AS (
    SELECT user_id,
           array_agg(game_id) AS monthly_game_ids,
           current_month,
           all_game_ids,
    FROM user_game
    WHERE current_month = '{{ current_month }}'
    GROUP BY user_id, current_month, all_game_ids
)
{% if prev_exists %}
, previous_interactions AS (
    SELECT * FROM {{ this }} WHERE current_month = {{ prev_month_expr }}
),
merged_interactions AS (
    SELECT COALESCE(pi.user_id, ni.user_id) AS user_id,
           '{{ current_month }}' AS current_month,
           ni.monthly_game_ids AS monthly_game_ids,
           CASE
               WHEN pi.user_id IS NULL THEN []::INT[]
               ELSE pi.monthly_game_ids END AS past_game_ids,
           COALESCE(pi.all_game_ids, ni.all_game_ids) AS all_game_ids,
    FROM new_interactions ni
    LEFT JOIN previous_interactions pi
    ON pi.user_id = ni.user_id
),
    final AS (
        SELECT *, array_filter(all_game_ids, x -> x NOT IN past_game_ids) AS future_game_ids
        FROM merged_interactions
    )
SELECT * FROM final
{% else %}
SELECT user_id,
       monthly_game_ids,
       []::INT[] AS past_game_ids,
       all_game_ids,
       all_game_ids AS future_game_ids,
       current_month
    FROM new_interactions
{% endif %}
