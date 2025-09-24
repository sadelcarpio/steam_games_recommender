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
{% set prev_exists = (load_relation(this) is not none) %}
{% set prev_month_expr = "DATE_TRUNC('month', DATE '" ~ current_month ~ "' - INTERVAL 1 MONTH)" %}

-- 1) Identify new first-time interactions for the current month only
WITH user_game_firsts AS (SELECT tf.user_id,
                                 tf.game_id,
                                 MIN(DATE_TRUNC('month', tf.timestamp_created)) AS first_month
                          FROM {{ ref('fact_reviews') }} AS tf
                          WHERE tf.voted_up = true
                          GROUP BY tf.user_id, tf.game_id),
     new_firsts AS (SELECT f.user_id,
                           f.game_id
                    FROM user_game_firsts f
                    WHERE f.first_month = DATE '{{ current_month }}'),
    {% if prev_exists %}
-- 2a) Previous month’s cumulative arrays
     prev AS (SELECT user_id,
                     past_game_ids,
                     all_game_ids
              FROM {{ this }}
              WHERE current_month = {{ prev_month_expr }}),

-- 3a) Build this month’s cumulative arrays by appending only new_firsts
     this_month AS (SELECT COALESCE(p.user_id, n.user_id) AS user_id,
                           DATE '{{ current_month }}'     AS current_month,
                           -- Append this month's new_firsts to last month's past
                           COALESCE(
                                   array_concat(
                                           COALESCE(p.past_game_ids, []::INT[]),
                                           array_agg(n.game_id ORDER BY n.game_id)
                                   ),
                                   COALESCE(p.past_game_ids, []::INT[])
                           )                              AS past_game_ids,
                           -- Maintain a per-user cumulative "all_game_ids" (union-like, via concat + distinct)
                           array_distinct(
                                   COALESCE(
                                           array_concat(
                                                   COALESCE(p.all_game_ids, []::INT[]),
                                                   array_agg(n.game_id ORDER BY n.game_id)
                                           ),
                                           COALESCE(p.all_game_ids, []::INT[])
                                   )
                           )                              AS all_game_ids
                    FROM prev p
                             FULL OUTER JOIN new_firsts n
                                             ON n.user_id = p.user_id
                    GROUP BY COALESCE(p.user_id, n.user_id), DATE '{{ current_month }}', p.past_game_ids,
                             p.all_game_ids)

SELECT t.user_id,
       t.current_month,
       t.past_game_ids,
       t.all_game_ids,
       array_filter(t.all_game_ids, x - > not array_has(t.past_game_ids, x)) AS future_game_ids
FROM this_month t
    {% else %} this_month AS (
    SELECT
    n.user_id,
    DATE '{{ current_month }}' AS current_month,
    []::INT[]                 AS past_game_ids,
    array_distinct(array_agg(n.game_id ORDER BY n.game_id)) AS all_game_ids
    FROM new_firsts n
    GROUP BY n.user_id, DATE '{{ current_month }}'
)
SELECT t.user_id,
       t.current_month,
       t.past_game_ids,
       t.all_game_ids,
       array_filter(t.all_game_ids, x - > not array_has(t.past_game_ids, x)) AS future_game_ids
FROM this_month t
{% endif %}
