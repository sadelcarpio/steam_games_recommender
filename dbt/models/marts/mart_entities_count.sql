-- models/marts/mart_entities_count.sql
{{ config(
    materialized='table'
) }}
SELECT
    'users' AS model,
    COUNT(DISTINCT user_id) AS row_count,
    CURRENT_TIMESTAMP AS snapshot_time
FROM {{ ref('dim_users') }}
UNION ALL
SELECT
    'games_features',
    COUNT(*),
    CURRENT_TIMESTAMP
FROM {{ ref('game_rolling_features') }}
UNION ALL
SELECT
    'games',
    COUNT(*),
    CURRENT_TIMESTAMP
FROM {{ ref('dim_games_imputed') }}
UNION ALL
SELECT
    'reviews',
    COUNT(*),
    CURRENT_TIMESTAMP
FROM {{ ref('fact_reviews') }}
