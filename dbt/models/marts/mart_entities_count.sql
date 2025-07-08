-- models/marts/mart_entities_count.sql
{{ config(
    materialized='table'
) }}
SELECT
    'users' AS model,
    COUNT(DISTINCT user_id) AS row_count,
    CURRENT_TIMESTAMP AS snapshot_time
FROM {{ ref('mart_user_features') }}
UNION ALL
SELECT
    'games_features',
    COUNT(*),
    CURRENT_TIMESTAMP
FROM {{ ref('mart_game_features_with_ids') }}
UNION ALL
SELECT
    'games',
    COUNT(*),
    CURRENT_TIMESTAMP
FROM {{ ref('mart_game_id_mapping') }}
UNION ALL
SELECT
    'reviews',
    COUNT(*),
    CURRENT_TIMESTAMP
FROM {{ ref('mart_review_features') }}
