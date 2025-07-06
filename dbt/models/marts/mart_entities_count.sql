-- models/marts/mart_entities_count.sql
{{ config(
    materialized='external',
    location='../data/marts/count.csv',
    format='csv'
) }}
SELECT
    'users' AS model,
    COUNT(DISTINCT user_id) AS row_count,
    CURRENT_TIMESTAMP AS snapshot_time
FROM {{ ref('mart_user_features') }}
UNION ALL
SELECT
    'games',
    COUNT(*),
    CURRENT_TIMESTAMP
FROM {{ ref('mart_game_features') }}
UNION ALL
SELECT
    'reviews',
    COUNT(*),
    CURRENT_TIMESTAMP
FROM {{ ref('mart_review_features') }}
