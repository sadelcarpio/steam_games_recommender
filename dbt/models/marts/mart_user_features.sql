-- models/marts/mart_user_features.sql
{{ config(
    materialized='external',
    location='../data/marts/user_features.parquet',
    format='parquet'
) }}

SELECT user_id,
       MAX(user_num_games_owned) OVER (PARTITION BY user_id) AS user_proxy_games_owned,
       timestamp_created AS feature_timestamp,
       COUNT(*) OVER (
            PARTITION BY user_id ORDER BY timestamp_created
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) - 1 AS num_reviews_written
FROM {{ ref('int_filtered_reviews') }}
ORDER BY user_id, feature_timestamp
