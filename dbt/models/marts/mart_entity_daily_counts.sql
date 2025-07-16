-- models/marts/mart_entity_daily_counts.sql
{{ config(
    materialized='incremental',
    unique_key='review_day'
) }}
WITH daily_reviews AS (
    SELECT
        DATE(timestamp_created) AS review_day,
    COUNT(*) AS daily_reviews_count
    FROM
        {{ ref('fact_reviews') }}
    {% if is_incremental() %}
        WHERE DATE(timestamp_created) > (SELECT MAX(review_day) FROM {{ this }})
    {% endif %}
    GROUP BY 1
),
daily_users AS (
    SELECT
        DATE(first_review_timestamp) AS review_day,
    COUNT(*) AS new_users
    FROM
        {{ ref('dim_users') }}
    {% if is_incremental() %}
        WHERE DATE(timestamp_created) > (SELECT MAX(review_day) FROM {{ this }})
    {% endif %}
    GROUP BY 1
),
daily_games AS (
    SELECT
        DATE(game_prerelease_date) AS review_day,
    COUNT(*) AS daily_games_count
    FROM
        {{ ref('dim_games') }}
    {% if is_incremental() %}
        WHERE DATE(timestamp_created) > (SELECT MAX(review_day) FROM {{ this }})
    {% endif %}
    GROUP BY 1
),
all_days AS (
    SELECT review_day FROM daily_reviews
    UNION
    SELECT review_day FROM daily_users
    UNION
    SELECT review_day FROM daily_games
)
SELECT
    d.review_day,
    COALESCE(r.daily_reviews_count, 0) AS daily_reviews_count,
    COALESCE(u.new_users, 0) AS new_users,
    COALESCE(g.daily_games_count, 0) AS daily_games_count
FROM all_days d
LEFT JOIN daily_reviews r ON d.review_day = r.review_day
LEFT JOIN daily_users u ON d.review_day = u.review_day
LEFT JOIN daily_games g ON d.review_day = g.review_day
