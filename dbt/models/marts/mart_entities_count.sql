-- models/marts/mart_entities_count.sql
{{ config(
    materialized='table'
) }}
WITH daily_reviews AS (
    SELECT
        DATE(timestamp_created) AS review_day,
    COUNT(*) AS daily_reviews_count
    FROM
        {{ ref('fact_reviews') }}
    GROUP BY 1
),
cumulative_reviews AS (
    SELECT
        review_day,
        SUM(daily_reviews_count) OVER (
            ORDER BY review_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_reviews_count
    FROM daily_reviews
),
daily_users AS (
    SELECT
        DATE(first_review_timestamp) AS review_day,
    COUNT(*) AS new_users
    FROM
        {{ ref('dim_users') }}
    GROUP BY 1
),
cumulative_users AS (
            SELECT
                review_day,
                SUM(new_users) OVER (
                    ORDER BY review_day
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_users_count
            FROM daily_users
),
daily_games AS (
    SELECT
        DATE(game_prerelease_date) AS review_day,
    COUNT(*) AS daily_games_count
    FROM
        {{ ref('dim_games') }}
    GROUP BY 1
),
cumulative_games AS (
        SELECT
            review_day,
            SUM(daily_games_count) OVER (
                ORDER BY review_day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_games_count
        FROM daily_games
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
    LAST_VALUE(r.cumulative_reviews_count IGNORE NULLS) OVER (ORDER BY d.review_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_review_count,
    LAST_VALUE(u.cumulative_users_count IGNORE NULLS) OVER (ORDER BY d.review_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_user_count,
    LAST_VALUE(g.cumulative_games_count IGNORE NULLS) OVER (ORDER BY d.review_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_game_count
FROM all_days d
LEFT JOIN cumulative_reviews r ON d.review_day = r.review_day
LEFT JOIN cumulative_users u ON d.review_day = u.review_day
LEFT JOIN cumulative_games g ON d.review_day = g.review_day
ORDER BY d.review_day
