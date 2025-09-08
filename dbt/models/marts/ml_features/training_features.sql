-- models/marts/ml_features/training_features.sql
{{ config(
    materialized='incremental',
    unique_key=['review_id'],
    enabled=false
) }}
WITH reviews AS (
    SELECT * FROM {{ ref('fact_reviews') }}
    {% if is_incremental() %}
    WHERE scrape_date > (SELECT MAX(scrape_date) FROM {{ this }})
   {% endif %}
),
reviews_pre AS (
    SELECT r.*,
           DATE_TRUNC('day', r.timestamp_created) AS game_review_day
    FROM reviews r
),
needed AS (
    SELECT game_id,
           MAX(game_review_day) AS max_review_day
    FROM reviews_pre
    GROUP BY game_id
),
game_features AS (
    SELECT gf.*
    FROM {{ ref('game_features') }} gf
    JOIN (SELECT DISTINCT game_id FROM reviews_pre) g USING (game_id)
    JOIN needed n ON n.game_id = gf.game_id
    WHERE gf.game_review_day <= n.max_review_day
),
reviews_sorted AS (
    SELECT * FROM reviews_pre
    ORDER BY game_id, game_review_day
),
game_features_sorted AS (
    SELECT * FROM game_features
    ORDER BY game_id, game_review_day
)
SELECT
    r.*,
    gf.*
FROM reviews_sorted r ASOF JOIN game_features_sorted gf
USING (game_id, "game_review_day")
