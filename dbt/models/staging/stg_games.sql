-- models/staging/stg_games.sql
{{ config(materialized='table') }}

WITH filtered AS (
    SELECT * FROM {{ source('raw', 'raw_games') }}
             WHERE type = 'game'
             AND coming_soon = false
)
SELECT appid                               AS game_id,
       name                                AS game_name,
       required_age                        AS game_required_age,
       is_free                             AS game_is_free,
       short_description                   AS game_short_description,
       supported_languages                 AS game_supported_languages, -- list
       category_ids                        AS game_category_ids,
       genres_list                         AS game_genres,              -- list
       review_score                        AS game_review_score,
       COALESCE(
            TRY_STRPTIME(release_date, '%b %d, %Y'),
            TRY_STRPTIME(release_date, '%d %b, %Y')
       ) AS game_release_date
FROM filtered
