-- models/staging/stg_games.sql
{{ config(materialized='view') }}

WITH filtered AS (SELECT *
                  FROM {{ source('raw', 'raw_games') }}
                  WHERE type = 'game'
                    AND coming_soon = FALSE)
SELECT appid                          AS game_id,
       name                           AS game_name,
       is_free                        AS game_is_free,
       developers                     AS game_developers, -- list
       publishers                     AS game_publishers, -- list
       categories                     AS game_categories, -- list
       genres                         AS game_genres,     -- list
       COALESCE(
               TRY_STRPTIME(release_date, '%b %d, %Y'),
               TRY_STRPTIME(release_date, '%d %b, %Y')
       )                              AS game_release_date,
       short_description              AS game_short_description,
       review_score                   AS game_review_score,
       review_score_desc              AS game_review_score_description,
FROM filtered
