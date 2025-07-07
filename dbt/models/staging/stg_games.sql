-- models/staging/stg_games.sql
{{ config(materialized='table') }}

WITH filtered AS (SELECT *
                  FROM {{ source('raw', 'raw_games') }}
                  WHERE type = 'game'
                    AND coming_soon = false)
SELECT appid               AS game_id,
       name                AS game_name,
       developers          AS game_developers,          -- list
       publishers          AS game_publishers,          -- list
       categories          AS game_categories,          -- list
       genres              AS game_genres,              -- list
       COALESCE(
               TRY_STRPTIME(release_date, '%b %d, %Y'),
               TRY_STRPTIME(release_date, '%d %b, %Y')
       )                   AS game_release_date,
       short_description   AS game_short_description,
       supported_languages AS game_supported_languages, -- list
       review_score        AS game_review_score,

FROM filtered
