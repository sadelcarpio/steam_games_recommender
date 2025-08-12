{{
    config(
        materialized='table',
        tags=['scraping']
    )
 }}
SELECT appid FROM {{ source('raw', 'app_ids') }} a
WHERE NOT EXISTS(
    SELECT 1 FROM {{ ref('stg_games') }} g
             WHERE g.game_id = a.appid
)
AND NOT EXISTS(
    SELECT 1 FROM {{ ref('non_game_ids') }} ng
             WHERE ng.appid = a.appid
)
