{{
    config(
        materialized='table',
        tags=['scraping']
    )
 }}
SELECT all_appids.appid FROM {{ source('raw', 'app_ids') }} all_appids
LEFT JOIN {{ ref('stg_games') }} games
ON all_appids.appid = games.game_id
WHERE games.game_id IS NULL
