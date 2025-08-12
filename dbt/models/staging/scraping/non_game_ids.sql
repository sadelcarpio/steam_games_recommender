{{
    config(
        materialized='view',
        tags=['scraping']
    )
}}
WITH latest AS (
    SELECT
        appid,
        type,
        scrape_date,
        ROW_NUMBER() OVER (PARTITION BY appid ORDER BY scrape_date DESC) AS rn
    FROM {{ source('raw', 'raw_games') }}
)
SELECT appid FROM latest WHERE rn = 1 AND type != 'game'