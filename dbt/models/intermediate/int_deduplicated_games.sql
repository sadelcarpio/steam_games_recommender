-- models/intermediate/int_deduplicated_games.sql
{{ config(materialized='view') }}

SELECT *
FROM {{ ref('stg_games') }} QUALIFY ROW_NUMBER() OVER (
    PARTITION BY game_name
    ORDER BY game_review_score DESC, game_id ASC
    ) = 1
