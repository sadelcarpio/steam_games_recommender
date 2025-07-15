-- models/intermediate/int_canonical_game_ids.sql
{{ config(materialized='view') }}

SELECT
    g.game_id AS original_game_id,
    c.game_id AS canonical_game_id
FROM {{ ref('int_deduplicated_games') }} c
JOIN {{ ref('stg_games') }} g ON g.game_name = c.game_name
