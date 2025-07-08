-- models/marts/mart_game_features_with_ids.sql
{{ config(
    materialized='table'
) }}
SELECT
    g.game_index,
    gf.*
FROM {{ ref('mart_game_features') }} gf JOIN {{ ref('mart_game_id_mapping') }} g USING (game_id)
