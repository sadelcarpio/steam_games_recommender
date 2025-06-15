{{ config(materialized='table') }}

SELECT
    *
FROM {{ ref('reviews') }} r
JOIN {{  ref('users') }} u ON r.user_id = u.user_id
JOIN {{  ref('games') }} g ON r.game_id = g.game_id
