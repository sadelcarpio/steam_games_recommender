-- models/intermediate/int_cleaned_reviews.sql
{{ config(materialized='view') }}

SELECT r.review_id,
       r.user_id,
       COALESCE(m.canonical_game_id, r.game_id) AS game_id,  -- maps to a single game id
       r.review,
       r.timestamp_created,
       r.written_during_early_access,
       r.voted_up,
       r.weighted_vote_score,
       r.votes_up,
       r.comment_count
FROM {{ ref('stg_reviews') }} r
LEFT JOIN {{  ref('int_canonical_game_ids') }} m ON r.game_id = m.original_game_id
-- filters reviews from non-games
WHERE COALESCE(m.canonical_game_id, r.game_id) IN (SELECT game_id FROM {{ ref('int_deduplicated_games') }})
