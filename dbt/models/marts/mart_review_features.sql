-- models/marts/mart_reviews_features.sql
{{ config(
    materialized='external',
    location='../data/marts/review_features.parquet',
    format='parquet'
) }}
SELECT u.user_index,
       g.game_index,
       g.game_name AS game_name,  -- for debugging purposes mostly
       r.review,
       r.voted_up,
       r.votes_up,
       r.weighted_vote_score,
       r.timestamp_created
FROM {{ ref('int_filtered_reviews') }} r
JOIN {{ ref('mart_game_features_with_ids') }} g
ON r.game_id = g.game_id
JOIN {{ ref('mart_user_id_mapping') }} u
ON r.user_id = u.user_id
