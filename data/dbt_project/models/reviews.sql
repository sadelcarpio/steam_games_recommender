-- models/reviews.sql
{{ config(materialized='view') }}

SELECT
    rec_id,
    author_id AS user_id,
    appid AS game_id,
    playtime_forever,
    playtime_last_two_weeks,
    playtime_at_review,
    last_played,
    review,
    timestamp_created,
    voted_up,
    votes_funny,
    weighted_vote_score,
    comment_count,
    steam_purchase,
    received_for_free,
    written_during_early_access,
    primarily_steam_deck
FROM {{ source('preprocessed', 'preprocessed_reviews') }}