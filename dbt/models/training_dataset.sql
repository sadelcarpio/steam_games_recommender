{{ config(materialized='table') }}

SELECT
    r.rec_id,
    g.game_id,
    u.user_id,
    g.game_name,
    g.game_required_age,
    g.game_is_free,
    g.game_short_description,
    g.game_supported_languages,
    g.game_categories,
    g.game_genres,
    g.game_total_positive_reviews,
    g.game_total_negative_reviews,
    g.game_total_reviews,
    g.game_review_score,
    u.user_num_games_owned,
    u.user_num_reviews,
    r.playtime_forever,
    r.playtime_at_review,
    r.playtime_last_two_weeks,
    r.last_played,
    r.review,
    r.timestamp_created,
    r.voted_up,
    r.votes_funny,
    r.votes_up,
    r.weighted_vote_score,
    r.comment_count,
    r.steam_purchase,
    r.received_for_free,
    r.written_during_early_access,
    r.primarily_steam_deck
FROM {{ ref('reviews') }} r
JOIN {{  ref('users') }} u ON r.user_id = u.user_id
JOIN {{  ref('games') }} g ON r.game_id = g.game_id
