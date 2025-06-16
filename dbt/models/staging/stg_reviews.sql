-- models/staging/stg_reviews.sql
{{ config(materialized='view') }}

SELECT rec_id AS review_id,
       author_id AS user_id,
       appid AS game_id,
       num_games_owned AS user_num_games_owned,
       playtime_forever,
       playtime_last_two_weeks,
       playtime_at_review,
       TO_TIMESTAMP(last_played) AS last_played_timestamp,
       review,
       TO_TIMESTAMP(timestamp_created) AS timestamp_created,
       votes_up,
       voted_up,
       weighted_vote_score,
       comment_count
FROM {{ source('raw', 'raw_reviews') }}
