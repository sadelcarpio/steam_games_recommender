-- models/staging/stg_reviews.sql
{{ config(materialized='view') }}

SELECT rec_id                                     AS review_id,
       author_id                                  AS user_id,
       appid                                      AS game_id,
       review,
       TO_TIMESTAMP(timestamp_created)::TIMESTAMP AS timestamp_created,
       scrape_date,  -- very important for incremental updates
       written_during_early_access,
       voted_up,  -- calculate overall game score
       weighted_vote_score,  -- gives the importance of review, calculate overall game score
       votes_up,  -- gives the importance of review
       comment_count  -- gives the importance of review
FROM {{ source('raw', 'raw_reviews') }}
