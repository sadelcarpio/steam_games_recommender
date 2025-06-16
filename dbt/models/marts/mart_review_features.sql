-- models/marts/mart_reviews_features.sql
{{ config(
    materialized='external',
    location='../data/marts/review_features.parquet',
    format='parquet'
) }}
SELECT user_id,
       game_id,
       review,
       voted_up,
       votes_up,
       weighted_vote_score,
       timestamp_created
FROM {{ ref('int_filtered_reviews') }}
