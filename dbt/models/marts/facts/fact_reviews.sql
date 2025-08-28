-- models/marts/facts/fact_reviews.sql
{{ config(
    materialized='incremental',
    unique_key='review_id',
    on_schema_change='append_new_columns'
) }}
SELECT r.review_id,
       u.user_index,
       g.game_index,
       u.user_id,
       g.game_id,
       r.review,
       r.written_during_early_access,
       r.voted_up,
       r.weighted_vote_score,
       r.votes_up,
       r.comment_count,
       r.timestamp_created,
       r.scrape_date
FROM {{ ref('int_deduplicated_reviews') }} r
         JOIN {{ ref('dim_games') }} g
              ON r.game_id = g.game_id
         JOIN {{ ref('dim_users') }} u
              ON r.user_id = u.user_id
{% if is_incremental() %}
WHERE r.scrape_date > (SELECT MAX(scrape_date) FROM {{ this }})
{% endif %}
