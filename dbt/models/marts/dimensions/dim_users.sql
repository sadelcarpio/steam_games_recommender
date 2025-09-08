-- models/marts/dimensions/dim_users.sql
{{ config(
    materialized='table'
) }}
SELECT user_id,
       first_review_timestamp
FROM (SELECT user_id,
             MIN(timestamp_created) AS first_review_timestamp
      FROM {{ ref('int_deduplicated_reviews') }}
      GROUP BY user_id)
