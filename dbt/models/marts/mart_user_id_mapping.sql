-- models/marts/mart_user_id_mapping.sql
SELECT user_id,
       ROW_NUMBER() OVER (ORDER BY first_review_timestamp) - 1 AS user_index
FROM (SELECT user_id,
             MIN(timestamp_created) AS first_review_timestamp
      FROM {{ ref('int_filtered_reviews') }}
      GROUP BY user_id)
