-- models/marts/mart_game_id_mapping.sql
SELECT game_id,
       ROW_NUMBER() OVER (ORDER BY first_review_timestamp) - 1 AS game_index
FROM (SELECT DISTINCT game_id,
                      MIN(timestamp_created) AS first_review_timestamp
      FROM {{ ref('int_filtered_reviews') }}
      GROUP BY game_id)
