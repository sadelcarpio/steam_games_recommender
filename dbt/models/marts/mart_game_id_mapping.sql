-- models/marts/mart_game_id_mapping.sql
SELECT game_id,
       ROW_NUMBER() OVER (ORDER BY game_id) - 1 AS game_index
FROM (SELECT DISTINCT game_id
      FROM {{ ref('int_filtered_reviews') }})
