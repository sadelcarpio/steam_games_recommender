-- models/marts/mart_user_id_mapping.sql
SELECT user_id,
       ROW_NUMBER() OVER (ORDER BY user_id) - 1 AS user_index
FROM (SELECT DISTINCT user_id FROM {{ ref('mart_user_features') }})
