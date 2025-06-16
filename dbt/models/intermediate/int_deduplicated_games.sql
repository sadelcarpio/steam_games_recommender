-- models/intermediate/int_deduplicated_games.sql
{{ config(materialized='table') }}

WITH deduplicated_games AS (SELECT *
                            FROM {{ ref('stg_games') }} QUALIFY
    ROW_NUMBER() OVER (
    PARTITION BY game_name
    ORDER BY game_review_score DESC
    ) = 1
    )
   , games_cat AS (
        SELECT *, UNNEST(game_category_ids) AS categoryid -- convert cat_ids to names
        FROM deduplicated_games
        )
SELECT game_id,
       game_release_date,
       game_name,
       game_required_age,
       game_is_free,
       game_short_description,
       game_supported_languages,
       list(cat.category) AS game_categories,
       game_genres,
       game_review_score
FROM games_cat gc
         JOIN {{ source('raw', 'categories') }} cat
              ON gc.categoryid = cat.categoryid
GROUP BY game_id, game_release_date, game_name, game_required_age, game_is_free,
         game_short_description, game_supported_languages, game_genres, game_review_score
