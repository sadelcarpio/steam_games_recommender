-- models/marts/dimensions/dim_games_imputed.sql
{{ config(
    materialized='table'
) }}
WITH games_imputed AS (SELECT g.game_id,
                              g.game_name,
                              g.game_developers,
                              g.game_publishers,
                              g.game_categories,
                              g.game_genres,
                              COALESCE(g.game_release_date, first_review.first_review_date) AS game_release_date,
                              g.game_short_description,
                              g.game_review_score,
                              g.game_review_score_description,
                              g.game_total_reviews,
                              g.game_total_positive_reviews,
                              g.game_total_negative_reviews
                       FROM {{ ref('int_deduplicated_games') }} g
                                LEFT JOIN (SELECT game_id,
                                                  MIN(timestamp_created) AS first_review_date
                                           FROM {{ ref('int_deduplicated_reviews') }}
                                           GROUP BY game_id) first_review
                                          USING (game_id))
SELECT *,
       ROW_NUMBER() OVER (ORDER BY release_date NULLS LAST) - 1 AS game_index
FROM games_imputed
