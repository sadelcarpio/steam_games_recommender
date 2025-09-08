-- models/marts/dimensions/dim_games.sql
{{ config(
    materialized='table'
) }}
WITH first_review AS (SELECT game_id,
                             MIN(timestamp_created) AS first_review_date
                      FROM {{ ref('int_deduplicated_reviews') }}
                      GROUP BY game_id),
     games_imputed AS (SELECT g.game_id,
                              g.game_name,
                              g.game_is_free,
                              g.game_developers,
                              g.game_publishers,
                              g.game_categories,
                              g.game_genres,
                              g.game_release_date                                           AS game_steam_release_date,
                              COALESCE(g.game_release_date, first_review.first_review_date) AS game_release_date,
                              CASE
                                  WHEN
                                      COALESCE(g.game_release_date, first_review.first_review_date) >
                                      first_review.first_review_date
                                      THEN first_review.first_review_date
                                  ELSE COALESCE(g.game_release_date, first_review.first_review_date)
                                  END                                                       AS game_prerelease_date,
                              g.game_short_description,
                              g.game_about,
                              g.game_detailed_description,
                              g.game_scrape_date,
                              g.game_review_score,
                              g.game_review_score_description
                       FROM {{ ref('int_deduplicated_games') }} g
                                LEFT JOIN first_review
                                          USING (game_id))
SELECT *
FROM games_imputed
WHERE game_release_date IS NOT NULL
