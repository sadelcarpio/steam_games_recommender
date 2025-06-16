-- models/games.sql
{{ config(materialized='table') }}

SELECT
    game_id,
    game_name,
    game_required_age,
    game_is_free,
    short_description AS game_short_description,
    game_supported_languages,
    game_categories,
    game_genres,
    game_total_positive_reviews,
    game_total_negative_reviews,
    game_total_reviews,
    game_review_score
FROM {{ source('preprocessed', 'preprocessed_games') }}
