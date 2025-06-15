-- models/games.sql
{{ config(materialized='view') }}

SELECT
    appid AS game_id,
    name AS game_name,
    required_age AS game_required_age,
    is_free AS game_is_free,
    short_description AS game_short_description,
    supported_languages AS game_supported_languages,
    categories AS game_categories,
    genres_list AS game_genres,
    total_positive_reviews AS game_total_positive_reviews,
    total_negative_reviews AS game_total_negative_reviews,
    total_reviews AS game_total_reviews,
    review_score AS game_review_score
FROM {{ source('preprocessed', 'preprocessed_games') }}
