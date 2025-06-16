-- models/marts/mart_game_features.sql
{{ config(
    materialized='external',
    location='../data/marts/game_features.parquet',
    format='parquet'
) }}

SELECT game_id,
       game_release_date,
       game_name,
       game_required_age,
       game_is_free,
       game_short_description,
       game_supported_languages,
       game_categories,
       game_genres,
       game_review_score
FROM {{ ref('int_deduplicated_games') }}
