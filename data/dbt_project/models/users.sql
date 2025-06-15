-- models/users.sql
{{ config(materialized='view') }}

SELECT
    author_id AS user_id,
    num_games_owned AS user_num_games_owned,
    num_reviews AS user_num_reviews
FROM {{ source('preprocessed', 'preprocessed_users') }}
