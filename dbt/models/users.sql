-- models/users.sql
{{ config(materialized='table') }}

SELECT
    user_id,
    user_num_games_owned,
    user_num_reviews
FROM {{ source('preprocessed', 'preprocessed_users') }}
