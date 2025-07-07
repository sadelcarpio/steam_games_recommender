-- models/marts/mart_user_features.sql
{{ config(
    materialized='view'
) }}

SELECT DISTINCT user_id
FROM {{ ref('int_filtered_reviews') }}
