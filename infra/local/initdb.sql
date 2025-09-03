CREATE DATABASE mlflow;
CREATE DATABASE games_scraping;

\connect games_scraping;

CREATE TABLE IF NOT EXISTS games_last_processed_reviews (
    game_id INTEGER PRIMARY KEY,
    last_processed_timestamp TIMESTAMP
);